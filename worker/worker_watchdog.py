#!/usr/bin/env python3

# Switches workers between queues based on queue load and worker activity

import argparse
import logging
import traceback
import requests
import json
import time
import datetime
import sys
import threading

import worker_functions.constants as constants

# zookeeper
from kazoo.client import KazooClient
from kazoo.handlers.threading import KazooTimeoutError
from kazoo.protocol.states import KazooState
import kazoo.exceptions

from worker_functions.zk_client import ZkClient
import worker_functions.connection_aux_functions as cf

# === Global config ===

# setup logging (required by kazoo)
log_formatter = logging.Formatter('%(asctime)s WATCHDOG %(levelname)s %(message)s')

stderr_handler = logging.StreamHandler()
stderr_handler.setFormatter(log_formatter)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
# TODO - remove debug level
logger.setLevel(logging.DEBUG)
logger.addHandler(stderr_handler)

# TODO
# parse args
def parse_args():
    parser = argparse.ArgumentParser(
        'Adjusts worker configuration based on current statistics.'
    )
    parser.add_argument(
        '-z', '--zookeeper',
        help='List of zookeeper servers from where configuration will be downloaded. If port is omitted, default zookeeper port is used.',
        nargs='+',
        default=['127.0.0.1:2181']
    )
    parser.add_argument(
        '-l', '--zookeeper-list',
        help='File with list of zookeeper servers. One server per line.',
        type=argparse.FileType('r')
    )
    parser.add_argument(
        '-u', '--username',
        help='Username for authentication on server.',
        default=None
    )
    parser.add_argument(
        '-p', '--password',
        help='Password for user authentication.',
        default=None
    )
    parser.add_argument(
        '-e', '--ca-cert',
        help='CA Certificate for SSL/TLS connection verification.',
        default=None
    )
    parser.add_argument(
        '--dry-run',
        help='Runs calculation of priorities only once, does not switch workers.',
        default=False,
        action='store_true'
    )
    return parser.parse_args()

class WorkerWatchdog(ZkClient):

    queue_request = 'http://{server}/api/queues'
    queue_request_ssl = 'https://{server}/api/queues'
    minimal_message_number = 10  # minimal number of messages to process before worker can be switched
    minimal_reconfiguration_time = 10  # time needed for worker to switch queue + time needed to apply configuration changes by watchdog

    def __init__(self, zookeeper_servers, username=None, password=None, ca_cert=None, dry_run=False, mq_servers_monitoring = [], logger = logging.getLogger(__name__)):
        # init zookeeper client
        super().__init__(
            zookeeper_servers=zookeeper_servers,
            username=username,
            password=password,
            ca_cert=ca_cert,
            logger=logger
        )
        # credentials to use with management console requests
        self.username = username
        self.password = password
        if not username:
            self.username = 'guest'
            self.password = 'guest'

        # last time when statistics were downloaded - type datetime.datetime()
        self.last_sample_time = None

        # MQ server list
        self.mq_servers_monitoring = mq_servers_monitoring
        self.mq_servers_lock = threading.Lock()

        # dry run for testing of priority calculation
        self.dry_run = dry_run
    
    def __del__(self):
        super().__del__()
    
    @staticmethod
    def get_queue_parametric_priority(length, avg_message_time, waiting_time, n_workers, administrative_priority = 0):
        """
        Calculates queue priority based on its parameters
        :param length: queue length / number of messages in the queue
        :param avg_message_time: average time needed to process one message from this queue
        :param waiting_time: time how long messages are waiting (calculated from time when oldest message was added to queue)
        :param n_workers: number of workers processing the queue
        :param administrative_priority: priority given by administrator to prioritize queue (default = 0, processing disabled = -1)
        :return: queue priority
        """
        # (length * avg_message_time)   ## time needed for processing all messages in queue
        return (length * avg_message_time) / (n_workers + 1) + waiting_time * (administrative_priority + 1)
    
    @staticmethod
    def get_queue_relative_priority(queue_priority, total_priority):
        """
        Calculates queue priority in range <0, 1>
        :param queue_priority: queue parametric priority
        :param total_priority: priority of all queues added together
        :return: queue priority relative to other queues
        """
        # prevent division by zero
        if not total_priority:
            total_priority = 1
        
        return queue_priority / total_priority

    def zk_get_queues(self):
        """
        Get queues and parameters defined in zookeeper
        :return: dictionary of queues defined in zookeeper and their attributes
        :raise: ZookeeperError if fails to get configurations from zookeeper
        """
        # get queues with configuration in zk
        try:
            queues_with_config = self.zk.get_children(constants.QUEUE)
        except kazoo.exceptions.NoNodeError:
            self.logger.info('No processing queues are defined in the system!')
            return {}
        
        # get queue stats from zookeeper
        queue_zk_stats = {}
        for queue in queues_with_config:
            try:
                queue_zk_stats[queue] = {}

                queue_zk_stats[queue]['modified'] = False

                queue_zk_stats[queue]['waiting_since'] = self.zk.get(
                    constants.QUEUE_STATS_WAITING_SINCE_TEMPLATE.format(queue_name = queue)
                )[0].decode('utf-8')

                queue_zk_stats[queue]['avg_msg_time'] = self.zk.get(
                    constants.QUEUE_STATS_AVG_MSG_TIME_TEMPLATE.format(queue_name = queue)
                )[0].decode('utf-8')

                if queue_zk_stats[queue]['avg_msg_time']:
                    queue_zk_stats[queue]['avg_msg_time'] = float.fromhex(queue_zk_stats[queue]['avg_msg_time'])
                else:
                    queue_zk_stats[queue]['avg_msg_time'] = 1

                queue_zk_stats[queue]['administrative_priority'] = int.from_bytes(self.zk.get(
                    constants.QUEUE_CONFIG_ADMINISTRATIVE_PRIORITY_TEMPLATE.format(queue_name = queue))[0],
                    constants.ZK_INT_BYTEORDER
                )

            except kazoo.exceptions.NoNodeError:
                self.logger.error(
                    'Queue {} does not have all fields defined in zookeeper!'
                    .format(queue)
                )
                self.logger.error('Skipping queue from scheduling!')
                del queue_zk_stats[queue]
            except ValueError:
                self.logger.error(
                    'Wrong format of zookeeper nodes data for queue {}!'
                    .format(queue)
                )
                del queue_zk_stats[queue]
        
        return queue_zk_stats
    
    def mq_get_queues(self):
        """
        Get queues and parameters defined in MQ broker
        :return: list of queues defined in MQ
        :raise: ConnectionError if fails to get status of queues from MQ servers
        :raise: JSONDecodeError if fails to parse server response
        """
        # check queue status using http api
        response = None
        if self.ca_cert:
            queue_request = self.queue_request_ssl
        else:
            queue_request = self.queue_request
        self.mq_servers_lock.acquire()
        try:
            for server in self.mq_servers_monitoring:
                try:
                    response = requests.get(
                        queue_request.format(server = cf.ip_port_to_string(server)),
                        auth=(self.username, self.password),
                        verify=self.ca_cert
                    )
                except requests.exceptions.RequestException:
                    self.logger.error(
                        'Failed to connect to broker monitoring api on server {}'
                        .format(cf.ip_port_to_string(server))
                    )
                    self.logger.error('Received error:\n{}'.format(traceback.format_exc()))
                else:
                    if not response.ok:
                        self.logger.error(
                            'Failed to get queue status from server {}'
                            .format(cf.ip_port_to_string(server))
                        )
                        self.logger.error('Status code: {status}, message: {reason}'.format(
                            status = response.status_code,
                            reason = response.reason
                        ))
                    else:
                        break
        finally:
            self.mq_servers_lock.release()
        
        if not response or not response.ok:
            raise ConnectionError('Failed to get status of queues from MQ servers!')

        queue_mq_stats = json.loads(response.content)
        
        return queue_mq_stats
    
    def get_priorities(self, queue_zk_stats, queue_mq_stats, worker_stats):
        """
        Get relative priorities of queues
        :param queue_zk_stats: dictionary with queue statistics from zookeeper
        :param queue_mq_stats: list of queue statistics from MQ monitoring
        :return: dictionary with relative priorities of queues
        """
        # calculate priorities of queues
        queue_priorities = {}
        total_priority = 0
        for queue in queue_mq_stats:
            # check if queue is processing queue
            # (have worker config associated with it)
            if queue['name'] not in queue_zk_stats:
                continue

            n_workers = 0
            for worker in worker_stats:
                if worker_stats[worker]['queue'] == queue['name']:
                    n_workers += 1

            if not queue_zk_stats[queue['name']]['waiting_since']:
                waiting_time = 0
            elif n_workers > 0:  # queue is not waiting for processing
                waiting_time = 0
            else:
                try:
                    waiting_time = (self.last_sample_time - datetime.datetime.fromisoformat(queue_zk_stats[queue['name']]['waiting_since'])).total_seconds()
                except Exception:
                    self.logger.warning('Failed to parse waiting time of queue {}, scheduling might not be accurate!'.format(queue['name']))
                    waiting_time = 0

            # calculate priority for the queue
            queue_priorities[queue['name']] = self.get_queue_parametric_priority(
                length=queue['messages'],
                avg_message_time=queue_zk_stats[queue['name']]['avg_msg_time'],
                waiting_time=waiting_time,
                n_workers=n_workers,
                administrative_priority=queue_zk_stats[queue['name']]['administrative_priority']
            )
            total_priority += queue_priorities[queue['name']]
        
        # calculate relative priorities
        for queue in queue_priorities:
            queue_priorities[queue] = self.get_queue_relative_priority(
                queue_priorities[queue],
                total_priority
            )
        
        # sort queues based on priority
        queue_priorities = dict(
            sorted(queue_priorities.items(), key=lambda item: item[1], reverse=True)
        )
        
        return queue_priorities
    
    def get_worker_stats(self):
        """
        Get worker statistics and information from zookeeper
        :return: dictionary with worker data
        :raise: ZookeeperError if zookeeper connection/communication fails
        """
        # get list of connected workers
        try:
            workers = self.zk.get_children(constants.WORKER_STATUS)
        except kazoo.exceptions.NoNodeError:
            self.logger.warning('Could not get worker statistics, no worker statistics are not defined in zookeeper!')
            return {}

        if not workers:
            self.logger.warning('Could not get worker statistics, no workers are connected!')
            return {}
        
        worker_stats = {}
        for worker in workers:
            # get worker status
            try:
                status = self.zk.get(constants.WORKER_STATUS_TEMPLATE.format(
                    worker_id = worker))[0].decode('utf-8')
            except kazoo.exceptions.NoNodeError:
                self.logger.warning(
                    'Worker {} does not have status field defined in zookeeper!'
                    .format(worker)
                )
                continue
            
            # skip dead and failed workers
            if status == constants.STATUS_DEAD:
                continue
            if status == constants.STATUS_FAILED:
                self.logger.warning('Failed worker found! Worker id: {}'.format(worker))
                # TODO
                # notify admin
                continue
            
            # add worker statistics
            try:
                worker_stats[worker] = {}
                worker_stats[worker]['modified'] = False
                worker_stats[worker]['status'] = status
                worker_stats[worker]['queue'] = self.zk.get(constants.WORKER_QUEUE_TEMPLATE.format(
                    worker_id = worker))[0].decode('utf-8')
                worker_stats[worker]['unlock_time'] = self.zk.get(constants.WORKER_UNLOCK_TIME.format(
                    worker_id = worker))[0].decode('utf-8')
            except kazoo.exceptions.NoNodeError:
                self.logger.warning(
                    'Worker {} does not have all fields defined in zookeeper!'
                    .format(worker)
                )
                del worker_stats[worker]
                continue
        
        if not worker_stats:
            self.logger.info('No running workers found!')
        
        return worker_stats

    def switch_worker_to_queue(self, worker, queue, worker_stats, queue_stats):
        """
        Switches worker to queue by modifying status.
        :param worker: id of worker to switch
        :param queue: name of queue to switch worker to
        :param worker_stats: dictionary with stats of workers obtained from zookeeper
        :param queue_stats: dictionary with stats of queues obtained from zookeeper
        """
        unlock_time_offset = queue_stats[queue]['avg_msg_time'] * self.minimal_message_number + self.minimal_reconfiguration_time
        worker_stats[worker]['unlock_time'] = (self.last_sample_time + datetime.timedelta(seconds=unlock_time_offset)).isoformat()
        worker_stats[worker]['queue'] = queue
        worker_stats[worker]['modified'] = True

        if queue_stats[queue]['waiting_since']:
            queue_stats[queue]['waiting_since'] = ''
            queue_stats[queue]['modified'] = True

        self.logger.info('Switching worker {worker} to queue {queue}'.format(
            worker = worker,
            queue = queue
        ))
    
    def set_queue_to_waiting(self, worker_stats, queue_mq_stats, queue_zk_stats):
        """
        Sets queue 'waiting_since' to current time if there are messages waiting in the queue
        and no workers are processing it.
        :param worker_stats: dictionary with stats of workers obtained from zookeeper
        :param queue_mq_stats: list of queue statistics obtained from MQ
        :param queue_zk_stats: dictionary with queue statistics obtained from zookeeper
        """
        # get list of queues that are being processed
        processed_queues = []
        for worker in worker_stats:
            if worker_stats[worker]['queue'] not in processed_queues:
                processed_queues.append(worker_stats[worker]['queue'])
        
        # get list of queues with messages
        queues_with_messages = []
        for queue in queue_mq_stats:
            if queue['messages'] and queue['name'] not in queues_with_messages:
                queues_with_messages.append(queue['name'])
        
        self.logger.debug('Processed queues: {}'.format(processed_queues))
        self.logger.debug('Queues with messages: {}'.format(queues_with_messages))
        # set waiting queues 'waiting_since' to time when statistics were downloaded
        for queue in queue_zk_stats:
            if queue in queues_with_messages and queue not in processed_queues:
                if not queue_zk_stats[queue]['waiting_since']:
                    queue_zk_stats[queue]['waiting_since'] = self.last_sample_time.isoformat()
                    queue_zk_stats[queue]['modified'] = True
                    self.logger.info('Setting queue {queue} to waiting since {time}'.format(
                        queue = queue,
                        time = self.last_sample_time.isoformat()
                    ))

    def apply_changes(self, worker_stats, queue_stats):
        """
        Apply configuration and statistics changes in zookeeper
        :param worker_stats: dictionary of worker config/stats to apply
        :param queue_stats: dictionary of queue stats to apply
        :raise: ZookeeperError if node does not exists or if zookeeper returns non-zero error code
        """
        # apply worker configuration changes
        for worker in worker_stats:
            if worker_stats[worker]['modified']:
                try:
                    self.zk.set(
                        path = constants.WORKER_UNLOCK_TIME.format(worker_id = worker),
                        value = worker_stats[worker]['unlock_time'].encode('utf-8')
                    )
                    self.zk.set(
                        path = constants.WORKER_QUEUE_TEMPLATE.format(worker_id = worker),
                        value = worker_stats[worker]['queue'].encode('utf-8')
                    )
                except kazoo.exceptions.ZookeeperError:
                    logger.error(
                        'Failed to update worker {} configurations in zookeeper due to zookeeper error!'
                        .format(worker)
                    )
                    raise
        
        # apply queue statistics changes
        for queue in queue_stats:
            if queue_stats[queue]['modified']:
                try:
                    self.zk.set(
                        path = constants.QUEUE_STATS_WAITING_SINCE_TEMPLATE.format(queue_name = queue),
                        value = queue_stats[queue]['waiting_since'].encode('utf-8')
                    )
                except kazoo.exceptions.ZookeeperError:
                    logger.error(
                        'Failed to update queue {} statistics in zookeeper due to zookeeper error!'
                        .format(queue)
                    )
                    raise

    def adjust_processing(self):
        """
        Adjusts processing by switching workers between queues.
        Checks for queue status and calculates priorities of queues based on current data.
        Updates waiting time of queues.
        """
        # time when last statistics starts to download
        self.last_sample_time = datetime.datetime.now(datetime.timezone.utc)

        # get queues and statistics from zookeeper
        # can raise ZookeeperError
        queue_zk_stats = self.zk_get_queues()

        if not queue_zk_stats:
            self.logger.info('No queues with configuration found!')
            return
        
        self.logger.debug('Queue zookeeper statistics:\n{}'.format(json.dumps(queue_zk_stats, indent=4)))

        # list of queues defined in MQ and their stats
        # can raise ConnectionError, JSONDecodeError
        queue_mq_stats = self.mq_get_queues()

        if not queue_mq_stats:
            self.logger.info('No queues found in MQ!')
            return
        
        self.logger.debug('Queue MQ statistics:\n{}'.format(json.dumps(queue_mq_stats, indent=4)))
        
        # get worker data / statistics
        worker_stats = self.get_worker_stats()

        if not worker_stats:
            return
        
        self.logger.debug('Worker statistics:\n{}'.format(json.dumps(worker_stats, indent=4)))

        # get queue priorities
        queue_priorities = self.get_priorities(queue_zk_stats, queue_mq_stats, worker_stats)
        self.logger.debug('Calculated queue priorities:\n{}'.format(json.dumps(queue_priorities, indent=4)))

        if not queue_priorities[list(queue_priorities.keys())[0]]:
            self.logger.info('All queues are empty, skipping scheduling')
            return

        # === calculate new system state ===
        # get list of free workers
        free_workers = []
        for worker in worker_stats:
            worker_queue = worker_stats[worker]['queue']
            # worker is not assigned to any queue
            if not worker_queue:
                free_workers.append(worker)
            # worker is processing queue with no messages
            elif queue_priorities[worker_queue] == 0:
                free_workers.append(worker)

        # switch free workers to queues
        if free_workers:
            self.logger.debug('Switching free workers')
            for worker in free_workers:
                # switch worker to queue witch highest priority
                self.switch_worker_to_queue(
                    worker = worker,
                    queue = list(queue_priorities.keys())[0],
                    worker_stats = worker_stats,
                    queue_stats = queue_zk_stats
                )
                # recalculate priorities based on current configuration
                queue_priorities = self.get_priorities(queue_zk_stats, queue_mq_stats, worker_stats)
                self.logger.debug('New queue priorities:\n{}'.format(json.dumps(queue_priorities, indent=4)))
        else:
            # switch worker from queue witch last priority that is being processed
            # to queue with highest priority (only one worker at the time)
            self.logger.debug('Switching worker from less prioritized queue')
            worker_switched = False
            queue_priorities_reverse = dict(sorted(queue_priorities.items(), key=lambda item: item[1]))
            for queue in queue_priorities_reverse:
                for worker in worker_stats:
                    try:
                        unlock_time = datetime.datetime.fromisoformat(worker_stats[worker]['unlock_time'])
                    except Exception:
                        self.logger.warning('Failed to parse worker unlock time of worker {}, scheduling might be inaccurate!'.format(worker))
                        unlock_time = self.last_sample_time
                    
                    # check if worker can switch
                    if (unlock_time - self.last_sample_time).total_seconds() > 0:
                        continue

                    # switch worker to prioritized queue
                    if worker_stats[worker]['queue'] == queue:
                        self.switch_worker_to_queue(
                            worker=worker,
                            queue=list(queue_priorities.keys())[0],
                            worker_stats=worker_stats,
                            queue_stats=queue_zk_stats
                        )
                        worker_switched = True
                        break
                    
                if worker_switched:
                    break
        
        if self.dry_run:
            return
        
        # update queue stats in zookeeper
        self.set_queue_to_waiting(worker_stats, queue_mq_stats, queue_zk_stats)

        # apply configuration and statistic changes
        self.apply_changes(worker_stats, queue_zk_stats)

    def zk_callback_update_mq_server_list(self, servers):
        """
        Updates MQ server list if changes
        :param servers: list of servers received from zookeeper
        """
        self.mq_servers_lock.acquire()
        self.mq_servers_monitoring = cf.server_list(servers)
        # set default ports
        for server in self.mq_servers_monitoring:
            if not server['port']:
                server['port'] = 15672
        self.mq_servers_lock.release()

    def run(self):
        """
        Main function of the watchdog.
        :return: execution status
        """
        # connect to zookeeper
        try:
            self.zk_connect()
        except KazooTimeoutError:
            self.logger.critical('Failed to connect to zookeeper!')
            return 1
        
        # register updater for mq server list
        self.zk.ensure_path(constants.WORKER_CONFIG_MQ_MONITORING_SERVERS)
        self.zk.ChildrenWatch(
            path=constants.WORKER_CONFIG_MQ_MONITORING_SERVERS,
            func=self.zk_callback_update_mq_server_list
        )

        # TODO
        # register watchdog as main, or put it to sleep if secondary (use kazoo lease)

        error_count = 0

        try:
            while True:
                try:
                    self.adjust_processing()
                except kazoo.exceptions.ZookeeperError:
                    self.logger.error('Zookeeper returned non-zero error code!')
                    self.logger.error('Received error:\n{}'.format(traceback.format_exc()))
                except ConnectionError as e:
                    self.logger.error(
                        '{}'.format(e)
                    )
                except json.JSONDecodeError as e:
                    self.logger.error('Failed to parse message queue statistics! Wrong message format!')
                    self.logger.error(
                        'Received error: {}'
                        .format(e)
                    )
                except Exception as e:
                    self.logger.error('Unknown error has occurred!')
                    self.logger.error('Received error:\n{}'.format(traceback.format_exc()))
                    if error_count > 2:
                        raise
                    error_count += 1
                else:
                    # reset error counter
                    error_count = 0

                # run only once if dryrun is defined
                if self.dry_run:
                    break

                # wait for 10 seconds until next run
                time.sleep(10)

        except KeyboardInterrupt:
            self.logger.info('Keyboard interrupt received!')
        except Exception:
            self.logger.error('Failed to recover!')
            self.logger.error('Exiting!')
            return 1
        
        return 0

def main():
    args = parse_args()

    zookeeper_servers = cf.zk_server_list(args.zookeeper)
    if args.zookeeper_list:
        zookeeper_servers = cf.zk_server_list(args.zookeeper_list)

    watchdog = WorkerWatchdog(
        zookeeper_servers=zookeeper_servers,
        username=args.username,
        password=args.password,
        ca_cert=args.ca_cert,
        dry_run=args.dry_run
    )

    return watchdog.run()


if __name__ == "__main__":
    sys.exit(main())
