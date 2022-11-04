#!/usr/bin/env python3

# controller for pero workers and distributed system

import sys
import argparse
import logging
import traceback
import requests
import json

# aux functions
import worker_functions.connection_aux_functions as cf
import worker_functions.constants as constants
from worker_functions.zk_client import ZkClient

# zookeeper
from kazoo.client import KazooClient
from kazoo.client import KazooState
from kazoo.exceptions import ZookeeperError, NoNodeError
from kazoo.handlers.threading import KazooTimeoutError


# setup logging (required by kazoo)
log_formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')

# use UTC time in log
log_formatter.converter = time.gmtime

stderr_handler = logging.StreamHandler()
stderr_handler.setFormatter(log_formatter)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
# TODO - remove debug level
logger.setLevel(logging.DEBUG)
logger.addHandler(stderr_handler)

def parse_args():
    parser = argparse.ArgumentParser('Controller for pero workers and distributed system')
    parser.add_argument(
        '--zookeeper', '-z',
        help='List of zookeeper servers in format IPv4:port and [IPv6]:port',
        nargs='+',
        default=['127.0.0.1:2181']
    )
    parser.add_argument(
        '--zookeeper-list', '-l',
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
        '-s', '--status',
        help='Get status of the workers',
        action='store_true'
    )
    parser.add_argument(
        '-q', '--queues',
        help='Get status of the queues declared in system',
        action='store_true'
    )
    parser.add_argument(
        '-i', '--switch',
        help='Switch worker to different queue. Specify worker id and target queue',
        nargs=2
    )
    parser.add_argument(
        '-d', '--shutdown',
        help='Shutdown worker with given id.',
        nargs='+'
    )
    parser.add_argument(
        '-r', '--remove',
        help='Remove worker record from zookeeper.',
        nargs='+'
    )
    parser.add_argument(
        '-c', '--command',
        help='Zookeeper command to run.'
    )
    return parser.parse_args()

class Controller(ZkClient):
    """
    Controller for workers and watchdog.
    """

    def get_worker_status(self):
        """
        Returns status of the workers.
        :param log: Logs worker status if True else just return dictionary
        :return: dictionary with status and queue of each worker
        :raise: ZookeeperError if server returns non-zero error code
        """
        workers = {}
        try:
            worker_ids = self.zk.get_children(constants.WORKER_STATUS)
            for worker in worker_ids:
                try:
                    workers[worker] = {
                        'status': self.zk.get(constants.WORKER_STATUS_TEMPLATE.format(worker_id = worker))[0].decode(),
                        'queue': self.zk.get(constants.WORKER_QUEUE_TEMPLATE.format(worker_id = worker))[0].decode()
                    }
                except NoNodeError:
                    workers[worker] = {
                        'status' : constants.STATUS_FAILED,
                        'queue': ''
                    }
        except NoNodeError:
            pass
        
        return workers
    
    def get_mq_queue_status(self):
        """
        Get status of queues from message broker monnitoring.
        :raise: ZookeeperError if server returns non-zero error code
        :raise: ConnectionError if fails to get status from MQ monitoring servers
        :raise: ValueError if no monitoring servers are set up
        """
        response = None
        try:
            monitoring_servers = cf.server_list(self.zk.get_children(constants.WORKER_CONFIG_MQ_MONITORING_SERVERS))
        except kazoo.exceptions.NoNodeError:
            raise ValueError('No monitoring servers are set up!')

        if self.ca_cert:
            queue_request = 'https://{server}/api/queues'
        else:
            queue_request = 'http://{server}/api/queues'
        
        for server in monitoring_servers:
            try:
                response = requests.get(
                    queue_request.format(server = cf.host_port_to_string(server)),
                    auth=(self.zk_auth['username'], self.zk_auth['password']),
                    verify=self.ca_cert
                )
            except requests.exceptions.RequestException:
                self.logger.error(
                    'Failed to connect to broker monitoring api on server {}'
                    .format(cf.host_port_to_string(server))
                )
                self.logger.debug('Received error:\n{}'.format(traceback.format_exc()))
            else:
                if not response.ok:
                    self.logger.error(
                        'Failed to get queue status from server {}'
                        .format(cf.host_port_to_string(server))
                    )
                    self.logger.error('Status code: {status}, message: {reason}'.format(
                        status = response.status_code,
                        reason = response.reason
                    ))
                else:
                    break
        
        if not response or not response.ok:
            raise ConnectionError('Failed to get status of queues from MQ servers!')
        
        return response
    
    def get_processing_queue_list(self):
        """
        Retruns list of processing queues with configuration in zookeeper.
        :raise: ZookeeperError if server returns non-zero error code
        """
        try:
            processing_queues = self.zk.get_children(constants.QUEUE)
        except NoNodeError:
            processing_queues = []
        
        return processing_queues

    def get_queue_status(self):
        """
        Returns status of queues in message broker.
        :raise: ZookeeperError if server returns non-zero error code
        :raise: ConnectionError if fails to get status from MQ monitoring servers
        :raise: ValueError if no monitoring servers are set up
        """
        output = {}
        response = self.get_mq_queue_status()  # raises ConnectionError

        processing_queues = self.get_processing_queue_list()
        queue_mq_stats = json.loads(response.content)
        workers = self.get_worker_status()

        n_workers_on_queue = {}
        for worker in workers:
            queue = workers[worker]['queue']
            if queue in n_workers_on_queue:
                n_workers_on_queue[queue] += 1
            else:
                n_workers_on_queue[queue] = 1

        for queue in queue_mq_stats:
            if queue['vhost'] != constants.MQ_VHOST:
                continue
            
            number_of_workers = 0
            if queue['name'] in n_workers_on_queue:
                number_of_workers = n_workers_on_queue[queue['name']]
            
            if queue['name'] in processing_queues:
                output[queue['name']] = {
                    'messages': queue["messages"],
                    'n_workers': number_of_workers,
                }
            else:
                output[queue['name']] = {
                    'messages': queue["messages"]
                }
        return output

    def switch_worker(self, worker, queue):
        """
        Switch given worker to given queue.
        :param zk: zookeeper connection
        :param worker: worker id
        :param queue: queue to switch to
        :raise: ZookeeperError if server returns non-zero error code
        :raise: ValueError if wrong worker or queue is specified
        """
        try:
            worker_ids = self.zk.get_children(constants.WORKER_STATUS)
        except NoNodeError:
            raise ValueError('No workers are connected')
        
        if worker not in worker_ids:
            raise ValueError(f'Worker with id {worker} does not exist!')
        
        # TODO
        # Add queue name validation
        processing_queues = self.get_processing_queue_list()
        if queue not in processing_queues:
            raise ValueError(f'Queue {queue} does not exist or isn\'t processing queue!')

        self.zk.set(constants.WORKER_QUEUE_TEMPLATE.format(worker_id = worker), queue.encode('utf-8'))

    def shutdown_worker(self, worker):
        """
        Shutdown given worker.
        :param zk: zookeeper connection
        :param worker: worker id
        :raise: ZookeeperError if server returns non-zero error code
        :raise: ValueError if wrong worker is specified
        """
        try:
            worker_ids = self.zk.get_children(constants.WORKER_STATUS)
        except NoNodeError:
            raise ValueError('No workers are connected')
        
        if worker not in worker_ids:
            raise ValueError(f'worker with id {worker} does not exist!')
        
        self.zk.set(constants.WORKER_ENABLED_TEMPLATE.format(worker_id = worker), 'false'.encode('utf-8'))

    def remove_worker(self, worker):
        """
        Removes worker record from zookeeper.
        :param zk: zookeeper connection
        :param worker: worker id
        :raise: ZookeeperError if zookeeper server returns non-zero error code
        :raise: ValueError if wrong worker is specified
        """
        try:
            worker_ids = self.zk.get_children(constants.WORKER_STATUS)
        except NoNodeError:
            raise ValueError('No workers are connected')
        
        if worker not in worker_ids:
            raise ValueError(f'worker with id {worker} does not exist!')
        
        self.zk.delete(constants.WORKER_STATUS_ID_TEMPLATE.format(worker_id = worker), recursive=True)

def main():
    args = parse_args()

    # connect to zk
    zookeeper_servers = cf.zk_server_list(args.zookeeper)
    if args.zookeeper_list:
        zookeeper_servers = cf.zk_server_list(args.zookeeper_list)

    controller = Controller(
        zookeeper_servers=zookeeper_servers,
        username=args.username,
        password=args.password,
        ca_cert=args.ca_cert,
        logger=logger
    )
    controller.zk_connect()

    # switch worker to different queue
    if args.switch:
        try:
            controller.switch_worker(args.switch[0], args.switch[1])
        except Exception as e:
            logger.error('Failed to switch worker {worker} to queue {queue}!'.format(
                worker = args.switch[0],
                queue = args.switch[1]
            ))
            logger.error('Received error message: {}'.format(e))

    # TODO
    # run command in zookeeper
    if args.command:
        raise NotImplemented("Running command in zookeeper is not supported yet!")

    # shutdown worker
    if args.shutdown:
        for worker in args.shutdown:
            try:
                controller.shutdown_worker(worker)
            except Exception as e:
                logger.error('Failed to shutdown worker {}!'.format(args.shutdown))
                logger.error('Received error message: {}'.format(e))
    
    # remove worker from zookeeper
    if args.remove:
        for worker in args.remove:
            try:
                controller.remove_worker(worker)
            except Exception as e:
                logger.error('Failed to remove worker {} from zookeeper!'.format(args.remove))
                logger.error('Received error message: {}'.format(e))
                #traceback.print_exc()

    # get worker status
    if args.status:
        try:
            worker_status = controller.get_worker_status()
        except ZookeeperError as e:
            logger.error('Failed to get status of the workers!')
            logger.error('Received error message: {}'.format(e))
        else:
            if not worker_status:
                logger.info('No workers are connected')
            for worker in worker_status:
                logger.info('Worker ID: {worker}, status: {status}, queue: {queue}'.format(
                    worker = worker,
                    status = worker_status[worker]['status'],
                    queue = worker_status[worker]['queue']
                ))
    
    # get queue status
    if args.queues:
        try:
            queue_status = controller.get_queue_status()
        except Exception as e:
            logger.error('Failed to get status of queues!')
            logger.error('Received error message: {}'.format(e))
        else:
            for queue in queue_status:
                logger.info('Queue name: {name}, number of messages: {messages}, number of workers on queue: {n_workers}'.format(
                    name = queue,
                    messages = queue_status[queue]['messages'],
                    n_workers = queue_status[queue]['n_workers'] if 'n_workers' in queue_status[queue] else 'not a processing queue!'
                ))
    
    return 0


# run the code
if __name__ == '__main__':
    sys.exit(main())