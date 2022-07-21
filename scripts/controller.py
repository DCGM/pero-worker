#!/usr/bin/env python3

# controller for pero workers and distributed system

import sys
import argparse
import logging
import traceback

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
        help='Zookeeper command to run'
    )
    return parser.parse_args()

class Controller(ZkClient):
    """
    Controller for workers and watchdog.
    """

    def get_worker_status(self):
        """
        Logs status of the workers.
        :raise: ZookeeperError if server returns non-zero error code
        """
        try:
            worker_ids = self.zk.get_children(constants.WORKER_STATUS)
            for worker in worker_ids:
                try:
                    worker_status = self.zk.get(constants.WORKER_STATUS_TEMPLATE.format(worker_id = worker))
                    worker_queue = self.zk.get(constants.WORKER_QUEUE_TEMPLATE.format(worker_id = worker))
                    self.logger.info('Worker ID: {worker}, status: {status}, queue: {queue}'.format(
                        worker = worker,
                        status = worker_status[0].decode(),
                        queue = worker_queue[0].decode()
                    ))
                except NoNodeError:
                    self.logger.info('Worker ID: {worker}, status: {status}, queue: '.format(
                        worker = worker,
                        status = constants.STATUS_FAILED
                    ))
        except NoNodeError:
            self.logger.info('No workers are connected')

    def switch_worker(self, worker, queue):
        """
        Switch given worker to given queue.
        :param zk: zookeeper connection
        :param worker: worker id
        :param queue: queue to switch to
        :raise: ZookeeperError if server returns non-zero error code
        """
        try:
            worker_ids = self.zk.get_children(constants.WORKER_STATUS)
        except NoNodeError:
            self.logger.info('No workers are connected')
            return
        
        if worker not in worker_ids:
            self.logger.error('Worker with id {} does not exist!'.format(worker))
            return
        
        # TODO
        # Add queue name validation

        self.zk.set(constants.WORKER_QUEUE_TEMPLATE.format(worker_id = worker), queue.encode('utf-8'))

    def shutdown_worker(self, worker):
        """
        Shutdown given worker.
        :param zk: zookeeper connection
        :param worker: worker id
        :raise: ZookeeperError if server returns non-zero error code
        """
        try:
            worker_ids = self.zk.get_children(constants.WORKER_STATUS)
        except NoNodeError:
            self.logger.info('No workers are connected')
            return
        
        if worker not in worker_ids:
            self.logger.error('worker with id {} does not exist!'.format(worker))
            return
        
        self.zk.set(constants.WORKER_ENABLED_TEMPLATE.format(worker_id = worker), 'false'.encode('utf-8'))

    def remove_worker(self, worker):
        """
        Removes worker record from zookeeper.
        :param zk: zookeeper connection
        :param worker: worker id
        :raise: ZookeeperError if zookeeper server returns non-zero error code
        """
        try:
            worker_ids = self.zk.get_children(constants.WORKER_STATUS)
        except NoNodeError:
            self.logger.info('No workers are connected')
            return
        
        if worker not in worker_ids:
            self.logger.error('Worker with id {} does not exist!'.format(worker))
            return
        
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

    # get worker status
    if args.status:
        try:
            controller.get_worker_status()
        except ZookeeperError as e:
            self.logger.error('Failed to get status of the workers!')
            self.logger.error('Received error message: {}'.format(e))
    
    # TODO
    # get queue status
    if args.queues:
        raise NotImplemented("Printing queue statistics is not supported yet!")

    # switch worker to different queue
    if args.switch:
        try:
            controller.switch_worker(args.switch[0], args.switch[1])
        except Exception as e:
            self.logger.error('Failed to switch worker {worker} to queue {queue}!'.format(
                worker = args.switch[0],
                queue = args.switch[1]
            ))
            self.logger.error('Received error message: {}'.format(e))

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
                self.logger.error('Failed to shutdown worker {}!'.format(args.shutdown))
                self.logger.error('Received error message: {}'.format(e))
    
    # remove worker from zookeeper
    if args.remove:
        for worker in args.remove:
            try:
                controller.remove_worker(worker)
            except Exception as e:
                self.logger.error('Failed to remove worker {} from zookeeper!'.format(args.remove))
                self.logger.error('Received error message: {}'.format(e))
                traceback.print_exc()
    
    return 0


# run the code
if __name__ == '__main__':
    sys.exit(main())