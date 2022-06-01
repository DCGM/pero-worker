#!/usr/bin/env python3

# controller for pero workers and distributed system

import sys
import argparse
import logging
import traceback

# aux functions
import worker_functions.connection_aux_functions as cf
import worker_functions.constants as constants

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
        '--status', '-s',
        help='Get status of the workers',
        action='store_true'
    )
    parser.add_argument(
        '--queues', '-q',
        help='Get status of the queues declared in system',
        action='store_true'
    )
    parser.add_argument(
        '--switch', '-i',
        help='Switch worker to different queue. Specify worker id and target queue',
        nargs=2
    )
    parser.add_argument(
        '--shutdown', '-d',
        help='Shutdown worker with given id.',
        nargs='+'
    )
    parser.add_argument(
        '--remove', '-r',
        help='Remove worker record from zookeeper.',
        nargs='+'
    )
    parser.add_argument(
        '--command', '-c',
        help='Zookeeper command to run'
    )
    return parser.parse_args()

def get_worker_status(zk):
    """
    Logs status of the workers.
    :param zk: zookeeper connection instance
    :raise: ZookeeperError if server returns non-zero error code
    """
    try:
        worker_ids = zk.get_children(constants.WORKER_STATUS)
        for worker in worker_ids:
            try:
                worker_status = zk.get(constants.WORKER_STATUS_TEMPLATE.format(worker_id = worker))
                worker_queue = zk.get(constants.WORKER_QUEUE_TEMPLATE.format(worker_id = worker))
                logger.info('Worker ID: {worker}, status: {status}, queue: {queue}'.format(
                    worker = worker,
                    status = worker_status[0].decode(),
                    queue = worker_queue[0].decode()
                ))
            except NoNodeError:
                logger.info('Worker ID: {worker}, status: {status}, queue: '.format(
                    worker = worker,
                    status = constants.STATUS_FAILED
                ))
    except NoNodeError:
        logger.info('No workers are connected')

def switch_worker(zk, worker, queue):
    """
    Switch given worker to given queue.
    :param zk: zookeeper connection
    :param worker: worker id
    :param queue: queue to switch to
    :raise: ZookeeperError if server returns non-zero error code
    """
    try:
        worker_ids = zk.get_children(constants.WORKER_STATUS)
    except NoNodeError:
        logger.info('No workers are connected')
        return
    
    if worker not in worker_ids:
        logger.error('Worker with id {} does not exist!'.format(worker))
        return
    
    # TODO
    # Add queue name validation

    zk.set(constants.WORKER_QUEUE_TEMPLATE.format(worker_id = worker), queue.encode('utf-8'))

def shutdown_worker(zk, worker):
    """
    Shutdown given worker.
    :param zk: zookeeper connection
    :param worker: worker id
    :raise: ZookeeperError if server returns non-zero error code
    """
    try:
        worker_ids = zk.get_children(constants.WORKER_STATUS)
    except NoNodeError:
        logger.info('No workers are connected')
        return
    
    if worker not in worker_ids:
        logger.error('worker with id {} does not exist!'.format(worker))
        return
    
    zk.set(constants.WORKER_ENABLED_TEMPLATE.format(worker_id = worker), 'false'.encode('utf-8'))

def remove_worker(zk, worker):
    """
    Removes worker record from zookeeper.
    :param zk: zookeeper connection
    :param worker: worker id
    :raise: ZookeeperError if zookeeper server returns non-zero error code
    """
    try:
        worker_ids = zk.get_children(constants.WORKER_STATUS)
    except NoNodeError:
        logger.info('No workers are connected')
        return
    
    if worker not in worker_ids:
        logger.error('Worker with id {} does not exist!'.format(worker))
        return
    
    zk.delete(constants.WORKER_STATUS_ID_TEMPLATE.format(worker_id = worker), recursive=True)

def main():
    args = parse_args()

    # connect to zk
    zookeeper_servers = cf.zk_server_list(args.zookeeper)
    if args.zookeeper_list:
        zookeeper_servers = cf.zk_server_list(args.zookeeper_list)

    zk = KazooClient(hosts=zookeeper_servers)

    try:
        zk.start(timeout=20)
    except KazooTimeoutError:
        logger.critical('Zookeeper connection timeout!')
        return 1

    # get worker status
    if args.status:
        try:
            get_worker_status(zk)
        except ZookeeperError as e:
            logger.error('Failed to get status of the workers!')
            logger.error('Received error message: {}'.format(e))
    
    # TODO
    # get queue status
    if args.queues:
        raise NotImplemented("Printing queue statistics is not supported yet!")

    # switch worker to different queue
    if args.switch:
        try:
            switch_worker(zk, args.switch[0], args.switch[1])
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
                shutdown_worker(zk, worker)
            except Exception as e:
                logger.error('Failed to shutdown worker {}!'.format(args.shutdown))
                logger.error('Received error message: {}'.format(e))
    
    # remove worker from zookeeper
    if args.remove:
        for worker in args.remove:
            try:
                remove_worker(zk, worker)
            except Exception as e:
                logger.error('Failed to remove worker {} from zookeeper!'.format(args.remove))
                logger.error('Received error message: {}'.format(e))
                traceback.print_exc()

    # close connection and exit
    zk.stop()
    zk.close()
    return 0


# run the code
if __name__ == '__main__':
    sys.exit(main())