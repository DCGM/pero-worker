#!/usr/bin/env python3

# manages configuration for pero workers, manages available queues

import sys
import os
import logging
import argparse

# worker libraries
import worker_functions.connection_aux_functions as cf
import worker_functions.constants as constants

# MQ
import pika

# zookeeper
import kazoo
from kazoo.client import KazooClient
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

def dir_path(path):
    """
    Check if path is directory path
    :param path: path to directory
    :return: path if path is directory path
    :raise: ArgumentTypeError if path is not directory path
    """
    if not os.path.exists(path):
        raise argparse.ArgumentTypeError(f"error: {path} is not a valid path")    
    if not os.path.isdir(path):
        raise argparse.ArgumentTypeError(f"error: {path} is not a directory")
    return path
    

def parse_args():
    parser = argparse.ArgumentParser('Pero configuration manager')
    parser.add_argument(
        '-z', '--zookeeper',
        help='List of zookeeper servers to use.',
        nargs='+',
        default=['127.0.0.1:2181']
    )
    parser.add_argument(
        '-l', '--zookeeper-list',
        help='File with list of zookeeper servers. One server per line.',
        type=argparse.FileType('r')
    )
    parser.add_argument(
        '-s', '--mq-servers',
        help='List of message queue servers where to get and send processing requests.',
        nargs='+'
    )
    parser.add_argument(
        '-m', '--mq-list',
        help='File containing list of message queue servers. One server per line.',
        type=argparse.FileType('r')
    )
    parser.add_argument(
        '--ftp-servers',
        help='List of ftp servers to upload to zookeeper.',
        nargs='+'
    )
    parser.add_argument(
        '--monitoring-servers',
        help='List of MQ monitring servers to upload to zookeeper.',
        nargs='+'
    )
    parser.add_argument(
        '-u', '--user',
        help='User name for servers.',
        default='pero'
    )
    parser.add_argument(
        '-p', '--password',
        help='Password for servers.',
        default='pero'
    )
    parser.add_argument(
        '-c', '--config',
        help='Path to configuration file.',
        type=argparse.FileType('r')
    )
    parser.add_argument(
        '-n', '--name',
        help='Name for identification of queue and configuration files.'
    )
    parser.add_argument(
        '-a', '--administrative-priority',
        help='Administrative priority for given queue.',
        type=int
    )
    parser.add_argument(
        '-f', '--file',
        help='File to upload. Argument can be used multiple times.',
        action='append'
    )
    parser.add_argument(
        '-t', '--target-file',
        help='Target path where to upload file. If \'-d/--delete\' is specified, marks remote file for deletion. Argument can be used multiple times.',
        action='append'
    )
    parser.add_argument(
        '-d', '--delete',
        help='Delete configuration and queues instead creating it.',
        default=False,
        action='store_true'
    )
    parser.add_argument(
        '-k', '--keep-config',
        help='Keep configuration and delete only queue. (Works only with \'-d/--delete\' argument.)',
        default=False,
        action='store_true'
    )
    parser.add_argument(
        '--update-mq-servers',
        help='Upload server list given by \'--mq-list\'/\'--mq-servers\' to zookeeper. '
             'Deletes given servers from zookeeper if \'--delete\' is set!',
        default=False,
        action='store_true'
    )
    parser.add_argument(
        '--update-ftp-servers',
        help='Upload server list given by \'--ftp-servers\' to zookeeper. '
             'Delete given server from zookeeper if \'--delete\' is set!',
        default=False,
        action='store_true'
    )
    parser.add_argument(
        '--update-monitoring-servers',
        help='Upload MQ monitoring servers - given by \'--mq-list\'/\'--mq-servers\' with port changed to default management port to zookeeper. '
             'If \'--monitoring-servers\' is given, list of monitoring servers is used instead, port is not changed in this case. '
             'If \'--delete\' is set, deletes servers from zookeper instead!',
        default=False,
        action='store_true'
    )
    return parser.parse_args()

def zk_upload_server_list(zk, server_list, path):
    """
    Uploads servers given by server_list as subnodes of path in zookeeper
    :param zk: zookeeper connection
    :param server_list: servers to upload
    :param path: path where to upload server list in zookeeper (for example constants.WORKER_CONFIG_MQ_SERVERS)
    :raise: ZookeeperError if server returns non-zero error code
    """
    for server in server_list:
        server_ip_port = cf.ip_port_to_string(server)
        logger.debug(os.path.join(path, server_ip_port))
        zk.ensure_path(os.path.join(path, server_ip_port))

def zk_delete_server_list(zk, server_list, path):
    """
    Deletes servers given by server_list from zookeeper configuration given by path
    :param zk: zookeeper connection
    :param server_list: servers to delete
    :param path: path to server list in zookeeper
    :raise: ZookeeperError if server returns non-zero error code
    """
    for server in server_list:
        server_ip_port = cf.ip_port_to_string(server)
        if zk.exists(os.path.join(path, server_ip_port)):
            zk.delete(os.path.join(path, server_ip_port))

def mq_connect(mq_servers):
    """
    Connects client to mq server
    :param mq_servers: mq servers to try
    :return: blocking connection to mq server or None
    """
    mq_connection = None
    for server in mq_servers:
        try:
            logger.info('Connectiong to MQ server {}'.format(cf.ip_port_to_string(server)))
            mq_connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host=server['ip'],
                    port=server['port'] if server['port'] else pika.ConnectionParameters.DEFAULT_PORT
                )
            )
        except pika.exceptions.AMQPError as e:
            logger.error('Connection failed! Received error: {}'.format(e))
            mq_connection = None
            continue
        else:
            break
    
    return mq_connection

def main():
    args = parse_args()

    if args.file or args.target_file:
        raise NotImplemented('File / Target File is not supported yet!')

    # get zookeeper server list
    zookeeper_servers = cf.zk_server_list(args.zookeeper)
    if args.zookeeper_list:
        zookeeper_servers = cf.zk_server_list(args.zookeeper_list)
    
    # connect to zookeeper
    zk = KazooClient(hosts=zookeeper_servers)
    try:
        zk.start(timeout=20)
    except KazooTimeoutError:
        logger.error('Zookeeper connection timeout!')
        return 1
    
    # get mq server list
    mq_servers = None
    if args.mq_servers:
        mq_servers = cf.server_list(args.mq_servers)
    elif args.mq_list:
        mq_servers = cf.server_list(args.mq_list)
    else:
        try:
            mq_servers = cf.server_list(zk.get_children(constants.WORKER_CONFIG_MQ_SERVERS))
        except kazoo.exceptions.NoNodeError:
            logger.error('Failed to obtain MQ server list from zookeeper!')
            mq_servers = None

    if not mq_servers:
        logger.error('MQ server list not available!')
        zk.stop()
        zk.close()
        return 1
    
    # connect to mq
    mq_connection = mq_connect(mq_servers)

    if not mq_connection:
        logger.error('Connection to all MQ servers failed!')
        zk.stop()
        zk.close()
        return 1

    mq_channel = mq_connection.channel()

    if not mq_channel:
        logger.error('Failed to open MQ channel!')
        zk.stop()
        zk.close()
        mq_connection.close()
        return 1
    
    if args.update_monitoring_servers:
        if args.monitoring_servers:
            monitoring_servers = cf.server_list(args.monitoring_servers)
        else:
            monitoring_servers = cf.server_list(args.mq_servers)
            for server in monitoring_servers:
                server['port'] = None

    if not args.delete:
        if args.update_mq_servers:
            zk_upload_server_list(zk, mq_servers, constants.WORKER_CONFIG_MQ_SERVERS)
            logger.info('MQ servers updated successfully!')
        
        if args.update_ftp_servers:
            zk_upload_server_list(zk, cf.server_list(args.ftp_servers), constants.WORKER_CONFIG_FTP_SERVERS)
            logger.info('FTP servers updated successfully!')

        if args.update_monitoring_servers:
            zk_upload_server_list(zk, monitoring_servers, constants.WORKER_CONFIG_MQ_MONITORING_SERVERS)
            logger.info('MQ monitoring servers updated successfully!')

        if args.name:
            # upload configuration
            if args.config:
                zk.ensure_path(constants.QUEUE_CONFIG_TEMPLATE.format(queue_name = args.name))
                zk.set(constants.QUEUE_CONFIG_TEMPLATE.format(queue_name = args.name), args.config.read().encode('utf-8'))
                zk.ensure_path(constants.QUEUE_STATS_AVG_MSG_TIME_TEMPLATE.format(queue_name = args.name))
                zk.ensure_path(constants.QUEUE_STATS_WAITING_SINCE_TEMPLATE.format(queue_name = args.name))
                zk.ensure_path(constants.QUEUE_CONFIG_ADMINISTRATIVE_PRIORITY_TEMPLATE.format(queue_name = args.name))
                if not int.from_bytes(
                    bytes=zk.get(constants.QUEUE_CONFIG_ADMINISTRATIVE_PRIORITY_TEMPLATE.format(queue_name = args.name))[0],
                    byteorder=constants.ZK_INT_BYTEORDER
                ):
                    zk.set(
                        path=constants.QUEUE_CONFIG_ADMINISTRATIVE_PRIORITY_TEMPLATE.format(queue_name = args.name),
                        value=int.to_bytes(0, sys.getsizeof(0), constants.ZK_INT_BYTEORDER)
                    )
                logger.info('Configuration file uploaded successfully!')
            
            if isinstance(args.administrative_priority, int):
                logger.info(
                    'Priority for queue {queue} set to {priority}'
                    .format(queue = args.name, priority = args.administrative_priority)
                )
                zk.ensure_path(constants.QUEUE_CONFIG_ADMINISTRATIVE_PRIORITY_TEMPLATE.format(queue_name = args.name))
                zk.set(
                    path=constants.QUEUE_CONFIG_ADMINISTRATIVE_PRIORITY_TEMPLATE.format(queue_name = args.name),
                    value=int.to_bytes(args.administrative_priority, sys.getsizeof(args.administrative_priority), constants.ZK_INT_BYTEORDER)
                )

            # create queue
            try:
                mq_channel.queue_declare(
                    queue=args.name,
                    arguments={'x-max-priority': 1},
                    durable=True
                )
            except ValueError as e:
                logger.error('Failed to declare queue {queue}! Received error: {error}'.format(
                    queue = args.name,
                    error = e
                ))
            else:
                logger.info('Queue {} created succesfully'.format(args.name))
    
    else:
        if args.update_mq_servers:
            zk_delete_server_list(zk, mq_servers, constants.WORKER_CONFIG_MQ_SERVERS)
            logger.info('MQ servers deleted successfully!')
        
        if args.update_ftp_servers:
            zk_delete_server_list(zk, cf.server_list(args.ftp_servers), constants.WORKER_CONFIG_FTP_SERVERS)
            logger.info('FTP servers deleted successfully!')
        
        if args.update_monitoring_servers:
            zk_delete_server_list(zk, monitoring_servers, constants.WORKER_CONFIG_MQ_MONITORING_SERVERS)
            logger.info('MQ monitoring servers deleted successfully!')
        
        if args.name:
            # delete configuration
            if not args.keep_config:
                if zk.exists(constants.QUEUE_TEMPLATE.format(queue_name = args.name)):
                    zk.delete(constants.QUEUE_TEMPLATE.format(queue_name = args.name))
                    logger.info('Configuration file deleted successfully!')
            
            # delete queue
            try:
                mq_channel.queue_delete(queue=args.name)
            except ValueError:
                logger.error('Queue with name {} does not exist!'.format(args.name))
            else:
                logger.info('Queue {} deleted'.format(args.name))
    
    # disconnect from zookeeper
    if zk.connected:
        zk.stop()
        zk.close()
    
    # disconnect from mq
    if mq_channel.is_open:
        mq_channel.close()
    if mq_connection.is_open:
        mq_connection.close()

    return 0

if __name__ == "__main__":
    sys.exit(main())
