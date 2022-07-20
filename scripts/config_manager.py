#!/usr/bin/env python3

# manages configuration for pero workers, manages available queues

import sys
import os
import logging
import argparse

# worker libraries
import worker_functions.connection_aux_functions as cf
import worker_functions.constants as constants
from worker_functions.mq_client import MQClient
from worker_functions.zk_client import ZkClient

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
        '-u', '--username',
        help='Username for server login.',
        default=None
    )
    parser.add_argument(
        '-p', '--password',
        help='Users password.',
        default=None
    )
    parser.add_argument(
        '--ca-cert',
        help='CA certificate for validation of server certificates during ssl/tls handshake.',
        default=None
    )
    parser.add_argument(
        '-c', '--config',
        help='Path to configuration file.',
        type=argparse.FileType('r')
    )
    parser.add_argument(
        '-r', '--remote-path',
        help='Path to remote OCR config file on FTP server.'
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

class ZkConfigManager(ZkClient):
    """
    Zookeeper configuration manager
    """

    def __init__(self, zk_servers, username='', password='', ca_cert=None, logger = logging.getLogger(__name__)):
        super().__init__(zk_servers, username=username, password=password, ca_cert=ca_cert, logger=logger)

    def __del__(self):
        super().__del__()
    

    def zk_upload_server_list(self, server_list, path):
        """
        Uploads servers given by server_list as subnodes of path in zookeeper
        :param server_list: servers to upload
        :param path: path where to upload server list in zookeeper (for example constants.WORKER_CONFIG_MQ_SERVERS)
        :raise: ZookeeperError if server returns non-zero error code
        """
        for server in server_list:
            server_ip_port = cf.ip_port_to_string(server)
            self.logger.debug(os.path.join(path, server_ip_port))
            self.zk.ensure_path(os.path.join(path, server_ip_port))

    def zk_delete_server_list(self, server_list, path):
        """
        Deletes servers given by server_list from zookeeper configuration given by path
        :param server_list: servers to delete
        :param path: path to server list in zookeeper
        :raise: ZookeeperError if server returns non-zero error code
        """
        for server in server_list:
            server_ip_port = cf.ip_port_to_string(server)
            if self.zk.exists(os.path.join(path, server_ip_port)):
                self.zk.delete(os.path.join(path, server_ip_port))
    
    def zk_get_server_list(self, path):
        """
        Returns list servers defined in zookeeper
        :return: list of MQ servers
        """
        try:
            server_list = cf.server_list(zk.get_children(path))
        except kazoo.exceptions.NoNodeError:
            self.logger.error('Failed to obtain server list from zookeeper!')
            raise
        return server_list
    
    def zk_upload_queue_config(self, queue, config):
        self.zk.ensure_path(constants.QUEUE_CONFIG_TEMPLATE.format(queue_name = queue))
        self.zk.ensure_path(constants.QUEUE_CONFIG_PATH_TEMPLATE.format(queue_name = queue))
        self.zk.set(constants.QUEUE_CONFIG_TEMPLATE.format(queue_name = queue), config.read().encode('utf-8'))
        self.zk.ensure_path(constants.QUEUE_STATS_AVG_MSG_TIME_TEMPLATE.format(queue_name = queue))
        self.zk.ensure_path(constants.QUEUE_STATS_WAITING_SINCE_TEMPLATE.format(queue_name = queue))
        self.zk.ensure_path(constants.QUEUE_CONFIG_ADMINISTRATIVE_PRIORITY_TEMPLATE.format(queue_name = queue))
        if not int.from_bytes(
            bytes=self.zk.get(constants.QUEUE_CONFIG_ADMINISTRATIVE_PRIORITY_TEMPLATE.format(queue_name = queue))[0],
            byteorder=constants.ZK_INT_BYTEORDER
        ):
            self.zk.set(
                path=constants.QUEUE_CONFIG_ADMINISTRATIVE_PRIORITY_TEMPLATE.format(queue_name = queue),
                value=int.to_bytes(0, sys.getsizeof(0), constants.ZK_INT_BYTEORDER)
            )

    def zk_upload_remote_config_path(self, queue, remote_path):
        self.zk.ensure_path(constants.QUEUE_CONFIG_PATH_TEMPLATE.format(queue_name = queue))
        self.zk.set(
            path=constants.QUEUE_CONFIG_PATH_TEMPLATE.format(queue_name = queue),
            value=remote_path.encode('utf-8')
        )
    
    def zk_upload_priority(self, queue, priority):
        self.zk.ensure_path(constants.QUEUE_CONFIG_ADMINISTRATIVE_PRIORITY_TEMPLATE.format(queue_name = queue))
        self.zk.set(
            path=constants.QUEUE_CONFIG_ADMINISTRATIVE_PRIORITY_TEMPLATE.format(queue_name = queue),
            value=int.to_bytes(priority, sys.getsizeof(priority), constants.ZK_INT_BYTEORDER)
        )
    
    def zk_delete_config(self, queue):
        if self.zk.exists(constants.QUEUE_TEMPLATE.format(queue_name = queue)):
            self.zk.delete(constants.QUEUE_TEMPLATE.format(queue_name = queue))
            self.logger.info('Configuration deleted successfully!')

class MQConfigManager(MQClient):
    """
    Message broker configuration manager
    """
    def __init__(self, mq_servers, username='', password='', ca_cert=None, logger = logging.getLogger(__name__)):
        super().__init__(mq_servers, username=username, password=password, ca_cert=ca_cert, logger=logger)

    def __del__(self):
        super().__del__()
    
    def mq_create_queue(self, name):
        try:
            self.mq_channel.queue_declare(
                queue=name,
                arguments={'x-max-priority': 1},
                durable=True
            )
        except ValueError as e:
            self.logger.error('Failed to declare queue {queue}! Received error: {error}'.format(
                queue = name,
                error = e
            ))
        else:
            self.logger.info('Queue {} created succesfully'.format(name))
    
    def mq_delete_queue(self, name):
        try:
            self.mq_channel.queue_delete(queue=name)
        except ValueError:
            self.logger.error('Queue with name {} does not exist!'.format(name))
        else:
            self.logger.info('Queue {} deleted'.format(name))

def main():
    args = parse_args()

    if args.file or args.target_file:
        raise NotImplemented('File / Target File is not supported yet!')

    # get zookeeper server list
    zookeeper_servers = cf.zk_server_list(args.zookeeper)
    if args.zookeeper_list:
        zookeeper_servers = cf.zk_server_list(args.zookeeper_list)
    
    # connect to zookeeper
    zk_config_manager = ZkConfigManager(zookeeper_servers, username = args.username, password = args.password, ca_cert = args.ca_cert)
    zk_config_manager.zk_connect()
    
    # get mq server list
    mq_servers = None
    if args.mq_servers:
        mq_servers = cf.server_list(args.mq_servers)
    elif args.mq_list:
        mq_servers = cf.server_list(args.mq_list)
    else:
        mq_servers = zk_config_manager.zk_get_server_list(constants.WORKER_CONFIG_MQ_SERVERS)

    if not mq_servers:
        logger.error('MQ server list not available!')
        return 1
    
    # connect to mq
    mq_config_manager = MQConfigManager(mq_servers, username = args.username, password = args.password, ca_cert = args.ca_cert)
    mq_config_manager.mq_connect()
    
    if args.update_monitoring_servers:
        if args.monitoring_servers:
            monitoring_servers = cf.server_list(args.monitoring_servers)
        else:
            monitoring_servers = cf.server_list(args.mq_servers)
            for server in monitoring_servers:
                server['port'] = None

    if not args.delete:
        if args.update_mq_servers:
            zk_config_manager.zk_upload_server_list(mq_servers, constants.WORKER_CONFIG_MQ_SERVERS)
            logger.info('MQ servers updated successfully!')
        
        if args.update_ftp_servers:
            zk_config_manager.zk_upload_server_list(cf.server_list(args.ftp_servers), constants.WORKER_CONFIG_FTP_SERVERS)
            logger.info('FTP servers updated successfully!')

        if args.update_monitoring_servers:
            zk_config_manager.zk_upload_server_list(monitoring_servers, constants.WORKER_CONFIG_MQ_MONITORING_SERVERS)
            logger.info('MQ monitoring servers updated successfully!')

        if args.name:
            # upload configuration
            if args.config:
                zk_config_manager.zk_upload_queue_config(args.name, args.config)
                logger.info('Configuration uploaded successfully!')
            
            if args.remote_path:
                logger.info('Setting remote config path to {}'.format(args.remote_path))
                zk_config_manager.zk_upload_remote_config_path(args.name, args.remote_path)
            
            if isinstance(args.administrative_priority, int):
                logger.info(
                    'Priority for queue {queue} set to {priority}'
                    .format(queue = args.name, priority = args.administrative_priority)
                )
                zk_config_manager.zk_upload_priority(args.name, args.administrative_priority)

            # create queue
            mq_config_manager.mq_create_queue(args.name)
    
    else:
        if args.update_mq_servers:
            zk_config_manager.zk_delete_server_list(mq_servers, constants.WORKER_CONFIG_MQ_SERVERS)
            logger.info('MQ servers deleted successfully!')
        
        if args.update_ftp_servers:
            zk_config_manager.zk_delete_server_list(cf.server_list(args.ftp_servers), constants.WORKER_CONFIG_FTP_SERVERS)
            logger.info('FTP servers deleted successfully!')
        
        if args.update_monitoring_servers:
            zk_config_manager.zk_delete_server_list(monitoring_servers, constants.WORKER_CONFIG_MQ_MONITORING_SERVERS)
            logger.info('MQ monitoring servers deleted successfully!')
        
        if args.name:
            # delete configuration
            if not args.keep_config:
                zk_config_manager.zk_delete_config(args.name)
            
            # delete queue
            mq_config_manager.mq_delete_queue(args.name)

    return 0

if __name__ == "__main__":
    sys.exit(main())
