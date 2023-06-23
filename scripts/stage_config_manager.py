#!/usr/bin/env python3

# manages configuration for pero workers, manages available stages

import sys
import os
import logging
import argparse
import datetime
import time

# worker libraries
import worker_functions.connection_aux_functions as cf
import worker_functions.constants as constants
from worker_functions.mq_client import MQClient
from worker_functions.zk_client import ZkClient
from worker_functions.sftp_client import SFTP_Client

# MQ
import pika

# zookeeper
import kazoo
from kazoo.client import KazooClient
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
        '-c', '--config',
        help='Path to configuration file.',
        type=argparse.FileType('r')
    )
    parser.add_argument(
        '-v', '--version',
        help='Set configuration version. Usually utc date and time is used. (default = now)',
        default=None
    )
    parser.add_argument(
        '--keep-version',
        help='Update configuration but not its version.',
        default=False,
        action='store_true'
    )
    parser.add_argument(
        '-r', '--remote-path',
        help='Path to remote OCR config file on FTP server. Removes the file if used with \'-d\' option.'
    )
    parser.add_argument(
        '-n', '--name',
        help='Name for stage identification.'
    )
    parser.add_argument(
        '-a', '--administrative-priority',
        help='Administrative priority for given stage.',
        type=int
    )
    parser.add_argument(
        '-f', '--file',
        help='File to upload to FTP server. (For usage with \'--remote-path\')'
    )
    parser.add_argument(
        '-s', '--show',
        help='Show config for given stage.'
    )
    parser.add_argument(
        '-i', '--list',
        help='List configured stages.',
        default=False,
        action='store_true'
    )
    parser.add_argument(
        '-d', '--delete',
        help='Delete configuration and queues of given stage instead creating it.',
        default=False,
        action='store_true'
    )
    parser.add_argument(
        '-k', '--keep-config',
        help='Keep configuration and delete only queue of given stage. (Works only with \'-d/--delete\' argument.)',
        default=False,
        action='store_true'
    )
    return parser.parse_args()

class ZkConfigManager(ZkClient):
    """
    Zookeeper configuration manager
    """

    def __init__(self, zk_servers, username='', password='', ca_cert=None, logger = logging.getLogger(__name__)):
        """
        :param zk_servers: list of zookeeper servers as string, servers are separated by comma
                           (for example '1.2.3.4:2181,example.com,example2.com:123')
        :param username: zookeeper authentication username
        :param password: zookeeper authentication password
        :param ca_cert: CA certificate to validate server identity
        :param logger: logger instance to use for logging
        """
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
            server_host_port = cf.host_port_to_string(server)
            self.logger.debug(os.path.join(path, server_host_port))
            self.zk.ensure_path(os.path.join(path, server_host_port))

    def zk_delete_server_list(self, server_list, path):
        """
        Deletes servers given by server_list from zookeeper configuration given by path
        :param server_list: servers to delete
        :param path: path to server list in zookeeper
        :raise: ZookeeperError if server returns non-zero error code
        """
        for server in server_list:
            server_host_port = cf.host_port_to_string(server)
            if self.zk.exists(os.path.join(path, server_host_port)):
                self.zk.delete(os.path.join(path, server_host_port))
    
    def zk_get_server_list(self, path):
        """
        Returns list servers defined in zookeeper
        :return: list of MQ servers
        """
        try:
            server_list = cf.server_list(self.zk.get_children(path))
        except kazoo.exceptions.NoNodeError:
            self.logger.error('Failed to obtain server list from zookeeper!')
            raise
        return server_list
    
    def zk_get_config_path(self, stage):
        """
        Returns path to additional config on ftp server if configured, empty string otherwise.
        :param stage: stage to get config path for
        :return: path to config on ftp server as string
        """
        if self.zk.exists(constants.QUEUE_CONFIG_PATH_TEMPLATE.format(queue_name = stage)):
            return self.zk.get(constants.QUEUE_CONFIG_PATH_TEMPLATE.format(queue_name = stage))[0].decode('utf-8')
        return ''
    
    def zk_show_config(self, stage):
        """
        Logs via logger configuration of stage present in zookeeper.
        :param stage: stage name
        """
        if self.zk.exists(constants.QUEUE_TEMPLATE.format(queue_name = stage)):
            self.logger.info(f'Stage {stage} configuration:')
            ocr_config = ''
            ocr_config_path = ''
            config_version = ''
            administrative_priority = 0
            if self.zk.exists(constants.QUEUE_CONFIG_TEMPLATE.format(queue_name = stage)):
                ocr_config = self.zk.get(constants.QUEUE_CONFIG_TEMPLATE.format(queue_name = stage))[0].decode('utf-8')
            if self.zk.exists(constants.QUEUE_CONFIG_PATH_TEMPLATE.format(queue_name = stage)):
                ocr_config_path = self.zk.get(constants.QUEUE_CONFIG_PATH_TEMPLATE.format(queue_name = stage))[0].decode('utf-8')
            if self.zk.exists(constants.QUEUE_CONFIG_VERSION_TEMPLATE.format(queue_name = stage)):
                config_version = self.zk.get(constants.QUEUE_CONFIG_VERSION_TEMPLATE.format(queue_name = stage))[0].decode('utf-8')
            if self.zk.exists(constants.QUEUE_CONFIG_ADMINISTRATIVE_PRIORITY_TEMPLATE.format(queue_name = stage)):
                administrative_priority = int.from_bytes(
                    bytes=self.zk.get(constants.QUEUE_CONFIG_ADMINISTRATIVE_PRIORITY_TEMPLATE.format(queue_name = stage))[0],
                    byteorder=constants.ZK_INT_BYTEORDER
                )
            self.logger.info(f'OCR Configuration:\n{ocr_config}')
            self.logger.info(f'OCR Model path on FTP server: {ocr_config_path}')
            self.logger.info(f'OCR Configuration version: {config_version}')
            self.logger.info(f'Queue administrative priority: {administrative_priority}')
        else:
            self.logger.info(f'Stage {stage} is not configured!')
    
    def zk_list_stages(self):
        """
        Logs via logger names of stages configured in zookeeper.
        """
        if self.zk.exists(constants.QUEUE):
            stages = self.zk.get_children(constants.QUEUE)
            self.logger.info('Configured stages:')
            for stage in stages:
                self.logger.info(stage)
    
    def zk_upload_stage_config(self, stage, config):
        """
        Uploads stage configuration to zookeeper and creates all necessary nodes.
        :param stage: name of the stage
        :param config: opened configuration file
        """
        self.zk.ensure_path(constants.QUEUE_CONFIG_TEMPLATE.format(queue_name = stage))
        self.zk.ensure_path(constants.QUEUE_CONFIG_PATH_TEMPLATE.format(queue_name = stage))
        self.zk.set(constants.QUEUE_CONFIG_TEMPLATE.format(queue_name = stage), config.read().encode('utf-8'))
        self.zk.ensure_path(constants.QUEUE_STATS_AVG_MSG_TIME_TEMPLATE.format(queue_name = stage))
        self.zk.ensure_path(constants.QUEUE_STATS_WAITING_SINCE_TEMPLATE.format(queue_name = stage))
        self.zk.ensure_path(constants.QUEUE_CONFIG_ADMINISTRATIVE_PRIORITY_TEMPLATE.format(queue_name = stage))
        if not int.from_bytes(
            bytes=self.zk.get(constants.QUEUE_CONFIG_ADMINISTRATIVE_PRIORITY_TEMPLATE.format(queue_name = stage))[0],
            byteorder=constants.ZK_INT_BYTEORDER
        ):
            self.zk.set(
                path=constants.QUEUE_CONFIG_ADMINISTRATIVE_PRIORITY_TEMPLATE.format(queue_name = stage),
                value=int.to_bytes(0, sys.getsizeof(0), constants.ZK_INT_BYTEORDER)
            )
    
    def zk_upload_stage_config_version(self, stage, config_version):
        """
        Changes stage's config version in zookeeper.
        :param stage: stage name
        :param config_version: new config version
        """
        self.zk.ensure_path(constants.QUEUE_CONFIG_VERSION_TEMPLATE.format(queue_name = stage))
        self.zk.set(
            path=constants.QUEUE_CONFIG_VERSION_TEMPLATE.format(queue_name = stage),
            value=config_version.encode('utf-8')
        )

    def zk_upload_remote_config_path(self, stage, remote_path):
        """
        Changes SFTP OCR path in zookeeper.
        :param stage: stage name
        :param remote_path: new path to OCR model
        """
        self.zk.ensure_path(constants.QUEUE_CONFIG_PATH_TEMPLATE.format(queue_name = stage))
        self.zk.set(
            path=constants.QUEUE_CONFIG_PATH_TEMPLATE.format(queue_name = stage),
            value=remote_path.encode('utf-8')
        )
    
    def zk_upload_priority(self, stage, priority):
        """
        Changes administrative priority of stage's queue.
        :param stage: stage name
        :param priority: stage priority
        """
        self.zk.ensure_path(constants.QUEUE_CONFIG_ADMINISTRATIVE_PRIORITY_TEMPLATE.format(queue_name = stage))
        self.zk.set(
            path=constants.QUEUE_CONFIG_ADMINISTRATIVE_PRIORITY_TEMPLATE.format(queue_name = stage),
            value=int.to_bytes(priority, sys.getsizeof(priority), constants.ZK_INT_BYTEORDER)
        )
    
    def zk_delete_config(self, stage):
        """
        Removes stage configuration from zookeeper.
        :param stage: stage name
        """
        if self.zk.exists(constants.QUEUE_TEMPLATE.format(queue_name = stage)):
            self.zk.delete(
                path = constants.QUEUE_TEMPLATE.format(queue_name = stage),
                recursive = True
            )
            self.logger.info('Configuration deleted successfully!')

class MQConfigManager(MQClient):
    """
    Message broker configuration manager
    """
    def __init__(self, mq_servers, username='', password='', ca_cert=None, logger = logging.getLogger(__name__)):
        """
        :param mq_servers: list of mq servers to use, each server is a dicrionary with fields host and port.
                           If port is None, default MQ port is used.
                           (example: [{'host':'123.123.123.123', 'port':4321}, {'host': 'example.com', 'port': None}])
        :param username: MQ authentication username
        :param password: MQ authentication password
        :param ca_cert: CA certificate for server identity verification
        :param logger: logger instance to use for logging
        """
        super().__init__(mq_servers, username=username, password=password, ca_cert=ca_cert, logger=logger)

    def __del__(self):
        super().__del__()
    
    def mq_create_queue(self, name):
        """
        Creates message queue for stage given by name.
        :param name: name of the stage / queue to create
        """
        try:
            self.mq_channel.queue_declare(
                queue=name,
                arguments={'x-max-priority': 1},
                durable=True
            )
        except ValueError as e:
            self.logger.error('Failed to declare queue for stage {stage}! Received error: {error}'.format(
                stage = name,
                error = e
            ))
        else:
            self.logger.info('Queue for stage {} created succesfully'.format(name))
    
    def mq_delete_queue(self, name):
        """
        Deletes stage queue given by name.
        :param name: name of the stage / queue to delete
        """
        try:
            self.mq_channel.queue_delete(queue=name)
        except ValueError:
            self.logger.error('Queue for stage {} does not exist!'.format(name))
        else:
            self.logger.info('Queue for stage {} deleted'.format(name))

def main():
    args = parse_args()

    # get zookeeper server list
    zookeeper_servers = cf.zk_server_list(args.zookeeper)
    if args.zookeeper_list:
        zookeeper_servers = cf.zk_server_list(args.zookeeper_list)
    
    # connect to zookeeper
    zk_config_manager = ZkConfigManager(zookeeper_servers, username = args.username, password = args.password, ca_cert = args.ca_cert)
    zk_config_manager.zk_connect()
    
    # get mq server list
    mq_servers = zk_config_manager.zk_get_server_list(constants.WORKER_CONFIG_MQ_SERVERS)

    # get ftp server list
    ftp_servers = zk_config_manager.zk_get_server_list(constants.WORKER_CONFIG_FTP_SERVERS)

    if not mq_servers:
        logger.error('MQ server list not available!')
        return 1
    
    # connect to mq
    mq_config_manager = MQConfigManager(mq_servers, username = args.username, password = args.password, ca_cert = args.ca_cert)
    mq_config_manager.mq_connect()

    # init sftp client
    sftp = SFTP_Client(
        sftp_servers=ftp_servers,
        username=args.username,
        password=args.password,
        logger=logger
    )

    if not args.delete:
        if args.file and args.remote_path:
            # upload file to sftp
            sftp.sftp_connect()
            sftp.sftp_ensure_dir(os.path.split(args.remote_path)[0])
            sftp.sftp_put(args.file, args.remote_path)
        elif args.file:
            logger.warning('File upload location (\'--remote-path\') not specified!')

        if args.name:
            # get config version
            if args.version == 'now' or not args.version:
                version = datetime.datetime.now(datetime.timezone.utc).isoformat()
            else:
                version = args.version
            
            # update config version
            if args.version and not args.keep_version:
                logger.info('Setting config version to {version}'.format(version = version))
                zk_config_manager.zk_upload_stage_config_version(args.name, version)
            
            # update path to remote configuration
            if args.remote_path:
                logger.info('Setting remote config path to {}'.format(args.remote_path))
                if not args.keep_version:
                    zk_config_manager.zk_upload_stage_config_version(args.name, version)
                zk_config_manager.zk_upload_remote_config_path(args.name, args.remote_path)

            # upload configuration
            if args.config:
                if not args.keep_version:
                    zk_config_manager.zk_upload_stage_config_version(args.name, version)
                zk_config_manager.zk_upload_stage_config(args.name, args.config)
                logger.info('Configuration uploaded successfully!')
            
            if isinstance(args.administrative_priority, int):
                logger.info(
                    'Priority for stage {stage} set to {priority}'
                    .format(stage = args.name, priority = args.administrative_priority)
                )
                zk_config_manager.zk_upload_priority(args.name, args.administrative_priority)

            # create queue
            mq_config_manager.mq_create_queue(args.name)
    
    else:
        remote_path = ''
        if args.name:
            # delete configuration
            if not args.keep_config:
                remote_path = zk_config_manager.zk_get_config_path(args.name)
                zk_config_manager.zk_delete_config(args.name)
            
            # delete queue
            mq_config_manager.mq_delete_queue(args.name)
        
        if args.remote_path or remote_path:
            if not remote_path:
                remote_path = args.remote_path
            sftp.sftp_connect()
            sftp.sftp_remove_file(remote_path)
            sftp.sftp_remove_empty_dir(os.path.split(remote_path)[0])

    if args.list:
        zk_config_manager.zk_list_stages()

    if args.show:
        zk_config_manager.zk_show_config(args.show)

    return 0

if __name__ == "__main__":
    sys.exit(main())
