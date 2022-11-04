#!/usr/bin/env python3

import sys
import os
import logging
import argparse
import datetime
import time

# worker libraries
import worker_functions.connection_aux_functions as cf
import worker_functions.constants as constants
from worker_functions.zk_client import ZkClient
from stage_config_manager import ZkConfigManager

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


def parse_args():
    parser = argparse.ArgumentParser('Pero processing system server configuration manager.')
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
        '--list-mq-servers',
        help='List message broker servers configured in zookeeper.',
        default=False,
        action='store_true'
    )
    parser.add_argument(
        '--list-ftp-servers',
        help='List FTP servers configured in zookeeper.',
        default=False,
        action='store_true'
    )
    parser.add_argument(
        '--list-monitoring-servers',
        help='List monitoring servers configured in zookeeper.',
        default=False,
        action='store_true'
    )

    parser.add_argument(
        '--add-mq-servers',
        help='List of message broker servers to add to configuration in zookeeper.',
        nargs='+',
        default=[]
    )
    parser.add_argument(
        '--add-ftp-servers',
        help='List of FTP servers to add to configuration in zookeeper.',
        nargs='+',
        default=[]
    )
    parser.add_argument(
        '--add-monitoring-servers',
        help='List of MQ monitring servers to add to configuration in zookeeper.',
        nargs='+',
        default=[]
    )

    parser.add_argument(
        '--rm-mq-servers',
        help='List of message broker servers to remove from configuration in zookeeper. (\'all\' removes all servers)',
        nargs='*',
        default=[]
    )
    parser.add_argument(
        '--rm-ftp-servers',
        help='List of FTP servers to remove from configuration in zookeeper. (\'all\' removes all servers)',
        nargs='*',
        default=[]
    )
    parser.add_argument(
        '--rm-monitoring-servers',
        help='List of MQ monitring servers to remove from configuration in zookeeper. (\'all\' removes all servers)',
        nargs='*',
        default=[]
    )
    
    return parser.parse_args()

def main():
    args = parse_args()

    # get zookeeper server list
    zookeeper_servers = cf.zk_server_list(args.zookeeper)
    if args.zookeeper_list:
        zookeeper_servers = cf.zk_server_list(args.zookeeper_list)
    
    # connect to zookeeper
    zk_config_manager = ZkConfigManager(zookeeper_servers, username = args.username, password = args.password, ca_cert = args.ca_cert)
    zk_config_manager.zk_connect()

    # remove
    if args.rm_mq_servers:
        if args.rm_mq_servers == ['all']:
            mq_servers = zk_config_manager.zk_get_server_list(constants.WORKER_CONFIG_MQ_SERVERS)
        else:
            mq_servers = cf.server_list(args.rm_mq_servers)
        zk_config_manager.zk_delete_server_list(mq_servers, constants.WORKER_CONFIG_MQ_SERVERS)
    if args.rm_ftp_servers:
        if args.rm_ftp_servers == ['all']:
            ftp_servers = zk_config_manager.zk_get_server_list(constants.WORKER_CONFIG_FTP_SERVERS)
        else:
            ftp_servers = cf.server_list(args.rm_ftp_servers)
        zk_config_manager.zk_delete_server_list(ftp_servers, constants.WORKER_CONFIG_FTP_SERVERS)
    if args.rm_monitoring_servers:
        if args.rm_monitoring_servers == ['all']:
            monitoring_servers = zk_config_manager.zk_get_server_list(constants.WORKER_CONFIG_MQ_MONITORING_SERVERS)
        else:
            monitoring_servers = cf.server_list(args.rm_monitoring_servers)
        zk_config_manager.zk_delete_server_list(monitoring_servers, constants.WORKER_CONFIG_MQ_MONITORING_SERVERS)
    
    # add
    if args.add_mq_servers:
        zk_config_manager.zk_upload_server_list(cf.server_list(args.add_mq_servers), constants.WORKER_CONFIG_MQ_SERVERS)
    if args.add_ftp_servers:
        zk_config_manager.zk_upload_server_list(cf.server_list(args.add_ftp_servers), constants.WORKER_CONFIG_FTP_SERVERS)
    if args.add_monitoring_servers:
        zk_config_manager.zk_upload_server_list(cf.server_list(args.add_monitoring_servers), constants.WORKER_CONFIG_MQ_MONITORING_SERVERS)
    
    # list
    if args.list_mq_servers:
        logger.info('MQ server list:')
        for server in zk_config_manager.zk_get_server_list(constants.WORKER_CONFIG_MQ_SERVERS):
            logger.info(cf.host_port_to_string(server))
    if args.list_ftp_servers:
        logger.info('FTP server list:')
        for server in zk_config_manager.zk_get_server_list(constants.WORKER_CONFIG_FTP_SERVERS):
            logger.info(cf.host_port_to_string(server))
    if args.list_monitoring_servers:
        logger.info('Monitoring server list:')
        for server in zk_config_manager.zk_get_server_list(constants.WORKER_CONFIG_MQ_MONITORING_SERVERS):
            logger.info(cf.host_port_to_string(server))

    return 0

if __name__ == "__main__":
    sys.exit(main())
