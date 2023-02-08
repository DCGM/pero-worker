#!/usr/bin/env python3

import sys
import os
import argparse
import configparser
import logging

import worker_functions.connection_aux_functions as cf
from worker_controller import ZkWorkerController

def dir_path(path):
    """
    Check if path is directory path
    :param path: path to directory
    :return: path if path is directory path
    :raise: ArgumentTypeError if path is not directory path
    """
    if os.path.isdir(path):
        return path
    raise argparse.ArgumentTypeError(f"{path} is not a valid path")

def parse_args():
    """
    Parses arguments given on commandline
    :return: namespace with parsed args
    """
    argparser = argparse.ArgumentParser('Runs worker for page processing')
    argparser.add_argument(
        '-c', '--config',
        help='Configuration file for worker - can be used instead of listed arguments.',
        default=None
    )
    argparser.add_argument(
        '-z', '--zookeeper',
        help='List of zookeeper servers from where configuration will be downloaded. If port is omitted, default zookeeper port is used.',
        nargs='+',
        default=[]
    )
    argparser.add_argument(
        '-i', '--id',
        help='Worker id for identification in zookeeper',
        default=None
    )
    argparser.add_argument(
        '--cache-directory',
        help='Path to directory where OCR files will be temporarily stored',
        type=dir_path
    )
    argparser.add_argument(
        '-u', '--username',
        help='Username for authentication on server.',
        default=None
    )
    argparser.add_argument(
        '-p', '--password',
        help='Password for user authentication.',
        default=None
    )
    argparser.add_argument(
        '-e', '--ca-cert',
        help='CA Certificate for SSL/TLS connection verification.',
        default=None
    )
    argparser.add_argument(
        '--disable-remote-logging',
        help='Disables logging to MQ.',
        default=False,
        action='store_true'
    )
    argparser.add_argument(
        '-q', '--log-queue',
        help='Name of log queue on MQ.',
        default=None
    )
    argparser.add_argument(
        '-d', '--debug',
        help='Enable debugging log output.',
        default=False,
        action='store_true'
    )
    return argparser.parse_args()

def main():
    args = parse_args()

    # set logger verbosity
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    if args.debug:
        logger.setLevel(logging.DEBUG)

    # set default args
    zk_servers = []
    username = None
    password = None
    ca_cert = None
    worker_id = None
    cache_dir = '/tmp'
    log_queue = 'log'
    disable_remote_logging = False

    # parse config file
    if args.config:
        config = configparser.ConfigParser()
        config.read(args.config)
        if 'WORKER' not in config:
            logger.warning('Worker configuration section not found in given configuration!')
        else:
            if 'zookeeper_servers' in config['WORKER']:
                zk_servers = cf.zk_server_list(config['WORKER']['zookeeper_servers'])
            if 'username' in config['WORKER']:
                username = config['WORKER']['username']
            if 'password' in config['WORKER']:
                password = config['WORKER']['password']
            if 'ca_cert' in config['WORKER']:
                ca_cert = config['WORKER']['ca_cert']
            if 'worker_id' in config['WORKER']:
                worker_id = config['WORKER']['worker_id']
            if 'cache_dir' in config['WORKER']:
                cache_dir = config['WORKER']['cache_dir']
            if 'log_queue' in config['WORKER']:
                log_queue = config['WORKER']['log_queue']
            if 'disable_remote_logging' in config['WORKER']:
                disable_remote_logging = True if config['WORKER']['disable_remote_logging'] == 'True'\
                    or config['WORKER']['disable_remote_logging'] == '1' else False
    
    # parse arguments
    if args.zookeeper:
        zk_servers = cf.zk_server_list(args.zookeeper)
    if args.username:
        username = args.username
    if args.password:
        password = args.password
    if args.ca_cert:
        ca_cert = args.ca_cert
    if args.id:
        worker_id = args.id
    if args.cache_directory:
        cache_dir = args.cache_directory
    if args.log_queue:
        log_queue = args.log_queue
    if args.disable_remote_logging:
        disable_remote_logging = args.disable_remote_logging

    # run worker
    worker = ZkWorkerController(
        zookeeper_servers = zk_servers,
        username = username,
        password = password,
        ca_cert = ca_cert,
        worker_id = worker_id,
        cache_dir = cache_dir,
        mq_log_queue = log_queue,
        disable_remote_logging = disable_remote_logging,
        logger = logger
    )
    return worker.run()


if __name__ == "__main__":
    sys.exit(main())
