#!/usr/bin/env python3

# manages configuration for pero workers, manages available queues

import sys
import os
import logging
import argparse
from ftplib import FTP, error_perm, error_reply

# worker libraries
import worker_functions.connection_aux_functions as cf
import worker_functions.constants as constants

# MQ
import pika

# zookeeper
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
    :return: path if path is direcotry path
    :raise: ArgumentTypeError if path is not direcotry path
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
        '-r', '--ftp-servers',
        help='List of ftp servers for uploading files.',
        nargs='+'
    )
    parser.add_argument(
        '-i', '--ftp-list',
        help='File containing list of ftp servers. One server per line.',
        type=argparse.FileType('r')
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
        '-n', '--name',
        help='Name for identification of queue and configuration files.'
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
    return parser.parse_args()

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

def ftp_create_dir(ftp, path):
    """
    Creates directory path in ftp
    :param ftp: ftp server connection
    :param path: directory path to create
    """
    path = path.split('/')
    current_path = ''
    for directory in path:
        if directory not in ftp.nlst(current_path):
            ftp.sendcmd('MKD {}'.format(os.path.join(current_path, directory)))
        current_path = os.path.join(current_path, directory)

def main():
    args = parse_args()

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
            mq_servers = zk.get_children(constants.WORKER_CONFIG_MQ_SERVERS)[0].decode('utf-8')
        except Exception:
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
    
    # Get ftp server list
    ftp_servers = None
    if args.ftp_servers:
        ftp_servers = cf.server_list(args.ftp_servers)
    elif args.ftp_list:
        ftp_servers = cf.server_list(args.frt_list)
    else:
        try:
            ftp_servers = zk.get_children(constants.WORKER_CONFIG_FTP_SERVERS)[0].decode('utf-8')
        except Exception:
            logger.error('Failed to obtain FTP srver list from zookeeper!')
            ftp_servers = None
    
    # connect to ftp
    ftp = cf.ftp_connect(ftp_servers)

    if not ftp:
        logger.error('Failed to connect to FTP!')
        zk.stop()
        zk.close()
        mq_channel.close()
        mq_connection.close()
        return 1
    
    try:
        ftp.login(args.user, args.password)
    except error_perm:
        logger.error('Failed to login to FTP!')
        zk.stop()
        zk.close()
        mq_channel.close()
        mq_connection.close()
        ftp.quit()
        return 1

    if not args.delete:
        # upload file to ftp
        if args.file:
            for i in range(0, len(args.file)):
                path = args.file[i]
                if i < len(args.target_file):
                    target = args.target_file[i]
                elif args.name:
                    target = os.path.join(args.name, os.path.basename(path))
                else:
                    logger.error('Failed to upload file {file}, target location not specified!'.format(path))
                    continue
                try:
                    # TODO
                    # store file to subfolders
                    with open(path, 'rb') as fd:
                        ftp.storbinary('STOR {}'.format(target), fd)
                except (FileNotFoundError, PermissionError) as e:
                    logger.error('Failed to upload file {}'.format(path))
                    logger.error('Received error: {}'.format(e))
                else:
                    logger.info('{file} successfully uploaded as {target}'.format(
                        file = path,
                        target = target
                    ))

        if args.name:
            # upload configuration
            if args.config:
                zk.ensure_path(constants.PROCESSING_CONFIG_TEMPLATE.format(config_name = args.name))
                zk.set(constants.PROCESSING_CONFIG_TEMPLATE.format(config_name = args.name), args.config.read().encode('utf-8'))
                logger.info('Configuration file uploaded successfully!')

            # create queue
            try:
                mq_channel.queue_declare(queue=args.name, arguments={'x-max-priority': 2})
            except ValueError as e:
                logger.error('Failed to declare queue {queue}! Received error: {error}'.format(
                    queue = args.name,
                    error = e
                ))
            else:
                logger.info('Queue {} created succesfully'.format(args.name))
    
    else:
        # delete ftp files
        for target in args.target_file:
            try:
                # TODO
                # search in subfolders
                file_found = False
                for f in ftp.nlst():
                    if target == f:
                        file_found = True
                if file_found:
                    ftp.delete(target)
                    logger.info('File {} deleted'.format(target))
                else:
                    logger.warn('File {} not found!'.format(target))
            except error_perm:
                logger.error('Insufficient permissions to delete {}!'.format(target))
            except error_reply as e:
                logger.error('Failed to delete file {}!'.format(target))
                logger.error('Received error: {}'.format(e))
        
        if args.name:
            # delete configuration
            if not args.keep_config:
                if zk.exists(constants.PROCESSING_CONFIG_TEMPLATE.format(config_name = args.name)):
                    zk.delete(constants.PROCESSING_CONFIG_TEMPLATE.format(config_name = args.name))
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
    
    # disconnect from ftp
    if ftp:
        try:
            ftp.quit()
        except AttributeError:
            # connection is closed
            pass

    return 0

if __name__ == "__main__":
    sys.exit(main())
