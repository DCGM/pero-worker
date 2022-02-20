#!/usr/bin/env python3

# manages configuration for pero workers, manages available queues

import sys
import os
import logging
import argparse
import worker_functions.connection_aux_functions as cf

# MQ
import pika

# zookeeper
from kazoo.client import KazooClient
from kazoo.handlers.threading import KazooTimeoutError

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
        '-c', '--config',
        help='Create configuration based on config file. Creates queue for configuration as well.',
        type=argparse.FileType('r')
    )
    parser.add_argument(
        '-n', '--name',
        help='Name for identification of queue and configuration files.',
        required=True
    )
    parser.add_argument(
        '-d', '--delete',
        help='Delete configuration and queues instead creating it.',
        default=False,
        action='store_true'
    )

def mq_connect(mq_servers):
    """
    Connects client to mq server
    :param mq_servers: mq servers to try
    :return: blocking connection to mq server or None
    """
    mq_connection = None
    for server in mq_servers:
        try:
            logger.info('Connectiong to MQ server {}'.format(mq_server))
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

    # TODO
    if not args.delete:
        # upload configuration
        # upload ocr

        # create queue
        mq_channel.queue_declare(queue=args.name, arguments={'x-max-priority': 2})
    
    else:
        # TODO
        # delete configuration
        # delete ocr
        
        # delete queue
        try:
            mq_channel.queue_delete(queue=args.name)
        except ValueError:
            logger.error('Queue with name {} does not exist!'.format(args.name))
    
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
