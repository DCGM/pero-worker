#!/usr/bin/env python3

# Uploads new tasks, downloads completed tasks

import sys
import os
import argparse
import traceback
import worker_functions.connection_aux_functions as cf

from kazoo.client import KazooClient
from kazoo.handlers.threading import KazooTimeoutError

def dir_path(path):
    """
    Check if path is directory path
    :param path: path to directory
    :return: path if path is direcotry path
    :raise: ArgumentTypeError if path is not direcotry path
    """
    if os.path.isdir(path):
        return path
    raise argparse.ArgumentTypeError(f"{path} is not a valid path")

def parse_args():
    parser = argparse.ArgumentParser('Uploads new tasks to queue, downloads completed tasks.')
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
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        '-u', '--upload',
        help='Upload data.',
        action='store_false'
    )
    mode.add_argument(
        '-d', '--download',
        help='Download data.',
        action='store_true'
    )
    data = parser.add_mutually_exclusive_group()
    data.add_argument(
        '-f', '--folder',
        help='Folder with data to upload / where data will be downloaded to.',
        type=dir_path
    )
    data.add_argument(
        '-i', '--image',
        help='Image to upload. Multiple files can be specified.',
        type=argparse.FileType('r')
    )
    parser.add_argument(
        '-q', '--queue',
        help='Queue to use.'
    )
    parser.add_argument(
        '--message',
        help='File where to save message for debugging purposes.',
        type=argparse.FileType('w')
    )
    return parser.parse_args()

class Publisher:
    def __init__(mq_servers):
        # list of servers to use
        self.mq_servers = mq_servers
        # connection to message broker
        self.mq_connection
        self.mq_channel
    
    def mq_connect(self):
        """
        Connect to message broker
        """
        # TODO
        # try all servers
        # add port configuration
        self.mq_server = self.mq_servers[0]
        logger.info('Connectiong to MQ server {}'.format(self.mq_server))
        self.mq_connection = pika.SelectConnection(
            pika.ConnectionParameters(
                host=self.mq_server,
            ),
            on_open_callback=self.mq_channel_create,
            on_open_error_callback=self.mq_connection_open_error,
            on_close_callback=self.mq_connection_close
        )
        

def main():
    args = parse_args()
    mq_servers = None

    # print error for invalid argument combination
    if args.download and args.image:
        sys.stderr.write('error: argument -i/--image not allowed with argument -d/--download\n')
        return 1
    
    # check for mq servers
    if args.mq_servers:
        mq_servers = args.mq_servers
    
    if args.mq_list:
        mq_servers = mq_list(args.mq_list)

    # get zookeeper server list
    zookeeper_servers = zkf.zk_server_list(args.zookeeper)
    if args.zookeeper_list:
        zookeeper_servers = zkf.zk_server_list(args.zookeeper_list)
    
    # get MQ server list from zookeeper
    if not mq_servers:
        # connect to zookeeper
        zk = KazooClient(hosts=zookeeper_servers)
        try:
            zk.start(timeout=20)
        except KazooTimeoutError:
            sys.stderr.writable('error: failed to connect to zookeeper!\n')
            return 1
        
        # get server list
        try:
            mq_servers = zk.get_children(constants.WORKER_CONFIG_MQ_SERVERS)[0].decode('utf-8')
        except Exception:
            sys.stderr.write('error: failed to obtain MQ server list from zookeeper!\n')
            mq_servers = None
        
        # close connection
        try:
            zk.stop()
            zk.close()
        except Exception:
            sys.stderr.write('error: failed to close connection to zookeeper!\n')
            traceback.print_exc()
        
        # if server list was not obtained, exit
        if not mq_servers:
            return 1

        # parse mq server list obtained from zookeeper
        mq_servers = parse_mq_servers(mq_servers)

    publisher = Publisher(mq_servers)

    # TODO
    # connect to mq servers
    # download tasks
    # upload tasks

    return 0


if __name__ == "__main__":
    sys.exit(main())
