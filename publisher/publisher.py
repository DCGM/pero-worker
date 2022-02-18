#!/usr/bin/env python3

# Uploads new tasks, downloads completed tasks

import sys
import os
import argparse
import traceback
import worker_functions.connection_aux_functions as cf

# MQ
import pika

# zookeeper
from kazoo.client import KazooClient
from kazoo.handlers.threading import KazooTimeoutError

# protobuf
from message_definitions.message_pb2 import ProcessingRequest, StageLog, Data
from google.protobuf.timestamp_pb2 import Timestamp

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
    parser.add_argument(
        '-d', '--download',
        help='Queue with data to download.'
    )
    parser.add_argument(
        '-r', '--directory',
        help='Directory with data to upload / where data will be downloaded to.',
        type=dir_path
    )
    parser.add_argument(
        '-i', '--images',
        help='List of images to upload.',
        type=argparse.FileType('r')
    )
    parser.add_argument(
        '-t', '--stages',
        help='Processing stages of tasks to upload.',
        nargs='+'
    )
    parser.add_argument(
        '-p', '--priority',
        help='Priority of tasks to upload.',
        type=int,
        default=0
    )
    return parser.parse_args()

class Publisher:
    def __init__(mq_servers):
        # list of servers to use
        self.mq_servers = mq_servers
        # connection to message broker
        self.mq_connection
        self.mq_channel
        # Flag to stop downloading from queue
        self.queue_empty = False
        # Flag to detect delivery failure
        self.delivery_failed = False
        # directory where to store results from queue
        self.directory = None
    
    def mq_connect(self):
        """
        Connect to message broker
        """
        for server in self.mq_servers:
            try:
                logger.info('Connectiong to MQ server {}'.format(self.mq_server))
                self.mq_connection = pika.BlockingConnection(
                    pika.ConnectionParameters(
                        host=server['ip'],
                        port=server['port'] if server['port'] else pika.ConnectionParameters.DEFAULT_PORT
                    )
                )
                logger.info('Opening channel to MQ.')
                self.mq_channel = self.mq_connection.channel()
            except pika.exceptions.AMQPError as e:
                logger.error('Connection failed! Received error: {}'.format(e))
                continue
            else:
                break
    
    def mq_save_result(self, channel, method, properties, body):
        """
        Callback to result to directory.
        :param channel: MQ channel
        :param method: delivery method
        :param properties: message properties
        :param body: message content
        """

        # check if any message was received
        if not method.NAME == 'Basic.GetOk':
            self.queue_empty = True

        message = ProcessingRequest().FromString(body)
        directory = os.path.join(self.directory, message.uuid)

        print(f'Downloaded message:')
        print(f'Message id : {message.uuid}')
        print(f'Page uuid  : {message.page_uuid}')

        with open(os.path.join(directory, f'{message.uuid}.info'), 'wb') as info:
            info.write(f'Message id : {message.uuid}\n')
            info.write(f'Page uuid  : {message.page_uuid}\n')
            info.write(f'Start time : {message.start_time}\n')
            info.write(f'Remaining processing stages:\n')
            for stage in message.processing_stages:
                info.write(f'  {stage}\n')
        for result in message.results:
            with open(os.path.join(directory, result.name), 'wb') as file:
                file.write(result.content)
        for log in message.logs:
            with open(os.path.join(output, f'{log.stage}.log'), 'w') as file:
                file.write(f'Host id    : {log.host_id}\n')
                file.write(f'Stage      : {log.stage}\n')
                file.write(f'Start time : {log.start}\n')
                file.write(f'End time   : {log.end}\n')
                file.write(f'Status     : {log.status}\n')
                file.write('*** Log ***\n')
                file.write(log.log)

        channel.basic_ack(delivery_tag=method.delivery_tag)

    
    def mq_get_results(self, queue, directory):
        """
        Receives result messages from queue and stores them to directory.
        :param queue: queue to download results from
        :param directory: directory where to store downloaded data
        """
        self.directory = directory
        print('Starting to download messages from queue {}'.format(queue))
        while True:
            self.mq_channel.get()
            if self.queue_empty:
                break
    
    @staticmethod
    def create_msg(image, stages):
        """
        Creates processing task message.
        :param image: image that will be added to the message
        :param stages: list of processing stages of the task
        """
        message = ProcessingRequest()

        # add message properties
        message.uuid = uuid.uuid4().hex
        message.page_uuid = uuid.uuid4().hex
        message.priority = 0
        Timestamp.GetCurrentTime(message.start_time)

        # add processing stages
        for stage in stages:
            message.processing_stages.append(stage)

        # add image to results
        img = message.results.add()
        img.name = os.path.basename(image.name)
        img.content = image.read()

        return message.SerializeToString()

    @staticmethod
    def get_files(directory):
        """
        Returns list of files with path in given directory.
        :param directory: directory
        :return: list of files in directory
        """
        files = []
        if os.path.exists(directory) and os.path.isdir(directory):
            sys.stderr.write(f'error: {directory} is not a directory!')
        
        for f in os.listdir(directory):
            f = os.path.join(directory, f)
            if not os.path.exists(f) or not os.path.isfile(f):
                sys.stderr.write(f'error: {f} is not a file!\n')
            try:
                f = open(f, 'rb')
                files.append(f)
            except OSError:
                sys.stderr.write(f'error: could not open file {f}!\n')
        
        return files

    def mq_confirm_delivery(self, method):
        """
        Receives confirmation from broker when message is uploaded.
        :param method: frame method
        """
        if not method.NAME == 'Basic.Ack':
            self.delivery_failed = True
            print('DEBUG: Callback')

    def mq_upload_tasks(self, stages, priority, direcotry=None, files=[]):
        """
        Uploads messages with new tasks to given queue.
        Separate task is created for each file.
        :param stages: processing stages of given task
        :param priority: priority of the task
        :param directory: directory with files to upload
        :param files: list of files to upload
        """
        if direcotry:
            dir_files = self.get_dir_content(direcotry)
            files = files + dir_files
        
        # register delivery confirmation callback
        self.mq_channel.confirm_delivery(mq_confirm_delivery)

        for f in files:
            try:
                image = open(f, 'rb')
            except OSError:
                sys.stderr.write(f'error: could not open file {f}!')
                continue
            
            message = self.create_msg(image, stages)

            self.mq_channel.basic_publish('', stages[0], message)
            print('DEBUG: Upload')

            # TODO
            # check if this runs before or after the callback
            if self.delivery_failed:
                sys.stderr.write('error: failed to deliver message to the broker!\n')
                sys.stderr.write(f'error: undelivered file: {os.path.basename(f)}!\n')
                break        

def main():
    args = parse_args()
    mq_servers = None
    
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
    publisher.mq_connect()

    error_code = 0
    # download results
    if args.download:
        if not args.directory:
            sys.stderr.write(f'error: output directory not specified!\n')
            error_code = 1
        if not error_code:
            try:
                publisher.mq_get_results(args.download, args.directory, stages)
            except Exception:
                sys.stderr.write(f'error: failed to get results from {args.download}!\n')
                traceback.print_exc()
    
    # upload tasks
    else:
        if not args.directory or not args.files:
            sys.stderr.write('error: no input files specified!\n')
            error_code = 1
        if not args.stages:
            sys.stderr.write('error: no processing stages specified!\n')
            error_code = 1
        if not error_code:
            try:
                publisher.mq_upload_tasks(args.stages, args.priority, args.directory, args.images)
            except Exception:
                sys.stderr.write('error: failed to upload tasks!\n')
                traceback.print_exc()

    publisher.disconnect()
    return error_code


if __name__ == "__main__":
    sys.exit(main())
