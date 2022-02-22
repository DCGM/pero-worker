#!/usr/bin/env python3

# Uploads new tasks, downloads completed tasks

import sys
import os
import argparse
import traceback
import logging
import uuid

# connection auxiliary formating functions
import worker_functions.connection_aux_functions as cf

# MQ
import pika

# zookeeper
from kazoo.client import KazooClient
from kazoo.handlers.threading import KazooTimeoutError

# protobuf
from message_definitions.message_pb2 import ProcessingRequest, StageLog, Data
from google.protobuf.timestamp_pb2 import Timestamp

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
        nargs='+'
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
    def __init__(self, mq_servers):
        # list of servers to use
        self.mq_servers = mq_servers
        # connection to message broker
        self.mq_connection = None
        self.mq_channel = None
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
                logger.info('Connectiong to MQ server {}'.format(cf.ip_port_to_string(server)))
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
    
    def mq_disconnect(self):
        """
        Stops connection to message broker
        """
        if self.mq_channel and self.mq_channel.is_open:
            self.mq_channel.close()
        
        if self.mq_connection and self.mq_connection.is_open:
            self.mq_connection.close()
    
    def mq_save_result(self, method, properties, body):
        """
        Callback to result to directory.
        :param method: delivery method
        :param properties: message properties
        :param body: message content
        """

        message = ProcessingRequest().FromString(body)
        directory = os.path.abspath(os.path.join(self.directory, message.uuid))
        os.mkdir(directory)

        logger.info(f'Downloaded message:')
        logger.info(f'Message id : {message.uuid}')
        logger.info(f'Page uuid  : {message.page_uuid}')

        with open(os.path.join(directory, f'{message.uuid}_info.txt'), 'w') as info:
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
            with open(os.path.join(directory, f'{log.stage}.log'), 'w') as file:
                file.write(f'Host id    : {log.host_id}\n')
                file.write(f'Stage      : {log.stage}\n')
                file.write(f'Start time : {log.start}\n')
                file.write(f'End time   : {log.end}\n')
                file.write(f'Status     : {log.status}\n')
                file.write('*** Log ***\n')
                file.write(log.log)

    
    def mq_get_results(self, queue, directory):
        """
        Receives result messages from queue and stores them to directory.
        :param queue: queue to download results from
        :param directory: directory where to store downloaded data
        """
        self.directory = directory
        logger.info('Starting to download messages from queue {}'.format(queue))
        n_messages = 0
        while True:
            method, properties, body = self.mq_channel.basic_get(queue=queue, auto_ack=False)
            if method == properties == body == None:
                if n_messages:
                    logger.info('Number of received messages: {}'.format(n_messages))
                else:
                    logger.info('Queue is empty, no messages were received')
                break
            # TODO
            # Remove output stage from list of remaining stages
            self.mq_save_result(method, properties, body)
            self.mq_channel.basic_ack(delivery_tag=method.delivery_tag)
            n_messages += 1
    
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

        return message

    @staticmethod
    def get_files(directory):
        """
        Returns list of files with path in given directory.
        :param directory: directory
        :return: list of files in directory
        """
        files = []
        if os.path.exists(directory) and os.path.isdir(directory):
            logger.error(f'{directory} is not a directory!')
        
        for f in os.listdir(directory):
            f = os.path.join(directory, f)
            if not os.path.exists(f) or not os.path.isfile(f):
                logger.error(f'{f} is not a file!')
                continue
            files.append(f)
        
        return files

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
            dir_files = self.get_files(direcotry)
            files = files + dir_files
        
        # register delivery confirmation callback
        self.mq_channel.confirm_delivery()

        for f in files:
            try:
                image = open(f, 'rb')
            except OSError:
                logger.error(f'Could not open file {f}!')
                continue
            
            message = self.create_msg(image, stages)
            image.close()

            logger.info(f'Uploading file {f}')
            logger.info(f'Assigned message id: {message.uuid}')
            logger.info(f'Assigned page uuid: {message.page_uuid}')

            try:
                self.mq_channel.basic_publish('', stages[0], message.SerializeToString(), properties=pika.BasicProperties(
                    delivery_mode=2
                ))
            except pika.exceptions.AMQPError as e:
                logger.error('Message was not confirmed by the MQ broker!')
                logger.error('Received error: {}'.format(e))

def main():
    args = parse_args()
    mq_servers = None
    
    # check for mq servers
    if args.mq_servers:
        mq_servers = cf.server_list(args.mq_servers)
    
    if args.mq_list:
        mq_servers = cf.server_list(args.mq_list)

    # get zookeeper server list
    zookeeper_servers = cf.zk_server_list(args.zookeeper)
    if args.zookeeper_list:
        zookeeper_servers = cf.zk_server_list(args.zookeeper_list)
    
    # get MQ server list from zookeeper
    if not mq_servers:
        # connect to zookeeper
        zk = KazooClient(hosts=zookeeper_servers)
        try:
            zk.start(timeout=20)
        except KazooTimeoutError:
            sys.stderr.writable('Failed to connect to zookeeper!')
            return 1
        
        # get server list
        try:
            mq_servers = zk.get_children(constants.WORKER_CONFIG_MQ_SERVERS)[0].decode('utf-8')
        except Exception:
            logger.error('Failed to obtain MQ server list from zookeeper!')
            mq_servers = None
        
        # close connection
        try:
            zk.stop()
            zk.close()
        except Exception:
            logger.error('Failed to close connection to zookeeper!')
            traceback.print_exc()
        
        # if server list was not obtained, exit
        if not mq_servers:
            return 1

        # parse mq server list obtained from zookeeper
        mq_servers = cf.server_list(mq_servers)

    # connect to mq
    publisher = Publisher(mq_servers)
    publisher.mq_connect()

    error_code = 0
    # download results
    if args.download:
        if not args.directory:
            logger.error(f'Output directory not specified!')
            error_code = 1
        if not error_code:
            try:
                publisher.mq_get_results(args.download, args.directory)
            except Exception:
                logger.error(f'Failed to get results from {args.download}!')
                traceback.print_exc()
    
    # upload tasks
    else:
        if not args.directory and not args.images:
            logger.error('No input files specified!')
            error_code = 1
        if not args.stages:
            logger.error('No processing stages specified!')
            error_code = 1
        if not error_code:
            try:
                publisher.mq_upload_tasks(args.stages, args.priority, args.directory, args.images)
            except Exception:
                logger.error('Failed to upload tasks!')
                traceback.print_exc()

    publisher.mq_disconnect()
    return error_code


if __name__ == "__main__":
    sys.exit(main())
