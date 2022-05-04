#!/usr/bin/env python3

# Uploads new tasks, downloads completed tasks

import sys
import os
import argparse
import traceback
import logging
import uuid
import datetime
from stats import StatsCounter

# connection auxiliary formating functions
import worker_functions.connection_aux_functions as cf
from worker_functions.mq_client import MQClient

# config
import worker_functions.constants as constants

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
    :return: path if path is directory path
    :raise: ArgumentTypeError if path is not directory path
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
        '-k', '--keep-running',
        help='Keeps downloading of messages running, waiting for new messages, instead of exiting after all message from queue are downloaded.',
        default=False,
        action='store_true'
    )
    parser.add_argument(
        '-r', '--directory',
        help='Directory with data to upload / where data will be downloaded to.',
        type=dir_path
    )
    parser.add_argument(
        '-i', '--images',
        help='List of images to upload.',
        nargs='+',
        default=[]
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
    parser.add_argument(
        '--count-statistics',
        help='Count detailed statistics about processed messages',
        default=False,
        action='store_true'
    )
    parser.add_argument(
        '--save-message',
        help='Save message data for future use.',
        default=False,
        action='store_true'
    )
    return parser.parse_args()

class Publisher(MQClient):
    def __init__(self, mq_servers, count_stats = False, save_message = False):
        super().__init__(
            mq_servers = mq_servers,
            logger = logger
        )
        # Flag to stop downloading from queue
        self.queue_empty = False
        # Flag to detect delivery failure
        self.delivery_failed = False
        # directory where to store results from queue
        self.directory = None
        # message counter
        self.n_messages = 0
        # statistics counter
        if count_stats:
            self.stats_counter = StatsCounter(logger = logger)
        else:
            self.stats_counter = None
        # save messages to file in protobuf format
        self.save_message = save_message
    
    def __del__(self):
        super().__del__()
    
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
            info.write(f'Start time : {message.start_time.ToDatetime().isoformat()}\n')
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
                file.write(f'Start time : {log.start.ToDatetime().isoformat()}\n')
                file.write(f'End time   : {log.end.ToDatetime().isoformat()}\n')
                file.write(f'Status     : {log.status}\n')
                file.write('*** Log ***\n')
                file.write(log.log)
        
        if self.stats_counter:
            self.stats_counter.update_message_statistics(message)
        
        if self.save_message:
            with open(os.path.join(directory, 'message_body'), 'wb') as file:
                file.write(body)
    
    def mq_get_results(self, queue, directory):
        """
        Receives result messages from queue and stores them to directory.
        :param queue: queue to download results from
        :param directory: directory where to store downloaded data
        """
        self.directory = directory
        logger.info('Starting to download messages from queue {}'.format(queue))
        self.n_messages = 0
        while True:
            method, properties, body = self.mq_channel.basic_get(queue=queue, auto_ack=False)
            if method == properties == body == None:
                if self.n_messages:
                    logger.info('Number of received messages: {}'.format(self.n_messages))
                    if self.stats_counter:
                        self.stats_counter.log_statistics()
                else:
                    logger.info('Queue is empty, no messages were received')
                break
            # TODO
            # Remove output stage from list of remaining stages
            self.mq_save_result(method, properties, body)
            self.mq_channel.basic_ack(delivery_tag=method.delivery_tag)
            self.n_messages += 1
    
    def mq_save_message(self, channel, method, properties, body):
        """
        Callback for queue consumption - saves messages to output directory
        :param channel: channel instance
        :param method: delivery method
        :param properties: message properties
        :param body: message body
        """
        self.mq_save_result(method, properties, body)
        self.mq_channel.basic_ack(delivery_tag=method.delivery_tag)
        self.n_messages += 1

    def mq_get_results_continuous(self, queue, directory):
        """
        Recives result messages from queue and stores them to directory.
        Keeps running and waiting for messages.
        :param queue: queue to download results from
        :param directory: directory where to store downloaded data
        """
        self.directory = directory
        logger.info('Starting to download messages from queue {}'.format(queue))

        self.mq_channel.basic_consume(queue=queue, on_message_callback=self.mq_save_message, auto_ack=False)
        try:
            self.mq_channel.start_consuming()
        except KeyboardInterrupt:
            logger.info('Total number of received messages: {}'.format(self.n_messages))
            if self.stats_counter:
                self.stats_counter.log_statistics()
    
    @staticmethod
    def create_msg(image, stages, priority):
        """
        Creates processing task message.
        :param image: image that will be added to the message
        :param stages: list of processing stages of the task
        :param priority: priority of the message
        """
        message = ProcessingRequest()

        # add message properties
        message.uuid = uuid.uuid4().hex
        message.page_uuid = uuid.uuid4().hex
        message.priority = priority
        Timestamp.FromDatetime(message.start_time, datetime.datetime.now(datetime.timezone.utc))

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
        if not os.path.exists(directory) or not os.path.isdir(directory):
            logger.error(f'{directory} is not a directory!')
            return files
        
        for f in os.listdir(directory):
            f = os.path.join(directory, f)
            if not os.path.exists(f) or not os.path.isfile(f):
                logger.error(f'{f} is not a file!')
                continue
            files.append(f)
        
        return files

    def mq_upload_tasks(self, stages, priority, directory=None, files=[]):
        """
        Uploads messages with new tasks to given queue.
        Separate task is created for each file.
        :param stages: processing stages of given task
        :param priority: priority of the task
        :param directory: directory with files to upload
        :param files: list of files to upload
        """
        if directory:
            dir_files = self.get_files(directory)
            files = files + dir_files
        
        # register delivery confirmation callback
        self.mq_channel.confirm_delivery()

        for f in files:
            try:
                image = open(f, 'rb')
            except OSError:
                logger.error(f'Could not open file {f}!')
                continue
            
            message = self.create_msg(image, stages, priority)
            image.close()

            logger.info(f'Uploading file {f}')
            logger.info(f'Assigned message id: {message.uuid}')
            logger.info(f'Assigned page uuid: {message.page_uuid}')

            try:
                self.mq_channel.basic_publish('', stages[0], message.SerializeToString(),
                    properties=pika.BasicProperties(delivery_mode=2, priority=message.priority),
                    mandatory=True
                )
            except pika.exceptions.UnroutableError as e:
                logger.error('Message was rejected by the MQ broker!')
                logger.error('Received error: {}'.format(e))
            except pika.exceptions.AMQPError as e:
                logger.error('Message was not confirmed by the MQ broker!')
                logger.error('Received error: {}'.format(e))

def zk_get_mq_servers(zookeeper_servers, logger = logging.getLogger(__name__)):
    """
    Get list of mq servers from zookeeper
    :param zookeeper_servers: list of zookeeper servers
    :param logger: logger to use
    :return: list of mq servers or None
    """
    mq_servers = None

    # connect to zookeeper
    zk = KazooClient(hosts=zookeeper_servers)
    try:
        zk.start(timeout=20)
    except KazooTimeoutError:
        logger.error('Failed to connect to zookeeper!')
        return None
    
    # get server list
    try:
        mq_servers = zk.get_children(constants.WORKER_CONFIG_MQ_SERVERS)
    except Exception:
        logger.error('Failed to obtain MQ server list from zookeeper!')
        traceback.print_exc()
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
        return None

    # parse mq server list obtained from zookeeper
    mq_servers = cf.server_list(mq_servers)
    
    return mq_servers

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
        mq_servers = zk_get_mq_servers(zookeeper_servers)

    if not mq_servers:
        logger.error('Failed to get mq servers!')
        return 1

    # connect to mq
    publisher = Publisher(mq_servers, count_stats=args.count_statistics, save_message=args.save_message)
    publisher.mq_connect()

    error_code = 0
    # download results
    if args.download:
        if not args.directory:
            logger.error(f'Output directory not specified!')
            error_code = 1
        if not error_code:
            try:
                if args.keep_running:
                    publisher.mq_get_results_continuous(args.download, args.directory)
                else:
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
