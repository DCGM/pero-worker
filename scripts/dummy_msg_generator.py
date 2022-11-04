#!/usr/bin/env python3

# generates dummy traffic for testing

import sys
import os
import argparse
import traceback
import logging
import uuid
import random
import datetime
import time

# connection auxiliary formatting functions
import worker_functions.connection_aux_functions as cf

from publisher import ZkPublisher
from worker_functions.mq_client import MQClient

# MQ
import pika

# protobuf
from message_definitions.message_pb2 import ProcessingRequest, StageLog, Data
from google.protobuf.timestamp_pb2 import Timestamp

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
    parser = argparse.ArgumentParser('Generates test trafic')
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
        '--size',
        help='Size of generated data in bytes.',
        type=int,
        default=0
    )
    parser.add_argument(
        '--size-range',
        help='Range of size of data in bytes. Usage: \'--size-range <min> <max>\'',
        type=int,
        nargs=2
    )
    parser.add_argument(
        '--size-delta',
        help='Diference in size of data. Only works with \'--size\'. Same as \'--range <size - delta> <size + delta>\'',
        type=int,
        default=0
    )
    parser.add_argument(
        '-n', '--number',
        help='Number of messages to generate at once.',
        type=int,
        default=1
    )
    parser.add_argument(
        '-i', '--interval',
        help='Interval in which the messages will be periodically generated',
        type=int,
        default=0  # disabled
    )
    parser.add_argument(
        '-c', '--count',
        help='Number of iterations (count how many times should periodic messages be generated)',
        type=int,
        default=0  # disabled
    )
    parser.add_argument(
        '-t', '--stages',
        help='Processing stages of tasks to upload.',
        nargs='+'
    )
    parser.add_argument(
        '-y', '--priority',
        help='Priority of tasks to upload.',
        type=int,
        default=0
    )
    return parser.parse_args()

class DummyMsgGenerator(MQClient):
    def gen_trafic(self, number, interval, count, stages, priority, size_min, size_max):
        """
        Generates messages with given parameters
        :param number: number of messages to generate at once
        :param interval: interval in which messages will be periodically generated
        :param count: repetition count (how many times messages will be generated)
        :param stages: stages of message
        :param priority: priority of message
        :param size_range_min: minimal size of message data
        :param size_range_max: maximal size of message data
        """
        self.mq_channel.confirm_delivery()

        iteration = 0
        unlimited = False
        if not count:
            unlimited = True

        while True:
            logger.info(
                'Iteration {iteration}, generating {number} messages'
                .format(iteration = iteration, number = number)
            )
            for i in range(0, number):
                data = os.urandom(int(random.uniform(size_min, size_max)))
                message = self.create_msg(data, stages, priority)
                try:
                    self.mq_channel.basic_publish('', stages[0], message.SerializeToString(),
                        properties=pika.BasicProperties(delivery_mode=2, priority=message.priority),
                        mandatory=True
                    )
                except pika.exceptions.UnroutableError as e:
                    logger.error('Message was rejected by the MQ broker!')
                    logger.error('Received error: {}'.format(e))

            logger.info('Waiting for {} seconds'.format(interval))
            time.sleep(interval)

            iteration += 1

            if not unlimited and iteration >= count:
                break
    
    @staticmethod
    def create_msg(data, stages, priority):
        """
        Creates processing task message.
        :param data: data used as content of the message
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
        msg_data = message.results.add()
        msg_data.name = 'data'
        msg_data.content = data

        return message


def main():
    args = parse_args()

    # get zookeeper server list
    zookeeper_servers = cf.zk_server_list(args.zookeeper)
    if args.zookeeper_list:
        zookeeper_servers = cf.zk_server_list(args.zookeeper_list)
    
    # get MQ server list from zookeeper
    zk_client = ZkPublisher(
        zookeeper_servers=zookeeper_servers,
        username=args.username,
        password=args.password,
        ca_cert=args.ca_cert,
        logger=logger
    )
    mq_servers = ZkPublisher.zk_get_mq_servers(zookeeper_servers)
    zk_client.zk_disconnect()
    
    if not mq_servers:
        logger.error('Failed to get list of MQ servers from zookeeper!')
        return 1

    # connect to mq
    generator = DummyMsgGenerator(
        mq_servers=mq_servers,
        username=args.username,
        password=args.password,
        ca_cert=args.ca_cert,
        logger=logger
    )

    if args.interval:
        heartbeat = args.interval + 20
    else:
        heartbeat = 60
    generator.mq_connect(heartbeat = heartbeat)

    if args.size_range:
        size_min = args.size_range[0]
        size_max = args.size_range[1]
    else:
        size_min = args.size - args.size_delta
        size_max = args.size + args.size_delta
    
    if size_min > size_max:
        logger.error('Minimal size cannot be biger than maximum size!')
        return 1
    
    if args.count < 0:
        logger.error('\'--count\' can\'t be lower than zero!')
        return 1

    try:
        generator.gen_trafic(args.number, args.interval, args.count, args.stages, args.priority, size_min, size_max)
    except KeyboardInterrupt:
        logger.info('Keyboard interrupt received! Exiting!')
    except Exception:
        logger.error('Failed to generate traffic!')
        logger.error('Received error:\n{}'.format(traceback.format_exc()))
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
