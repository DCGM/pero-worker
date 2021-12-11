#!/usr/bin/env python3

# Worker for page processing

import pika # AMQP protocol library for queues
import logging
import time
import sys
import os # filesystem
import argparse
import uuid
import json # nice logging of configuration objects

# protobuf
from message_definitions.message_pb2 import ProcessingRequest, StageLog, Data
from google.protobuf.timestamp_pb2 import Timestamp

# zookeeper
from kazoo.client import KazooClient
from kazoo.handlers.threading import KazooTimeoutError

# setup logging (kazoo requires at least minimum config)
logging.basicConfig()

# setup logger
log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

stderr_handler = logging.StreamHandler()
stderr_handler.setFormatter(log_formatter)

logger = logging.getLogger(__name__)
logger.addHandler(stderr_handler)

# === Global config ===
tag = None

# === Functions ===

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
    """
    Parses arguments given on commandline
    :return: namespace with parsed args
    """
    argparser = argparse.ArgumentParser('Worker for page processing.')
    argparser.add_argument(
        '-z', '--zookeeper-servers',
        help='List of zookeeper servers from where configuration will be downloaded. If port is omitted, default zookeeper port is used.',
        nargs='+', # TODO - check if addresses are valid
        default=['127.0.0.1:2181']
    )
    argparser.add_argument(
        '-i', '--id',
        help='Worker id for identification in zookeeper.'
    )
    argparser.add_argument(
        '-b', '--broker-servers',
        help='List of message queue broker servers where to get and send processing requests.',
        nargs='+', # TODO - check if addresses are valid
        default=['127.0.0.1']
    )
    argparser.add_argument(
        '-q', '--queue',
        help='Queue with messages to process.'
    )
    argparser.add_argument(
        '--ocr',
        help='Folder with OCR data and config file in .ini format.',
        type=dir_path
    )
    return argparser.parse_args()


# === OCR Functions ===

def ocr_process_request(processing_request):
    """
    Processes processing request using OCR
    :param processing_request: processing request to work on
    :return: processing request for next stage
    """
    # TODO
    # ocr processing

    # tmp functionality
    img = open(f'~/skola/bakalarka/temp/{processing_request.data.name}', 'w')
    img.write(processing_request.data.content)
    img.close()
    return processing_request


# === Zookeeper Functions ===

def zk_gen_id(zk):
    """
    Generate new id for the worker.
    :param zk: active zookeeper connection
    :return: worker id
    """
    worker_id = uuid.uuid4().hex

    # check for conflicts in zookeeper clients
    while zk.exists(f'/pero/worker/{worker_id}'):
        worker_id = uuid.uuid4().hex
    
    return worker_id


# === Message Broker Functions ===

def mq_process_request(channel, method, properties, body):
    """
    Process message received from message broker channel
    :param channel: channel from which the message is originated
    :param method: message delivery method
    :param properties: additional message properties
    :param body: message body (actual processing request)
    """
    global tag
    processing_request = ProcessingRequest().FromString(body)
    current_stage = processing_request.processing_stages.pop(0)
    logger.debug(f'Request stage: {current_stage}')

    processed_request = ocr_process_request(processing_request)
    
    # acknowledge the request after successfull processing
    if processed_request:
        # TODO
        # send message to output channel
        channel.basic_ack(delivery_tag = method.delivery_tag)
    # reject request on failure and disconnect from queue
    else:
        channel.basic_cancel(consumer_tag = tag)

def mq_connect(broker):
    """
    Connect to message broker
    :param broker: broker to connect to
    :return: AMQP channel to given broker
    :raise: pika.exceptions.ConnectionBlockedTimeout if connection fails to establish
    """
    # connect
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=broker))
    return connection.channel()

def mq_queue_input_setup(channel, queue):
    """
    Setup parameters for input queue on channel
    :param channel: channel to connected broker
    :param queue: queue to get messages from
    :return: worker tag for canceling the connection
    :raise: ValueError if queue is not declared or channel is not initialized
    """
    # set prefetch length for this consumer
    channel.basic_qos(prefetch_count=5)

    # register callback for data processing, return worker tag
    return channel.basic_consume(queue, mq_process_request)


# === Main ===

def main():
    # get commandline arguments
    args = parse_args()

    # TODO
    # connect to zookeeper
    # get configurations from zookeeper

    # get channel to message broker
    for broker in args.broker:
        channel = mq_connect(args.broker)
        if channel:
            break
    
    if not channel:
        logger.error(f'Failed to connect to the message broker {broker}!')
        return 1
    
    # setup queue for data receiving, get worker tag
    try:
        global tag
        tag = mq_queue_input_setup(channel, args.queue)
    except ValueError:
        logger.error(f'Failed to start consuming from queue {args.queue}!')
        channel.close()  # close mq channel
        return 1

    # start processing queue
    exit_code = 0
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        pass
    except pika.exceptions.ChannelClosed:
        logger.error('Channel closed by broker!')
        exit_code = 1
    except Exception:
        # TODO
        # add traceback to error
        logger.error('Unknown error has occured! Exititng!')
        exit_code = 1
    finally:
        # close mq channel
        logger.info('Closing connections.')
        channel.close()

    return exit_code


# Run
if __name__ == '__main__':
    sys.exit(main())
