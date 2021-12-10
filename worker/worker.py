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


# === Functions ===


def parse_args():
    """
    Parses arguments given on commandline
    :return: namespace with parsed args
    """
    
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
    
    def queue_arg_parser(queue_args):
        """
        Generates queue arguments dict.
        Checks if qeueu arguments are in correct format.
        :param queue_args: Queue args list to check
        :return: queue arguments dict or None if no arguments are supplied
        :raise: ValueError in arguments are not in Key=Value format
        """
        # create queue argumets dict
        if queue_args:
            queue_arguments = {}
            for arg in queue_args:
                n = 0
                for char in arg:
                    if char == '=':
                        n += 1
                if n != 1:
                    raise ValueError('Wrong queue argument format!')
                # add argument to arguments
                queue_arguments[arg.split('=')[0]] = arg.split('=')[-1]
            
            # return generated arguments
            return queue_arguments
        
        # default
        return None
    
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
        default=['127.0.0.1:5672']
    )
    argparser.add_argument(
        '-q', '--queue',
        help='Queue with messages to process.'
    )
    argparser.add_argument(
        '-a', '--queue-args',
        help='Queue declaration arguments. Format is Key=Value, multiple arguments have to be separated by space.',
        nargs='+',
        default=None
    )
    argparser.add_argument(
        '--ocr',
        help='Folder with OCR data and config file in .ini format.',
        type=dir_path
    )
    return argparser.parse_args()


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

def mq_process_request(channel, method, properties, body):
    """
    Process message received from message broker channel
    :param channel: channel from which the message is originated
    :param method: message delivery method
    :param properties: additional message properties
    :param body: message body (actual processing request)
    """
    #processing_request = None
    # TODO
    # process the request
    
    # acknowledge the request after successfull processing
    #channel.basic_ack(delivery_tag = method.delivery_tag)

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

def mq_queue_input_setup(channel, queue, queue_args=None):
    """
    Setup input queue on channel
    :param channel: channel to connected broker
    :param queue: queue to get messages from
    :param queue_args: addititonal queue arguments
    :raise: ValueError if queue cannot be declared
    """
    # create queue
    channel.queue_declare(queue, arguments=queue_args)

    # set prefetch length for this consumer
    channel.basic_qos(prefetch_count=5)

    # register callback for data processing
    channel.basic_consume(queue, mq_process_request)


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
        logger.error('Failed to connect to the message broker!')
        return 1
    
    # setup queue to receive data from
    try:
        mq_queue_input_setup(channel, args.queue, args.queue_args)
    except ValueError:
        logger.error(f'Failed to declare queue {args.queue} with argumets {json.dumps(args.queue_args)}!')
        channel.close()  # close connection to mq
        return 1

    # start processing queue
    exit_code = 0
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        pass
    except Exception:
        # TODO
        # add traceback to error
        logger.error('Unknown error has occured! Exititng!')
        exit_code = 1
    finally:
        # close mq channel
        channel.close()

    return exit_code


# Run
if __name__ == '__main__':
    sys.exit(main())
