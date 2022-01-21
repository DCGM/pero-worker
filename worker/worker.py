#!/usr/bin/env python3

# Worker for page processing

import pika  # AMQP protocol library for queues
import logging
import time
import sys
import os  # filesystem
import argparse
import uuid
#import json  # nice logging of configuration objects
import traceback  # logging
import configparser

# load image and data for processing
import cv2
import pickle
import magic
import numpy as np

# pero OCR
from pero_ocr.document_ocr.layout import PageLayout
from pero_ocr.document_ocr.page_parser import PageParser

# protobuf
from message_definitions.message_pb2 import ProcessingRequest, StageLog, Data
from google.protobuf.timestamp_pb2 import Timestamp

# zookeeper
from kazoo.client import KazooClient
from kazoo.handlers.threading import KazooTimeoutError
import kazoo.exceptions as zk_exceptions

# constants
import worker_functions.constants as constants
import worker_functions.zk_functions as zkf


# === Global config ===

# global variables
worker_id = None  # id to identify worker instance
tmp_folder = None  # folder where to store temporary files
config = None  # configuration for the page parser
ocr_path = None  # Path to folder with config and OCR data

# setup logging (required by kazoo)
log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

stderr_handler = logging.StreamHandler()
stderr_handler.setFormatter(log_formatter)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
# TODO - remove debug level
logger.setLevel(logging.DEBUG)
logger.addHandler(stderr_handler)


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
    argparser = argparse.ArgumentParser('Worker for page processing')
    argparser.add_argument(
        '-z', '--zookeeper',
        help='List of zookeeper servers from where configuration will be downloaded. If port is omitted, default zookeeper port is used.',
        nargs='+', # TODO - check if addresses are valid
        default=['127.0.0.1:2181']
    )
    argparser.add_argument(
        '-l', '--zookeeper-list',
        help='File with list of zookeeper servers. One server per line.',
        type=argparse.FileType('r')
    )
    argparser.add_argument(
        '-i', '--id',
        help='Worker id for identification in zookeeper'
    )
    argparser.add_argument(
        '-b', '--broker-servers',
        help='List of message queue broker servers where to get and send processing requests',
        nargs='+', # TODO - check if addresses are valid
        default=['127.0.0.1']
    )
    argparser.add_argument(
        '-q', '--queue',
        help='Queue with messages to process'
    )
    argparser.add_argument(
        '--ocr',
        help='Folder with OCR data and config file in .ini format',
        type=dir_path
    )
    argparser.add_argument(
        '--tmp-folder',
        help='Path to folder where temporary files will be stored',
        type=dir_path
    )
    return argparser.parse_args()

def clean_tmp_files(path):
    """
    Removes temporary files and directories recursively
    :param path: path to file/folder to clean up
    """
    # path is not valid
    if not path or not os.path.exists(path):
        return
    
    # remove temp file
    if os.path.isfile(path):
        os.unlink(path)
        return
    
    # clean files from temp directory
    for file in os.listdir(path):
        clean_tmp_files(path)
    
    # remove temp direcotry
    os.rmdir(path)

def clean_up(zk, channel, tmp):
    """
    Cleans up connections and temporary files before worker stops
    :param zk: zookeeper connection
    :param channel: mq channel
    :tmp: temporary files path
    """
    if zk and zk.connected:
        zk.stop()
        zk.close()
    
    if channel and channel.is_open:
        channel.close()
    
    clean_tmp_files(tmp)

# === OCR Functions ===

def ocr_test_process_request(processing_request, worker_id):
    """
    Test method to check if commuincation is working
    :param processing_request: processing request to work on
    :param worker_id: id of the worker
    :return: processing status
    """
    # Gen log
    log = processing_request.logs.add()
    log.host_id = worker_id
    log.stage = processing_request.processing_stages[0]
    logger.debug(f'Stage: {processing_request.processing_stages[0]}')
    logger.debug(f'Log avaliable for: {log.stage}')
    Timestamp.GetCurrentTime(log.start)

    # Save image
    for result in processing_request.results:
        with open(f'/home/pavel/skola/bakalarka/temp/worker/{result.name}', 'wb') as img:
            img.write(result.content)
            log.status = "OK"
    
    if not log.status:
        log.status = "NOK"

    Timestamp.GetCurrentTime(log.end)
    log.log = "Lorem Ipsum"
    
    return True

def ocr_process_request(processing_request, worker_id, config, ocr_path):
    """
    Process processing request using OCR
    :param processing_request: processing request to work on
    :param worker_id: id of this worker
    :param config: configuration for OCR
    :param ocr_path: path to OCR data
    :return: processing status
    """
    # Gen log
    log = processing_request.logs.add()
    log.host_id = worker_id
    log.stage = processing_request.processing_stages[0]
    logger.debug(f'Stage: {log.stage}')
    Timestamp.GetCurrentTime(log.start)

    # Load data
    img = None
    xml_in = None
    logits_in = None

    for i, data in enumerate(processing_request.results):
        ext = os.path.splitext(data.name)[1]
        datatype = magic.from_buffer(data.content, mime=True)
        logger.debug(f'File: {data.name}, type: {datatype}')
        if datatype.split('/')[0] == 'image':  # recognize image
            img = cv2.imdecode(np.fromstring(processing_request.results[i].content, dtype=np.uint8), 1)
        if ext == '.xml':  # pagexml is missing xml header - type can't be recognized - type = text/plain
            xml_in = processing_request.results[i].content
        if ext == '.logits':  # type = application/octet-stream
            logits_in = processing_request.results[i].content
    
    # Run processing
    page_parser = PageParser(config, ocr_path)  # TODO - init ocr when input queue is selected (usign zk)!
    if xml_in:
        logger.debug('Loading page layout from xml')
        page_layout = PageLayout()
        page_layout.from_pagexml_string(xml_in)
    else:
        logger.debug('Creating new page layout')
        page_layout = PageLayout(id=processing_request.page_uuid, page_size=(img.shape[0], img.shape[1]))
    if logits_in:
        logger.debug('Loading logits')
        page_layout.load_logits(logits_in)
    page_layout = page_parser.process_page(img, page_layout)
    
    # Save output
    xml_out = page_layout.to_pagexml_string()
    try:
        logits_out = page_layout.save_logits_bytes()
    except Exception:
        logits_out = None

    for data in processing_request.results:
        ext = os.path.splitext(data.name)[1]
        if ext == '.xml':
            data.content = xml_out.encode('utf-8')
        if ext == '.logits':
            if logits_out:
                data.content = logits_out
    
    if xml_out and not xml_in:
        xml = processing_request.results.add()
        xml.name = 'page.xml'
        xml.content = xml_out.encode('utf-8')
    
    if logits_out and not logits_in:
        logits = processing_request.results.add()
        logits.name = 'page.logits'
        logits.content = logits_out
    
    # Log end of processing
    Timestamp.GetCurrentTime(log.end)
    # TODO - log content
    #log.log = "Lorem Ipsum"

    return True


# === Zookeeper Functions ===

def zk_gen_id(zk):
    """
    Generate new id for the worker
    :param zk: active zookeeper connection
    :return: worker id
    """
    worker_id = uuid.uuid4().hex

    # check for conflicts in zookeeper clients
    while zk.exists(f'/pero/worker/{worker_id}'):
        worker_id = uuid.uuid4().hex

    return worker_id

def zk_get_mq_servers(zk):
    """
    Returns list of message broker servers obtained from zookeeper
    :param zk: zookeeper connection
    :return: list of message broker servers
    """
    try:
        mq_servers = zk.get_children(constants.WORKER_CONFIG_MQ_SERVERS)[0].decode('utf-8')
    except zk_exceptions.NoNodeError:
        logger.debug('Failed to get MQ server addresses.')
        mq_servers = []
    
    return mq_servers

def zk_set_status(zk, worker_id, status):
    """
    Set worker status in zookeeper
    :param zk: zookeeper connection
    :param worker_id: id of the worker
    :param status: status to set
    :raise: NoNodeError if status node path is not initialized
    :raise: ZookeeperError if server returns non zero value
    """
    zk.set(constants.WORKER_STATUS_TEMPLATE.format(worker_id = worker_id), status.encode('UTF-8'))

def zk_set_queue(zk, worker_id, queue):
    """
    Set queue that is beeing processed by worker in zookeeper
    :param zk: zookeeper connection
    :param worker_id: id of the worker
    :param queue: queue to set
    :raise: NoNodeError if queue node path is not initialized
    :raise: ZookeeperError if server returns non zero error code
    """
    zk.set(constants.WORKER_QUEUE_TEMPLATE.format(worker_id = worker_id), queue.encode('UTF-8'))

def zk_worker_init(zk, worker_id):
    """
    Initializes state of worker in zookeeper
    :param zk: zookeeper connection
    :param worker_id: id of the worker
    :raise: NodeExistsError if worker with this id is already defined in zookeeper
    :raise: ZookeeperError if server returns non zero value
    """
    zk.create(constants.WORKER_STATUS_TEMPLATE.format(worker_id=worker_id), constants.STATUS_STARTING.encode('UTF-8'), makepath=True)
    zk.create(constants.WORKER_QUEUE_TEMPLATE.format(worker_id=worker_id))
    zk.create(constants.WORKER_ENABLED_TEMPLATE.format(worker_id=worker_id), 'True'.encode('UTF-8'))

# === Message Broker Functions ===

def mq_process_request(channel, method, properties, body):
    """
    Callback function for processing messages received from message broker channel
    :param channel: channel from which the message is originated
    :param method: message delivery method
    :param properties: additional message properties
    :param body: message body (actual processing request)
    :raise: ValueError if output queue is not declared on broker
    """
    global worker_id
    global tmp_folder
    global config
    global ocr_path

    processing_request = ProcessingRequest().FromString(body)
    current_stage = processing_request.processing_stages[0]
    logger.info(f'Processing request: {processing_request.uuid}')
    logger.debug(f'Request stage: {current_stage}')

    status = ocr_process_request(processing_request, worker_id, config, ocr_path)
    processing_request.processing_stages.pop(0)
    next_stage = processing_request.processing_stages[0]
    
    # acknowledge the request after successfull processing
    if status:
        # TODO
        # send message to output channel
        channel.basic_publish('', next_stage, processing_request.SerializeToString())
        channel.basic_ack(delivery_tag = method.delivery_tag)
    # reject request on failure and disconnect from queue
    else:
        channel.basic_cancel(consumer_tag = worker_id)

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

def mq_queue_input_setup(channel, queue, worker_id):
    """
    Setup parameters for input queue on channel
    :param channel: channel to connected broker
    :param queue: queue to get messages from
    :param worker_id: id of this worker
    :raise: pika.exceptions.ChannelClosedByBroker if queue is not declared on broker
    """
    # set prefetch length for this consumer
    channel.basic_qos(prefetch_count=5)

    # register callback for data processing
    channel.basic_consume(queue, mq_process_request, consumer_tag = worker_id)


# === Main ===

def main():
    # global vars:
    global worker_id
    global tmp_folder
    global config
    global ocr_path

    # get commandline arguments
    args = parse_args()

    # connect to zookeeper
    zookeeper_servers = zkf.zk_server_list(args.zookeeper)
    if args.zookeeper_list:
        zookeeper_servers = zkf.zk_server_list(args.zookeeper_list)

    zk = KazooClient(hosts=zookeeper_servers)
    
    try:
        zk.start(timeout=20)
    except KazooTimeoutError:
        logger.critical('Zookeeper connection timeout!')
        return 1

    # get configurations from zookeeper
    try:
        # TODO
        #broker_servers = zk_get_mq_servers(zk)
        broker_servers = args.broker_servers
    except KazooTimeoutError as e:
        logger.critical('Zookeeper connection failed!')
        logger.critical(traceback.format_exc())
        clean_up(zk, None, tmp_folder)
        return 1

    # generate worker id
    try:
        worker_id = zk_gen_id(zk) if not args.id else args.id
    except zk_exceptions.ZookeeperError:
        logger.critical('Failed to check worker id in zookeeper!')
        logger.critical(traceback.format_exc())
        clean_up(zk, None, tmp_folder)
        return 1
    logger.info(f'Worker ID: {worker_id}')

    # set zookeeper status
    try:
        zk_worker_init(zk, worker_id)
    except zk_exceptions.NodeExistsError:
        logger.error('Worker configuration found in zookeeper! Recovering node.')
    except Exception:
        logger.critical('Worker registration in zookeeper failed!')
        logger.critical(traceback.format_exc())
        clean_up(zk, None, tmp_folder)
        return 1

    # setup folder for temporary data
    tmp_folder = args.tmp_folder if args.tmp_folder else f'/tmp/pero-worker-{worker_id}'
    if not os.path.isdir(tmp_folder):
        os.makedirs(tmp_folder)
    
    # load OCR config
    config = configparser.ConfigParser()
    config.read(os.path.join(args.ocr, 'config.ini'))
    ocr_path = args.ocr

    # get channel to message broker
    for broker in broker_servers:
        channel = mq_connect(broker)
        if channel:
            break
    
    if not channel:
        logger.error(f'Failed to connect to the message broker {broker}!')
        clean_up(zk, channel, tmp_folder)
        return 1
    
    # setup queue for data receiving
    try:
        mq_queue_input_setup(channel, args.queue, worker_id)
    except pika.exceptions.ChannelClosedByBroker as e:
        logger.critical(f'Failed to start consuming from queue {args.queue}!')
        logger.critical('Channel error: {}'.format(e))
        clean_up(zk, channel, tmp_folder)
        return 1
    
    # set zookeeper status
    try:
        zk_set_queue(zk, worker_id, args.queue)
        zk_set_status(zk, worker_id, constants.STATUS_PROCESSING)
    except zk_exceptions.ZookeeperError as e:
        logger.error('Failed to set status in zookeeper!')
        logger.error('Zookeeper error: {}'.format(e))

    # start processing queue
    exit_code = 0
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        logger.info('Shutdown signal received')
    except pika.exceptions.ChannelClosed:
        logger.critical('Channel closed by broker!')
        logger.critical(traceback.format_exc())
        exit_code = 1
    except Exception:
        logger.critical('Unknown error has occured! Exititng!')
        logger.critical(traceback.format_exc())
        exit_code = 1
    finally:
        try:  # set status in zookeeper
            zk_set_status(zk, worker_id, constants.STATUS_FAILED if exit_code else constants.STATUS_DEAD)
        except Exception as e:
            logger.error('Failed to set status in zookeeper!')
            logger.error('Received error: {}'.format(e))
        logger.info('Closing connections and cleaning up')
        clean_up(zk, channel, tmp_folder)

    return exit_code


# Run
if __name__ == '__main__':
    sys.exit(main())
