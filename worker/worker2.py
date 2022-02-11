#!/usr/bin/env python3

# Worker for page processing

import pika  # AMQP protocol library for queues
import logging
#import time
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
        help='Directory with OCR data and config file in .ini format',
        type=dir_path
    )
    argparser.add_argument(
        '--tmp-directory',
        help='Path to directory where temporary files will be stored',
        type=dir_path
    )
    return argparser.parse_args()

class Worker(object):
    """
    Pero processing worker
    """

    def __init__(self, zookeeper_servers, broker_servers = [], worker_id = None, tmp_directory = None, enabled = True):
        """
        Initialize worker
        :param worker_id: worker identifier
        :param tmp_directory: path to temporary directory
        """

        # worker configuration
        self.worker_id = worker_id
        self.tmp_directory = tmp_directory
        self.zookeeper_servers = zookeeper_servers
        self.broker_servers = broker_servers

        # configuration for the page parser and selected orc
        self.config = None
        self.ocr = None

        # worker connections
        self.channel = None
        self.mq_connection = None
        self.zk = None

        # selected broker server
        self._mq_server = None

        # worker status
        self.status = constants.STATUS_DEAD
        self.queue = None
        self.enabled = enabled

    def clean_tmp_files(self, path = self.tmp_directory):
        """
        Removes temporary files and directories recursively
        :param path: path to file/directory to clean up
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

    def __del__(self):
        """
        Cleans up connections and temporary files before worker stops
        """
        logger.info('Closing connection and cleaning up')
        self.mq_disconnect()
        self.zk_disconnect()
        self.clean_tmp_files()

        # TODO - test if needed - loop should be running in run() function
        # wait for connection to broker to close
        #self.mq_connection.ioloop.start()
    
    def zk_disconnect(self):
        """
        Disconnects from zookeeper
        """
        if self.zk and self.zk.connected:
            self.zk.stop()
            self.zk.close()

    def zk_connect(self):
        """
        Connects worker to the zookeeper
        """

        # skip if connection if active
        if self.zk and self.zk.connected:
            return

        # create new connection
        self.zk = KazooClient(hosts=self.zookeeper_servers)

        try:
            self.zk.start(timeout=20)
        except KazooTimeoutError:
            logger.error('Zookeeper connection timeout!')
            raise
    
    @zk.DataWatch(constants.WORKER_QUEUE_TEMPLATE.format(worker_id=self.worker_id))
    def switch_queue(self, data, status):
        """
        Zookeeper callback reacting on notification to switch queue
        :param data: new queue name as byte string
        :param status: status of the zookeeper queue node
        """
        queue = data.decode('utf-8')
        if queue != self.queue:
            self.stop_processing()
            self.update_queue(queue)
            self.start_processing()
    
    @zk.DataWatch(constants.WORKER_ENABLED_TEMPLATE.format(worker_id=self.worker_id))
    def shutdown(self, data, status):
        """
        Shutdown the worker if set to disabled state.
        :param async_object: zookeeper object passed into callback
        """
        enabled = data.decode('utf-8').lower()
        if enabled != 'true':
            self.mq_disconnect()
    
    def gen_id(self):
        """
        Generate new id for the worker
        :raise: ZookeeperError if zookeeper returns non zero error code
        """
        self.worker_id = uuid.uuid4().hex

        # check for conflicts in zookeeper clients
        while self.zk.exists(
            constants.WORKER_STATUS_ID_TEMPLATE.format(
            worker_id = self.worker_id
        )):
            self.worker_id = uuid.uuid4().hex
    
    def get_mq_servers(self):
        """
        Updates list of message broker servers
        :raise: ZookeeperError if zookeeper returns non zero error code
        """
        try:
            self.broker_servers = self.zk.get_children(
                constants.WORKER_CONFIG_MQ_SERVERS)[0].decode('utf-8')
        except zk_exceptions.NoNodeError:
            logger.error('Failed to obtain MQ server list')
    
    def update_status(self, status):
        """
        Updates worker status
        :param status: new status
        :raise: NoNodeError if status node path is not initialized
        :raise: ZookeeperError if server returns non zero value
        """
        self.status = status
        self.zk.set(constants.WORKER_STATUS_TEMPLATE.format(
            worker_id = self.worker_id),
            self.status.encode('utf-8')
        )

    def update_queue(self, queue):
        """
        Updates processed queue
        :param queue: new queue to process
        :raise: NoNodeError if status node path is not initialized
        :raise: ZookeeperError if server returns non zero value
        """
        self.queue = queue
        self.zk.set(constants.WORKER_QUEUE_TEMPLATE.format(worker_id = worker_id), self.queue.encode('utf-8'))
    
    def init_worker_status(self):
        """
        Initializes state of worker in zookeeper
        :raise: NodeExistsError if worker with this id is already defined in zookeeper
        :raise: ZookeeperError if server returns non zero value
        """
        # set worker id
        self.zk.create(
            constants.WORKER_STATUS_TEMPLATE.format(
                worker_id = self.worker_id
            ),
            self.status.encode('utf-8'),
            makepath=True
        )
        # set worker queue
        self.zk.create(constants.WORKER_QUEUE_TEMPLATE.format(
            worker_id = self.worker_id
        ))
        #set if worker is enabled or disabled
        self.zk.create(constants.WORKER_ENABLED_TEMPLATE.format(
            worker_id = self.worker_id
        ), 'true'.encode('utf-8'))
    
    def recover_status(self):
        """
        Recovers worker to previous state after reconnecting to zookeeper
        :raise: ZookeeperError if server returns non zero value
        """
        update_status(constants.STATUS_STARTING)
        self.queue = self.zk.get(constants.WORKER_QUEUE_TEMPLATE.format(
            worker_id = self.worker_id
        )).decode('utf-8')
        self.enabled = self.zk.get(constatns.WORKER_ENABLED_TEMPLATE.format(
            worker_id = self.worker_id
        )).decode('utf-8')
    
    def create_tmp_dir(self):
        """
        Creates tmp directory
        """
        if not self.tmp_directory:
            return
        
        if os.path.isdir(self.tmp_directory):
            return
        
        os.makedirs(self.tmp_directory)
    
    def mq_disconnect(self):
        """
        Stops connection to message broker
        """
        if self.channel and self.channel.is_open:
            self.stop_processing()
            self.channel.close()
        
        if self.mq_connection and self.mq_connection.is_open:
            self.mq_connection.close()
    
    def mq_connect(self):
        """
        Connect to message broker
        """
        # TODO - try all servers!
        self._mq_server = self.broker_servers[0]
        self.mq_connection = pika.SelectConnection(
            parameters = pika.ConnectionParameters(
                host=self._mq_server[0],
                port=self._mq_server[1]
            ),
            on_open_callback=self.mq_channel_create,
            on_open_error_callback=self.mq_connection_open_error,
            on_close_callback=self.mq_connection_close
        )
    
    def mq_connection_open_error(self, connection):
        """
        Set worker status to failed - connection to broker failed to open.
        :param connection: broker connection - same as self.mq_connection
        """
        logger.error('Connection to message broker failed!')
        self.update_status(constants.STATUS_FAILED)
    
    def mq_connection_close(self, connection, reason):
        """
        Set worker status based on connection.
        :param connection: broker connection - same as self.mq_connection
        :param reason: reason why was connection closed
        """
        logger.warning('Connection to message broker closed, reason: {}'.format(reason))
        self.update_status(constants.STATUS_DEAD)  # TODO - report status

    def mq_channel_create(self, connection):
        """
        Create broker channel
        :param connection: broker connection - same as self.mq_connection
        """
        self.mq_connection.channel(on_open_callback=self.mq_channel_setup)

    def mq_channel_setup(self, channel):
        """
        Setup parameters for message broker channel
        :param channel: Created channel object
        """
        self.channel = channel

        # set number of prefetched messages
        self.channel.basic_qos(prefetch_count=5)
    
    def mq_queue_setup(self):
        """
        Setup parameters for input queue
        """
        # register callback for data processing
        channel.basic_consume(
            self.queue,
            self.process_request,
            consumer_tag = self.worker_id
        )
    
    def mq_process_request(self, channel, method, properties, body):
        """
        Callback function for processing messages received from message broker channel
        :param channel: channel from which the message is originated
        :param method: message delivery method
        :param properties: additional message properties
        :param body: message body (actual processing request)
        :raise: ValueError if output queue is not declared on broker
        """
        processing_request = ProcessingRequest().FromString(body)
        current_stage = processing_request.processing_stages[0]

        logger.info(f'Processing request: {processing_request.uuid}')
        logger.debug(f'Request stage: {current_stage}')
        try:
            self.ocr_process_request(processing_request)
        except RuntimeError as e:
            # Processing failed - send message to output queue, no further processing is possible
            logger.error('Failed to process request! Received error: {error}'.format(error=e))
            output_stage = processing_request.processing_stages[-1]
            channel.basic_publish('', output_stage, processing_request.SerializeToString())
        except Exception:
            logger.error('Failed to process request {request_id} using stage {stage}!'.format(
                request_id = processing_request.uuid,
                stage = current_stage
            ))
            logger.error(traceback.format_exc())
            channel.basic_nack(delivery_tag = method.delivery_tag)
        
        processing_request.processing_stages.pop(0)
        next_stage = processing_request.processing_stages[0]
        
        # send request to output queue and
        # acknowledge the request after successfull processing
        # TODO
        # add validation if message was received by mq
        channel.basic_publish('', next_stage, processing_request.SerializeToString())
        channel.basic_ack(delivery_tag = method.delivery_tag)

    def ocr_process_request(self, processing_request):
        """
        Process processing request using OCR
        :param processing_request: processing request to work on
        :return: processing status
        """
        # TODO
        # add logger output to message log
        # gen log
        log = processing_request.logs.add()
        log.host_id = self.worker_id
        log.stage = processing_request.processing_stages[0]
        Timestamp.GetCurrentTime(log.start)

        # load data
        img = None
        xml_in = None
        logits_in = None

        for i, data in enumerate(processing_request.results):
            ext = os.path.splitext(data.name)[1]
            datatype = magic.from_buffer(data.content, mime=True)
            logger.debug(f'File: {data.name}, type: {datatype}')
            if datatype.split('/')[0] == 'image':  # recognize image
                img = cv2.imdecode(np.fromstring(processing_request.results[i].content, dtype=np.uint8), 1)
            if ext == 'xml':  # pagexml is missing xml header - type can't be recognized - type = text/plain
                xml_in = processing_request.results[i].content.decode('utf-8')
            if ext == 'logits':  # type = application/octet-stream
                logits_in = processing_request.results[i].content
        
        # run processing
        try:
            page_parser = PageParser(self.config, self.ocr)  # TODO - move pageparser to load config
            if xml_in:
                page_layout = PageLayout().from_pagexml_string(xml_in)
            else:
                page_layout = PageLayout(id=processing_request.page_uuid, page_size=(img.shape[0], img.shape[1]))
            if logits_in:
                page_layout.load_logits(logits_in)
            page_layout = page_parser.process_page(img, page_layout)
        except Exception as e:
            # processing failed
            log.log += '{error}\n'.format(error = e)
            log.log += traceback.format_exc()
            Timestamp.GetCurrentTime(log.end)
            raise RuntimeError('{error}'.format(error = e))
        
        # save output
        xml_out = page_layout.to_pagexml_string()
        logits_out = page_layout.save_logits_bytes()

        for data in processing_request.results:
            ext = os.path.splitext(data.name)[1]
            if ext == 'xml':
                data.content = xml_out.encode('utf-8')
            if ext == 'logits':
                data.content = logits_out
        
        if xml_out and not xml_in:
            xml = processing_request.results.add()
            xml.name = 'page.xml'
            xml.content = xml_out.encode('utf-8')
        
        if logits_out and not logits_in:
            logits = processing_request.results.add()
            logits.name = 'page.logits'
            logits.content = logits_out
        
        # log end of processing
        Timestamp.GetCurrentTime(log.end)
        # TODO - log content
        #log.log = "Lorem Ipsum"

    def ocr_config_load(self, config):
        """
        Loads ocr config
        :param config: config name
        """
        # TODO
        pass

    def ocr_load(self, location):
        """
        Loads ocr data from given location
        :param location: location of the data
        """
        # TODO
        pass
    
    def stop_processing(self):
        """
        Stops processing of current queue.
        Runs only if channel is open.
        """
        if self.channel.is_open:
            try:
                self.channel.cancel(self.worker_id)
            except ValueError:
                # worker was not processing
                pass
    
    def run(self):
        """
        Start the worker
        """
        # connect to the zookeeper
        try:
            self.zk_connect()
        except KazooTimeoutError:
            logger.critical('Failed to connect to zookeeper!')
            logger.critical('Initialization aboarded!')
            return 1
        
        # generate id
        if not self.worker_id:
            self.gen_id()

        # initialize worker status and sync with zookeeper
        try:            
            self.init_worker_status()
        except zk_exceptions.NodeExistsError:
            logger.info('Recovering failed node!')
            try:
                self.recover_status()
            except:
                logger.critical('Setting up zookeeper status failed!')
                return 1
        
        # setup tmp directory
        if not self.tmp_directory:
            self.tmp_directory = f'/tmp/pero-worker-{self.worker_id}'
        self.create_tmp_dir()

        # zookeeper callbacks
        # callbacks are added on object creation - this section is empty

        # load config if recovering worker
        if self.queue:
            pass
            # TODO
            # load config

            # TODO
            # Load OCR
        
        # setup MQ connection and channel
        try:
            self.mq_connect()
        except Exception:
            logger.critical('Failed to connect to message broker servers!')
            logger.critical(traceback.format_exc())
            return 1

        # start processing queue
        try:
            # uses this thread for managing the MQ connection
            # until the connection is closed
            self.mq_connection.ioloop.start()
        except Exception:
            logger.critical('Connection to message broker failed!')
            logger.critical(traceback.format_exc())
            return 1
        
        return 0

def main():
    args = parse_args()

    zk_servers = zkf.zk_server_list(args.zookeeper_servers)

    worker = Worker(
        zookeeper_servers=zk_servers,
        broker_servers=args.broker_servers,
        tmp_directory=args.tmp_directory if args.tmp_directory else None
    )

    # TODO
    # remove when config loading is done
    worker.config = configparser.ConfigParser()
    worker.config.read(os.path.join(args.ocr, 'config.ini'))
    worker.ocr = args.ocr

    return worker.run()

# run the module
if __name__ == "__main__":
    sys.exit(main())