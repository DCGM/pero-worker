#!/usr/bin/env python3

# Worker for page processing

import pika  # AMQP protocol library for queues
import ssl
import logging
import time
import sys
import os  # filesystem
import argparse
import uuid
import json  # configuration loading
import traceback  # logging
import configparser
import threading
import datetime
import zipfile
import tarfile
from io import StringIO  # logging to message

# dummy processing
import random

# load image and data for processing
import cv2
import pickle
import magic
import numpy as np
import torch

# pero OCR
from pero_ocr.document_ocr.layout import PageLayout
from pero_ocr.document_ocr.page_parser import PageParser

# protobuf
from message_definitions.message_pb2 import ProcessingRequest, StageLog, Data
from google.protobuf.timestamp_pb2 import Timestamp

# zookeeper
from kazoo.client import KazooClient
from kazoo.handlers.threading import KazooTimeoutError
from kazoo.protocol.states import KazooState
import kazoo.exceptions

# constants
import worker_functions.constants as constants
import worker_functions.connection_aux_functions as cf
from worker_functions.sftp_client import SFTP_Client


# === Global config ===

# setup logging (required by kazoo)
log_formatter = logging.Formatter('%(asctime)s WORKER %(levelname)s %(message)s')

stderr_handler = logging.StreamHandler()
stderr_handler.setFormatter(log_formatter)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
# TODO - remove debug level
logger.setLevel(logging.DEBUG)
logger.addHandler(stderr_handler)
logger.propagate = False


# === Functions ===

def dir_path(path):
    """
    Check if path is directory path
    :param path: path to directory
    :return: path if path is directory path
    :raise: ArgumentTypeError if path is not directory path
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
        nargs='+',
        default=['127.0.0.1:2181']
    )
    argparser.add_argument(
        '-l', '--zookeeper-list',
        help='File with list of zookeeper servers. One server per line.',
        type=argparse.FileType('r')
    )
    argparser.add_argument(
        '-i', '--id',
        help='Worker id for identification in zookeeper',
        default=None
    )
    argparser.add_argument(
        '-b', '--broker-servers',
        help='List of message queue broker servers where to get and send processing requests',
        nargs='+',
        default=[]
    )
    argparser.add_argument(
        '-f', '--ftp-servers',
        help='List of ftp servers.',
        nargs='+',
        default=[]
    )
    argparser.add_argument(
        '--tmp-directory',
        help='Path to directory where temporary files will be stored',
        type=dir_path
    )
    argparser.add_argument(
        '-u', '--username',
        help='Username for authentication on server.',
        default=None
    )
    argparser.add_argument(
        '-p', '--password',
        help='Password for user authentication.',
        default=None
    )
    argparser.add_argument(
        '-e', '--ca-cert',
        help='CA Certificate for SSL/TLS connection verification.',
        default=None
    )
    return argparser.parse_args()

class Worker(object):
    """
    Pero processing worker
    """

    def __init__(
        self,
        zookeeper_servers,
        mq_servers = [],
        ftp_servers = [],
        username = None,
        password = None,
        ca_cert = None,
        worker_id = None,
        tmp_directory = None,
        logger = logging.getLogger(__name__)
    ):
        """
        Initialize worker
        :param zookeeper_servers: list of zookeeper servers
        :param mq_servers: list of message broker servers
        :param ftp_servers: list of ftp servers
        :param username: username for authentication on server
        :param password: password for user authentication
        :param ca_cert: path to CA certificate to verify SSL/TLS connection
        :param worker_id: worker identifier
        :param tmp_directory: path to temporary directory
        :param logger: logger instance to use for logging
        """

        # worker configuration
        self.worker_id = worker_id
        self.tmp_directory = tmp_directory
        self.zookeeper_servers = zookeeper_servers
        self.mq_servers = mq_servers
        self.ftp_servers = ftp_servers

        # locks for config updates
        self.mq_server_lock = threading.Lock()  # guards mq_servers list and mq_connection_retry count
        self.ftp_servers_lock = threading.Lock()

        # number of mq connection retry
        self.mq_connection_retry = 0

        # configuration for the page parser and selected orc
        self.config = None
        self.config_version = None
        self.ocr_path = None
        self.page_parser = None

        # server login credentials
        self.username = username
        self.password = password
        self.ca_cert = ca_cert

        if username:
            # authentication credentials (SASL) for zookeeper
            self.zk_auth = {
                'mechanism': 'DIGEST-MD5',
                'username': str(username),
                'password': str(password)
            }
            # authentication for RabbitMQ
            self.mq_auth = pika.credentials.PlainCredentials(str(username), str(password))
        else:
            self.zk_auth = None
            self.mq_auth = pika.connection.ConnectionParameters.DEFAULT_CREDENTIALS
        
        # SSL/TLS settings for RabbitMQ
        if ca_cert:
            context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
            context.verify_mode = ssl.CERT_REQUIRED
            context.load_verify_locations(ca_cert)
            self.ssl_options = pika.SSLOptions(context)
        else:
            self.ssl_options = pika.connection.ConnectionParameters.DEFAULT_SSL_OPTIONS

        # worker connections
        self.mq_channel = None
        self.mq_connection = None
        self.zk = None

        # selected broker server
        self.mq_server = None

        # worker status
        self.status = constants.STATUS_STARTING
        self.queue = ''
        self.queue_config_version = -1
        self.enabled = True

        # processing lock
        self.processing_lock = threading.Lock()
        self.switch_queue_lock = threading.Lock()
        self.status_lock = threading.Lock()

        # queue statistics
        self.queue_stats_processed_messages = 0  # number of processed messages
        self.queue_stats_total_processing_time = 0  # time spend on processing messages
        self.queue_stats_lock = None  # zookeeper lock for queue stats update

        # logging
        self.logger = logger

        # logging to message log
        self.message_logger = logging.getLogger()
        self.message_logger.setLevel(logging.DEBUG)

    def clean_tmp_files(self, path=None):
        """
        Removes temporary files and directories recursively
        :param path: path to file/directory to clean up
        """
        # default path = temp directory
        if not path:
            path = self.tmp_directory
        
        # if temp directory is not set - nothing to clean
        if not path:
            return
        
        # path is not valid
        if not os.path.exists(path):
            return
        
        # remove temp file
        if os.path.isfile(path):
            os.unlink(path)
            return
        
        # clean files from temp directory
        for file_name in os.listdir(path):
            self.clean_tmp_files(os.path.join(path, file_name))
        
        # remove temp directory
        os.rmdir(path)

    def __del__(self):
        """
        Cleans up connections and temporary files before worker stops
        """
        self.logger.info('Closing connection and cleaning up')
        #self.update_status(constants.STATUS_DEAD)
        self.mq_disconnect()
        self.zk_disconnect()
        self.clean_tmp_files()
        self.logger.debug('Cleanup complete!')
    
    def sftp_connect(self):
        """
        Creates new SFTP connection
        """
        self.ftp_servers_lock.acquire()
        sftp = SFTP_Client(self.ftp_servers, self.username, self.password, self.logger)
        self.ftp_servers_lock.release()
        sftp.sftp_connect()
        return sftp
    
    def zk_disconnect(self):
        """
        Disconnects from zookeeper
        """
        if self.zk and self.zk.connected:
            try:
                self.zk.stop()
                self.zk.close()
            except kazoo.exceptions.KazooException as e:
                self.logger.error('Failed to close zookeeper connection!')
                self.logger.error('Received error: {}'.format(traceback.format_exc()))

    def zk_connect(self):
        """
        Connects worker to the zookeeper
        """
        # skip if connection is active
        if self.zk and self.zk.connected:
            return

        # create new connection
        self.zk = KazooClient(
            hosts=self.zookeeper_servers,
            ca=self.ca_cert,
            use_ssl=True if self.ca_cert else False,
            sasl_options=self.zk_auth
        )

        try:
            self.zk.start(timeout=20)
        except KazooTimeoutError:
            self.logger.error('Zookeeper connection timeout!')
            raise
    
    def zk_callback_switch_queue(self, data, status, *args):
        """
        Zookeeper callback reacting on notification to switch queue
        :param data: new queue name as byte string
        :param status: status of the zookeeper queue node
        :param args: additional arguments (like event)
        """
        # get new queue
        queue = data.decode('utf-8')

        # prevent multiple queue switching at same time
        self.switch_queue_lock.acquire()

        # check if queue wasn't switched again during waiting
        if self.queue_config_version >= status.version:
            self.switch_queue_lock.release()
            return
        
        self.queue_config_version = status.version
        
        # check if there is something to do
        if queue == self.queue:
            self.switch_queue_lock.release()
            return
        
        # wait until current request is processed
        # and block processing of all new requests from this queue
        self.processing_lock.acquire()

        # stop processing of cureent queue
        self.stop_processing()
        
        # unblock processing for channel to close
        self.switch_queue_lock.release()
        self.processing_lock.release()
        if not self.mq_channel_closed(timeout=6):
            self.logger.warning('Switching queue without closing channel!')

        # and report statistics
        self.update_status(constants.STATUS_RECONFIGURING)
        self.update_queue_statistics()

        # reset statistics in the case that update failed
        self.queue_stats_processed_messages = self.queue_stats_total_processing_time = 0

        # switch queue
        self.logger.info('Switching queue to {}'.format(queue))
        self.queue = queue

        # get lock for given queue statistic from the zookeeper
        try:
            self.queue_stats_lock = self.zk.Lock(
                constants.QUEUE_STATS_AVG_MSG_TIME_LOCK_TEMPLATE.format(queue_name = self.queue),
                identifier=self.worker_id
            )
        except Exception:  # kazoo.exceptions.ZookeeperError
            self.update_status(constants.STATUS_FAILED)
            self.logger.error(
                'Failed to switch queue to {}, could not get lock for average message time statistics!'
                .format(self.queue)
            )
            self.logger.error('Received error:\n{}'.format(traceback.format_exc()))
            return

        # load OCR data and configuration
        try:
            self.ocr_load(self.queue)
        except Exception:
            self.update_status(constants.STATUS_FAILED)
            self.logger.error(
                'Failed to switch queue to {}, could not get configuration for given processing stage!'
                .format(self.queue)
            )
            self.logger.error('Received error:\n{}'.format(traceback.format_exc()))
        else:
            self.logger.debug('Queue config version: {}'.format(self.config_version))
            self.update_status(constants.STATUS_IDLE)
            # start processing of new queue
            self.mq_channel_create(self.mq_connection)
    
    def zk_callback_shutdown(self, data, status, *args):
        """
        Shutdown the worker if set to disabled state.
        :param data: shutdown notification
        :param status: new zookeeper node status
        :param args: additional arguments (like event)
        """
        enabled = data.decode('utf-8').lower()
        if enabled != 'true':
            self.logger.info('Shutdown signal received!')
            self.enabled = False
            self.update_status(constants.STATUS_DEAD)
            self.mq_disconnect()
    
    def zk_callback_update_mq_server_list(self, servers):
        """
        Updates list of mq broker servers
        :param servers: list of servers
        """
        self.mq_server_lock.acquire()
        self.mq_servers = cf.server_list(servers)
        self.mq_connection_retry = 0
        self.mq_server_lock.release()
    
    def zk_callback_update_ftp_server_list(self, servers):
        """
        Updates list of ftp servers
        :param servers: list of servers
        """
        self.ftp_servers_lock.acquire()
        self.ftp_servers = cf.server_list(servers)
        self.ftp_servers_lock.release()
    
    def update_queue_statistics(self):
        """
        Updates statistics of queue in zookeeper
        """
        if not self.queue_stats_processed_messages:
            return
        
        # get averate processing time for current queue
        avg_msg_time = self.queue_stats_total_processing_time / self.queue_stats_processed_messages
        self.logger.debug('Updating queue statistics for queue {}'.format(self.queue))

        # Zookeeper deadlock workaround
        # retries to acquire the queue_stats_lock when first acquisition fails
        retry = 2
        while True:
            try:
                self.queue_stats_lock.acquire(timeout=20)
            except kazoo.exceptions.LockTimeout:
                self.logger.error('Failed to acquire lock for time statistics!')
                retry -= 1
                if retry <= 0:
                    self.logger.error('Failed to update statistics for queue {}!'.format(self.queue))
                    return
                else:
                    self.logger.info('Trying to acquire lock again')
            else:
                break
        
        try:
            # get queue average processing time from zookeeper
            zk_msg_time = self.zk.get(constants.QUEUE_STATS_AVG_MSG_TIME_TEMPLATE.format(
                queue_name = self.queue
            ))[0].decode('utf-8')

            # calculate new queue average processing time
            if zk_msg_time:
                zk_msg_time = float.fromhex(zk_msg_time)
                avg_msg_time = avg_msg_time + zk_msg_time / 2
            
            # update queue statistics in zookeeper
            self.zk.set(
                path=constants.QUEUE_STATS_AVG_MSG_TIME_TEMPLATE.format(queue_name = self.queue),
                value=float(avg_msg_time).hex().encode('utf-8')
            )
        except kazoo.exceptions.ZookeeperError:
            self.logger.error(
                'Failed to update average processing time for queue {} due to zookeeper error!'
                .format(self.queue)
            )
            self.logger.error('Received error:\n{}'.format(traceback.format_exc()))
        except ValueError:
            self.logger.error(
                'Failed to update average processing time for queue {} due to wrong number format in zookeeper!'
                .format(self.queue)
            )
        except Exception:
            self.logger.error(
                'Failed to update average processing time for queue {} due to unknown error!'
                .format(self.queue)
            )
            self.logger.error('Received error:\n{}'.format(traceback.format_exc()))
        else:
            # reset counters
            self.queue_stats_total_processing_time = self.queue_stats_processed_messages = 0
            self.logger.debug('Statistics updated!')
        finally:
            self.queue_stats_lock.release()
    
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
    
    def update_status(self, status):
        """
        Updates worker status
        :param status: new status
        :raise: NoNodeError if status node path is not initialized
        :raise: ZookeeperError if server returns non zero value
        """
        self.status_lock.acquire()
        self.status = status

        if self.zk.state != KazooState.CONNECTED:
            self.logger.error('Failed to update status in zookeeper. Zookeeper connection lost!')
            self.status_lock.release()
            return
        
        # TODO
        # catch errors
        self.zk.set(constants.WORKER_STATUS_TEMPLATE.format(
            worker_id = self.worker_id),
            self.status.encode('utf-8')
        )
        self.status_lock.release()
    
    def init_worker_status(self):
        """
        Initializes state of worker in zookeeper
        :raise: NodeExistsError if worker with this id is already defined in zookeeper
        :raise: ZookeeperError if server returns non zero value
        """
        # TODO
        # catch errors
        # change create to ensure path

        # set worker status
        self.zk.create(
            constants.WORKER_STATUS_TEMPLATE.format(
                worker_id = self.worker_id
            ),
            self.status.encode('utf-8'),
            makepath=True
        )
        # initialize worker queue path
        self.zk.create(constants.WORKER_QUEUE_TEMPLATE.format(
            worker_id = self.worker_id
        ))
        # set worker to enabled state
        self.zk.create(constants.WORKER_ENABLED_TEMPLATE.format(
            worker_id = self.worker_id
        ), 'true'.encode('utf-8'))
        # initialize worker unlock time path
        self.zk.create(constants.WORKER_UNLOCK_TIME.format(
            worker_id = self.worker_id
        ))
    
    def create_tmp_dir(self):
        """
        Creates tmp directory
        """
        if not self.tmp_directory:
            return
        
        if os.path.isdir(self.tmp_directory):
            return
        
        os.makedirs(self.tmp_directory)
    
    def mq_channel_closed(self, timeout = 3):
        """
        Wait for channel to close
        :param timeout: wait maximum of (timeout * 10) second for channel to close
        :return: True if channel closes in time, False otherwise
        :precondition: channel close() method is called
        """
        if not self.mq_channel:
            return True
        
        if not self.mq_channel.is_closed:
            self.logger.info('Waiting for channel to close')

        while not self.mq_channel.is_closed and timeout:
            time.sleep(10)
            timeout -= 1
    
        if not self.mq_channel.is_closed and not timeout:
            self.logger.error('Channel close timeout exceeded!')
            return False
        
        return True
    
    def mq_disconnect(self):
        """
        Stops connection to message broker
        """
        if self.mq_channel and self.mq_channel.is_open:
            self.stop_processing()
        
        if self.mq_connection and self.mq_connection.is_open:
            if not self.mq_channel_closed(timeout=3):
                self.logger.warning('Closing connection without channel closed!')
            try:
                self.mq_connection.close()
            except pika.exceptions.ConnectionWrongStateError:
                # connection failed after channel was disconnected
                pass
            self.mq_connection.ioloop.stop()

    def mq_connect(self):
        """
        Connect to message broker
        """
        # prevent mq server list update during connecting
        self.mq_server_lock.acquire()

        if not self.mq_servers:
            self.logger.error('No MQ servers available!')
            self.update_status(constants.STATUS_FAILED)
            self.mq_server_lock.release()
            return
        
        if self.mq_connection_retry == len(self.mq_servers):
            self.logger.error('Failed to connect to any MQ servers!')
            self.update_status(constants.STATUS_FAILED)
            self.mq_server_lock.release()
            return

        # select server to connect to
        if not self.mq_server:
            self.mq_server = self.mq_servers[0]
        elif self.mq_server == self.mq_servers[-1]:
            self.mq_server = self.mq_servers[0]
        else:
            for i in range(0, len(self.mq_servers)):
                if self.mq_server == self.mq_servers[i]:
                    self.mq_server = self.mq_servers[i + 1]
                    break
        
        self.mq_connection_retry += 1

        self.mq_server_lock.release()
        
        # connect to selected mq server
        self.logger.info('Connecting to MQ server {}'.format(cf.host_port_to_string(self.mq_server)))
        self.mq_connection = pika.SelectConnection(
            pika.ConnectionParameters(
                host=self.mq_server['host'],
                port=self.mq_server['port'] if self.mq_server['port'] else pika.ConnectionParameters.DEFAULT_PORT,
                credentials=self.mq_auth,
                ssl_options=self.ssl_options,
                virtual_host=constants.MQ_VHOST,
                heartbeat=600,
                connection_attempts=10
            ),
            on_open_callback=self.mq_connection_open_ok,
            on_open_error_callback=self.mq_connection_open_error,
            on_close_callback=self.mq_connection_close
        )
    
    def mq_connection_open_ok(self, connection):
        """
        Callback for reporting successfully established connection to mq broker.
        :param connection: message broker connection - same as self.mq_connection
        """
        self.logger.info('Connection to MQ establisted successfully')
        self.mq_server_lock.acquire()
        self.mq_connection_retry = 0
        self.mq_server_lock.release()
    
    def mq_connection_open_error(self, connection, error):
        """
        Set worker status to failed - connection to broker failed to open.
        :param connection: broker connection - same as self.mq_connection
        :param error: exception generated on close
        """
        self.logger.error('Connection to MQ failed to open! Received error: {}'.format(error))
        self.mq_connect()
    
    def mq_connection_close(self, connection, reason):
        """
        Set worker status based on connection.
        :param connection: broker connection - same as self.mq_connection
        :param reason: reason why was connection closed
        """
        self.logger.warning('Connection to MQ closed, reason: {}'.format(reason))
        if not self.enabled:
            self.update_status(constants.STATUS_DEAD)
        else:
            self.mq_connect()

    def mq_channel_create(self, connection):
        """
        Create broker channel
        :param connection: broker connection - same as self.mq_connection
        """
        self.logger.info('Setting up MQ channel')
        self.mq_connection.channel(on_open_callback=self.mq_channel_setup)

    def mq_channel_setup(self, channel):
        """
        Setup parameters for message broker channel
        :param channel: Created channel object
        """
        # set number of prefetched messages
        channel.basic_qos(prefetch_count=5)
        
        self.mq_channel = channel

        self.logger.info('MQ Channel setup complete')

        if self.queue:
            self.mq_queue_setup()
    
    def mq_queue_setup(self):
        """
        Setup parameters for input queue
        """
        if not self.mq_channel or not self.mq_channel.is_open:
            self.logger.error('Queue setup failed, mq channel is not open!')
            return

        # register callback for data processing
        self.mq_channel.basic_consume(
            self.queue,
            self.mq_process_request,
            consumer_tag = self.worker_id,
            auto_ack=False
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
        self.processing_lock.acquire()
        self.update_status(constants.STATUS_PROCESSING)

        # add try/catch for parsing request (can be something else than protobuf)
        try:
            processing_request = ProcessingRequest().FromString(body)
        except Exception as e:
            self.logger.error('Failed to parse received request!')
            self.logger.error('Received error:\n{}'.format(traceback.format_exc()))
            channel.basic_nack(delivery_tag = method.delivery_tag, requeue = True)
        
        current_stage = processing_request.processing_stages[0]

        self.logger.info(f'Processing request: {processing_request.uuid}')
        self.logger.debug(f'Request stage: {current_stage}')

        # log start of processing
        start_time = datetime.datetime.now(datetime.timezone.utc)
        log = processing_request.logs.add()
        log.host_id = self.worker_id
        log.stage = processing_request.processing_stages[0]
        log.version = self.config_version
        log.status = 'Failed'
        Timestamp.FromDatetime(log.start, start_time)

        # configure message logging
        log_buffer = StringIO()
        buffer_handler = logging.StreamHandler(log_buffer)
        self.message_logger.addHandler(buffer_handler)

        try:
            dummy = self.config['WORKER']['DUMMY']
        except KeyError:
            dummy = False

        try:
            if dummy:
                self.dummy_process_request(processing_request)
            else:
                self.ocr_process_request(processing_request)
        except Exception as e:
            self.logger.error('Failed to process request {request_id} using stage {stage}!'.format(
                request_id = processing_request.uuid,
                stage = current_stage
            ))
            self.logger.error('Received error:\n{}'.format(traceback.format_exc()))

            # Timestamp
            end_time = datetime.datetime.now(datetime.timezone.utc)
            Timestamp.FromDatetime(log.end, end_time)

            # Processing failed - send message to output queue, no further processing is possible
            next_stage = processing_request.processing_stages[-1]
        else:
            log.status = 'OK'

            # Timestamp
            end_time = datetime.datetime.now(datetime.timezone.utc)
            Timestamp.FromDatetime(log.end, end_time)

            # worker statistics update
            spend_time = (end_time - start_time).total_seconds()
            self.queue_stats_total_processing_time += spend_time
            self.queue_stats_processed_messages += 1

            # select next processing stage
            processing_request.processing_stages.pop(0)
            next_stage = processing_request.processing_stages[0]
        finally:
            # add buffered log to the message
            log.log = log_buffer.getvalue()

            # remove handler to prevent memory leak (buffer is discarded and never used again)
            self.message_logger.removeHandler(buffer_handler)

            # send request to output queue and
            # acknowledge the request after successful processing
            while True:
                try:
                    channel.basic_publish('', next_stage, processing_request.SerializeToString(),
                        properties=pika.BasicProperties(delivery_mode=2, priority=processing_request.priority),  # persistent delivery mode
                        mandatory=True  # raise exception if message is rejected
                    )
                except pika.exceptions.UnroutableError as e:
                    self.logger.error('Failed to deliver processing request {request_id} to queue {queue}!'.format(
                        request_id = processing_request.uuid,
                        queue = next_stage
                    ))
                    self.logger.error('Received error: {}'.format(e))

                    if next_stage == processing_request.processing_stages[-1]:
                        self.logger.error('Request was pushed back to queue {}!'.format(log.stage))
                        channel.basic_nack(delivery_tag = method.delivery_tag, requeue = True)
                    else:
                        self.logger.warning('Sending request to output queue instead!')
                        log.log += 'Delivery to queue {} failed!\n'.format(next_stage)
                        log.status = 'Failed'
                        next_stage = processing_request.processing_stages[-1]
                        continue
                else:
                    self.logger.debug('Acknowledging request {request_id} from queue {queue}'.format(
                        request_id = processing_request.uuid,
                        queue = log.stage
                    ))
                    channel.basic_ack(delivery_tag = method.delivery_tag)
                break

            # update status and statistics
            if self.queue_stats_processed_messages > 10:
                self.update_queue_statistics()
            self.update_status(constants.STATUS_IDLE)
            self.processing_lock.release()

            # wait until canceled during queue switching
            self.switch_queue_lock.acquire()
            self.switch_queue_lock.release()
    
    def dummy_process_request(self, processing_request):
        """
        Waits some time to simulate processing
        :param processing_reques: processing request to work on
        """
        # get log for this stage
        for log in processing_request.logs:
            if log.stage == processing_request.processing_stages[0]:
                break
        
        # get data size
        try:
            data_size = len(processing_request.results[0].content)
        except Exception:
            error_msg = 'Failed to get request data from results!'
            received_error = 'Received error:\n{}'.format(traceback.format_exc())
            for msg in (error_msg, received_error):
                self.logger.error(msg)
                self.message_logger.error(msg)
            raise

        # get processing stage configuration
        try:
            try:
                processing_time_scale = float(self.config['WORKER']['TIME_SCALE'])
            except KeyError:
                processing_time_scale = 1
            try:
                time_delta = float(self.config['WORKER']['TIME_DELTA'])
            except KeyError:
                time_delta = 0
            try:
                processing_time_diff_min = float(self.config['WORKER']['TIME_DIFF_MIN'])
                processing_time_diff_max = float(self.config['WORKER']['TIME_DIFF_MAX'])
            except KeyError:
                processing_time_diff_min = processing_time_diff_max = 0
        except (TypeError, ValueError):
            error_msg = 'Wrong dummy config time format for processing stage {}, value must be number!'.format(log.stage)
            self.logger.error(error_msg)
            self.message_logger.error(error_msg)
            raise
        
        if not processing_time_diff_max and not processing_time_diff_min:
            processing_time_diff_min = -time_delta
            processing_time_diff_max = time_delta
        
        if processing_time_diff_min > processing_time_diff_max:
            error_msg = 'Wrong dummy config for processing stage {}, minimal processing time cannot be higher than maximal processing time!'.format(log.stage)
            self.logger.error(error_msg)
            self.message_logger.error(error_msg)
            raise

        # get processing time
        processing_time = data_size * processing_time_scale + random.uniform(processing_time_diff_min, processing_time_diff_max)
        if processing_time < 0:
            processing_time = 0

        # processing
        self.logger.info('Dummy processing, waiting for {} seconds'.format(processing_time))
        self.message_logger.info('Dummy processing time: {} seconds'.format(processing_time))
        self.message_logger.info('Request data size: {}'.format(data_size))
        if processing_time_scale != 1:
            self.message_logger.info('Processing time scale: {}'.format(processing_time_scale))
        if time_delta:
            self.message_logger.info('Time delta: {}'.format(time_delta))
        elif processing_time_diff_max or processing_time_diff_min:
            self.message_logger.info('Time difference min: {}'.format(processing_time_diff_min))
            self.message_logger.info('Time difference max: {}'.format(processing_time_diff_max))
        time.sleep(processing_time)

    def ocr_process_request(self, processing_request):
        """
        Process processing request using OCR
        :param processing_request: processing request to work on
        """
        # load data
        img = None
        img_name = 'page'
        xml_in = None
        logits_in = None

        for i, data in enumerate(processing_request.results):
            ext = os.path.splitext(data.name)[1]
            datatype = magic.from_buffer(data.content, mime=True)
            self.logger.debug(f'File: {data.name}, type: {datatype}')
            if datatype.split('/')[0] == 'image':  # recognize image
                img = cv2.imdecode(np.fromstring(processing_request.results[i].content, dtype=np.uint8), 1)
                img_name = processing_request.results[i].name
            if ext == '.xml':  # pagexml is missing xml header - type can't be recognized - type = text/plain
                xml_in = processing_request.results[i].content
            if ext == '.logits':  # type = application/octet-stream
                logits_in = processing_request.results[i].content
        
        # run processing
        try:
            if xml_in:
                page_layout = PageLayout()
                page_layout.from_pagexml_string(xml_in)
            else:
                page_layout = PageLayout(id=img_name, page_size=(img.shape[0], img.shape[1]))
            if logits_in:
                page_layout.load_logits(logits_in)
            page_layout = self.page_parser.process_page(img, page_layout)
        except Exception as e:
            # processing failed
            self.message_logger.error('{error}'.format(error = e))
            self.message_logger.error('Received traceback:\n{}'.format(traceback.format_exc()))
            raise
        
        # save output
        xml_out = page_layout.to_pagexml_string()
        try:
            logits_out = page_layout.save_logits_bytes()
        except Exception:
            self.logger.debug('No logits available to save!')
            self.logger.debug('Received error:\n{}'.format(traceback.format_exc()))
            logits_out = None

        for data in processing_request.results:
            ext = os.path.splitext(data.name)[1]
            if ext == '.xml':
                data.content = xml_out.encode('utf-8')
            if ext == '.logits':
                data.content = logits_out
        
        if xml_out and not xml_in:
            xml = processing_request.results.add()
            xml.name = '{}_page.xml'.format(img_name)
            xml.content = xml_out.encode('utf-8')
        
        if logits_out and not logits_in:
            logits = processing_request.results.add()
            logits.name = '{}.logits'.format(img_name)
            logits.content = logits_out

    @staticmethod
    def unpack_archive(archive, path):
        """
        Unpacks tar or zip archive
        :param archive: path to archive to unpack
        :param path: path where archive will be unpacked to
        """
        if tarfile.is_tarfile(archive):
            with tarfile.open(archive, 'r') as arch:
                arch.extractall(path)
        else:
            with zipfile.ZipFile(archive, 'r') as arch:
                arch.extractall(path)

    def ocr_load(self, name):
        """
        Loads ocr for stage given by name
        :param name: name of config/queue of the stage
        """
        # clear current ocr from gpu cache
        torch.cuda.empty_cache()

        # create ocr directory
        self.ocr_path = os.path.join(self.tmp_directory, name)
        if os.path.exists(self.ocr_path):
            self.clean_tmp_files(self.ocr_path)
        os.mkdir(self.ocr_path)

        # config
        config_file_name = os.path.join(self.ocr_path, 'config.ini')

        # get config from zookeeper and save to file
        try:
            config_content = self.zk.get(constants.QUEUE_CONFIG_TEMPLATE.format(queue_name=name))[0].decode('utf-8')
            config_version = self.zk.get(constants.QUEUE_CONFIG_VERSION_TEMPLATE.format(queue_name=name))[0].decode('utf-8')
        except (kazoo.exceptions.NoNodeError, kazoo.exceptions.ZookeeperError) as e:
            self.update_status(constants.STATUS_FAILED)
            self.logger.error('Failed to get configuration for queue {}'.format(name))
            raise
        with open(config_file_name, 'w') as config_file:
            config_file.write(config_content)

        # load config
        self.config = configparser.ConfigParser()
        self.config.read(config_file_name)
        self.config_version = config_version

        try:
            dummy = self.config['WORKER']['DUMMY']
        except KeyError:
            dummy = None
        
        # do not load ocr if loaded stage is dummy
        if dummy:
            return
        
        # get path of ocr data on ftp servers
        try:
            config_path = self.zk.get(constants.QUEUE_CONFIG_PATH_TEMPLATE.format(queue_name=name))[0].decode('utf-8')
        except (kazoo.exceptions.NoNodeError, kazoo.exceptions.ZookeeperError) as e:
            self.update_status(constants.STATUS_FAILED)
            self.logger.error('Failed to get OCR data path on FTP server for queue {}'.format(name))
            raise

        # no additional files required
        if not config_path:
            return

        # connect to ftp servers
        sftp = self.sftp_connect()

        # get ocr data
        archive_name = '{}.archive'.format(self.queue)
        archive_path = os.path.join(self.tmp_directory, archive_name)
        try:
            sftp.sftp_get(f'{config_path}', archive_path)
        except Exception:
            self.logger.error('Failed to retrieve file {remote_path} and sage it to {path}!'.format(
                remote_path = config_path,
                path = archive_name
            ))
            raise
        else:
            self.logger.debug('File {remote_path} successfully saved as {path}!'.format(
                remote_path = config_path,
                path = archive_name
            ))
        try:
            self.unpack_archive(archive_path, self.ocr_path)
        except Exception:
            self.logger.error('Failed to unpack OCR data for queue {queue}!'.format(self.queue))
            raise
        else:
            self.logger.debug('OCR data unpacked successfully!')
            # remove unneeded archive to save space
            self.clean_tmp_files(archive_path)
        
        sftp.sftp_disconnect()

        # load ocr for page processing
        self.page_parser = PageParser(self.config, self.ocr_path)
    
    def stop_processing(self):
        """
        Stops processing of current queue.
        Runs only if channel is open.
        """
        if not self.mq_channel:
            return
        
        if not self.mq_channel.is_open:
            return

        try:
            self.mq_channel.basic_cancel(self.worker_id)
        except ValueError:
            # worker was not processing
            pass

        # redeliver received unprocessed messages
        self.mq_channel.basic_recover(True)

        self.mq_channel.close()
    
    def run(self):
        """
        Start the worker
        """
        # connect to the zookeeper
        try:
            self.zk_connect()
        except KazooTimeoutError:
            self.logger.critical('Failed to connect to zookeeper!')
            self.logger.critical('Initialization aboarded!')
            return 1
        
        # generate id
        if not self.worker_id:
            self.gen_id()
        self.logger.info('Worker id: {}'.format(self.worker_id))

        # initialize worker status and sync with zookeeper
        try:            
            self.init_worker_status()
        except kazoo.exceptions.ZookeeperError:
            self.logger.critical('Failed to register worker in zookeeper!')
            self.logger.critical('Received error:\n{}'.format(traceback.format_exc()))
            return 1
        
        # setup tmp directory
        if not self.tmp_directory:
            self.tmp_directory = f'/tmp/pero-worker-{self.worker_id}'
        self.create_tmp_dir()

        # get MQ and FTP server list before connecting to MQ
        try:
            self.zk_callback_update_mq_server_list(
                self.zk.get_children(constants.WORKER_CONFIG_MQ_SERVERS)
            )
            self.zk_callback_update_ftp_server_list(
                self.zk.get_children(constants.WORKER_CONFIG_FTP_SERVERS)
            )
        except kazoo.exceptions.ZookeeperError:
            self.logger.critical('Failed to get lists of MQ and FTP servers!')
            self.logger.critical('Received error:\n{}'.format(traceback.format_exc()))
            return 1

        # register zookeeper callbacks:
        # update MQ server list
        self.zk.ChildrenWatch(
            path=constants.WORKER_CONFIG_MQ_SERVERS,
            func=self.zk_callback_update_mq_server_list
        )
        # update ftp server list
        self.zk.ChildrenWatch(
            path=constants.WORKER_CONFIG_FTP_SERVERS,
            func=self.zk_callback_update_ftp_server_list
        )
        # switch queue callback
        self.zk.DataWatch(
            path=constants.WORKER_QUEUE_TEMPLATE.format(worker_id=self.worker_id),
            func=self.zk_callback_switch_queue
        )
        # shutdown callback
        self.zk.DataWatch(
            path=constants.WORKER_ENABLED_TEMPLATE.format(worker_id=self.worker_id),
            func=self.zk_callback_shutdown
        )

        # setup MQ connection and channel
        try:
            self.mq_connect()
        except Exception:
            self.logger.critical('Failed to connect to message broker servers!')
            self.logger.critical(traceback.format_exc())
            return 1

        if self.queue:
            self.update_status(constants.STATUS_PROCESSING)
        else:
            self.update_status(constants.STATUS_IDLE)

        self.logger.info('Worker is running')
        # start processing queue
        try:
            # uses this thread for managing the MQ connection
            # until the connection is closed
            self.mq_connection.ioloop.start()  # TODO - run connection in different thread
        except KeyboardInterrupt:
            # TODO - debug keyboard interrupt - sometimes does not stop the ioloop
            self.logger.info('Keyboard interrupt received! Shutting down!')
            self.enabled = False
            self.mq_disconnect()
            self.update_status(constants.STATUS_DEAD)
        except Exception:
            self.logger.critical('Connection to message broker failed!')
            self.logger.critical(traceback.format_exc())
            self.update_status(constants.STATUS_FAILED)
            return 1
        
        return 0

def main():
    args = parse_args()

    # validate server lists and convert them to correct format for each connection
    zk_servers = cf.zk_server_list(args.zookeeper)
    mq_servers = cf.server_list(args.broker_servers) if args.broker_servers else None
    ftp_servers = cf.server_list(args.ftp_servers) if args.ftp_servers else None
    
    # select tmp directory
    tmp_dir = args.tmp_directory if args.tmp_directory else None

    worker = Worker(
        username=args.username,
        password=args.password,
        ca_cert=args.ca_cert,
        zookeeper_servers=zk_servers,
        mq_servers=mq_servers,
        ftp_servers=ftp_servers,
        tmp_directory=tmp_dir,
        worker_id=args.id,
        logger=logging.getLogger(__name__)
    )

    return worker.run()

# run the module
if __name__ == "__main__":
    sys.exit(main())
