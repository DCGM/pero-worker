#!/usr/bin/env python3

# Worker for page processing

import pika  # AMQP protocol library for queues
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
from ftplib import FTP, all_errors, error_perm

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
from kazoo.protocol.states import KazooState
import kazoo.exceptions

# constants
import worker_functions.constants as constants
import worker_functions.connection_aux_functions as cf


# === Global config ===

# setup logging (required by kazoo)
log_formatter = logging.Formatter('%(asctime)s %(levelname)s WORKER: %(message)s')

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
        help='Worker id for identification in zookeeper'
    )
    argparser.add_argument(
        '-b', '--broker-servers',
        help='List of message queue broker servers where to get and send processing requests',
        nargs='+',
        default=['127.0.0.1']
    )
    argparser.add_argument(
        '-f', '--ftp-servers',
        help='List of ftp servers.',
        nargs='+',
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
    argparser.add_argument(
        '-u', '--user',
        help='Username for server authentication.',
        default='pero'  # this is for testing only!
    )
    argparser.add_argument(
        '-p', '--password',
        help='Password for user authentication.',
        default='pero'  # this is for testing only!
    )
    return argparser.parse_args()

class Worker(object):
    """
    Pero processing worker
    """

    def __init__(self, user, password, zookeeper_servers, mq_servers = [], ftp_servers = [], worker_id = None, tmp_directory = None):
        """
        Initialize worker
        :param worker_id: worker identifier
        :param tmp_directory: path to temporary directory
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
        self.ocr = None

        # ftp login
        self.user = user
        self.password = password

        # worker connections
        self.mq_channel = None
        self.mq_connection = None
        self.zk = None
        self.ftp = None

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
        self.queue_stats_lock = None  # lock for accessing the stats in zookeeper

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
        logger.info('Closing connection and cleaning up')
        #logger.debug('Cleanup phase: update status')
        #self.update_status(constants.STATUS_DEAD)
        logger.debug('Cleanup phase: disconnect mq')
        self.mq_disconnect()
        logger.debug('Cleanup phase: disconnect ftp')
        self.ftp_disconnect()
        logger.debug('Cleanup phase: disconnect zk')
        self.zk_disconnect()
        logger.debug('Cleanup phase: cleanup tmp files')
        self.clean_tmp_files()
        logger.debug('Cleanup complete!')
    
    def ftp_connect(self):
        """
        Connects to ftp
        """
        self.ftp = cf.ftp_connect(self.ftp_servers)
        if not self.ftp:
            raise ConnectionError('Failed to connect to ftp servers!')
        self.ftp.login(self.user, self.password)
    
    def ftp_disconnect(self):
        """
        Disconnects from ftp
        """
        if self.ftp:
            try:
                self.ftp.quit()
            except AttributeError:
                # connection is already closed
                pass
            except Exception:
                # failed to disconnect the polite way
                # close the connection (the ugly way)
                try:
                    self.ftp.close()
                except Exception:
                    logger.error('Error occured during disconnecting from FTP!')
                    logger.error('{}'.format(traceback.format_exc()))
    
    def zk_disconnect(self):
        """
        Disconnects from zookeeper
        """
        if self.zk and self.zk.connected:
            try:
                self.zk.stop()
                self.zk.close()
            except kazoo.exceptions.KazooException as e:
                logger.error('Failed to close zookeeper connection!')
                logger.error('Received error: {}'.format(traceback.format_exc()))

    def zk_connect(self):
        """
        Connects worker to the zookeeper
        """
        # skip if connection is active
        if self.zk and self.zk.connected:
            return

        # create new connection
        self.zk = KazooClient(hosts=self.zookeeper_servers)

        try:
            self.zk.start(timeout=20)
        except KazooTimeoutError:
            logger.error('Zookeeper connection timeout!')
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

        # stop processing of cureent queue and report statistics
        self.stop_processing()
        self.update_status(constants.STATUS_RECONFIGURING)
        self.update_queue_statistics()

        # switch queue
        self.queue = queue
        try:
            self.queue_stats_lock = self.zk.Lock(
                constants.QUEUE_STATS_AVG_MSG_TIME_LOCK_TEMPLATE.format(queue_name = queue),
                identifier=self.worker_id
            )
        except kazoo.exceptions.ZookeeperError as e:
            logger.error('Failed to get lock for average message time statistics for queue {}!'.format(
                queue
            ))
            self.update_status(constants.STATUS_FAILED)
            self.switch_queue_lock.release()
            self.processing_lock.release()
            return

        # load OCR data and configuration
        try:
            self.ocr_load(queue)
        except Exception:
            self.update_status(constants.STATUS_FAILED)
            logger.error('Failed to switch queue to {}, could not get configuration for given processing phase!'.format(
                queue
            ))
            logger.error('{}'.format(traceback.format_exc()))
        else:
            self.update_status(constants.STATUS_IDLE)
            # start processing of new queue
            self.mq_channel_create(self.mq_connection)
        finally:
            # unblock processing and switching to new queue
            self.switch_queue_lock.release()
            self.processing_lock.release()
    
    def zk_callback_shutdown(self, data, status, *args):
        """
        Shutdown the worker if set to disabled state.
        :param data: shutdown notification
        :param status: new zookeeper node status
        :param args: additional arguments (like event)
        """
        enabled = data.decode('utf-8').lower()
        if enabled != 'true':
            logger.info('Shutdown signal received!')
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
        logger.debug('Updating queue statistics for queue {}'.format(self.queue))
        with self.queue_stats_lock:
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

                # reset counters
                self.queue_stats_total_processing_time = self.queue_stats_processed_messages = 0

            except kazoo.exceptions.ZookeeperError:
                logger.error(
                    'Failed to update average processing time for queue {} due to zookeeper error!'
                    .format(self.queue)
                )
                logger.error('Received error:\n{}'.format(traceback.format_exc()))
            except ValueError:
                logger.error(
                    'Failed to update average processing time for queue {} due to wrong number format in zookeeper!'
                    .format(self.queue)
                )
            except Exception:
                logger.error(
                    'Failed to update average processing time for queue {} due to unknown error!'
                    .format(self.queue)
                )
                logger.error('Received error:\n{}'.format(traceback.format_exc()))
    
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
            logger.error('Failed to update status in zookeeper. Zookeeper connection lost!')
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
    
    def mq_disconnect(self):
        """
        Stops connection to message broker
        """
        if self.mq_channel and self.mq_channel.is_open:
            self.stop_processing()
        
        if self.mq_connection and self.mq_connection.is_open:
            self.mq_connection.close()
            self.mq_connection.ioloop.stop()

    def mq_connect(self):
        """
        Connect to message broker
        """
        # prevent mq server list update during connecting
        self.mq_server_lock.acquire()

        if not self.mq_servers:
            logger.error('No MQ servers available!')
            self.update_status(constants.STATUS_FAILED)
            self.mq_server_lock.release()
            return
        
        if self.mq_connection_retry == len(self.mq_servers):
            logger.error('Failed to connect to any MQ servers!')
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
        logger.info('Connecting to MQ server {}'.format(cf.ip_port_to_string(self.mq_server)))
        self.mq_connection = pika.SelectConnection(
            pika.ConnectionParameters(
                host=self.mq_server['ip'],
                port=self.mq_server['port'] if self.mq_server['port'] else pika.ConnectionParameters.DEFAULT_PORT
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
        logger.info('Connection to MQ establisted successfully')
        self.mq_server_lock.acquire()
        self.mq_connection_retry = 0
        self.mq_server_lock.release()
    
    def mq_connection_open_error(self, connection, error):
        """
        Set worker status to failed - connection to broker failed to open.
        :param connection: broker connection - same as self.mq_connection
        :param error: exception generated on close
        """
        logger.error('Connection to MQ failed to open! Received error: {}'.format(error))
        self.mq_connect()
    
    def mq_connection_close(self, connection, reason):
        """
        Set worker status based on connection.
        :param connection: broker connection - same as self.mq_connection
        :param reason: reason why was connection closed
        """
        logger.warning('Connection to MQ closed, reason: {}'.format(reason))
        if not self.enabled:
            self.update_status(constants.STATUS_DEAD)
        else:
            self.mq_connect()

    def mq_channel_create(self, connection):
        """
        Create broker channel
        :param connection: broker connection - same as self.mq_connection
        """
        logger.info('Setting up MQ channel')
        self.mq_connection.channel(on_open_callback=self.mq_channel_setup)

    def mq_channel_setup(self, channel):
        """
        Setup parameters for message broker channel
        :param channel: Created channel object
        """
        # set number of prefetched messages
        channel.basic_qos(prefetch_count=5)
        
        self.mq_channel = channel

        logger.info('MQ Channel setup complete')

        if self.queue:
            self.mq_queue_setup()
    
    def mq_queue_setup(self):
        """
        Setup parameters for input queue
        """
        if not self.mq_channel or not self.mq_channel.is_open:
            logger.error('Queue setup failed, mq channel is not open!')
            return

        # register callback for data processing
        self.mq_channel.basic_consume(
            self.queue,
            self.mq_process_request,
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
        self.processing_lock.acquire()
        self.update_status(constants.STATUS_PROCESSING)
        # TODO
        # add try/catch for parsing request (can be something else than protobuf)
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
            channel.basic_ack(delivery_tag = method.delivery_tag)
        except Exception:
            # TODO - handle same way as runtime error!
            logger.error('Failed to process request {request_id} using stage {stage}!'.format(
                request_id = processing_request.uuid,
                stage = current_stage
            ))
            logger.error(traceback.format_exc())
            channel.basic_nack(delivery_tag = method.delivery_tag)
        else:
            processing_request.processing_stages.pop(0)
            next_stage = processing_request.processing_stages[0]
            
            # send request to output queue and
            # acknowledge the request after successfull processing
            # TODO
            # add validation if message was received by mq
            channel.basic_publish('', next_stage, processing_request.SerializeToString())
            channel.basic_ack(delivery_tag = method.delivery_tag)
        finally:
            self.update_status(constants.STATUS_IDLE)
            if self.queue_stats_processed_messages > 10:
                self.update_queue_statistics()
            self.processing_lock.release()

            # wait until canceled during queue switching
            self.switch_queue_lock.acquire()
            self.switch_queue_lock.release()

    def ocr_process_request(self, processing_request):
        """
        Process processing request using OCR
        :param processing_request: processing request to work on
        :return: processing status
        """
        # TODO
        # add logger output to message log

        start_time = datetime.datetime.now(datetime.timezone.utc)
        # gen log
        log = processing_request.logs.add()
        log.host_id = self.worker_id
        log.stage = processing_request.processing_stages[0]
        #Timestamp.GetCurrentTime(log.start)
        Timestamp.FromDatetime(log.start, start_time)

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
            if ext == '.xml':  # pagexml is missing xml header - type can't be recognized - type = text/plain
                xml_in = processing_request.results[i].content
            if ext == '.logits':  # type = application/octet-stream
                logits_in = processing_request.results[i].content
        
        # run processing
        page_parser = PageParser(self.config, self.ocr)  # TODO - move pageparser to load config
        try:
            if xml_in:
                page_layout = PageLayout()
                page_layout.from_pagexml_string(xml_in)
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
        try:
            logits_out = page_layout.save_logits_bytes()
        except Exception:
            logits_out = None

        for data in processing_request.results:
            ext = os.path.splitext(data.name)[1]
            if ext == '.xml':
                data.content = xml_out.encode('utf-8')
            if ext == '.logits':
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
        #Timestamp.GetCurrentTime(log.end)
        end_time = datetime.datetime.now(datetime.timezone.utc)
        Timestamp.FromDatetime(log.end, end_time)

        # worker statistics
        spend_time = (end_time - start_time).total_seconds()
        self.queue_stats_total_processing_time += spend_time
        self.queue_stats_processed_messages += 1

    def ocr_load(self, name):
        """
        Loads ocr for phase given by name
        :param name: name of config/queue of the phase
        """
        # create ocr directory
        self.ocr = os.path.join(self.tmp_directory, name)
        if os.path.exists(self.ocr):
            self.clean_tmp_files(self.ocr)
        os.mkdir(self.ocr)

        # config
        config_file_name = os.path.join(self.ocr, 'config.ini')

        # get config from zookeeper and save to file
        try:
            config_content = self.zk.get(constants.QUEUE_CONFIG_TEMPLATE.format(queue_name=name))[0].decode('utf-8')
        except (kazoo.exceptions.NoNodeError, kazoo.exceptions.ZookeeperError) as e:
            self.update_status(constants.STATUS_FAILED)
            logger.error('Failed to get configuration for queue {}'.format(name))
            # stop reconfiguration if config can't be downloaded
            raise
        with open(config_file_name, 'w') as config_file:
            config_file.write(config_content)
        
        # load config
        self.config = configparser.ConfigParser()
        self.config.read(config_file_name)

        # connect to ftp servers
        try:
            self.ftp_connect()
        except error_perm:
            logger.critical('Wrong FTP server password!')
            raise
        except Exception:
            logger.critical('Failed to connect to ftp servers!')
            raise

        # get ocr data
        for section in self.config:
            for key in self.config[section]:
                try:
                    value = json.loads(self.config[section][key])
                    logger.debug('Successfully parsed config field: {section}:{key}:{value}'.format(
                        section = section,
                        key = key,
                        value = self.config[section][key]
                    ))
                except json.decoder.JSONDecodeError:
                    # not a valid json
                    logger.debug('Failed to parse config field: {section}:{key}:{value}'.format(
                        section = section,
                        key = key,
                        value = self.config[section][key]
                    ))
                else:
                    if isinstance(value, dict):
                        if 'url' in value and 'path' in value:
                            try:
                                with open(os.path.join(self.ocr, value['path']), 'wb') as fd:
                                    self.ftp.retrbinary('RETR {}'.format(value['url']), fd.write)
                            except (PermissionError, all_errors) as e:
                                logger.error('Failed to retreive file {url} and save it to {path}!'.format(
                                    path = value['path'],
                                    url = value['url']
                                ))
                                # stop configuration
                                self.ftp_disconnect()
                                raise
                            else:
                                logger.debug('File {url} saved to {path}'.format(
                                    url = value['url'],
                                    path = value['path']
                                ))
                                # set path to file
                                self.config[section][key] = value['path']
        self.ftp_disconnect()
    
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
            logger.critical('Failed to connect to zookeeper!')
            logger.critical('Initialization aboarded!')
            return 1
        
        # generate id
        if not self.worker_id:
            self.gen_id()
        logger.info('Worker id: {}'.format(self.worker_id))

        # initialize worker status and sync with zookeeper
        try:            
            self.init_worker_status()
        except kazoo.exceptions.ZookeeperError:
            logger.critical('Failed to register worker in zookeeper!')
            logger.critical('Received error:\n{}'.format(traceback.format_exc()))
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
            logger.critical('Failed to get lists of MQ and FTP servers!')
            logger.critical('Received error:\n{}'.format(traceback.format_exc()))
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
            logger.critical('Failed to connect to message broker servers!')
            logger.critical(traceback.format_exc())
            return 1

        if self.queue:
            self.update_status(constants.STATUS_PROCESSING)
        else:
            self.update_status(constants.STATUS_IDLE)

        logger.info('Worker is running')
        # start processing queue
        try:
            # uses this thread for managing the MQ connection
            # until the connection is closed
            self.mq_connection.ioloop.start()  # TODO - run connection in diferent thread
        except KeyboardInterrupt:
            # TODO - debug keyboard interrupt - sometimes does not stop the ioloop
            logger.info('User interrupt received! Shutting down!')
            self.mq_disconnect()
            self.update_status(constants.STATUS_DEAD)
        except Exception:
            logger.critical('Connection to message broker failed!')
            logger.critical(traceback.format_exc())
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
        user=args.user,
        password=args.password,
        zookeeper_servers=zk_servers,
        mq_servers=mq_servers,
        ftp_servers=ftp_servers,
        tmp_directory=tmp_dir,
    )

    return worker.run()

# run the module
if __name__ == "__main__":
    sys.exit(main())
