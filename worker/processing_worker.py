#!/usr/bin/env python3

# Worker for page processing

import pika  # AMQP protocol library for queues
import logging
import time
import sys
import os  # filesystem
import traceback  # logging
from io import StringIO  # logging to message
import random  # dummy processing

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

# constants
import worker_functions.constants as constants
import worker_functions.connection_aux_functions as cf

# MQ client
from worker_functions.mq_client import MQClient

class ProcessingWorker(MQClient):
    """
    Worker for request processing.
    Processes requests received from MQ queue and submits results back to MQ.
    """
    def __init__(self, manager = None, mq_servers = [], username = '', password = '', ca_cert = None, logger = logging.getLogger(__name__)):
        """
        Initializes worker.
        :param manager: manager object where to report status and get up to date parameters
        :param mq_servers: list of MQ servers to connect to
        :param username: username for MQ server authentication
        :param password: password for MQ server authentication
        :param ca_cert: MQ server CA certificate
        :param logger: logger to use for log messages
        """
        # init the MQClient
        super().__init__(
            mq_servers = mq_servers,
            username = username,
            password = password,
            ca_cert = ca_cert,
            logger = logger
        )

        # set manager object
        self.manager = manager

        # TODO
        # move this to separate object for request processing
        # - move this to separate object for processing
        # - config - dependency for dummy_processing
        # - config_version - dependency for ocr_processing and dummy processing
        # - page_parser - dependency for ocr processing
        # - remove set_stage_config method and pass in object for stage processing when calling run()
        # stage configuration
        self.config = None
        self.config_version = None
        self.page_parser = None
    
    def set_stage_config(self, config, config_version, page_parser):
        """
        Sets up config for stage processing.
        :param config: processing configuration for ocr
        :param config_version: version of processing configuration
        :param page_parser: page parser configured using current stage configuration
        TODO - move this to separate object for processing
        """
        self.config = config
        self.config_version = config_version
        self.page_parser = page_parser
    
    def get_mq_servers(self):
        """
        Returns current list of MQ servers.
        List is taken from manager object if it present, otherwise self.mq_servers is used
        :return: list of MQ servers
        """
        if self.manager:
            return self.manager.get_mq_servers()
        else:
            return self.mq_servers
    
    def report_status(self, status):
        """
        Report status change to manager object.
        :param status: current status (taken from constants.STATUS_*)
        """
        if self.manager:
            self.manager.report_status(status)
    
    def report_statistics(stage, processed_request_count, total_processing_time):
        """
        Update statistics about processed requests.
        :param stage: processed stage
        :param processed_request_count: number of reqeusts processed
        :param total_processing_time: total time spent on processing processed requests
        """
        if self.manager:
            self.manager.report_statistics(stage, processed_request_count, total_processing_time)
    
    def processing_enabled(self):
        """
        Check if processing was enabled / disabled by manager.
        """
        if self.manager:
            return self.manager.processing_enabled()
        else:
            return True
    
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

    def mq_process_request(self, channel, method, properties, body):
        """
        Callback function for processing messages (requests) received from message broker channel
        :param channel: channel from which the message is originated
        :param method: message delivery method
        :param properties: additional message properties
        :param body: message body (actual processing request)
        :raise: ValueError if output queue is not declared on broker
        :return: time spent on processing
        """
        spent_time = 0
        processing_request = ProcessingRequest().FromString(body)
        
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

        # TODO
        # Move to init -- pass in dummy processor
        try:
            dummy = self.config['WORKER']['DUMMY']
        except KeyError:
            dummy = False

        try:
            if dummy:
                self.dummy_process_request(processing_request)
            else:
                self.ocr_process_request(processing_request)
        except KeyboardInterrupt:
            raise
        except Exception as e:
            self.logger.error('Failed to process request {request_id} in stage {stage}!'.format(
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
            spent_time = (end_time - start_time).total_seconds()

            # select next processing stage
            processing_request.processing_stages.pop(0)
            next_stage = processing_request.processing_stages[0]
        finally:
            # add buffered log to the message
            buffer_handler.flush()
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
                except KeyboardInterrupt:
                    raise
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
        
        return spent_time
    
    def run(stage):
        """
        Main method of the worker.
        Starts processing of configured stage.
        :param stage: stage name
        :return: execution status
        """
        processed_request_count = 0
        total_processing_time = 0
        status_code = 0
        requeue_messages = False  # Requeue unacknowledged messages after connection failure

        # process queue for given stage
        try:
            while self.processing_enabled():
                # connect to MQ
                try:
                    self.mq_connect_retry(max_retry = 0)  # Try to connect to MQ servers until success
                except ConnectionError as e:
                    self.logger.error(f'{e}')
                    self.report_status(constants.STATUS_CONNECTION_FAILED)
                    status_code = 1
                    break
                else:
                    self.report_status(constants.STATUS_PROCESSING)
                
                if requeue_message:
                    try:
                        self.mq_channel.basic_recover(requeue = True)
                    except pika.exceptions.AMQPError as e:
                        self.logger.error('Failed to requeue messages due to connection failure!')
                        continue
                    except ValueError:  # no messages to requeue
                        self.logger.debug(f'No messages to requeue. Received error: {e}')

                # get processing request
                try:
                    method, properties, body = self.mq_channel.basic_get(queue=stage, auto_ack=False)
                except pika.exceptions.AMQPError as e:
                    # connection to MQ failed
                    self.logger.error('Failed to get new processing requests due to MQ connection error!')
                    self.report_status(constants.STATUS_CONNECTION_FAILED)
                    continue
                
                # all requests were processed
                if method == properties == body == None:
                    self.logger.info(f'Queue for stage {stage} is empty, switching to idle state.')
                    self.report_status(constants.STATUS_IDLE)
                    break

                # process request
                try:
                    spent_time = self.mq_process_request(self.channel, method, properties, body)
                except KeyboardInterrupt:
                    raise
                except pika.exceptions.AMQPError as e:
                    # connection to MQ failed
                    self.logger.error('Failed to send processing results due to MQ connection error!')
                    self.report_status(constants.STATUS_CONNECTION_FAILED)
                    requeue_message = True
                except Exception as e:
                    self.logger.error('Failed to process request due to unknown failure!')
                    self.logger.error(traceback.format_exc())
                    self.report_status(constants.STATUS_PROCESSING_FAILED)
                    status_code = 1
                    break
                else:
                    if spent_time:
                        total_processing_time += spent_time
                        processed_request_count += 1
                    if processed_request_count > 10:
                        self.report_statistics(stage, processed_request_count, total_processing_time)
                        total_processing_time = 0
                        processed_request_count = 0

        except KeyboardInterrupt:
            self.logger.info('Keyboard interrupt received, shutting down!')
            self.mq_disconnect()
            self.report_status(constants.STATUS_DEAD)
        else:
            # requeue messages
            try:
                self.mq_channel.basic_recover(requeue = True)
            except KeyboardInterrupt:
                pass
            except pika.exceptions.AMQPError as e:
                self.logger.error('Failed to requeue messages due to connection failure!')
            except ValueError:  # no messages to requeue
                self.logger.debug(f'No messages to requeue. Received error: {e}')
            
            # update statistics
            try:
                self.report_statistics(stage, processed_request_count, total_processing_time)
            except KeyboardInterrupt:
                pass
        
        return status_code
