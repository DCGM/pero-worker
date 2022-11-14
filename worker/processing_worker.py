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
    
    def report_statistics(stage, processed_request_count):
        """
        Update statistics about processed requests.
        :param stage: processed stage
        :param processed_request_count: number of reqeusts processed
        """
        if self.manager:
            self.manager.report_statistics(stage, processed_request_count)
    
    def processing_enabled(self):
        """
        Check if processing was enabled / disabled by manager.
        """
        if self.manager:
            return self.manager.processing_enabled()
        else:
            return True
    
    def mq_process_request(self, channel, method, properties, body):
        """
        Callback function for processing messages (requests) received from message broker channel
        :param channel: channel from which the message is originated
        :param method: message delivery method
        :param properties: additional message properties
        :param body: message body (actual processing request)
        :raise: ValueError if output queue is not declared on broker
        """
        # TODO
        pass
    
    def run(stage):
        """
        Main method of the worker.
        Starts processing of configured stage.
        :return: execution status
        """
        processed_request_count = 0
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
                    self.logger.info('Queue for stage {self.stage} is empty, switching to idle state.')
                    self.report_status(constants.STATUS_IDLE)
                    break

                # process request
                try:
                    self.mq_process_request(self.channel, method, properties, body)
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
                    # TODO
                    # send and ack request
                    processed_request_count += 1
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
                self.report_statistics(stage, processed_request_count)
            except KeyboardInterrupt:
                pass
        
        return status_code
