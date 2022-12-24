#!/usr/bin/env python3

# Worker for page processing

import pika  # AMQP protocol library for queues
import logging
import sys
import os  # filesystem
import traceback  # logging

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
    def __init__(self, controller = None, mq_servers = [], username = '', password = '', ca_cert = None, logger = logging.getLogger(__name__)):
        """
        Initializes worker.
        :param controller: controller object where to report status and get up to date parameters
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

        # set controller object
        self.controller = controller

        # request processor for current stage
        self.request_processor = None
    
    def get_mq_servers(self):
        """
        Returns current list of MQ servers.
        List is taken from controller object if it present, otherwise self.mq_servers is used
        :return: list of MQ servers
        """
        if self.controller:
            return self.controller.get_mq_servers()
        else:
            return self.mq_servers
    
    def report_status(self, status):
        """
        Report status change to controller object.
        :param status: current status (taken from constants.STATUS_*)
        """
        if self.controller:
            self.controller.update_status(status)
    
    def report_statistics(self, stage, processed_request_count, total_processing_time):
        """
        Update statistics about processed requests.
        :param stage: processed stage
        :param processed_request_count: number of reqeusts processed
        :param total_processing_time: total time spent on processing processed requests
        """
        if self.controller:
            self.controller.update_statistics(stage, processed_request_count, total_processing_time)
    
    def processing_enabled(self):
        """
        Check if processing was enabled / disabled by controller.
        """
        if self.controller:
            return self.controller.processing_enabled()
        else:
            return True
    
    def get_id(self):
        """
        Gets this worker id.
        :return worker_id
        """
        return self.controller.get_id()
    
    def publish_request(self, processing_request, next_stage):
        """
        Publish processing request to MQ queue for next stage.
        :param processing_request: processing request to publish
        :param next_stage: next stage queue name
        """
        channel.basic_publish(
            exchange='',
            routing_key=next_stage,  # queue name (stage name)
            body=processing_request.SerializeToString(),
            properties=pika.BasicProperties(
                delivery_mode=2,  # persistent delivery mode
                priority=processing_request.priority
            ),
            mandatory=True  # raise exception if message is rejected
        )
    
    def process_request(self, channel, method, properties, body):
        """
        MQ callback for request processing.
        Parses request from message body, uses 'message_processor' to process the request.
        Routes result to output queue.
        :param channel: channel from which the message is originated
        :param method: message delivery method
        :param properties: additional message properties
        :param body: message body (actual processing request)
        """
        try:
            processing_request = ProcessingRequest().FromString(body)
        except Exception as e:
            self.logger.error('Failed to parse received request!')
            self.logger.debug(f'Received error:\n{traceback.format_exc()}')
            channel.basic_nack(delivery_tag = method.delivery_tag, requeue = True)
            raise
        
        log = processing_request.logs.add()
        log.host_id = self.get_id()

        # Raise exception from OCR library when processing failes unrecoverably
        spent_time = self.request_processor.process_request(processing_request, log)

        # select next processing stage
        processing_request.processing_stages.pop(0)
        next_stage = processing_request.processing_stages[0]
        if log.status != 'OK':
            next_stage = processing_request.processing_stages[-1]

        try:
            self.publish_request(processing_request, next_stage)
        except pika.exceptions.UnroutableError as e:
            self.logger.error(f'Failed to deliver processing request {processing_request.uuid} to queue {next_stage}')
            self.logger.error(f'Received error: {e}')

            self.logger.warning('Sending request to output queue instead!')
            log.log += f'Delivery to queue {next_stage} failed!\n'
            log.status = 'Failed'
            next_stage = processing_request.processing_stages[-1]
            
            try:
                self.publish_request(processing_request, next_stage)
            except pika.exceptions.UnroutableError as e:
                self.logger.error(f'Failed to deliver processing request {processing_request.uuid} to output queue {next_stage}')
                self.logger.error(f'Received error: {e}')
                self.logger.error(f'Request was pushed back to queue {log.stage}!')
                channel.basic_nack(delivery_tag = method.delivery_tag, requeue = True)
            else:
                self.logger.debug(f'Acknowledging request {processing_request.uuid} from queue {log.stage}')
                channel.basic_ack(delivery_tag = method.delivery_tag)
        else:
            self.logger.debug(f'Acknowledging request {processing_request.uuid} from queue {log.stage}')
            channel.basic_ack(delivery_tag = method.delivery_tag)
        
        return spent_time
    
    def set_request_processor(self, request_processor):
        """
        Sets request processor.
        :param request_processor: request processor to set
        """
        self.request_processor = request_processor
    
    def get_request_processor(self):
        """
        Return current request processor.
        :return: request processor
        """
        return self.request_processor
        
    def run(self, request_processor=None):
        """
        Main method of the worker.
        Starts processing of configured stage.
        :param request_processor: request_processor to use (default = use existing one)
        :return: execution status
        """
        if request_processor:
            self.set_request_processor(request_processor)
        
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
                
                if requeue_messages:
                    try:
                        self.mq_channel.basic_recover(requeue = True)
                    except pika.exceptions.AMQPError as e:
                        self.logger.error('Failed to requeue messages due to connection failure!')
                        continue  # Try to connect again
                    except ValueError:  # no messages to requeue
                        self.logger.debug(f'No messages to requeue. Received error: {e}')
                    requeue_messages = False

                # get processing request
                try:
                    method, properties, body = self.mq_channel.basic_get(
                                                    queue=self.request_processor.stage,
                                                    auto_ack=False
                                                )
                except pika.exceptions.AMQPError as e:
                    # connection to MQ failed
                    self.logger.error('Failed to get new processing requests due to MQ connection error!')
                    self.report_status(constants.STATUS_CONNECTION_FAILED)
                    continue
                
                # all requests were processed
                if method == properties == body == None:
                    self.logger.info(f'Queue for stage {self.request_processor.stage} is empty, switching to idle state.')
                    self.report_status(constants.STATUS_IDLE)
                    break

                # process request
                try:
                    spent_time = self.process_request(self.channel, method, properties, body)
                except KeyboardInterrupt:
                    raise
                except pika.exceptions.AMQPError as e:
                    # connection to MQ failed
                    self.logger.error('Failed to send processing results due to MQ connection error!')
                    self.report_status(constants.STATUS_CONNECTION_FAILED)
                    requeue_messages = True
                except Exception as e:
                    self.logger.error('Failed to process request due to unknown failure!')
                    self.logger.error(f'Received traceback:\n{traceback.format_exc()}')
                    self.report_status(constants.STATUS_PROCESSING_FAILED)
                    status_code = 1
                    break
                else:
                    if spent_time:
                        total_processing_time += spent_time
                        processed_request_count += 1
                    if processed_request_count > 10:
                        self.report_statistics(
                            self.request_processor.stage,
                            processed_request_count,
                            total_processing_time
                        )
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
                self.report_statistics(
                    self.request_processor.stage,
                    processed_request_count,
                    total_processing_time
                )
            except KeyboardInterrupt:
                pass
        
        return status_code
