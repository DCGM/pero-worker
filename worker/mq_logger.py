#!/usr/bin/env python3

import logging
import logging.handlers
import queue
import traceback

# RabbitMQ
import pika

# MQ client
from worker_functions.mq_client import MQClient


class TimeoutQueueListener(logging.handlers.QueueListener):
    def dequeue(self, block=True, timeout=None):
        """
        Dequeue with timeout.
        Overrides default 'dequeue' method that allows only blocking or non-blocking get.
        :param block: if True, blocks until record is read from queue
        :param timeout: timeout for getting record from queue, works only if block=True
        """
        return self.queue.get(block=block, timeout=timeout)

class SilentQueueHandler(logging.handlers.QueueHandler):
    def enqueue(self, record):
        """
        Enqueue record without raising error if queue is full. Silently drops record instead.
        :param record: record to enqueue
        """
        try:
            self.queue.put_nowait(record)
        except queue.Full:
            # queue is full, record is discarded
            pass

class MQLogger(MQClient):
    def __init__(
        self,
        controller = None,
        mq_servers = [],
        username = '',
        password = '',
        ca_cert = None,
        mq_log_queue = 'log',
        log_formatter = None,
        logger = logging.getLogger(__name__)
    ):
        """
        Initializes MQ logger.
        :param controller: controller object where to report status and get up to date parameters
        :param mq_servers: list of MQ servers to connect to
        :param username: username for MQ server authentication
        :param password: password for MQ server authentication
        :param ca_cert: MQ server CA certificate
        :param mq_log_queue: name of the log queue on MQ server
        :param log_formatter: log formatter string
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

        # set MQ queue to send messages to
        self.mq_log_queue = mq_log_queue

        # init logging handler and listener
        self.queue_listener = self.setup_queue_logging(log_formatter)
        
    
    def setup_queue_logging(self, log_formatter):
        """
        Sets up logging format, inits log handler and listener.
        :param log_formatter: Log formatter to format logs received from queue.
        :return: QueueListener listening on logger output queue
        """
        q = queue.Queue(maxsize=200)
        queue_handler = SilentQueueHandler(q)
        if log_formatter:
            queue_handler.setFormatter(log_formatter)
        self.logger.addHandler(queue_handler)

        return TimeoutQueueListener(q, respect_handler_level=True)
    
    def get_mq_servers(self):
        """
        Returns list of MQ servers.
        :return: list of configured MQ servers
        """
        if not self.controller:
            return self.mq_servers
        else:
            return self.controller.get_mq_servers()
    
    def shutdown_received(self):
        """
        Check if worker received shutdown.
        :return: True if shutdown signal was received, else False
        """
        if not self.controller:
            return False
        else:
            return self.controller.shutdown_received()

    def run(self):
        """
        Runns logger.
        """
        log_message = None  # remember log message for retry send if connection fails
        try:
            while not self.shutdown_received():
                # connect to MQ servers
                try:
                    # Try to connect to MQ 3-times, then check for shutdown again
                    self.mq_connect_retry(max_retry = 3)
                except ConnectionError as e:
                    self.logger.error(f'Logger failed to connect to MQ, received error: {e}')
                    continue
                
                # get messages from local queue
                if not log_message:
                    try:
                        # timeout prevents deadlock on worker shutdown
                        log_message = self.queue_listener.dequeue(block=True, timeout=30)
                    except queue.Empty:
                        continue

                # upload messages to MQ
                try:
                    self.mq_channel.basic_publish(
                        exchange='',
                        routing_key=self.mq_log_queue,
                        body=log_message.message,
                        properties=pika.BasicProperties(
                            delivery_mode=2  # persistent delivery mode
                        ),
                        mandatory=True  # raise exception if message is rejected
                    )
                except pika.exceptions.UnroutableError as e:
                    self.logger.error(f'Failed to send log message to queue {self.mq_log_queue}!')
                    self.logger.error(f'Received error: {e}')
                except pika.exceptions.AMQPError as e:
                    self.logger.error('Failed to send log message to MQ due to connection error!')
                    self.logger.error(f'Received error: {e}')
                except KeyboardInterrupt:
                    raise
                except Exception:
                    self.logger.error('Failed to send log message to MQ due to unknown error!')
                    self.logger.error(f'Received error:\n{traceback.format_exc()}')
                else:
                    # upload successfull
                    log_message = None
                
        except KeyboardInterrupt:
            self.logger.info('Keyboard interrupt received!')
            self.mq_disconnect()
