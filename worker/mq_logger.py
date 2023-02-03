#!/usr/bin/env python3

import logging
import logging.handlers
import queue
import traceback

# RabbitMQ
import pika

# MQ client
from worker_functions.mq_client import MQClient


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
        q = queue.Queue()
        queue_handler = logging.handlers.QueueHandler(q)
        if log_formatter:
            queue_handler.setFormatter(log_formatter)
        self.logger.addHandler(queue_handler)

        return logging.handlers.QueueListener(q, respect_handler_level=True)
    
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
            while True:
                # connect to MQ servers
                try:
                    self.mq_connect_retry(max_retry = 0)  # Try to connect to MQ servers until success
                except ConnectionError as e:
                    self.logger.error(f'Logger failed to connect to MQ, received error: {e}')
                    if self.shutdown_received():
                        break
                    else:
                        continue
                
                # TODO - create own implementation of queue_listener to add timeout
                # get messages from local queue
                if not log_message:
                    log_message = self.queue_listener.dequeue(block=True)

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
                
                if self.shutdown_received():
                    break
        except KeyboardInterrupt:
            self.logger.info('Keyboard interrupt received!')
            self.mq_disconnect()


