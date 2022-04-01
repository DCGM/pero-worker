#!/usr/bin/env python3

# MQ client

import pika
import logging

import worker_functions.connection_aux_functions as cf

class MQClient:
    """
    Blocking MQ client
    """
    def __init__(self, mq_servers = [], logger = logging.getLogger(__name__)):
        # list of mq servers
        self.mq_servers = mq_servers
        # logger
        self.logger = logger

        # connection properties
        self.mq_connection = None
        self.mq_channel = None
    
    def mq_connect(self, heartbeat = 60):
        """
        Connect to message broker
        """
        for server in self.mq_servers:
            try:
                self.logger.info('Connectiong to MQ server {}'.format(cf.ip_port_to_string(server)))
                self.mq_connection = pika.BlockingConnection(
                    pika.ConnectionParameters(
                        host=server['ip'],
                        port=server['port'] if server['port'] else pika.ConnectionParameters.DEFAULT_PORT,
                        heartbeat=heartbeat
                    )
                )
                self.logger.info('Opening channel to MQ.')
                self.mq_channel = self.mq_connection.channel()
            except pika.exceptions.AMQPError as e:
                self.logger.error('Connection failed! Received error: {}'.format(e))
                continue
            else:
                self.logger.info('MQ connection and channel opened successfully!')
                break
    
    def mq_disconnect(self):
        """
        Stops connection to message broker
        """
        if self.mq_channel and self.mq_channel.is_open:
            self.mq_channel.close()
        
        if self.mq_connection and self.mq_connection.is_open:
            self.mq_connection.close()
    
    def __del__(self):
        self.mq_disconnect()
