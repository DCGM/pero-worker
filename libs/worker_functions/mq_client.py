#!/usr/bin/env python3

# MQ client

import pika
import logging
import ssl

import worker_functions.connection_aux_functions as cf
import worker_functions.constants as constants

class MQClient:
    """
    Blocking MQ client
    """
    def __init__(self, mq_servers = [], username = '', password = '', ca_cert = None, logger = logging.getLogger(__name__)):
        # list of mq servers
        self.mq_servers = mq_servers

        # ssl/tls
        if ca_cert:
            context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
            context.verify_mode = ssl.CERT_REQUIRED
            context.load_verify_locations(ca_cert)
            self.ssl_options = pika.SSLOptions(context)
        else:
            self.ssl_options = pika.connection.ConnectionParameters.DEFAULT_SSL_OPTIONS

        # authentication
        if username:
            self.mq_auth = pika.credentials.PlainCredentials(str(username), str(password))
        else:
            self.mq_auth = pika.connection.ConnectionParameters.DEFAULT_CREDENTIALS

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
                        port=server['port'] if server['port'] else pika.connection.ConnectionParameters.DEFAULT_PORT,
                        ssl_options=self.ssl_options,
                        credentials=self.mq_auth,
                        heartbeat=heartbeat,
                        virtual_host=constants.MQ_VHOST
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
