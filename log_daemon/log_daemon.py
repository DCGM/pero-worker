#!/usr/bin/env python3

import os
import sys
import logging
import time
import threading
import copy
import datetime
import traceback
import argparse
import configparser

import pika

import worker_functions.connection_aux_functions as cf
import worker_functions.constants as constants
from worker_functions.mq_client import MQClient
from worker_functions.zk_client import ZkClient

class LogDaemonZkClient(ZkClient):
    """
    Client for obtaining configuration from zookeeper
    """
    def __init__(self, zookeeper_servers, username = '', password = '', ca_cert = None, logger = logging.getLogger(__name__)):
        super(LogDaemonZkClient, self).__init__(
            zookeeper_servers=zookeeper_servers,
            username=username,
            password=password,
            ca_cert=ca_cert,
            logger=logger
        )
        # list of MQ servers
        self.mq_servers = []
        # lock for MQ servers getters/setters
        self.mq_server_lock = threading.Lock()

    def set_mq_server_list(self, servers):
        """
        Sets list of mq broker servers
        :param servers: list of servers
        """
        self.mq_server_lock.acquire()
        self.mq_servers = cf.server_list(servers)
        self.mq_server_lock.release()
    
    def get_mq_servers(self):
        """
        Returns list of MQ servers where processing request are downloaded from.
        :return: list of MQ servers
        """
        self.mq_server_lock.acquire()
        servers = copy.deepcopy(self.mq_servers)
        self.mq_server_lock.release()
        return servers
    
    def run(self):
        """
        Start ZK client activity.
        """
        self.zk_connect()
        # register callback for getting MQ server configuration
        self.zk.ChildrenWatch(
            path=constants.WORKER_CONFIG_MQ_SERVERS,
            func=self.set_mq_server_list
        )

class LogDaemon(MQClient):
    """
    Collects logs from message broker and saves them to file.
    """
    def __init__(
        self,
        zookeeper_servers = [],
        username = '',
        password = '',
        ca_cert = None,
        log_queue = 'log',
        output_directory = 'logs/',
        log_rotation_period = 10080,  # weekly
        number_of_files = 0,
        logger = logging.getLogger(__name__)
    ):
        """
        Initializes LogDaemon.
        :param zookeeper_servers: list of zookeeper servers to connect to
        :param username: username for zookeeper and MQ authentication
        :param password: password for zookeeper and MQ authentication
        :param ca_cert: CA certificate for zookeeper and MQ authentication
        :param log_queue: log queue from where to receive log messages
        :param output_directory: output directory where to store received logs
        :param log_rotation_period: time when old log file will be closed and newone created (in minutes)
        :param logger: logger to use for logging own log messages
        """
        # init MQClient
        super(LogDaemon, self).__init__(
            mq_servers=[],
            username=username,
            password=password,
            ca_cert=ca_cert,
            logger=logger
        )

        self.log_queue = log_queue
        self.output_directory = output_directory
        self.log_file = None
        self.log_file_creation_timestamp = None
        self.log_rotation_period = datetime.timedelta(minutes=log_rotation_period)
        self.number_of_files = number_of_files

        if not os.path.isdir(self.output_directory):
            os.makedirs(self.output_directory, exist_ok=True)

        # zookeeper client for obtaining list of MQ servers
        self.zk_client = LogDaemonZkClient(
            zookeeper_servers=zookeeper_servers,
            username=username,
            password=password,
            ca_cert=ca_cert,
            logger=logger
        )
    
    def remove_old_logs(self):
        """
        Removes old log files, keeps at most 'self.number_of_files' logs.
        :raise: OSError if file cannot be removed
        """
        if not self.number_of_files:
            return
        
        logs = os.listdir(self.output_directory)
        logs.sort()
        while len(logs) > self.number_of_files:
            os.unlink(os.path.join(self.output_directory, logs.pop(0)))
    
    def save_log_message(self, log_message):
        """
        Saves received log message to file.
        :param log_message: log message to save
        """
        if not self.log_file or datetime.datetime.now(datetime.timezone.utc) - self.log_file_creation_timestamp > self.log_rotation_period:
            if self.log_file:
                self.log_file.close()
                self.remove_old_logs()
            self.log_file_creation_timestamp = datetime.datetime.now(datetime.timezone.utc)
            # line buffering=1 -- line buffered file (required for flushing buffer to log after each write)
            self.log_file = open(os.path.join(self.output_directory, f'pero_{self.log_file_creation_timestamp.isoformat()}.log'), 'a', buffering=1)
        self.log_file.write(f'{log_message}\n')

    def process_log_message(self, channel, method, properties, body):
        """
        MQ callback for queue consumption - saves log messages to output directory
        :param channel: channel instance
        :param method: delivery method
        :param properties: mq message properties
        :param body: mq message body
        """
        self.save_log_message(body.decode('utf-8'))
        self.mq_channel.basic_ack(delivery_tag=method.delivery_tag)
    
    def get_mq_servers(self):
        """
        Returns list of currently configured MQ servers.
        (Overrides default MQClient get_mq_servers.)
        :return: list of MQ servers
        """
        return self.zk_client.get_mq_servers()

    def run(self):
        """
        Starts receiving logs from MQ log queue.
        """
        self.zk_client.run()
        try:
            while True:
                try:
                    self.mq_connect_retry()
                except ConnectionError as e:
                    self.logger.error('Failed to connect to MQ servers!')
                    self.logger.debug(f'Received error: {e}')
                    continue

                self.mq_channel.basic_consume(queue=self.log_queue, on_message_callback=self.process_log_message, auto_ack=False)

                try:
                    self.mq_channel.start_consuming()
                except pika.exceptions.AMQPError as e:
                    self.logger.error('MQ connection error, recovering!')
                    self.logger.error(f'Received error: {e}')
        except KeyboardInterrupt:
            self.logger.info('Result receiving stoped!')
        except OSError:
            self.logger.error(f'Failed to create file in output direcotry {self.output_directory}!')
        except Exception:
            self.logger.error('Unknown error has occured!')
            self.logger.error(f'Received error: {traceback.format_exc()}')
        self.mq_disconnect()
        if self.log_file:
            self.log_file.close()

def parse_args():
    parser = argparse.ArgumentParser('Daemon for receiving log messages from MQ server.')
    parser.add_argument(
        '-c', '--config',
        help='Configuration file - can be used instead of listed arguments.',
        default=None
    )
    parser.add_argument(
        '-z', '--zookeeper',
        help='List of zookeeper servers from where configuration will be downloaded. If port is omitted, default zookeeper port is used.',
        nargs='+',
        default=[]
    )
    parser.add_argument(
        '-u', '--username',
        help='Username for authentication on server.',
        default=None
    )
    parser.add_argument(
        '-p', '--password',
        help='Password for user authentication.',
        default=None
    )
    parser.add_argument(
        '-e', '--ca-cert',
        help='CA Certificate for SSL/TLS connection verification.',
        default=None
    )
    parser.add_argument(
        '-q', '--log-queue',
        help='Name of log queue on MQ.',
        default=None
    )
    parser.add_argument(
        '-o', '--output-dir',
        help='Directory where logs will be saved. If directory does not exists, then will be created.',
        default=None
    )
    parser.add_argument(
        '-r', '--log-rotation-period',
        help='Log rotation period in minutes. After this time passes daemon will start logging to new file.',
        type=int,
        default=None
    )
    parser.add_argument(
        '-n', '--number-of-files',
        help='Maximum number of log files to keep.',
        type=int,
        default=0
    )
    parser.add_argument(
        '-d', '--debug',
        help='Enable debugging log output.',
        default=False,
        action='store_true'
    )
    return parser.parse_args()

def main():
    args = parse_args()

    log_formatter = logging.Formatter('%(asctime)s LOGD %(levelname)s %(message)s')
    log_formatter.converter = time.gmtime

    logd_handler = logging.StreamHandler()
    logd_handler.setFormatter(log_formatter)

    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    if args.debug:
        logger.setLevel(logging.DEBUG)
    logger.addHandler(logd_handler)

    # defaults
    zookeeper_servers = []
    username = None
    password = None
    ca_cert = None
    log_queue = 'log'
    output_directory = 'logs'
    log_rotation_period = 10080
    number_of_files = 0

    if args.config:
        config = configparser.ConfigParser()
        config.read(args.config)
        if 'LOGD' not in config:
            logger.warning('Logging daemon section not found in given config file!')
        else:
            if 'zookeeper_servers' in config['LOGD']:
                zookeeper_servers = cf.zk_server_list(config['LOGD']['zookeeper_servers'])
            if 'username' in config['LOGD']:
                username = config['LOGD']['username']
            if 'password' in config['LOGD']:
                password = config['LOGD']['password']
            if 'ca_cert' in config['LOGD']:
                ca_cert = config['LOGD']['ca_cert']
            if 'log_queue' in config['LOGD']:
                log_queue = config['LOGD']['log_queue']
            if 'output_dir' in config['LOGD']:
                output_directory = config['LOGD']['output_dir']
            if 'log_rotation_period' in config['LOGD']:
                log_rotation_period = int(config['LOGD']['log_rotation_period'])
            if 'number_of_files' in config['LOGD']:
                number_of_files = int(config['LOGD']['number_of_files'])

    # parse arguments
    if args.zookeeper:
        zookeeper_servers = cf.zk_server_list(args.zookeeper)
    if args.username:
        username = args.username
    if args.password:
        password = args.password
    if args.ca_cert:
        ca_cert = args.ca_cert
    if args.log_queue:
        log_queue = args.log_queue
    if args.output_dir:
        output_directory = args.output_dir
    if args.log_rotation_period:
        log_rotation_period = args.log_rotation_period
    if args.number_of_files:
        number_of_files = args.number_of_files
    
    # run daemon
    log_daemon = LogDaemon(
        zookeeper_servers=zookeeper_servers,
        username=username,
        password=password,
        ca_cert=ca_cert,
        log_queue=log_queue,
        output_directory=output_directory,
        log_rotation_period=log_rotation_period,
        number_of_files=number_of_files,
        logger=logger
    )
    log_daemon.run()
    return 0


if __name__ == "__main__":
    sys.exit(main())
