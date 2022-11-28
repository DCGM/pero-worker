#!/usr/bin/env python3

# Worker controller for coordination and configuration

import sys
import os
import logging
import time
import datetime
import uuid
import traceback
import configparser
import threading
import zipfile
import tarfile

# load image and data for processing
import cv2
import pickle
import magic
import numpy as np
import torch

# zookeeper
from kazoo.client import KazooClient
from kazoo.handlers.threading import KazooTimeoutError
from kazoo.protocol.states import KazooState
import kazoo.exceptions

# constants
import worker_functions.constants as constants
import worker_functions.connection_aux_functions as cf
from worker_functions.sftp_client import SFTP_Client
from worker_functions.zk_client import ZkClient

# setup logging (required by kazoo)
log_formatter = logging.Formatter('%(asctime)s WORKER %(levelname)s %(message)s')

# use UTC time in log
log_formatter.converter = time.gmtime

stderr_handler = logging.StreamHandler()
stderr_handler.setFormatter(log_formatter)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
# TODO - remove debug level
logger.setLevel(logging.DEBUG)
logger.addHandler(stderr_handler)
logger.propagate = False

class WorkerController(ZkClient):
    """
    Worker controller - manages configuration and performes coordination of processing.
    Uses Apache Zookeeper for coordiantion.
    """
    def __init__(
        self,
        zookeeper_servers,
        username='',
        password='',
        ca_cert=None,
        worker_id=None,
        tmp_directory=None,
        logger=logging.getLogger(__name__)
    ):
        """
        Initializes worker controller
        :param zookeeper_servers: list of zookeeper servers
        :param username: username for authentication with zookeeper servers
        :param password: password for authentication with zookeeper servers
        :param ca_cert: path to CA certificate to verify SSL/TLS connection
        :param worker_id: worker identifier for identification in zookeeper
        :param tmp_directory: path to directory where temporary files will be stored
        :param logger: logger instance to use for logging
        """

        # initialize zookeeper client
        super().__init__(
            zookeeper_servers = zookeeper_servers,
            username = username,
            password = password,
            ca_cert = ca_cert,
            logger = logger
        )

        # worker config
        self.worker_id = worker_id
        self.tmp_directory = tmp_directory

        # stage cofig
        self.stage = ''

        # synchronization
        self.switch_stage_lock = threading.Lock()
        self.stage_config_version = -1  # version of zookeeper node containing stage config
        self.init_complete = False  # indicates if worker already registered itself in zookeeper
        # zookeeper locks
        self.stage_stats_lock = None  # lock for updating statistics about current queue
    
    def create_tmp_dir(self):
        """
        Creates tmp directory
        """
        if not self.tmp_directory:
            return
        
        if os.path.isdir(self.tmp_directory):
            return
        
        os.makedirs(self.tmp_directory)
    
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
    
    def sftp_connect(self):
        """
        Creates new SFTP connection
        """
        self.ftp_servers_lock.acquire()
        sftp = SFTP_Client(self.ftp_servers, self.username, self.password, self.logger)
        self.ftp_servers_lock.release()
        sftp.sftp_connect()
        return sftp
    
    def init_zookeeper_connection_status(self):
        """
        Creates ephemeral node in zookeeper indicating that worker is connected.
        """
        self.zk.create(
            constants.WORKER_STATUS_CONNECTED_TEMPLATE.format(worker_id = self.worker_id),
            'true'.encode('utf-8'),
            ephemeral = True  # ZK automaticaly deletes node when worker disconnects
        )

    def init_worker_status(self):
        """
        Initializes state of worker in zookeeper
        :raise: NodeExistsError if worker with this id is already defined in zookeeper
        :raise: ZookeeperError if server returns non zero value
        :raise: ValueError if worker id is duplicate
        """
        if self.worker_id:
            if self.zk.exists(
                constants.WORKER_STATUS_ID_TEMPLATE.format(worker_id = self.worker_id)
                )\
                and\
                self.zk.exists(
                constants.WORKER_STATUS_CONNECTED_TEMPLATE.format(worker_id = self.worker_id)
                ):
                    raise ValueError(f'Worker ID {self.worker_id} is duplicate to other worker!')
        else:
            self.worker_id = uuid.uuid4().hex
            while self.zk.exists(
                constants.WORKER_STATUS_ID_TEMPLATE.format(worker_id = self.worker_id)
                ):
                    self.worker_id = uuid.uuid4().hex

        # set worker initial status
        self.zk.ensure_path(
            constants.WORKER_STATUS_TEMPLATE.format(worker_id = self.worker_id),
            self.status.encode('utf-8')
        )
        self.zk.ensure_path(
            constants.WORKER_QUEUE_TEMPLATE.format(worker_id = self.worker_id)
        )
        # set worker to enabled state
        self.zk.ensure_path(
            constants.WORKER_ENABLED_TEMPLATE.format(worker_id = self.worker_id),
            'true'.encode('utf-8')
        )
        self.zk.ensure_path(
            constants.WORKER_UNLOCK_TIME.format(worker_id = self.worker_id)
        )
        self.init_zookeeper_connection_status()
        self.init_complete = True
    
    def connection_state_listener(self, state):
        """
        Handles reconnection to zookeeper state changes.
        :param state: zookeeper state passed in when connection event occures
        """
        if state == KazooState.CONNECTED:
            if self.worker_id and self.init_complete:
                self.init_zookeeper_connection_status()
    
    def set_mq_server_list(self, servers):
        """
        Sets list of mq broker servers
        :param servers: list of servers
        """
        self.mq_server_lock.acquire()
        self.mq_servers = cf.server_list(servers)
        self.mq_server_lock.release()
    
    def set_ftp_server_list(self, servers):
        """
        Sets list of ftp servers
        :param servers: list of servers
        """
        self.ftp_servers_lock.acquire()
        self.ftp_servers = cf.server_list(servers)
        self.ftp_servers_lock.release()
    
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
            # TODO
            # add processing worker shutdown and queue logger shutdown
            # add main thread release / send signal to main thread to shutdown
    
    def zk_callback_switch_stage(self, data, status, *args):
        """
        Zookeeper callback reacting to notification to switch stage
        :param data: new stage name as byte string
        :param status: status of the zookeeper stage node
        :param args: additional arguments (like event)
        """
        # get new stage
        stage = data.decode('utf-8')

        # prevent multiple callbacks to switch stage at once
        self.switch_stage_lock.acquire()

        # check if stage wasn't switched again during waiting
        if self.stage_config_version >= status.version:
            self.switch_stage_lock.release()
            return
        self.stage_config_version = status.version

        # check if there is something to do
        if stage == self.stage:
            self.switch_stage_lock.release()
            return
        
        self.update_status(constants.STATUS_CONFIGURING)
        self.stop_processing()

        self.logger.info(f'Switching stage to {stage}')
        self.stage = stage
        
        # get lock for given stage statistic from the zookeeper
        try:
            self.stage_stats_lock = self.zk.Lock(
                constants.QUEUE_STATS_AVG_MSG_TIME_LOCK_TEMPLATE.format(queue_name = self.queue),
                identifier=self.worker_id
            )
        except Exception:  # kazoo.exceptions.ZookeeperError
            self.update_status(constants.STATUS_FAILED)
            self.logger.error(
                'Failed to switch stage to {}, could not get lock for average message time statistics!'
                .format(self.stage)
            )
            self.logger.error('Received error:\n{}'.format(traceback.format_exc()))
            return
        
        # TODO
        # load OCR model and config
        try:
            self.configure_ocr(stage)
        except Exception as e:
            self.logger.error(f'Failed to get configuration for processing stage {stage}!')
            self.logger.debug('Received error:\n{}'.format(traceback.format_exc()))
            self.update_status(constants.STATUS_CONFIGURATION_FAILED)
            return
        
        self.logger.info(f'Stage switched to {stage}')
        self.update_status(constants.STATUS_IDLE)
        # start processing new stage
        self.start_processing(stage)
    
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

        # initialize worker status and sync with zookeeper
        try:            
            self.init_worker_status()
        except kazoo.exceptions.ZookeeperError:
            self.logger.critical('Failed to register worker in zookeeper!')
            self.logger.critical('Received error:\n{}'.format(traceback.format_exc()))
            return 1
        
        self.logger.info('Worker id: {}'.format(self.worker_id))
        
        # setup tmp directory
        if not self.tmp_directory:
            self.tmp_directory = f'/tmp/pero-worker-{self.worker_id}'
        self.create_tmp_dir()

        # get MQ and FTP server list before connecting to MQ
        try:
            self.set_mq_server_list(
                self.zk.get_children(constants.WORKER_CONFIG_MQ_SERVERS)
            )
            self.set_ftp_server_list(
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
            func=self.set_mq_server_list
        )
        # update ftp server list
        self.zk.ChildrenWatch(
            path=constants.WORKER_CONFIG_FTP_SERVERS,
            func=self.set_ftp_server_list
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
        # connection listener
        self.zk.add_listener(self.connection_state_listener)

        if self.queue:
            self.update_status(constants.STATUS_PROCESSING)
        else:
            self.update_status(constants.STATUS_IDLE)

        self.logger.info('Worker is running')
        
        # TODO
        # main loop to check workers and queue logger and handle errors
        
        return 0