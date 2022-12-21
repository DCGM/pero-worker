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
from cache import OCRFileCache

# abstract class def
from abc import ABC, abstractmethod

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

class WorkerController(ABC):
    """
    Worker controller - manages configuration and performes coordination of processing.
    """

    self.worekr_id = None
    self.enabled = False

    @abstractmethod
    def get_mq_servers(self):
        """
        Returns list of MQ servers where processing request are downloaded from.
        :return: list of MQ servers
        """
        pass

    @abstractmethod
    def report_status(self, status):
        """
        Receives status of the processing worker.
        :param status: processing status defined in constants.STATUS_*
        """
        pass

    @abstractmethod
    def report_statistics(self, stage, processed_request_count, total_processing_time):
        """
        Receives processing statistics for given stage from processing worker.
        :param stage: name of stage of processed requests
        :param processed_request_count: number of processed requests from given stage
        :param total_processing_time: total processing time of given number of requests
        """
        pass

    def processing_enabled(self):
        """
        Returns if processing is currently enabled.
        :return: bool True if processing is enabled, false otherwise
        """
        return self.enabled

    def get_id(self):
        """
        Returns worker id.
        :return: worker id
        """
        return self.worker_id


class ZkWorkerController(WorkerController, ZkClient):
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
        cache_dir=None,
        logger=logging.getLogger(__name__)
    ):
        """
        Initializes worker controller
        :param zookeeper_servers: list of zookeeper servers
        :param username: username for authentication with zookeeper servers
        :param password: password for authentication with zookeeper servers
        :param ca_cert: path to CA certificate to verify SSL/TLS connection
        :param worker_id: worker identifier for identification in zookeeper
        :param cache_dir: path to directory where OCR files will be stored
        :param logger: logger instance to use for logging
        """

        # initialize zookeeper client
        super(WorkerController).__init__(
            zookeeper_servers = zookeeper_servers,
            username = username,
            password = password,
            ca_cert = ca_cert,
            logger = logger
        )

        # worker config
        self.worker_id = worker_id
        self.cache_dir = cache_dir if cache_dir else '/tmp'
        self.ocr_file_cache = None

        # stage cofig
        self.stage = ''

        # synchronization
        self.status_lock = threading.Lock()
        self.switch_stage_lock = threading.Lock()
        self.enabled_lock = threading.Lock()
        self.stage_config_version = -1  # version of zookeeper node containing stage config
        self.init_complete = False  # indicates if worker has already registered itself in zookeeper
        # zookeeper locks
        self.stage_stats_lock = None  # lock for updating statistics about current queue
    
    def sftp_connect(self):
        """
        Creates new SFTP connection
        """
        self.ftp_servers_lock.acquire()
        sftp = SFTP_Client(self.ftp_servers, self.username, self.password, self.logger)
        self.ftp_servers_lock.release()
        sftp.sftp_connect()
        return sftp
    
    def update_status(self, status):
        """
        Updates worker status
        :param status: new status
        :raise: NoNodeError if status node path is not initialized
        :raise: ZookeeperError if server returns non zero value
        """
        self.status_lock.acquire()
        self.status = status
        try:
            self.zk.set(constants.WORKER_STATUS_TEMPLATE.format(
                worker_id = self.worker_id),
                self.status.encode('utf-8')
            )
        except kazoo.exceptions.NoNodeError:
            self.logger.error('Failed to update status in zookeeper - path is not initialized!')
        except kazoo.exceptions.ZookeeperError:
            self.logger.error('Failed to update status in zookeeper - server error!')
        except Exception:
            self.logger.error('Failed to update status in zookeeper - unknown error has occured!')
            self.logger.debug(f'Received error:\n{traceback.format_exc()}')
        finally:
            self.status_lock.release()

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
    
    def disable_processing(self):
        """
        Disable processing.
        """
        self.enabled_lock.acquire()
        self.enabled = False
        self.enabled_lock.release()
    
    def enable_processing(self):
        """
        Enable processing.
        """
        self.enabled_lock.acquire()
        self.enabled = True
        self.enabled_lock.release()

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
            self.disable_processing()
            self.update_status(constants.STATUS_DEAD)
            # TODO
            # add processing worker shutdown and queue logger shutdown
            # add main thread release / send signal to main thread to shutdown
    
    def update_stage_statistics(self, processed_request_count, total_processing_time):
        """
        Updates statistics for current stage.
        :param processed_request_count: number of processed requests
        :param total_processing_time: total processing time for given number of processed requests
        """
        if not processed_request_count:
            return
        
        avg_msg_time = total_processing_time / processed_request_count
        self.logger.debug(f'Updating stage statistics for stage {self.stage}')

        # Zookeeper lock deadlock workaround
        # retries to acquire the lock when first acquisition fails
        retry = 2
        while True:
            try:
                self.stage_stats_lock.acquire(timeout = 20)
            except kazoo.exceptions.LockTimeout:
                self.logger.warning(f'Failed to acquire lock for time statistics for stage {self.stage}!')
                retry -= 1
                if retry <= 0:
                    self.logger.error(f'Failed to update statistics for stage {self.stage}!')
                    return
                else:
                    self.logger.info(f'Trying to acquire lock again')
            except Exception:
                self.logger.error(f'Failed to update statistics for stage {self.stage}!')
                self.logger.error(f'Received error:\n{traceback.format_exc()}')
                return
            else:
                break
        
        try:
            # get average processing time from zookeeper
            zk_msg_time = self.zk.get(
                constants.QUEUE_STATS_AVG_MSG_TIME_TEMPLATE
                .format(queue = self.stage)
            )[0].decode('utf-8')

            # calculate new stage average processing time
            if zk_msg_time:
                zk_msg_time = float.fromhex(zk_msg_time)
                avg_msg_time = avg_msg_time + zk_msg_time / 2
            
            # update statistics in zookeeper
            self.zk.set(
                path=constants.QUEUE_STATS_AVG_MSG_TIME_TEMPLATE.format(queue=self.stage),
                value=float(avg_msg_time).hex().encode('utf-8')
            )
        except kazoo.exceptions.ZookeeperError:
            self.logger.error(
                f'Failed to update average processing time for stage {self.stage} due to zookeeper error!'
            )
            self.logger.error(f'Received error:\n{traceback.format_exc()}')
        except ValueError:
            self.logger.error(
                f'Failed to update average processing time for stage {self.stage} due to wrong number format in zookeeper!'
            )
        except Exception:
            self.logger.error(
                f'Failed to update average processing time for stage {self.stage} due to unknown error!'
            )
            self.logger.error(f'Received error:\n{traceback.format_exc()}')
        else:
            self.logger.debug('Statistics updated!')
        finally:
            self.stage_stats_lock.release()
    
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
        self.disable_processing()

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
        # Create persistent cache if worker id is persistent
        if self.worker_id:
            self.ocr_file_cache = OCRFileCache(
                cache_dir=os.path.join(f'{self.cache_dir}', f'pero-worker-{self.worker_id}'),
                auto_cleanup=False
            )
        
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

        # create auto-removable cache if worker id is auto-generated
        if not self.ocr_file_cache:
            self.ocr_file_cache = OCRFileCache(
                cache_dir=os.path.join(f'{self.cache_dir}', f'pero-worker-{self.worker_id}'),
                auto_cleanup=True
            )

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

        # TODO
        # add logging to MQ

        if self.queue:
            self.update_status(constants.STATUS_PROCESSING)
        else:
            self.update_status(constants.STATUS_IDLE)

        self.logger.info('Worker is running')
        
        # TODO
        # main loop to check workers and queue logger and handle errors
        
        return 0