#!/usr/bin/env python3

# Worker controller for coordination and configuration

import os
import logging
import time
import uuid
import traceback
import threading
import copy

# zookeeper
from kazoo.client import KazooClient
from kazoo.handlers.threading import KazooTimeoutError
from kazoo.protocol.states import KazooState
from kazoo.retry import KazooRetry
import kazoo.exceptions

# constants
import worker_functions.constants as constants
import worker_functions.connection_aux_functions as cf
from worker_functions.sftp_client import SFTP_Client
from worker_functions.zk_client import ZkClient

try:
    from worker.cache import OCRFileCache
    from worker.processing_worker import ProcessingWorker
    from worker.request_processor import get_request_processor
    from worker.mq_logger import MQLogger
except ModuleNotFoundError:
    from cache import OCRFileCache
    from processing_worker import ProcessingWorker
    from request_processor import get_request_processor
    from mq_logger import MQLogger

# abstract class def
from abc import ABC, abstractmethod

class WorkerController(ABC):
    """
    Worker controller - manages configuration and performes coordination of processing.
    """

    worker_id = None
    enabled = False

    @abstractmethod
    def get_mq_servers(self):
        """
        Returns list of MQ servers where processing request are downloaded from.
        :return: list of MQ servers
        """
        pass

    @abstractmethod
    def update_status(self, status):
        """
        Receives status of the processing worker.
        :param status: processing status defined in constants.STATUS_*
        """
        pass

    @abstractmethod
    def update_statistics(self, stage, processed_request_count, total_processing_time):
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
        cache_dir='/tmp',
        mq_log_queue='log',
        disable_remote_logging = False,
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
        :param mq_log_queue: name of log queue on MQ
        :param disable_mq_logging: if set to True worker will not log to MQ log queue
        :param logger: logger instance to use for logging
        """

        # initialize zookeeper client
        super(WorkerController, self).__init__(
            zookeeper_servers = zookeeper_servers,
            username = username,
            password = password,
            ca_cert = ca_cert,
            logger = logger,
            connection_retry=KazooRetry(
                max_tries=0,
                delay=0,
                max_delay=0,
                ignore_expire=True
            )
        )

        # auth config
        self.username = username
        self.password = password
        self.ca_cert = ca_cert

        # worker config
        self.worker_id = worker_id
        self.cache_dir = cache_dir
        self.ocr_file_cache = None
        self.status = constants.STATUS_STARTING

        # stage cofig
        self.request_processor = None
        self.processing_worker = None

        # MQ logger
        self.mq_logger = None
        self.mq_log_queue = mq_log_queue
        self.disable_mq_logging = disable_remote_logging

        # threads
        self.processing_thread = None
        self.logging_thread = None

        # connection data copies
        self.ftp_servers = []
        self.mq_servers = []
        self.mq_server_lock = threading.Lock()  # guards mq_servers list
        self.ftp_servers_lock = threading.Lock()

        # synchronization
        self.status_lock = threading.Lock()
        self.switch_stage_lock = threading.Lock()
        self.enabled_lock = threading.Lock()
        self.shutdown_lock = threading.Lock()
        self.stage_node_version = -1  # version of zookeeper node containing current stage
        self.shutdown = False  # flag for remote shutdown call
        # zookeeper locks
        self.stage_stats_locks = {}  # locks for updating statistics for given stage
        # guard for lock update - prevents using locks in self.stage_stats_locks when beeing changed
        self.stage_stats_locks_guard_lock = threading.Lock()
    
    def sftp_connect(self):
        """
        Creates new SFTP connection
        """
        self.ftp_servers_lock.acquire()
        sftp = SFTP_Client(copy.deepcopy(self.ftp_servers), self.username, self.password, self.logger)
        self.ftp_servers_lock.release()
        sftp.sftp_connect()
        return sftp
    
    def get_status(self, timeout=-1):
        """
        Get current status.
        :return: self.status
        """
        if self.status_lock.acquire(timeout=timeout):
            status = self.status
            self.status_lock.release()
        else:
            status = 'UNKNOWN'
        return status
    
    def update_status(self, status=None):
        """
        Updates worker status
        :param status: new status (if omited, just updates current status to zookeeper)
        :raise NoNodeError if status node path is not initialized
        :raise ZookeeperError if server returns non zero value
        """
        self.status_lock.acquire()
        if status:
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
        :raise NodeExistsError if worker with this id is already defined in zookeeper
        :raise ZookeeperError if server returns non zero value
        :raise ValueError if worker id is duplicate
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

        self.zk.ensure_path(
            constants.WORKER_STATUS_TEMPLATE.format(worker_id = self.worker_id)
        )
        self.zk.ensure_path(
            constants.WORKER_QUEUE_TEMPLATE.format(worker_id = self.worker_id)
        )
        self.zk.ensure_path(
            constants.WORKER_ENABLED_TEMPLATE.format(worker_id = self.worker_id)
        )
        self.zk.ensure_path(
            constants.WORKER_UNLOCK_TIME.format(worker_id = self.worker_id)
        )
        # set worker initial status
        self.zk.set(
            path=constants.WORKER_STATUS_TEMPLATE.format(worker_id = self.worker_id),
            value=self.status.encode('utf-8')
        )
        # set worker to enabled state
        self.zk.set(
            path=constants.WORKER_ENABLED_TEMPLATE.format(worker_id = self.worker_id),
            value='true'.encode('utf-8')
        )
        self.init_zookeeper_connection_status()
    
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
    
    def processing_enabled(self):
        """
        Returns if processing is currently enabled.
        :return: bool True if processing is enabled, false otherwise
        """
        self.enabled_lock.acquire()
        enabled = self.enabled
        self.enabled_lock.release()
        return enabled
    
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
    
    def get_mq_servers(self):
        """
        Returns list of MQ servers where processing request are downloaded from.
        :return: list of MQ servers
        """
        self.mq_server_lock.acquire()
        servers = copy.deepcopy(self.mq_servers)
        self.mq_server_lock.release()
        return servers
    
    def zk_callback_shutdown(self, data, status, *args):
        """
        Shutdown the worker if set to disabled state.
        :param data: shutdown notification
        :param status: new zookeeper node status
        :param args: additional arguments (like event)
        """
        # enabled indicates if worker is enabled, not just processing
        enabled = data.decode('utf-8').lower()
        if enabled != 'true':
            self.logger.info('Shutdown signal received!')
            self.shutdown_lock.acquire()
            self.shutdown = True
            self.shutdown_lock.release()
    
    def shutdown_received(self):
        """
        Returns if worker has received shutdown command.
        :return: bool True if worker received remote shutdown call else False
        """
        self.shutdown_lock.acquire()
        shutdown = self.shutdown
        self.shutdown_lock.release()
        return shutdown
    
    def update_statistics(self, stage, processed_request_count, total_processing_time):
        """
        Updates statistics for given stage.
        :param stage: name of stage of processed requests
        :param processed_request_count: number of processed requests from given stage
        :param total_processing_time: total processing time of given number of requests
        """
        if not processed_request_count:
            return
        
        self.stage_stats_locks_guard_lock.acquire()

        avg_msg_time = total_processing_time / processed_request_count
        self.logger.debug(f'Updating stage statistics for stage {stage}')

        # Zookeeper lock deadlock workaround
        # retries to acquire the lock when first acquisition fails
        retry = 2
        while True:
            try:
                self.stage_stats_locks[stage].acquire(timeout = 20)
            except kazoo.exceptions.LockTimeout:
                self.logger.warning(f'Failed to acquire lock for time statistics for stage {stage}!')
                retry -= 1
                if retry <= 0:
                    self.logger.error(f'Failed to update statistics for stage {stage}!')
                    self.stage_stats_locks_guard_lock.release()
                    return
                else:
                    self.logger.info(f'Trying to acquire lock again')
            except kazoo.exceptions.ConnectionClosedError:
                self.logger.error(f'Failed to update statistics for stage {stage} due to zookeeper connection error!')
                self.stage_stats_locks_guard_lock.release()
                return
            except Exception:
                self.logger.error(f'Failed to update statistics for stage {stage}!')
                self.logger.error(f'Received error:\n{traceback.format_exc()}')
                self.stage_stats_locks_guard_lock.release()
                return
            else:
                break
        
        try:
            # get average processing time from zookeeper
            zk_msg_time = self.zk.get(
                constants.QUEUE_STATS_AVG_MSG_TIME_TEMPLATE
                .format(queue_name = stage)
            )[0].decode('utf-8')

            # calculate new stage average processing time
            if zk_msg_time:
                zk_msg_time = float.fromhex(zk_msg_time)
                avg_msg_time = avg_msg_time + zk_msg_time / 2
            
            # update statistics in zookeeper
            self.zk.set(
                path=constants.QUEUE_STATS_AVG_MSG_TIME_TEMPLATE.format(queue_name=stage),
                value=float(avg_msg_time).hex().encode('utf-8')
            )
        except kazoo.exceptions.ZookeeperError:
            self.logger.error(
                f'Failed to update average processing time for stage {stage} due to zookeeper error!'
            )
            self.logger.debug(f'Received error:\n{traceback.format_exc()}')
        except ValueError:
            self.logger.error(
                f'Failed to update average processing time for stage {stage} due to wrong number format in zookeeper!'
            )
        except Exception:
            self.logger.error(
                f'Failed to update average processing time for stage {stage} due to unknown error!'
            )
            self.logger.error(f'Received error:\n{traceback.format_exc()}')
        else:
            self.logger.info(f'Statistics for stage {stage} updated!')
        finally:
            self.stage_stats_locks[stage].release()
            self.stage_stats_locks_guard_lock.release()
    
    def start_processing(self):
        """
        Runs processing with current configuration.
        """
        if self.processing_thread:
            if self.processing_thread.is_alive():
                self.disable_processing()
            self.processing_thread.join()

        self.processing_thread = threading.Thread(
            target=self.processing_worker.run,
            args=[self.request_processor]
        )
        self.enable_processing()
        self.processing_thread.start()
    
    def re_run_processing(self):
        """
        Runs processing with current configuration again.
        """
        if self.processing_enabled():
            if not self.processing_thread.is_alive():
                self.start_processing()
        else:
            self.start_processing()
    
    def add_stage_to_cache(self, stage, version):
        """
        Adds stage configuration and ocr model to cache.
        :param stage: stage name
        :param version: stage version
        :raise kazoo.exceptions.ZookeeperError if fails to get configuration from zookeeper
        :raise ConnectionError if fails to connect to sftp servers
        """
        config = self.zk.get(constants.QUEUE_CONFIG_TEMPLATE.format(queue_name=stage))[0].decode('utf-8')
        try:
            ftp_path = self.zk.get(constants.QUEUE_CONFIG_PATH_TEMPLATE.format(queue_name=stage))[0].decode('utf-8')
        except kazoo.exceptions.NoNodeError:
            # model not required (dummy configuration)
            ftp_path = ''
        
        # raises OSError and some others related to filesystem and connection operations
        self.ocr_file_cache.add_stage(
            stage=stage,
            config=config,
            version=version,
            sftp_path=ftp_path,
            sftp_client=self.sftp_connect() if ftp_path else None
        )

    def configure_processing(self, stage, version):
        """
        Configures processing for given stage.
        :param stage: stage name
        :param version: stage version
        """
        try:
            current_version = self.ocr_file_cache.get_stage_version(stage)
        except ValueError:
            # stage not in cache
            current_version = ''
        
        if version != current_version or version == '':
            # raises ConnectionErrors, OSErrors, ...
            self.add_stage_to_cache(stage, version)
        
        self.request_processor = get_request_processor(
            stage=stage,
            config=self.ocr_file_cache.get_stage_config(stage),
            config_path=self.ocr_file_cache.get_stage_data_path(stage),
            config_version=version,
            logger=self.logger  # TODO - change to processing specific logger
        )

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

        # if worker is shuting down, do not change state
        if self.shutdown_received():
            self.switch_stage_lock.release()
            return

        # check if stage wasn't switched again during waiting
        if self.stage_node_version >= status.version:
            self.switch_stage_lock.release()
            return
        
        # check if stage name is not empty - processing disabled
        if not stage:
            self.disable_processing()
            self.switch_stage_lock.release()
            return
        
        self.stage_node_version = status.version

        # Load stage version
        try:
            stage_version = self.zk.get(
                    constants.QUEUE_CONFIG_VERSION_TEMPLATE
                    .format(queue_name = stage)
                )[0].decode('utf-8')
        except kazoo.exceptions.ZookeeperError:
            self.update_status(constants.STATUS_CONFIGURATION_FAILED)
            self.logger.warning(
                f'Failed to get version of stage {stage} configuration from zookeeper!'
            )
            stage_version = ''

        # check if there is something to do
        if self.request_processor and stage_version \
            and stage == self.request_processor.stage \
            and stage_version == self.request_processor.config_version:
                self.logger.info(f'Running processing for stage {self.request_processor.stage} again')
                self.re_run_processing()
                self.switch_stage_lock.release()
                return
        
        self.update_status(constants.STATUS_CONFIGURING)
        self.logger.info(f'Switching stage to {stage}')
        
        # remove locks for unused stages
        # (keep current stage lock for updating statistics when processing finish)
        self.stage_stats_locks_guard_lock.acquire()
        if self.request_processor:
            for key in list(self.stage_stats_locks.keys()):
                if key != self.request_processor.stage:
                    self.stage_stats_locks.pop(key)

        # add lock for new stage
        if not self.request_processor or stage != self.request_processor.stage:
            try:
                self.stage_stats_locks[stage] = self.zk.Lock(
                    constants.QUEUE_STATS_AVG_MSG_TIME_LOCK_TEMPLATE.format(queue_name = stage),
                    identifier=self.worker_id
                )
            except kazoo.exceptions.ZookeeperError:
                self.update_status(constants.STATUS_CONFIGURATION_FAILED)
                self.logger.error(
                    f'Failed to switch stage to {stage}, could not get lock for average message time statistics!'
                )
                self.logger.debug(f'Received error:\n{traceback.format_exc()}')
                self.switch_stage_lock.release()
                self.stage_stats_locks_guard_lock.release()
                return
        
        self.stage_stats_locks_guard_lock.release()
        
        self.disable_processing()
        
        # load OCR model and config
        try:
            self.configure_processing(stage, stage_version)
        except KeyboardInterrupt:
            self.switch_stage_lock.release()
            raise
        except Exception as e:
            self.logger.error(f'Failed to get configuration for processing stage {stage}!')
            self.logger.debug(f'Received error:\n{traceback.format_exc()}')
            self.update_status(constants.STATUS_CONFIGURATION_FAILED)
            self.logger.error('Removing stage configuration from local cache!')
            self.ocr_file_cache.remove_stage(stage)
            self.switch_stage_lock.release()
            return
        
        self.logger.info(f'Stage switched to {stage}')
        self.update_status(constants.STATUS_IDLE)
        # start processing new stage
        self.start_processing()
        self.switch_stage_lock.release()
    
    def run_mq_logger(self):
        """
        Starts MQ logger activity in background thread.
        """
        if self.disable_mq_logging:
            return
        
        self.logging_thread = threading.Thread(
            target=self.mq_logger.run
        )
        self.logging_thread.start()
    
    def setup_mq_logger(self, log_formatter):
        """
        Initializes MQ logger.
        """
        if self.disable_mq_logging:
            return
        
        self.mq_logger = MQLogger(
            controller = self,
            username = self.username,
            password = self.password,
            ca_cert = self.ca_cert,
            mq_log_queue = self.mq_log_queue,
            log_formatter = log_formatter,
            logger = self.logger
        )
    
    def setup_logger(self):
        """
        Adds worker id and hostname to log messages.
        """
        host_worker_id = f'{os.uname().nodename} {self.worker_id}'
        log_formatter = logging.Formatter('%(asctime)s ' + host_worker_id + ' %(levelname)s %(message)s')
        # use UTC time in log
        log_formatter.converter = time.gmtime
        worker_handler = logging.StreamHandler()
        worker_handler.setFormatter(log_formatter)
        self.logger.addHandler(worker_handler)
        self.logger.propagate = False
        self.setup_mq_logger(log_formatter)
    
    def register_zookeeper_callbacks(self):
        """
        Registers zookeeper callbacks for getting status updates.
        """
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
            func=self.zk_callback_switch_stage
        )
        # shutdown callback
        self.zk.DataWatch(
            path=constants.WORKER_ENABLED_TEMPLATE.format(worker_id=self.worker_id),
            func=self.zk_callback_shutdown
        )
    
    def recover_zookeeper_locks(self):
        """
        recovers state of distributed locks after zookeeper connection failure
        """
        with self.stage_stats_locks_guard_lock:
            for stage in list(self.stage_stats_locks.keys()):
                try:
                    self.stage_stats_locks[stage] = self.zk.Lock(
                        constants.QUEUE_STATS_AVG_MSG_TIME_LOCK_TEMPLATE.format(queue_name = stage),
                        identifier=self.worker_id
                    )
                except kazoo.exceptions.ZookeeperError:
                    self.logger.error(f'Failed to recover stage lock for stage {stage}!')
    
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

        # setup logging format
        self.setup_logger()

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
        
        # run mq logger
        self.run_mq_logger()

        # init processing worker
        self.processing_worker = ProcessingWorker(
            controller=self,
            username=self.username,
            password=self.password,
            ca_cert=self.ca_cert,
            logger=self.logger
        )

        self.update_status(constants.STATUS_IDLE)

        self.register_zookeeper_callbacks()

        self.logger.info('Worker is running')

        try:
            while not self.shutdown_received():
                # TODO
                # check workers and queue logger and handle errors

                # debug messages
                self.logger.debug('Periodic info:')
                self.logger.debug(f'Zookeeper connected: {self.zk.connected}')
                self.logger.debug(f'Status: {self.get_status(timeout=10)}')
                if not self.zk.connected:
                    try:
                        self.zk_connect()
                    except KazooTimeoutError:
                        self.logger.error('Failed to connect to zookeeper, next retry in 1 minute!')
                    else:
                        # recover watchs
                        self.register_zookeeper_callbacks()
                        # recover locks
                        self.recover_zookeeper_locks()
                        # recover worker status in zookeeper
                        self.init_zookeeper_connection_status()
                        self.update_status()
                        self.logger.info('Connection to zookeeper recovered!')
                
                if not self.disable_mq_logging and not self.logging_thread.is_alive():
                    self.logging_thread.join(10)
                    self.run_mq_logger()
                    self.logger.warning('MQ logger was recovered from failure!')

                time.sleep(60)
        except KeyboardInterrupt:  # shutdown via SigInt signal
            with self.shutdown_lock:
                self.shutdown = True
            self.logger.info('Interrupt received, shutting down!')
        
        # wait until last stage is switched
        self.switch_stage_lock.acquire()
        self.switch_stage_lock.release()

        self.disable_processing()
        
        # wait for processing worker to finish current task
        if self.processing_thread:
            self.processing_thread.join(timeout=5*60)
        
        # stop MQ logger
        if self.logging_thread:
            self.logging_thread.join(timeout=5*60)

        # TODO
        # kill processing worker if can't finish before timeout

        # TODO
        # cleanup MQ logger

        self.update_status(constants.STATUS_DEAD)

        # Run destructors explicitly - otherwise might not be called
        # as finalizers in python are not guaranteed to run
        # run cache destructor
        del self.ocr_file_cache
        # run processing worker destructor
        del self.processing_worker
        # run MQ logger destructor
        del self.mq_logger

        return 0
