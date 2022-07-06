#!/usr/bin/env python3

# constants for pero worker

# zookeeper paths

PERO_PATH = '/pero'

WORKER_STATUS = PERO_PATH + '/worker/status'
WORKER_STATUS_ID_TEMPLATE = WORKER_STATUS + '/{worker_id}'
WORKER_STATUS_TEMPLATE = WORKER_STATUS_ID_TEMPLATE + '/status'  # current worker status
WORKER_QUEUE_TEMPLATE = WORKER_STATUS_ID_TEMPLATE + '/queue'  # input queue
WORKER_ENABLED_TEMPLATE = WORKER_STATUS_ID_TEMPLATE + '/enabled'  # worker enabled
WORKER_UNLOCK_TIME = WORKER_STATUS_ID_TEMPLATE + '/unlock_time'  # time after which it is possible to switch worker to another queue

WORKER_CONFIG = PERO_PATH + '/worker/config'  # configs for all workers
WORKER_CONFIG_MQ_SERVERS = WORKER_CONFIG + '/mq_servers'
WORKER_CONFIG_MQ_MONITORING_SERVERS = WORKER_CONFIG + '/mq_monitoring_servers'
WORKER_CONFIG_FTP_SERVERS = WORKER_CONFIG + '/ftp_servers'

QUEUE = PERO_PATH + '/queue'
QUEUE_TEMPLATE = QUEUE + '/{queue_name}'
QUEUE_CONFIG_TEMPLATE = QUEUE_TEMPLATE + '/config'  # configs for processing (config.ini)
QUEUE_CONFIG_PATH_TEMPLATE = QUEUE_TEMPLATE + '/config_path'  # path to config on FTP server
QUEUE_CONFIG_ADMINISTRATIVE_PRIORITY_TEMPLATE = QUEUE_TEMPLATE + '/administrative_priority'  # priority set by administrator to prefer queue
QUEUE_STATS_AVG_MSG_TIME_TEMPLATE = QUEUE_TEMPLATE + '/avg_msg_time'  # average time needed for processing message from this queue
QUEUE_STATS_AVG_MSG_TIME_LOCK_TEMPLATE = QUEUE_TEMPLATE + '/avg_msg_time_lock'  # lock for avg_msg_time access
QUEUE_STATS_WAITING_SINCE_TEMPLATE = QUEUE_TEMPLATE + '/waiting_since'  # time when oldest message was written to the queue

# zookeeper int data settings
ZK_INT_BYTEORDER = 'big'  # Big endian - MSB is at the beginning of the byte object

# worker status
STATUS_STARTING = 'STARTING'
STATUS_PROCESSING = 'PROCESSING'
STATUS_RECONFIGURING = 'RECONFIGURING'
STATUS_IDLE = 'IDLE'
STATUS_FAILED = 'FAILED'
STATUS_DEAD = 'DEAD'

