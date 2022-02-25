#!/usr/bin/env python3

# constants for pero worker

# zookeeper paths

PERO_PATH = '/pero'

WORKER_STATUS = PERO_PATH + '/worker/status'
WORKER_STATUS_ID_TEMPLATE = WORKER_STATUS + '/{worker_id}'
WORKER_STATUS_TEMPLATE = WORKER_STATUS + '/{worker_id}/status'  # current worker status
WORKER_QUEUE_TEMPLATE = WORKER_STATUS + '/{worker_id}/queue'  # input queue
WORKER_ENABLED_TEMPLATE = WORKER_STATUS + '/{worker_id}/enabled'  # worker enabled

WORKER_CONFIG = PERO_PATH + '/worker/config'  # configs for all workers
WORKER_CONFIG_TEMPLATE = WORKER_CONFIG + '/{config_key}'
WORKER_CONFIG_MQ_SERVERS = WORKER_CONFIG + '/mq_servers'

PROCESSING_CONFIG = PERO_PATH + '/config'  # configs for processing (config.ini)
PROCESSING_CONFIG_TEMPLATE = PROCESSING_CONFIG + '/{config_name}'


# worker status
STATUS_STARTING = 'STARTING'
STATUS_PROCESSING = 'PROCESSING'
STATUS_RECONFIGURING = 'RECONFIGURING'
STATUS_IDLE = 'IDLE'
STATUS_FAILED = 'FAILED'
STATUS_DEAD = 'DEAD'

