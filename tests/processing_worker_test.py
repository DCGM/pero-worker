import os
import sys
import argparse
import configparser
import logging
import time

# processing worker
from worker.processing_worker import ProcessingWorker
import libs.worker_functions.connection_aux_functions as cf
import libs.worker_functions.constants as constants

# dummy message generator
from scripts.dummy_msg_generator import DummyMsgGenerator

# processor
from worker.request_processor import get_request_processor

# dummy controller
from worker.worker_controller import WorkerController


# setup logging
log_formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')

# use UTC time in log
log_formatter.converter = time.gmtime

stderr_handler = logging.StreamHandler()
stderr_handler.setFormatter(log_formatter)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

logger.setLevel(logging.DEBUG)
logger.addHandler(stderr_handler)


class DummyController(WorkerController):
    def __init__(self, mq_servers, logger = logging.getLogger(__name__)):
        self.logger = logger
        self.mq_servers = mq_servers
        self.worker_id = 'test1'
        self.enabled = True
    
    def get_mq_servers(self):
        return self.mq_servers
    
    def update_status(self, status):
        self.logger.info(f'Requested status update to {status}')
    
    def update_statistics(self, stage, processed_request_count, total_processing_time):
        self.logger.info('Requested statistics update.')
        self.logger.info(f'Stage: {stage}')
        self.logger.info(f'Processed request count: {processed_request_count}')
        self.logger.info(f'Total processing time: {total_processing_time}')

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-b', '--broker-server',
        help='Message broker IP address.',
        default='127.0.0.1:5672'
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
    return parser.parse_args()

def generate_test_data(mq_servers, username, password, ca_cert):
    # connect to mq
    generator = DummyMsgGenerator(
        mq_servers=mq_servers,
        username=username,
        password=password,
        ca_cert=ca_cert,
        logger=logger
    )
    generator.mq_connect()
    try:
        generator.gen_trafic(
            number=30,                           # number of requests generated
            interval=0,                          # interval between request batches
            count=1,                             # number of batches
            stages=['dummy1', 'dummy2', 'out'],  # list of processing stages
            priority=0,                          # request priority
            size_min=2,                         # minimum size of request data
            size_max=3                          # maximum size of request data
        )
    except KeyboardInterrupt:
        logger.info('Keyboard interrupt received! Exiting!')
    except Exception:
        logger.error('Failed to generate traffic!')
        logger.error('Received error:\n{}'.format(traceback.format_exc()))
        return 1

    return 0

def main():
    # Before running this test, ensure RabbitMQ service is started
    # and pass in the ip and port of the service

    # create worker dummy controller that will observe its behaviour
    args = parse_args()
    mq_servers = cf.server_list(args.broker_server)
    controller = DummyController(mq_servers)
    config = configparser.ConfigParser()
    config.read('tests/request_processor_test_data/dummy1.ini')
    processor = get_request_processor('dummy1', config, None, '1')

    # create processing worker and configure it
    processing_worker = ProcessingWorker(
        controller=controller,
        username=args.username,
        password=args.password,
        ca_cert=args.ca_cert,
        logger=logger
    )

    # Generate test data
    if generate_test_data(mq_servers, args.username, args.password, args.ca_cert):
        return 1

    # start processing
    processing_worker.run(processor)

    # configure stage 2
    config = configparser.ConfigParser()
    config.read('tests/request_processor_test_data/dummy2.ini')
    processor = get_request_processor('dummy2', config, None, '1')

    # process stage 2
    processing_worker.run(processor)

    # exit
    return 0


if __name__ == "__main__":
    sys.exit(main())
