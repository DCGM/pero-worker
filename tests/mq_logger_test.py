#!/usr/bin/env python3

import sys
import os
import logging
import time
import argparse

from worker.mq_logger import MQLogger
import worker_functions.connection_aux_functions as cf

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

def main():
    args = parse_args()
    log_formatter = logging.Formatter('%(asctime)s TEST %(levelname)s %(message)s')
    log_formatter.converter = time.gmtime

    stderr_handler = logging.StreamHandler()
    stderr_handler.setFormatter(log_formatter)

    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    logger.addHandler(stderr_handler)

    mq_logger = MQLogger(
        mq_servers=cf.server_list([args.broker_server]),
        username=args.username,
        password=args.password,
        ca_cert=args.ca_cert,
        log_formatter=log_formatter,
        logger=logger
    )
    logger.info('TEST_INFO_1')
    logger.info('TEST_INFO_2')
    logger.warning('TEST_WARNING_1')
    logger.error('TEST_ERROR_1')
    mq_logger.run()
    return 0


if __name__ == "__main__":
    sys.exit(main())
