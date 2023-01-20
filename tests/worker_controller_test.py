#!/usr/bin/env python3

import sys
import os
import logging

# zk client
from libs.worker_functions.zk_client import ZkClient

# controller
from worker.worker_controller import ZkWorkerController
from worker.cache import OCRFileCache

class DummyProcessingClient:
    def __init__(self, logger = logging.getLogger(__name__)):
        self.logger = logger
    def run(self, request_processor):
        self.logger.info()
        

def main():
    return 0


if __name__ == "__main__":
    sys.exit(main())
