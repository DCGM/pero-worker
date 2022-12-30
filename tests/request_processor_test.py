#!/usr/bin/env python3

import os
import sys
import argparse
import configparser

# request processor
from worker.request_processor import PeroOcrRequestProcessor, DummyRequestProcessor, get_request_processor

# cache
from worker.cache import OCRFileCache

# protobuf
from message_definitions.message_pb2 import ProcessingRequest, StageLog, Data
from google.protobuf.timestamp_pb2 import Timestamp

# Test data description

# For DummyRequestProcessor there is request with file containing 8 charactes.
DUMMY_TEST_FILENAME = '5f6a4c863d5943e88e41301fbfcbc093.protobuf'
# This should make DummyRequestProcessor wait 8 seconds before continuing.

# For PeroOcrRequestProcessor there is request containing test image.
OCR_TEST_FILENAME = '39cb5c6d292b4ebeae63dd9b1b739382.protobuf'
# PeroOcrRequestProcessor should be able to process image using sample OCR model,
# pero_eu_cz_print_newspapers_2020-10-09.tar.gz, provided by Michal Hradiš.
# OCR model is not part of this repository.

TEST_FILES_PATH = 'tests/request_processor_test_data'
MODEL_UNPACK_PATH = os.path.join(TEST_FILES_PATH, 'tmp')

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--ocr-model',
        help='Path to ocr model.',
        default=None
    )
    return parser.parse_args()

def test_dummy(config_name):
    processing_request_path = os.path.join(TEST_FILES_PATH, DUMMY_TEST_FILENAME)
    with open(processing_request_path, 'rb') as f:
        processing_request = ProcessingRequest().FromString(f.read())
    
    assert len(processing_request.results) == 1
    assert len(processing_request.logs) == 0

    config_path = os.path.join(TEST_FILES_PATH, f'{config_name}.ini')
    config = configparser.ConfigParser()
    config.read(config_path)
    
    dummy = get_request_processor('test', config, config_path, '123')
    log = processing_request.logs.add()
    dummy.process_request(processing_request, log)

    print(f'Stage: {log.stage}')
    print(f'Start time: {log.start.ToDatetime()}')
    print(f'End time: {log.end.ToDatetime()}')
    print(f'Processing status: {log.status}')
    print(f'Logs content: {log.log}')
    print(f'Config version: {log.version}')
    assert log.version == '123'
    assert log.status == 'OK'
    assert log.stage == 'test'
    assert len(processing_request.results) == 1
    assert len(processing_request.logs) == 1

def test_ocr(ocr_model_path):
    if not ocr_model_path:
        return 1
    
    config_path = MODEL_UNPACK_PATH
    config = configparser.ConfigParser()
    config.read(os.path.join(config_path, 'config.ini'))

    processing_request_path = os.path.join(TEST_FILES_PATH, OCR_TEST_FILENAME)
    with open(processing_request_path, 'rb') as f:
        processing_request = ProcessingRequest().FromString(f.read())
    
    assert len(processing_request.results) == 1
    assert len(processing_request.logs) == 0

    processor = get_request_processor('test', config, config_path, '123')
    log = processing_request.logs.add()
    processor.process_request(processing_request, log)

    processor.cleanup()

    print(f'Stage: {log.stage}')
    print(f'Start time: {log.start.ToDatetime()}')
    print(f'End time: {log.end.ToDatetime()}')
    print(f'Processing status: {log.status}')
    print(f'Logs content: {log.log}')
    print(f'Config version: {log.version}')
    assert log.version == '123'
    assert log.status == 'OK'
    assert log.stage == 'test'
    assert len(processing_request.results) == 3
    assert len(processing_request.logs) == 1

    return 0

def main():
    args = parse_args()

    # test dummy processing
    for i in (1, 2, 3):
        print("="*80)
        test_dummy(f'dummy{i}')
    print("="*80)

    # test OCR processing
    if args.ocr_model:
        try:
            OCRFileCache.unpack_archive(args.ocr_model, MODEL_UNPACK_PATH)
            test_ocr(args.ocr_model)
        finally:
            # cleanup the unpacked ocr model
            if os.path.exists(MODEL_UNPACK_PATH):
                for f in os.listdir(MODEL_UNPACK_PATH):
                    os.unlink(os.path.join(MODEL_UNPACK_PATH, f))
                os.rmdir(MODEL_UNPACK_PATH)
    else:
        print('Skipping PeroOcrRequestProcessor test, model not set!')
    print("="*80)

    return 0

if __name__ == "__main__":
    sys.exit(main())
