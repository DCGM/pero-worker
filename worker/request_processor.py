#!/usr/bin/env python3

# Processor for request processing

import os
import logging
import time  # dummy processing
import random  # dummy processing
import datetime
import traceback
from io import StringIO  # logging to message
from abc import ABC, abstractmethod

# pero OCR
from pero_ocr.core.layout import PageLayout
from pero_ocr.document_ocr.page_parser import PageParser

# protobuf
from message_definitions.message_pb2 import ProcessingRequest, StageLog, Data
from google.protobuf.timestamp_pb2 import Timestamp

# load image and data for processing
import cv2
import magic
import numpy as np
import torch

class RequestProcessor(ABC):
    """
    Request processor - processes requests using process request method.
    """
    def __init__(self, stage, config, config_path, config_version, logger = logging.getLogger(__name__)):
        """
        :param stage: processing stage name
        :param config: processing configuration
        :param config_path: processing configuration and model path
        :param config_version: version of processing configuration
        :param logger: logger to use for logging
        """
        self.stage = stage
        self.config = config
        self.config_version = config_version
        self.logger = logger
    
    @abstractmethod
    def _process_request_data(self, processing_request):
        """
        Processes processing request data.
        :param processing_request: processing request to process
        """
        pass

    def process_request(self, processing_request, log):
        """
        Process processing request.
        Public method for processing request, sets up logging etc. and calls '_process_request_data' to do processing.
        :param processing_request: processing request to process
        :param log: log where stage processing log messages will be added
        :return: time spent by processing
        :raise AttributeError if log instance does not exist
        :raise Exception received from '_process_request_data' method when processing fails
        """
        log.stage = processing_request.processing_stages[0]
        log.status = 'Failed'
        log.version = self.config_version
        start_time = datetime.datetime.now(datetime.timezone.utc)
        Timestamp.FromDatetime(log.start, start_time)
        
        # configure message logging
        log_buffer = StringIO()
        buffer_handler = logging.StreamHandler(log_buffer)
        self.logger.addHandler(buffer_handler)

        try:
            self._process_request_data(processing_request)
        except Exception:
            raise
        else:
            log.status = 'OK'
        finally:
            # add buffered log to the message
            buffer_handler.flush()
            log.log += log_buffer.getvalue()
            # remove handler to prevent memory leak (buffer is discarded and never used again)
            self.logger.removeHandler(buffer_handler)
        # Timestamp
        end_time = datetime.datetime.now(datetime.timezone.utc)
        Timestamp.FromDatetime(log.end, end_time)
        spent_time = (end_time - start_time).total_seconds()
        return spent_time

    def cleanup(self):
        """
        Cleanup method to clean temp files, memory, etc. at the end of processing.
        Does nothing by default.
        """
        return

def get_request_processor(stage, config, config_path, config_version, logger = logging.getLogger(__name__)):
    """
    Factory method to decide if dummy or ocr processor will be used.
    :param stage: processing stage name
    :param config: processing configuration
    :param config_path: processing configuration and model path
    :param config_version: version of processing configuration
    :param logger: logger to use for logging
    :return: RequestProcessor instance with given configuration
    """
    try:
        dummy = config['WORKER']['DUMMY']
    except KeyError:
        dummy = None
    
    try:
        fail_test = config['WORKER']['FAILTEST']
    except KeyError:
        fail_test = None
    
    if dummy:
        return DummyRequestProcessor(stage, config, config_path, config_version, logger)
    elif fail_test:
        return FailTestRequestProcessor(stage, config, config_path, config_version, logger)
    else:
        return PeroOcrRequestProcessor(stage, config, config_path, config_version, logger)

class PeroOcrRequestProcessor(RequestProcessor):
    """
    Request processor for processing requests using pero ocr.
    """
    def __init__(self, stage, config, config_path, config_version, logger = logging.getLogger(__name__)):
        """
        :param stage: processing stage name
        :param config: processing configuration
        :param config_path: processing configuration and model path
        :param config_version: version of processing configuration
        :param logger: logger to use for logging
        """
        self.stage = stage
        self.config = config
        self.config_version = config_version
        self.logger = logger

        self.page_parser = PageParser(self.config, config_path)

    def _process_request_data(self, processing_request):
        """
        Processes request data using OCR.
        :param processing_request: request to process
        """
        # load data
        img = None
        img_name = 'page'
        xml_in = None
        logits_in = None

        for i, data in enumerate(processing_request.results):
            ext = os.path.splitext(data.name)[1]
            datatype = magic.from_buffer(data.content, mime=True)
            self.logger.debug(f'File: {data.name}, type: {datatype}')
            if datatype.split('/')[0] == 'image':  # recognize image
                img = cv2.imdecode(np.fromstring(processing_request.results[i].content, dtype=np.uint8), 1)
                img_name = processing_request.results[i].name
            if ext == '.xml':  # pagexml is missing xml header - type can't be recognized - type = text/plain
                xml_in = processing_request.results[i].content
            if ext == '.logits':  # type = application/octet-stream
                logits_in = processing_request.results[i].content
        
        # run processing
        try:
            if xml_in:
                page_layout = PageLayout()
                page_layout.from_pagexml_string(xml_in)
            else:
                page_layout = PageLayout(id=img_name, page_size=(img.shape[0], img.shape[1]))
            if logits_in:
                page_layout.load_logits(logits_in)
            page_layout = self.page_parser.process_page(img, page_layout)
        except Exception as e:
            self.logger.error('{error}'.format(error = e))
            self.logger.error('Received traceback:\n{}'.format(traceback.format_exc()))
            # processing failed
            raise
        
        # save output
        xml_out = page_layout.to_pagexml_string()
        try:
            logits_out = page_layout.save_logits_bytes()
        except KeyboardInterrupt:
            raise
        except Exception:
            self.logger.debug('No logits available to save!')
            self.logger.debug('Received error:\n{}'.format(traceback.format_exc()))
            logits_out = None

        for data in processing_request.results:
            ext = os.path.splitext(data.name)[1]
            if ext == '.xml':
                data.content = xml_out.encode('utf-8')
            if ext == '.logits':
                data.content = logits_out
        
        if xml_out and not xml_in:
            xml = processing_request.results.add()
            xml.name = '{}_page.xml'.format(img_name)
            xml.content = xml_out.encode('utf-8')
        
        if logits_out and not logits_in:
            logits = processing_request.results.add()
            logits.name = '{}.logits'.format(img_name)
            logits.content = logits_out
    
    def cleanup(self):
        """
        Clean up OCR model from GPU.
        """
        torch.cuda.empty_cache()

class DummyRequestProcessor(RequestProcessor):
    """
    Dummy request processor for simulation of processing.
    """
    def _process_request_data(self, processing_request):
        """
        Simulates processing on given request.
        :param processing_request: request to simulate processing on
        """
        # get data size
        try:
            data_size = len(processing_request.results[0].content)
        except Exception:
            error_msg = 'Failed to get request data from results!'
            received_error = 'Received error:\n{}'.format(traceback.format_exc())
            for msg in (error_msg, received_error):
                self.logger.error(msg)
            raise

        # get processing stage configuration
        try:
            try:
                processing_time_scale = float(self.config['WORKER']['TIME_SCALE'])
            except KeyError:
                processing_time_scale = 1
            try:
                time_delta = float(self.config['WORKER']['TIME_DELTA'])
            except KeyError:
                time_delta = 0
            try:
                processing_time_diff_min = float(self.config['WORKER']['TIME_DIFF_MIN'])
                processing_time_diff_max = float(self.config['WORKER']['TIME_DIFF_MAX'])
            except KeyError:
                processing_time_diff_min = processing_time_diff_max = 0
        except (TypeError, ValueError):
            self.logger.error('Wrong dummy config time format for processing stage {}, value must be number!'.format(log.stage))
            raise
        
        if not processing_time_diff_max and not processing_time_diff_min:
            processing_time_diff_min = -time_delta
            processing_time_diff_max = time_delta
        
        if processing_time_diff_min > processing_time_diff_max:
            self.logger.error('Wrong dummy config for processing stage {}, minimal processing time cannot be higher than maximal processing time!'.format(log.stage))
            raise

        # get processing time
        processing_time = data_size * processing_time_scale + random.uniform(processing_time_diff_min, processing_time_diff_max)
        if processing_time < 0:
            processing_time = 0

        # processing simulation
        self.logger.info('Dummy processing, waiting for {} seconds'.format(processing_time))
        self.logger.info('Dummy processing time: {} seconds'.format(processing_time))
        self.logger.info('Request data size: {}'.format(data_size))
        if processing_time_scale != 1:
            self.logger.info('Processing time scale: {}'.format(processing_time_scale))
        if time_delta:
            self.logger.info('Time delta: {}'.format(time_delta))
        elif processing_time_diff_max or processing_time_diff_min:
            self.logger.info('Time difference min: {}'.format(processing_time_diff_min))
            self.logger.info('Time difference max: {}'.format(processing_time_diff_max))
        time.sleep(processing_time)


class FailTestRequestProcessor(RequestProcessor):
    """
    Fail test request processor - fails processing on every request to test worker reaction.
    """
    def _process_request_data(self, processing_request):
        """
        Fails processing on given request.
        """
        raise RuntimeError('Simulated processing failure generated by Fail Test Request Processor!')
