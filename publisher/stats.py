#!/usr/bin/env python3

# Collects and calculates statistics from worker trafic

import argparse
import os
import sys
import datetime
import logging
import copy

# protobuf
from message_definitions.message_pb2 import ProcessingRequest, StageLog, Data
from google.protobuf.timestamp_pb2 import Timestamp

# setup logging (required by kazoo)
log_formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')

stderr_handler = logging.StreamHandler()
stderr_handler.setFormatter(log_formatter)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
# TODO - remove debug level
logger.setLevel(logging.DEBUG)
logger.addHandler(stderr_handler)

def dir_path(path):
    """
    Check if path is directory path
    :param path: path to directory
    :return: path if path is directory path
    :raise: ArgumentTypeError if path is not directory path
    """
    if not os.path.exists(path):
        raise argparse.ArgumentTypeError(f"error: {path} is not a valid path")    
    if not os.path.isdir(path):
        raise argparse.ArgumentTypeError(f"error: {path} is not a directory")
    return path

def parse_args():
    parser = argparse.ArgumentParser('Calculate worker trafic statistics')
    parser.add_argument(
        '-d', '--directory',
        help='Directory with processing results',
        required=True,
        type=dir_path
    )
    return parser.parse_args()

class StatsCounter:

    stage_template = {
        'wait': 0,  # total time messages spend in queue, waiting for processing in this stage (failed messages are not included)
        'time': 0,  # total processing time of all messages for given stage (failed messages are not included)
        'count': 0,  # message count  (failed messages exclueded)
        'failed': 0  # failed message count
    }

    pipeline_template = {
        'stages': [],  # pipeline stages messages goes through
        'time': 0,  # total pipeline time
        'count': 0   # total pipeline message count
    }

    def __init__(self, logger = logging.getLogger(__name__)):
        self.logger = logger
        self.time = 0  # total processing time (failed messages exclueded)
        self.count = 0  # total message count (failed messages exclueded)
        self.failed = 0  # total number of failed messages
        self.stages = {}  # processing stages
        self.pipelines = []  # processing pipelines
    
    def log_statistics(self):
        """
        Logs statistics using logger
        """
        self.logger.info(
            'Average message total processing time: {}'
            .format((self.time / self.count) if self.count else 0)
        )
        self.logger.info('Message count: {}'.format(self.count))
        self.logger.info('Failed message count: {}'.format(self.failed))
        for stage in self.stages:
            self.logger.info('Statistics for stage {}:'.format(stage))
            self.logger.info(
                'Average stage processing time: {}'
                .format((self.stages[stage]['time'] / self.stages[stage]['count']) if self.stages[stage]['count'] else 0)
            )
            self.logger.info(
                'Average stage waiting time: {}'
                .format((self.stages[stage]['wait'] / self.stages[stage]['count']) if self.stages[stage]['count'] else 0))
            self.logger.info('Total stage message count: {}'.format(self.stages[stage]['count']))
            self.logger.info('Total stage failed messages: {}'.format(self.stages[stage]['failed']))
        for pipeline in self.pipelines:
            pipeline_stages = ''
            for stage in pipeline['stages']:
                if pipeline_stages:
                    pipeline_stages = '{stages} -> {stage}'.format(stages = pipeline_stages, stage = stage)
                else:
                    pipeline_stages = '{}'.format(stage)
            self.logger.info('Statistics for pipeline {}:'.format(pipeline_stages))
            self.logger.info(
                'Average pipeline total processing time: {}'
                .format((pipeline['time'] / pipeline['count']) if pipeline['count'] else 0)
            )
            self.logger.info('Total pipeline message count: {}'.format(pipeline['count']))

    def update_message_statistics(self, message):
        """
        Updates statistics with data from message
        :param message: message to process
        """
        stages = []
        msg_start_time = Timestamp.ToDatetime(message.start_time)
        msg_end_time = msg_start_time
        last_stage_end_time = None
        for log in message.logs:
            
            # update failed message count
            if log.status != 'OK':
                if log.stage not in self.stages:
                    self.stages[log.stage] = copy.deepcopy(self.stage_template)
                self.stages[log.stage]['failed'] += 1
                self.failed += 1
                return  # do not update time if message failed to process
            
            start_time = Timestamp.ToDatetime(log.start)
            end_time = Timestamp.ToDatetime(log.end)
            
            # msg end time = end time of last stage
            if end_time > msg_end_time:
                msg_end_time = end_time
            
            # stage waiting time
            if not last_stage_end_time:
                wait_time = (start_time - msg_start_time).total_seconds()
            else:
                wait_time = (start_time - last_stage_end_time).total_seconds()
            last_stage_end_time = end_time
            
            # add stage statistics
            stages.append({'name': log.stage, 'time': (end_time - start_time).total_seconds(), 'wait': wait_time})
        
        time = (msg_end_time - msg_start_time).total_seconds()
        
        # update global statistics
        for stage in stages:
            if stage['name'] not in self.stages:
                self.stages[stage['name']] = copy.deepcopy(self.stage_template)
            self.stages[stage['name']]['time'] += stage['time']
            self.stages[stage['name']]['count'] += 1
            self.stages[stage['name']]['wait'] += stage['wait']
        
        self.time += time
        self.count += 1

        # update pipeline statistics
        for pipeline in self.pipelines:
            if len(pipeline['stages']) != len(stages):
                continue
            match = True
            for i in range(len(stages)):
                if pipeline['stages'][i] != stages[i]['name']:
                    match = False
                    break
            if not match:
                continue
            pipeline['time'] += time
            pipeline['count'] += 1
            return
        
        pipeline = copy.deepcopy(self.pipeline_template)
        pipeline['time'] += time
        pipeline['count'] += 1
        pipeline['stages'] = [stage['name'] for stage in stages]
        self.pipelines.append(pipeline)

def main():
    args = parse_args()

    stats_counter = StatsCounter(logger=logger)
    messages = os.listdir(args.directory)
    for message in messages:
        message_path = os.path.join(args.directory, message)
        try:
            message_data = open(os.path.join(message_path, 'message_body'), 'rb')
        except FileNotFoundError:
            continue

        stats_counter.update_message_statistics(ProcessingRequest.FromString(message_data.read()))
        message_data.close()
    
    stats_counter.log_statistics()
    return 0

if __name__ == "__main__":
    sys.exit(main())
