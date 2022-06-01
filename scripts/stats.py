#!/usr/bin/env python3

# Collects and calculates statistics from worker traffic

import argparse
import os
import sys
import datetime
import logging
import copy

# protobuf
from message_definitions.message_pb2 import ProcessingRequest, StageLog, Data
from google.protobuf.timestamp_pb2 import Timestamp

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
    parser = argparse.ArgumentParser('Calculate worker traffic statistics')
    parser.add_argument(
        '-d', '--directory',
        help='Directory with processing results',
        required=True,
        type=dir_path
    )
    parser.add_argument(
        '-r', '--raw',
        help='Print raw data, do not print time and loglevel of message.',
        action='store_true',
        default=False
    )
    parser.add_argument(
        '-s', '--short',
        help='Excludes timelines from output and prints only average stats.',
        action='store_true',
        default=False
    )
    return parser.parse_args()

class StatsCounter:

    record_template = {
        'start': 0,  # start time
        'end': 0     # end time
    }

    stage_template = {
        'wait': [],  # total time messages spend in queue, waiting for processing in this stage (failed messages are not included)
        'time': [],  # total processing time of all messages for given stage (failed messages are not included)
        'failed': 0  # failed message count
    }

    pipeline_template = {
        'stages': [],  # pipeline stages messages goes through
        'time': [],  # total pipeline time
    }

    def __init__(self, logger = logging.getLogger(__name__)):
        self.logger = logger
        self.time = []  # total processing time (failed messages exclueded)
        self.failed = 0  # total number of failed messages
        self.stages = {}  # processing stages
        self.pipelines = []  # processing pipelines
    
    @staticmethod
    def get_time_average(timeline):
        time = 0
        for t in timeline:
            time += (t['end'] - t['start']).total_seconds()
        return time / len(timeline)

    @staticmethod
    def get_pipeline_stages(pipeline):
        """
        Generates pipeline stage list in printable format
        """
        pipeline_stages = ''
        for stage in pipeline['stages']:
            if pipeline_stages:
                pipeline_stages = '{stages} -> {stage}'.format(stages = pipeline_stages, stage = stage)
            else:
                pipeline_stages = '{}'.format(stage)
        return pipeline_stages

    def log_timeline(self, timeline):
        """
        Logs timeline
        """
        timeline.sort(key=lambda item: item['start'])
        for record in timeline:
            self.logger.info('{start} {end} {duration}'.format(
                start = record['start'].isoformat(),
                end = record['end'].isoformat(),
                duration = (record['end'] - record['start']).total_seconds()
            ))
    
    def log_total_statistics(self):
        """
        Logs total statistics for messages
        """
        self.logger.info(
            'Average message total processing time: {}'
            .format(self.get_time_average(self.time))
        )
        self.logger.info('Message count: {}'.format(len(self.time)))
        self.logger.info('Failed message count: {}'.format(self.failed))

    def log_stages_statistics(self):
        """
        Logs statistics for stages
        """
        for stage in self.stages:
            self.logger.info('Statistics for stage {}:'.format(stage))
            self.logger.info(
                'Average stage processing time: {}'
                .format(self.get_time_average(self.stages[stage]['time']))
            )
            self.logger.info(
                'Average stage waiting time: {}'
                .format(self.get_time_average(self.stages[stage]['wait']))
            )
            self.logger.info('Total stage message count: {}'.format(len(self.stages[stage]['time'])))
            self.logger.info('Total stage failed messages: {}'.format(self.stages[stage]['failed']))

    def log_pipeline_statistics(self):
        """
        Logs statistics for pipelines
        """
        for pipeline in self.pipelines:
            self.logger.info('Statistics for pipeline {}:'.format(self.get_pipeline_stages(pipeline)))
            self.logger.info(
                'Average pipeline total processing time: {}'
                .format(self.get_time_average(pipeline['time']))
            )
            self.logger.info('Total pipeline message count: {}'.format(len(pipeline['time'])))

    def log_timelines(self):
        """
        Logs timelines
        """
        self.logger.info('Total timeline statistics:')
        self.log_timeline(self.time)
        self.logger.info('Timeline statistics for stages:')
        for stage in self.stages:
            self.logger.info('Processing timeline for stage {}:'.format(stage))
            self.log_timeline(self.stages[stage]['time'])
            self.logger.info('Wait timeline for stage {}:'.format(stage))
            self.log_timeline(self.stages[stage]['wait'])
        self.logger.info('Timeline statistics for pipelines:')
        for pipeline in self.pipelines:
            self.logger.info('Timeline statistics for pipeline: {}'.format(self.get_pipeline_stages(pipeline)))
            self.log_timeline(pipeline['time'])

    def log_statistics(self, short = False):
        """
        Logs statistics using logger
        :param short: exclude timelines from output
        """
        self.log_total_statistics()
        self.log_stages_statistics()
        self.log_pipeline_statistics()
        if not short:
            self.log_timelines()

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

            processing_time = copy.deepcopy(self.record_template)
            processing_time['start'] = Timestamp.ToDatetime(log.start)
            processing_time['end'] = Timestamp.ToDatetime(log.end)
            
            # msg end time = end time of last stage
            if processing_time['end'] > msg_end_time:
                msg_end_time = processing_time['end']
            
            # stage waiting time
            wait_time = copy.deepcopy(self.record_template)
            wait_time['end'] = processing_time['start']
            if not last_stage_end_time:
                wait_time['start'] = msg_start_time
            else:
                wait_time['start'] = last_stage_end_time
            last_stage_end_time = processing_time['end']
            
            # add stage statistics
            stages.append({'name': log.stage, 'time': processing_time, 'wait': wait_time})
        
        time = copy.deepcopy(self.record_template)
        time['start'] = msg_start_time
        time['end'] = msg_end_time
        
        # update global statistics
        for stage in stages:
            if stage['name'] not in self.stages:
                self.stages[stage['name']] = copy.deepcopy(self.stage_template)
            self.stages[stage['name']]['time'].append(stage['time'])
            self.stages[stage['name']]['wait'].append(stage['wait'])
        
        self.time.append(time)

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
            pipeline['time'].append(time)
            return
        
        pipeline = copy.deepcopy(self.pipeline_template)
        pipeline['time'].append(time)
        pipeline['stages'] = [stage['name'] for stage in stages]
        self.pipelines.append(pipeline)

def main():
    args = parse_args()

    # setup logging
    log_formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    if args.raw:
        log_formatter = logging.Formatter('%(message)s')

    stderr_handler = logging.StreamHandler()
    stderr_handler.setFormatter(log_formatter)
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    logger.addHandler(stderr_handler)

    # get stats
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
    
    stats_counter.log_statistics(args.short)
    return 0

if __name__ == "__main__":
    sys.exit(main())
