#!/usr/bin/env python3

# Auxiliary publisher for worker testing

import argparse
import sys
import os
import pika
import uuid
import datetime

# protobuf
from message_definitions.message_pb2 import ProcessingRequest, StageLog, Data
from google.protobuf.timestamp_pb2 import Timestamp

output = ''

def parse_args():
    argparser = argparse.ArgumentParser('Test app, sends data in PERO message format to MQ')
    argparser.add_argument(
        '-i', '--image',
        help='Input image.',
        type=argparse.FileType('rb')
    )
    argparser.add_argument(
        '-b', '--broker',
        help='MQ broker ip address.'
    )
    argparser.add_argument(
        '-p', '--broker-port',
        help='MQ broker port.',
        type=int
    )
    argparser.add_argument(
        '-s', '--stages',
        help='Stages to process.',
        nargs='+'
    )
    argparser.add_argument(
        '-o', '--output-folder',
        help='Folder where to write output data.'
    )

    return argparser.parse_args()


def create_msg(image, stages):
    message = ProcessingRequest()

    # add message properties
    message.uuid = uuid.uuid4().hex
    message.page_uuid = uuid.uuid4().hex
    message.priority = 0
    Timestamp.FromDatetime(message.start_time, datetime.datetime.now(datetime.timezone.utc))

    # add processing stages
    for stage in stages:
        message.processing_stages.append(stage)

    # add image to results
    img = message.results.add()
    img.name = os.path.basename(image.name)
    img.content = image.read()

    return message


def consume(channel, method, properties, body):
    global output

    message = ProcessingRequest().FromString(body)
    print(f'Message id : {message.uuid}')
    print(f'Page uuid  : {message.page_uuid}')
    print(f'Priority   : {message.priority}')
    print(f'Start time : {message.start_time}')
    print(f'Remaining processing stages:')
    for i, stage in enumerate(message.processing_stages):
        print(f' {i} : {stage}')
    print(f'Result file names:')
    for i, result in enumerate(message.results):
        print(f' {i} : {result.name}')
        with open(os.path.join(output, result.name), 'wb') as file:
            file.write(result.content)
    print(f'Stage logs avaliable for:')
    for i, log in enumerate(message.logs):
        print(f' {i} : {log.stage}')
        with open(os.path.join(output, log.stage), 'w') as file:
            file.write(log.log)
    
    channel.basic_ack(delivery_tag = method.delivery_tag)
    channel.close()


def main():
    args = parse_args()

    # check if output folder points to directory
    if not os.path.isdir(args.output_folder):
        raise ValueError(f'{args.output_folder} is not a directory!')
    
    # set output folder
    global output
    output = args.output_folder
    
    # connect to broker
    connection = pika.BlockingConnection(pika.ConnectionParameters(
        host=args.broker,
        port=args.broker_port
        ))
    channel = connection.channel()

    # declare queues for stages
    for stage in args.stages:
        channel.queue_declare(queue=stage ,arguments={'x-max-priority': 2})
    
    # register callback for consuming on output queue
    channel.basic_consume(args.stages[-1], on_message_callback=consume)

    # create test message
    message = create_msg(args.image, args.stages)

    # send test message
    channel.basic_publish('', args.stages[0], message.SerializeToString())

    # wait for the processed message
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        pass

    # exit
    if channel.is_open:
        channel.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())