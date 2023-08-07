#!/bin/sh

# source virtual environment to run scripts
. ../../.venv/bin/activate

# source env variables with username and password
. ./test.env

# configure the servers in zookeeper
python ../../scripts/server_config_manager.py \
    -z 172.26.0.50:2181 \
    -u "$USERNAME" \
    -p "$PASSWORD" \
    -e certificates/ca.pem \
    --add-mq-servers 172.26.0.51 \
    --add-monitoring-servers 172.26.0.51 \
    --add-ftp-servers 172.26.0.52:22

# configure rabbitmq users
python ../../scripts/mq_server_config_manager.py \
    -z 172.26.0.50:2181 \
    -u "$USERNAME" \
    -p "$PASSWORD" \
    -e certificates/ca.pem \
    --mq-user guest \
    --mq-password guest \
    -a "$USERNAME" "$PASSWORD" \
    -t monitoring management administrator

# remove default rabbitmq user
python ../../scripts/mq_server_config_manager.py \
    -z 172.26.0.50:2181 \
    -u "$USERNAME" \
    -p "$PASSWORD" \
    -e certificates/ca.pem \
    -d -a guest

# configure rabbitmq vhost
python ../../scripts/mq_server_config_manager.py \
    -z 172.26.0.50:2181 \
    -u "$USERNAME" \
    -p "$PASSWORD" \
    -e certificates/ca.pem \
    -o pero \
    -r 'PERO default vhost'

# configure rabbitmq vhost user access
python ../../scripts/mq_server_config_manager.py \
    -z 172.26.0.50:2181 \
    -u "$USERNAME" \
    -p "$PASSWORD" \
    -e certificates/ca.pem \
    -a "$USERNAME" \
    -v pero

# configure logging queue
python ../../scripts/stage_config_manager.py \
    -z 172.26.0.50:2181 \
    -u "$USERNAME" \
    -p "$PASSWORD" \
    -e certificates/ca.pem \
    -n log
