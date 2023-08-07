#!/bin/sh

# cleanup
[ -d data ] && rm -r data
[ -d config ] && rm -r config
[ -d certificates ] && rm -r certificates
[ -f test.env ] && rm test.env

# create data and config folders
mkdir -p data/sftp
mkdir -p data/zookeeper/data
mkdir -p data/zookeeper/datalog
mkdir -p data/rabbitmq
mkdir -p config/rabbitmq/certs

# copy configs
cp ../../sample-config/rabbitmq/rabbitmq.conf config/rabbitmq/
cp -r ../../sample-config/zookeeper config/

# generate ssl certificates and set permissions for services to read them
sh gen_certs.sh
chmod 644 certificates/*

# copy certificates and keys to config folders
cp certificates/rabbit-server-*.pem config/rabbitmq/certs/
cp certificates/ca.pem config/rabbitmq/certs/
cp certificates/zk_*.jks config/zookeeper/

# generate docker environment file
sh gen_env.sh

# run the services
docker compose -f ./tests-docker-compose.yaml --env-file ./test.env up -d

# configure the services
#sh config_services.sh
