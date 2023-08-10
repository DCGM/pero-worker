#!/bin/sh

docker build -f docker/Dockerfile.watchdog -t 'pero-watchdog' .
docker build -f docker/Dockerfile.log_daemon -t 'pero-logd' .