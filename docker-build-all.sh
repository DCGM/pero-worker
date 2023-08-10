#!/bin/sh

docker build -f docker/Dockerfile.watchdog -t 'pero-watchdog' .
docker build -f docker/Dockerfile.log_daemon -t 'pero-logd' .

docker build -f Dockerfile -t pero-ocr https://github.com/DCGM/pero-ocr.git#develop
docker build -f docker/Dockerfile.worker -t 'pero-worker' .
