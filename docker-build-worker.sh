#!/bin/sh

docker build -f Dockerfile -t pero-ocr https://github.com/DCGM/pero-ocr.git#develop
docker build -f docker/Dockerfile.worker -t 'pero-worker' .