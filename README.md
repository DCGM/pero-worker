# pero-worker

Project contains "worker" and "watchdog" for OCR processing system that uses pero-ocr package.

Processing system uses 5 components.
- Worker for processing data
- Watchdog for task planing and scheduling
- RabbitMQ message broker for task distribution
- Zookeeper for coordination of the workers and storing the configuration
- FTP for storing OCR binary files.

## setup

Docker is used in this example. Please visit https://docs.docker.com/engine/install/ and folow instructions for your operating system.
Use installation instruction for Apache zookeeper, RabbitMQ and your favourite FTP server, if you don't want to use docker.

Installing requirements and create python venv for the project:
```
sh install_dependencies.sh
```

Source created virtual environment:
```
. ./.venv/bin/activate
```

Starting required services:
```
docker run -d --rm -p2181:2181 --name="zookeeper" zookeeper
```
```
docker run -d --rm --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:management
```
```
docker run --name ftp --detach --env FTP_USER=pero --env FTP_PASS=pero --network bridge --publish 20-21:20-21/tcp --publish 40000-40009:40000-40009/tcp --volume /home/$USER/ftp:/home/pero garethflowers/ftp-server
```

## Initial system configuration

Set default server addresses and ports for auto-configuration:
```
python scripts/config_manager.py -z 127.0.0.1 -s 127.0.0.1 --ftp-servers 127.0.0.1 --update-mq-servers --update-ftp-servers --update-monitoring-servers
```

Create processing stages for OCR pipeline:
```
python scripts/config_manager.py --name ocr_stage_x --config path/to/ocr_stage_x/config.ini --remote-path path/to/aditional/data/on/ftp/server.tar.xz
```
Please note that you must upload aditional files to FTP server manually. Command above specifies just path used by worker to download these files from the server. To upload files use your favourite FTP client.

For more details on configurations please visit pero-ocr git (https://github.com/DCGM/pero-ocr) and webpage (https://pero.fit.vutbr.cz/) to get more information.

Create output queue from where results can be downloaded. Output queue is stage without processing configuration.
```
python scripts/config_manager.py --name out
```

## Running worker and watchdog

```
python worker/worker.py -z 127.0.0.1
```
```
python worker/worker_watchdog.py -z 127.0.0.1
```

## processing

Uploading images for processing:
```
python scripts/publisher.py --stages stage1 stage2 stage3 out --images input/file/1 input/file/2
```

Downloading results:
```
python scripts/publisher.py --directory output/directory/path --download out
```
If you want to keep downloading images from ```out``` stage, add ```--keep-running``` argument at the end of the command above.
