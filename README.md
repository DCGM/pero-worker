# pero-worker

Project contains "worker" and "watchdog" for OCR processing system that uses pero-ocr package.

Processing system uses 5 components.
- Worker for processing data
- Watchdog for task planing and scheduling
- RabbitMQ message broker for task distribution
- Zookeeper for coordination of the workers and storing the configuration
- SFTP for storing OCR binary files.

## setup

Docker is used in this example. Please visit https://docs.docker.com/engine/install/ and follow instructions for your operating system.
Use installation instruction for Apache zookeeper, RabbitMQ and your favourite SFTP server, if you don't want to use docker.

Installation script was tested under Debian 11 and is APT dependent.

Installing requirements and create python venv for the project:
```
sh install_dependencies.sh
```

Source created virtual environment:
```
. ./.venv/bin/activate
```

Download pero-ocr to `pero-ocr/` folder:
```
git submodule init
git submodule update
```
Or do this manually by cloning pero-ocr from https://github.com/DCGM/pero-ocr.git

### Generating self-signed certificates for services

1. Generate CA (Certification authority) key for signing and certificate for verification of server certificates (validity is set to 10 years in this example).
    ```
    openssl genrsa -aes256 -out ca-key.pem 4096
    openssl req -new -x509 -sha256 -days 3650 -key ca-key.pem -out ca.pem
    ```

2. Generate server certificate (for RabbitMQ / Zookeeper server).
- Generate server key and certificate signing request
    ```
    openssl genrsa -out server-key.pem 4096
    openssl req -new -sha256 -subj "/CN=server-hostname-or-ip" -key server-key.pem -out server-cert.csr
    ```
- Set server additional properties, server identification, usage of the key, etc. to extension file.
    ```
    echo "subjectAltName=DNS:hostname.domain,IP:1.2.3.4" >> extfile.cnf
    echo extendedKeyUsage = serverAuth >> extfile.cnf
    ```
- Create the signed certificate (validity is set to 10 years in this example).
    ```
    openssl x509 -req -sha256 -days 3650 -in server-cert.csr -CA ca.pem -CAkey ca-key.pem -out server-cert.pem -extfile extfile.cnf -CAcreateserial
    ```

3. Configure server to use the certificate.
- RabbitMQ - can be simply set in config file:
    ```
    # Disable non-TLS connections
    listeners.tcp = none

    # Enable AMQPS
    listeners.ssl.default = 5671
    ssl_options.cacertfile = /etc/rabbitmq/certs/ca.pem
    ssl_options.certfile   = /etc/rabbitmq/certs/rabbit-server-cert.pem
    ssl_options.keyfile    = /etc/rabbitmq/certs/rabbit-server-key.pem
    ssl_options.verify     = verify_peer
    # disable mutual authentication (clients does not need to have their own certificates)
    ssl_options.fail_if_no_peer_cert = false

    # Enable HTTPS (management console)
    management.listener.port = 15671
    management.listener.ssl = true
    management.listener.ssl_opts.cacertfile = /etc/rabbitmq/certs/ca.pem
    management.listener.ssl_opts.certfile = /etc/rabbitmq/certs/rabbit-server-cert.pem
    management.listener.ssl_opts.keyfile = /etc/rabbitmq/certs/rabbit-server-key.pem
    ```

- Zookeeper - requires additional steps.

    Zookeeper uses java keystores and truststores for certificate management.
    Truststore contains trusted certificates (CA certificate) for verification of other peers connecting to this node.
    Keystore contains this node's key and certificate for authentication with other peers.

    To configure keystore for zookeeper server, generated certificate and key must be converted to `pkcs12` file.
    This file contains server certificate, server key and certificate of CA that signed the server certificate.
    ```
    openssl pkcs12 -export -in zookeeper-server-cert.pem -inkey zookeeper-server-key.pem -out zookeeper-server.p12 -name zk_server -CAfile ca.pem -caname root
    ```

    Created file `zookeeper-server.p12` can be converted to java `JKS` keystore using java keytool.
    Store is protected by password specified by parameter `-deststorepass`.
    If the `zookeeper-server.p12` is protected by password, it must be specified by `-srcstorepass` parameter.
    ```
    keytool -importkeystore -deststorepass zk_keystore_pass -destkeystore ./zk_keystore.jks -srckeystore zookeeper-server.p12 -srcstoretype PKCS12 -srcstorepass zk_keystore_pass -alias zk_server -deststoretype JKS
    ```

    Truststore is also required by the Zookeeper for SSL/TLS connection to work.
    Even if client does not use certificates, this store must not be empty or errors will arise.
    Mutual authentication can be disabled in the config later.
    To generate `JKS` format truststore with CA certificate use following command:
    ```
    keytool -importcert -alias ca_cert -file certs/ca.pem -keystore ./zk_truststore.jks -storepass zk_truststore_pass -storetype JKS
    ```

    Add following lines to `zoo.cfg` configuration file:
    ```
    # port config
    secureClientPort=2181

    # General ssl settings
    serverCnxnFactory=org.apache.zookeeper.server.NettyServerCnxnFactory

    # Quorum ssl settings
    sslQuorum=true
    ssl.quorum.keyStore.location=/conf/zk_keystore.jks
    ssl.quorum.keyStore.password=zk_keystore_pass
    ssl.quorum.trustStore.location=/conf/zk_truststore.jks
    ssl.quorum.trustStore.password=zk_truststore_pass

    # Client ssl settings
    ssl.keyStore.location=/conf/zk_keystore.jks
    ssl.keyStore.password=zk_keystore_pass
    ssl.trustStore.location=/conf/zk_truststore.jks
    ssl.trustStore.password=zk_truststore_pass

    # Enable client to connect without certificate (disable mutual authentication)
    ssl.clientAuth=none
    ```

### Enable username:password authentication
- RabbitMQ - user can be added only to running server.

    Go to management web console and login as `guest:guest`.
    Then to to tab users and add new users with permissions you want (in this example it would be `pero:pero`).
    If added user is not administrator, make sure it have access (`tags`) to `monitoring` and `management` as these are needed by worker and watchdog to work correctly.

    In production environment it is also good idea to change password of the default user `guest` or even better, add another admin account and then delete `guest` entirely.

- Zookeeper - user have to be added before server is started.

    Zookeeper does not have its own implementation of authentication, it uses third party authentication providers like SASL (Simple Authentication and Security Layer), Kerberos, Mutual SSL/TLS authentication, etc.
    Authentication provider can be configured using `authProvider` option in config file. In this example SASL is used.
    Following lines needs to be added to config to enforce SASL authentication:
    ```
    # Authentication
    authProvider.sasl=org.apache.zookeeper.server.auth.SASLAuthenticationProvider
    enforce.auth.enabled=true
    enforce.auth.schemes=sasl
    ```

    Then path to the additional configuration file with user database have to be provided.
    This config file is required by SASL Java library, however zookeeper does not supports it in config, so it have to be added via server JVM flag.
    Create file java.env in the same directory as zoo.cfg (here `/conf/`) and add following line:
    ```
    SERVER_JVMFLAGS="-Djava.security.auth.login.config=/conf/auth.conf"
    ```

    Then create SASL JAAS config file in location you specified via JVM flag (here `/conf/auth.conf`) and add username and password for each user.
    Here two users are added, `super` with password `admin_pass` and `pero` with password `pero`.
    On the first line is the specification of algorithm used to hash passwords of these users. Here it is the default MD5.
    ```
    Server {
        org.apache.zookeeper.server.auth.DigestLoginModule required
        user_super="admin_pass"
        user_pero="pero";
    };
    ```

### Example configs
Configuration for RabbitMQ and Zookeeper are avaliable in subfolders `zookeeper` and `rabbitmq`. SFTP server users are configured via docker container parameter.
Worker, watchdog and all tools supports `--user` and `--password` options.

### Additional info on SSL/TLS and authentication
Zookeeper:  
https://zookeeper.apache.org/doc/r3.8.0/zookeeperAdmin.html  
https://docs.confluent.io/platform/current/security/zk-security.html  
https://cwiki.apache.org/confluence/display/ZOOKEEPER/ZooKeeper+SSL+User+Guide

RabbitMQ:  
https://www.rabbitmq.com/ssl.html

## Starting required services.
Mounted paths are required for data persistency and configurations.
Service can recover this data after restart, so it does not have to be set up again from scratch.
```
docker run  -d --rm -p 2181:2181 \
            -v /home/"$USER"/zookeeper-data:/data \
            -v /home/"$USER"/zookeeper-datalog:/datalog \
            -v /home/pavel/projects/PERO/zookeeper-config/zoo.cfg:/conf/zoo.cfg \
            -v /home/pavel/projects/PERO/zookeeper-config/zk_truststore.jks:/conf/zk_truststore.jks \
            -v /home/pavel/projects/PERO/zookeeper-config/zk_keystore.jks:/conf/zk_keystore.jks \
            -v /home/pavel/projects/PERO/zookeeper-config/java.env:/conf/java.env \
            -v /home/pavel/projects/PERO/zookeeper-config/auth.conf:/conf/auth.conf \ 
            --name="zookeeper" zookeeper
```
```
docker run  -d --rm -p 5672:5672 -p 15672:15672 \
            -v /home/"$USER"/rabbitmq:/var/lib/rabbitmq \
            -v /home/pavel/projects/PERO/rabbit-config/rabbitmq.conf:/etc/rabbitmq/rabbitmq.conf \
            -v /home/pavel/projects/PERO/rabbit-config/certs:/etc/rabbitmq/certs \
            --hostname "$(hostname)" \
            --name rabbitmq rabbitmq:management
```
```
docker run  -d --rm -p 2222:22 \
            -v /home/"$USER"/ftp:/home/pero/ \
            --name sftp atmoz/sftp:alpine pero:pero
```

## Initial system configuration

Set default server addresses and ports for auto-configuration:
```
python scripts/config_manager.py -z 127.0.0.1 -s 127.0.0.1 --ftp-servers 127.0.0.1:2222 --update-mq-servers --update-ftp-servers --update-monitoring-servers
```

Create processing stages for OCR pipeline:
```
python scripts/config_manager.py --name ocr_stage_x --config path/to/ocr_stage_x/config.ini --remote-path path/to/additional/data/on/ftp/server.tar.xz
```
Please note that you must upload additional files to SFTP server manually. Command above specifies just path used by worker to download these files from the server. To upload files use your favourite SFTP client.

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

## Processing

Uploading images for processing:
```
python scripts/publisher.py --stages stage1 stage2 stage3 out --images input/file/1 input/file/2
```

Downloading results:
```
python scripts/publisher.py --directory output/directory/path --download out
```
If you want to keep downloading images from ```out``` stage, add ```--keep-running``` argument at the end of the command above.


## Additional info

System was tested with these versions of libraries:
```
kazoo==2.8.0
pika==1.2.0
protobuf==3.19.4
python-magic==0.4.25
requests==2.27.1
numpy==1.21.5
opencv-python==4.5.5.62
lxml==4.7.1
scipy==1.7.3
numba==0.55.1
torch==1.10.2
torchvision==0.11.3
brnolm==0.2.0
scikit-learn==1.0.2
scikit-image==0.19.1
tensorflow-gpu==2.8.0
shapely==1.8.0
pyamg==4.2.1
imgaug==0.4.0
arabic_reshaper==2.1.3
```
Python version used during development was `Python 3.9.2` but it should work with latest versions of python and libraries as well.