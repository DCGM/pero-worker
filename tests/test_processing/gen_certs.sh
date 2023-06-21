#!/bin/sh

OUTPUT_FOLDER="$(pwd)"/certificates
mkdir -p "$OUTPUT_FOLDER"

# generate CA password
openssl rand -base64 48 > "$OUTPUT_FOLDER"/ca-key-passwd.txt

# generates unencrypted CA key - to encrypt key add -aes256 option
openssl genrsa -aes256 \
        -passout file:"$OUTPUT_FOLDER"/ca-key-passwd.txt \
        -out "$OUTPUT_FOLDER"/ca-key.pem \
        4096

# generate CA certificate
openssl req -new -x509 -sha256 \
        -days 3650 \
        -subj "/CN=pero-ca" \
        -key "$OUTPUT_FOLDER"/ca-key.pem \
        -out "$OUTPUT_FOLDER"/ca.pem \
        -passin file:"$OUTPUT_FOLDER"/ca-key-passwd.txt

# generate rabbitmq key and certificate signing request
openssl genrsa -out "$OUTPUT_FOLDER"/rabbit-server-key.pem 4096
openssl req -new -sha256 \
        -subj "/CN=pero-server" \
        -key "$OUTPUT_FOLDER"/rabbit-server-key.pem \
        -out "$OUTPUT_FOLDER"/rabbit-server-cert.csr

# set additional server properties
echo "subjectAltName=IP:172.26.0.51" > "$OUTPUT_FOLDER"/rabbit-extfile.cnf
echo extendedKeyUsage = serverAuth >> "$OUTPUT_FOLDER"/rabbit-extfile.cnf

# generate rabbitmq server certificate
openssl x509 -req -sha256 \
        -days 3650 \
        -in "$OUTPUT_FOLDER"/rabbit-server-cert.csr \
        -CA "$OUTPUT_FOLDER"/ca.pem \
        -CAkey "$OUTPUT_FOLDER"/ca-key.pem \
        -out "$OUTPUT_FOLDER"/rabbit-server-cert.pem \
        -extfile "$OUTPUT_FOLDER"/rabbit-extfile.cnf \
        -passin file:"$OUTPUT_FOLDER"/ca-key-passwd.txt \
        -CAcreateserial

# generate zookeeper key and certificate signing request
openssl genrsa -out "$OUTPUT_FOLDER"/zookeeper-server-key.pem 4096
openssl req -new -sha256 \
        -subj "/CN=pero-server" \
        -key "$OUTPUT_FOLDER"/zookeeper-server-key.pem \
        -out "$OUTPUT_FOLDER"/zookeeper-server-cert.csr

# set additional server properties
echo "subjectAltName=IP:172.26.0.50" > "$OUTPUT_FOLDER"/zookeeper-extfile.cnf
echo extendedKeyUsage = serverAuth >> "$OUTPUT_FOLDER"/zookeeper-extfile.cnf

# generate zookeeper server certificate
openssl x509 -req -sha256 \
        -days 3650 \
        -in "$OUTPUT_FOLDER"/zookeeper-server-cert.csr \
        -CA "$OUTPUT_FOLDER"/ca.pem \
        -CAkey "$OUTPUT_FOLDER"/ca-key.pem \
        -out "$OUTPUT_FOLDER"/zookeeper-server-cert.pem \
        -extfile "$OUTPUT_FOLDER"/zookeeper-extfile.cnf \
        -passin file:"$OUTPUT_FOLDER"/ca-key-passwd.txt \
        -CAcreateserial

# create PKCS12 keystore
openssl pkcs12 -export \
        -in "$OUTPUT_FOLDER"/zookeeper-server-cert.pem \
        -inkey "$OUTPUT_FOLDER"/zookeeper-server-key.pem \
        -out "$OUTPUT_FOLDER"/zookeeper-server.p12 \
        -name zk_server \
        -CAfile "$OUTPUT_FOLDER"/ca.pem \
        -caname root \
        -password pass:zk_keystore_pass

# create zookeeper JKS keystore from the PKCS12
keytool -importkeystore \
        -deststorepass zk_keystore_pass \
        -destkeystore "$OUTPUT_FOLDER"/zk_keystore.jks \
        -srckeystore "$OUTPUT_FOLDER"/zookeeper-server.p12 \
        -srcstoretype PKCS12 \
        -srcstorepass zk_keystore_pass \
        -alias zk_server \
        -deststoretype JKS

# create zookeeper JKS trust store with CA cert
keytool -importcert \
        -alias ca_cert \
        -file "$OUTPUT_FOLDER"/ca.pem \
        -keystore "$OUTPUT_FOLDER"/zk_truststore.jks \
        -storepass zk_truststore_pass \
        -storetype JKS \
        -noprompt

chmod 644 "$OUTPUT_FOLDER"/*

