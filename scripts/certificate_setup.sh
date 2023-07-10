#!/bin/sh

BASEDIR=$(dirname "$(readlink -f "$0")")
OUTPUT_DIR="$BASEDIR/../config"

CERTIFICATES_OUTPUT_DIR="$OUTPUT_DIR/certificates"
CA_KEY_PASSWORD='RANDOM'

HOSTS=
IPS=

add_host() {
    if [ -z "$HOSTS" ]; then
        HOSTS="DNS:$1"
    else
        HOSTS="$HOSTS,DNS:$1"
    fi
}

add_ip() {
    if [ -z "$IPS" ]; then
        IPS="IP:$1"
    else
        IPS="$IPS,IP:$1"
    fi
}

# parse args
while true; do
    case "$1" in
        --host )
            add_host "$2"
            shift 2
            ;;
        --ip )
            add_ip "$2"
            shift 2
            ;;
        --output-dir )
            OUTPUT_DIR="$2"
            shift 2
            ;;
        --ca-key-password )
            CA_KEY_PASSWORD="$2"
            shift 2
            ;;
        * )
            break
            ;;
    esac
done

# DEBUG
#echo "$HOSTS"
#echo "$IPS"
#echo "$CA_KEY_PASSWORD"

if [ -z "$HOSTS" ] && [ -z "$IPS" ]; then
    echo "No server hostnames or ips were given!" >&2
    exit 1
fi

# create directories
mkdir -p "$CERTIFICATES_OUTPUT_DIR"

# generate CA password
if [ "$CA_KEY_PASSWORD" = 'RANDOM' ]; then
    CA_KEY_PASSWORD="$(openssl rand -base64 48)"
    echo "$CA_KEY_PASSWORD" > "$CERTIFICATES_OUTPUT_DIR"/ca-key-passwd.txt
fi

export CA_KEY_PASSWORD

# generates encrypted CA key
openssl genrsa -aes256 \
        -passout env:CA_KEY_PASSWORD \
        -out "$CERTIFICATES_OUTPUT_DIR"/ca-key.pem \
        4096

# generate CA certificate
openssl req -new -x509 -sha256 \
        -days 3650 \
        -subj "/CN=pero-ca" \
        -key "$CERTIFICATES_OUTPUT_DIR"/ca-key.pem \
        -out "$CERTIFICATES_OUTPUT_DIR"/ca.pem \
        -passin env:CA_KEY_PASSWORD

# generate rabbitmq key and certificate signing request
openssl genrsa -out "$CERTIFICATES_OUTPUT_DIR"/rabbit-server-key.pem 4096
openssl req -new -sha256 \
        -subj "/CN=pero-server" \
        -key "$CERTIFICATES_OUTPUT_DIR"/rabbit-server-key.pem \
        -out "$CERTIFICATES_OUTPUT_DIR"/rabbit-server-cert.csr

# set additional server properties
alt_names="subjectAltName="
if [ -n "$HOSTS" ]; then
    alt_names="$alt_names$HOSTS"
    if [ -n "$IPS" ]; then
        alt_names="$alt_names,$IPS"
    fi
else
    alt_names="$alt_names$IPS"
fi
echo "$alt_names" > "$CERTIFICATES_OUTPUT_DIR"/extfile.cnf
echo extendedKeyUsage = serverAuth >> "$CERTIFICATES_OUTPUT_DIR"/extfile.cnf

# generate rabbitmq server certificate
openssl x509 -req -sha256 \
        -days 3650 \
        -in "$CERTIFICATES_OUTPUT_DIR"/rabbit-server-cert.csr \
        -CA "$CERTIFICATES_OUTPUT_DIR"/ca.pem \
        -CAkey "$CERTIFICATES_OUTPUT_DIR"/ca-key.pem \
        -out "$CERTIFICATES_OUTPUT_DIR"/rabbit-server-cert.pem \
        -extfile "$CERTIFICATES_OUTPUT_DIR"/extfile.cnf \
        -passin env:CA_KEY_PASSWORD \
        -CAcreateserial

# generate zookeeper key and certificate signing request
openssl genrsa -out "$CERTIFICATES_OUTPUT_DIR"/zookeeper-server-key.pem 4096
openssl req -new -sha256 \
        -subj "/CN=pero-server" \
        -key "$CERTIFICATES_OUTPUT_DIR"/zookeeper-server-key.pem \
        -out "$CERTIFICATES_OUTPUT_DIR"/zookeeper-server-cert.csr

# generate zookeeper server certificate
openssl x509 -req -sha256 \
        -days 3650 \
        -in "$CERTIFICATES_OUTPUT_DIR"/zookeeper-server-cert.csr \
        -CA "$CERTIFICATES_OUTPUT_DIR"/ca.pem \
        -CAkey "$CERTIFICATES_OUTPUT_DIR"/ca-key.pem \
        -out "$CERTIFICATES_OUTPUT_DIR"/zookeeper-server-cert.pem \
        -extfile "$CERTIFICATES_OUTPUT_DIR"/extfile.cnf \
        -passin env:CA_KEY_PASSWORD \
        -CAcreateserial

# create PKCS12 keystore
openssl pkcs12 -export \
        -in "$CERTIFICATES_OUTPUT_DIR"/zookeeper-server-cert.pem \
        -inkey "$CERTIFICATES_OUTPUT_DIR"/zookeeper-server-key.pem \
        -out "$CERTIFICATES_OUTPUT_DIR"/zookeeper-server.p12 \
        -name zk_server \
        -CAfile "$CERTIFICATES_OUTPUT_DIR"/ca.pem \
        -caname root \
        -password pass:zk_keystore_pass

# create zookeeper JKS keystore from the PKCS12
keytool -importkeystore \
        -deststorepass zk_keystore_pass \
        -destkeystore "$CERTIFICATES_OUTPUT_DIR"/zk_keystore.jks \
        -srckeystore "$CERTIFICATES_OUTPUT_DIR"/zookeeper-server.p12 \
        -srcstoretype PKCS12 \
        -srcstorepass zk_keystore_pass \
        -alias zk_server \
        -deststoretype JKS

# create zookeeper JKS trust store with CA cert
keytool -importcert \
        -alias ca_cert \
        -file "$CERTIFICATES_OUTPUT_DIR"/ca.pem \
        -keystore "$CERTIFICATES_OUTPUT_DIR"/zk_truststore.jks \
        -storepass zk_truststore_pass \
        -storetype JKS \
        -noprompt

chmod 644 "$CERTIFICATES_OUTPUT_DIR"/*

