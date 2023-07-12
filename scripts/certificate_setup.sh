#!/bin/sh

BASEDIR=$(dirname "$(readlink -f "$0")")
OUTPUT_DIR="$BASEDIR/../config"

CERTIFICATES_OUTPUT_DIR="$OUTPUT_DIR/certificates"
CA_KEY_PASSWORD='RANDOM'
CA_KEY=
CA_CERT=

ZK_KEYSTORE_PASSWORD=zk_keystore_pass
ZK_TRUSTSTORE_PASSWORD=zk_truststore_pass

ZOOKEEPER_HOSTS=
MQ_HOSTS=

# checks if given string is valid ip address
check_ip(){
	# ipv4 checking
	if echo "$1" | grep -Eq '^(([0-9]{1,2}|1[0-9][0-9]|2[0-4][0-9]|25[0-5])\.){3}([0-9]{1,2}|1[0-9][0-9]|2[0-4][0-9]|25[0-5])$'
	then
		return 0 # is ipv4
	fi

	# ipv6 checking
	if echo "$1" | grep -Eq '^(([0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}|::([0-9a-fA-F]{1,4}:){0,6}[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,6}:[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,5}:([0-9a-fA-F]{1,4}:){0,1}[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,4}:([0-9a-fA-F]{1,4}:){0,2}[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,3}:([0-9a-fA-F]{1,4}:){0,3}[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,2}:([0-9a-fA-F]{1,4}:){0,4}[0-9a-fA-F]{1,4}|[0-9a-fA-F]{1,4}::([0-9a-fA-F]{1,4}:){0,5}[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,7}:)$'
	then
		return 0 # is ipv6
	fi

	# not an ip addr
	return 1
}

# adds host name to host list given by second argument (ZK, MQ)
add_hostname() {
    if [ "$2" = "ZK" ]; then
        if [ -z "$ZOOKEEPER_HOSTS" ]; then
            ZOOKEEPER_HOSTS="DNS:$1"
        else
            ZOOKEEPER_HOSTS="$ZOOKEEPER_HOSTS,DNS:$1"
        fi
    else  # MQ
        if [ -z "$MQ_HOSTS" ]; then
            MQ_HOSTS="DNS:$1"
        else
            MQ_HOSTS="$MQ_HOSTS,DNS:$1"
        fi
    fi
}

# adds ip address to host list given by second argument (ZK, MQ)
add_ip() {
    if [ "$2" = "ZK" ]; then
        if [ -z "$ZOOKEEPER_HOSTS" ]; then
            ZOOKEEPER_HOSTS="IP:$1"
        else
            ZOOKEEPER_HOSTS="$ZOOKEEPER_HOSTS,IP:$1"
        fi
    else  # MQ
        if [ -z "$MQ_HOSTS" ]; then
            MQ_HOSTS="IP:$1"
        else
            MQ_HOSTS="$MQ_HOSTS,IP:$1"
        fi
    fi
}

# adds correct host record to the list of hosts given by second argument
# $1 : host ip or hostname without port number
# $2 : ZK or MQ to add to list of Zookeeper servers or Message broker servers
add_host() {
    if check_ip "$1"
    then
        add_ip "$1" "$2"
    else
        add_hostname "$1" "$2"
    fi
}

print_help() {
    echo "Setup tool for PERO-Worker SSL/TLS certificates."
    echo "Generates certificates for the worker system services."
    echo "Exaple usage:"
    echo "$ sh certificate_setup.sh -m 123.123.123.123 -z example.org"
    echo "Exaple generates certificate authority key and certificate and then"
    echo "generates key and certificate for Zookeeper server located at"
    echo "example.org and Message broker server at 123.123.123.123."
    echo "Options:"
    echo "  -z|--zookeeper   Zookeeper server hostname or ip address to add."
    echo "                   Can be specified multiple times. Hostname or ip"
    echo "                   have to be entered without port number."
    echo "  -m|--mq          Message Broker server hostname or ip address to"
    echo "                   add. Can be specified multiple times. Hostname"
    echo "                   or ip have to be entered without port number."
    echo "  -c|--ca-key      CA key to use for signing server certificates."
    echo "                   If omitted, CA key and certificate will be"
    echo "                   generated."
    echo "  -e|--ca-cert     CA certificate to use with CA key. If omitted,"
    echo "                   CA key and certificate will be generated."
    echo "  -p|--ca-key-password"
    echo "                   CA key encription password. If omitted and key is"
    echo "                   not supplied, password will be generated to file"
    echo "                   located in the output directory."
    echo "  --zk-keystore-password"
    echo "                   Password for zookeeper keystore where certificates"
    echo "                   are stored. If omitted, default password is used."
    echo "  --zk-truststore-password"
    echo "                   Password for zookeeper truststore, where trusted"
    echo "                   certificates are stored. If omitted, default"
    echo "                   password is used."
    echo "  -o|--output-dir  Output directory where generated certificates will"
    echo "                   be placed. Default is '../config/certificates'."
    echo "  -h|--help        Prints this help message and exits the program."
}

# parse args
while true; do
    case "$1" in
        --zookeeper|-z )
            add_host "$2" ZK
            shift 2
            ;;
        --mq|-m )
            add_host "$2" MQ
            shift 2
            ;;
        --ca-key|-c )
            CA_KEY=$2
            shift 2
            ;;
        --ca-cert|-e )
            CA_CERT=$2
            shift 2
            ;;
        --ca-key-password|-p )
            CA_KEY_PASSWORD="$2"
            shift 2
            ;;
        --zk-keystore-password )
            ZK_KEYSTORE_PASSWORD="$2"
            shift 2
            ;;
        --zk-truststore-password )
            ZK_TRUSTSTORE_PASSWORD="$2"
            shift 2
            ;;
        --output-dir|-o )
            OUTPUT_DIR="$2"
            shift 2
            ;;
        --help|-h )
            print_help
            exit 0
            ;;
        * )
            break
            ;;
    esac
done

# DEBUG
#echo "$ZOOKEEPER_HOSTS"
#echo "$MQ_HOSTS"
#echo "$CA_KEY_PASSWORD"

# create directories
mkdir -p "$CERTIFICATES_OUTPUT_DIR"

# generate CA password
if [ "$CA_KEY_PASSWORD" = 'RANDOM' ] && [ -z "$CA_KEY" ]; then
    CA_KEY_PASSWORD="$(openssl rand -base64 48)"
    echo "$CA_KEY_PASSWORD" > "$CERTIFICATES_OUTPUT_DIR"/ca-key-passwd.txt
fi

export CA_KEY_PASSWORD

# generate CA key and certificate
if [ -z "$CA_KEY" ] || [ -z "$CA_CERT" ]; then
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
    
    CA_KEY="$CERTIFICATES_OUTPUT_DIR"/ca-key.pem
    CA_CERT="$CERTIFICATES_OUTPUT_DIR"/ca.pem
fi

export CA_KEY
export CA_CERT

# generate rabbitmq key and certificate
if [ -n "$MQ_HOSTS" ]; then
    # generate rabbitmq key and certificate signing request
    openssl genrsa -out "$CERTIFICATES_OUTPUT_DIR"/rabbit-server-key.pem 4096
    openssl req -new -sha256 \
            -subj "/CN=pero-MQ-server" \
            -key "$CERTIFICATES_OUTPUT_DIR"/rabbit-server-key.pem \
            -out "$CERTIFICATES_OUTPUT_DIR"/rabbit-server-cert.csr

    # set additional server properties
    echo "subjectAltName=$MQ_HOSTS" > "$CERTIFICATES_OUTPUT_DIR"/rabbit-extfile.cnf
    echo extendedKeyUsage = serverAuth >> "$CERTIFICATES_OUTPUT_DIR"/rabbit-extfile.cnf

    # generate rabbitmq server certificate
    openssl x509 -req -sha256 \
            -days 3650 \
            -in "$CERTIFICATES_OUTPUT_DIR"/rabbit-server-cert.csr \
            -CA "$CA_CERT" \
            -CAkey "$CA_KEY" \
            -out "$CERTIFICATES_OUTPUT_DIR"/rabbit-server-cert.pem \
            -extfile "$CERTIFICATES_OUTPUT_DIR"/rabbit-extfile.cnf \
            -passin env:CA_KEY_PASSWORD \
            -CAcreateserial
fi

# generate zookeeper key and certificate
if [ -n "$ZOOKEEPER_HOSTS" ]; then
    # generate zookeeper key and certificate signing request
    openssl genrsa -out "$CERTIFICATES_OUTPUT_DIR"/zookeeper-server-key.pem 4096
    openssl req -new -sha256 \
            -subj "/CN=pero-ZK-server" \
            -key "$CERTIFICATES_OUTPUT_DIR"/zookeeper-server-key.pem \
            -out "$CERTIFICATES_OUTPUT_DIR"/zookeeper-server-cert.csr
    
    # set additional server properties
    echo "subjectAltName=$ZOOKEEPER_HOSTS" > "$CERTIFICATES_OUTPUT_DIR"/zookeeper-extfile.cnf
    echo extendedKeyUsage = serverAuth >> "$CERTIFICATES_OUTPUT_DIR"/zookeeper-extfile.cnf

    # generate zookeeper server certificate
    openssl x509 -req -sha256 \
            -days 3650 \
            -in "$CERTIFICATES_OUTPUT_DIR"/zookeeper-server-cert.csr \
            -CA "$CA_CERT" \
            -CAkey "$CA_KEY" \
            -out "$CERTIFICATES_OUTPUT_DIR"/zookeeper-server-cert.pem \
            -extfile "$CERTIFICATES_OUTPUT_DIR"/zookeeper-extfile.cnf \
            -passin env:CA_KEY_PASSWORD \
            -CAcreateserial

    # create PKCS12 keystore
    openssl pkcs12 -export \
            -in "$CERTIFICATES_OUTPUT_DIR"/zookeeper-server-cert.pem \
            -inkey "$CERTIFICATES_OUTPUT_DIR"/zookeeper-server-key.pem \
            -out "$CERTIFICATES_OUTPUT_DIR"/zookeeper-server.p12 \
            -name zk_server \
            -CAfile "$CA_CERT" \
            -caname root \
            -password pass:"$ZK_KEYSTORE_PASSWORD"

    # create zookeeper JKS keystore from the PKCS12
    keytool -importkeystore \
            -deststorepass "$ZK_KEYSTORE_PASSWORD" \
            -destkeystore "$CERTIFICATES_OUTPUT_DIR"/zk_keystore.jks \
            -srckeystore "$CERTIFICATES_OUTPUT_DIR"/zookeeper-server.p12 \
            -srcstoretype PKCS12 \
            -srcstorepass "$ZK_KEYSTORE_PASSWORD" \
            -alias zk_server \
            -deststoretype JKS \
            -noprompt

    # create zookeeper JKS trust store with CA cert
    keytool -importcert \
            -alias ca_cert \
            -file "$CA_CERT" \
            -keystore "$CERTIFICATES_OUTPUT_DIR"/zk_truststore.jks \
            -storepass "$ZK_TRUSTSTORE_PASSWORD" \
            -storetype JKS \
            -noprompt
fi

# add read permissions to group and others to prevent
# permissions issues in service containers
chmod 644 "$CERTIFICATES_OUTPUT_DIR"/*

