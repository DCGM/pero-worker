#!/bin/sh

run_app () {
    if [ -z "${1}" ]; then
        return 1
    fi

    python3 ./log_daemon/log_daemon.py ${1}
}

cmd=""

if [ -n "${USE_CONFIG}" ]; then
    run_app "${cmd} -c ${CONFIG_FILE:-/etc/pero/logd.ini}"
    exit
fi

# add zookeeper servers
cmd="${cmd} -z ${ZOOKEEPER_SERVERS:-127.0.0.1}"

# username, password
cmd="${cmd} -u ${USERNAME:-pero} -p ${PASSWORD:-pero_pass}"

# add certificate path
cmd="${cmd} -e ${CA_CERT:-/etc/pero/certificates/ca.pem}"

# log queue
cmd="${cmd} -q ${LOG_QUEUE:-log}"

# output dir
cmd="${cmd} -o ${OUTPUT_DIR:-/var/log/pero}"

# log rotation period
if [ -n "${LOG_ROTATION_PERIOD}" ]; then
    cmd="${cmd} -r ${LOG_ROTATION_PERIOD}"
fi

# number of files to keep
cmd="${cmd} -n ${NUMBER_OF_FILES:-20}"

# debug
if [ -n "${DEBUG}" ]; then
    cmd="${cmd} -d"
fi

run_app "${cmd}"
