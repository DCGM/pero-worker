#!/bin/sh

run_app () {
    if [ -z "${1}" ]; then
        return 1
    fi

    . ./.venv/bin/activate
    python ./worker/run_worker.py ${1}
}

cmd=""

if [ -n "${USE_CONFIG}" ]; then
    run_app "${cmd} -c ${CONFIG_FILE:-/etc/pero/worker.ini}"
    exit
fi

# add zookeeper servers
cmd="${cmd} -z ${ZOOKEEPER_SERVERS:-127.0.0.1}"

# add id
if [ -n "${WORKER_ID}" ]; then
    cmd="${cmd} -i ${WORKER_ID}"
fi

# add tmp dir path
cmd="${cmd} --cache-directory ${CACHE_DIR:-/tmp}"

# username, password
cmd="${cmd} -u ${USERNAME:-pero} -p ${PASSWORD:-pero_pass}"

# add certificate path
cmd="${cmd} -e ${CA_CERT:-/etc/pero/certificates/ca.pem}"

# disable remote logging
if [ -n "${DISABLE_REMOTE_LOGGING}" ]; then
    cmd="${cmd} --disable-remote-logging"
fi

# log queue
cmd="${cmd} -q ${LOG_QUEUE:-log}"

# debug
if [ -n "${DEBUG}" ]; then
    cmd="${cmd} -d"
fi

run_app "${cmd}"
