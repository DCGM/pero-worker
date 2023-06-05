#!/bin/sh

run_app () {
    if [ -z "${1}" ]; then
        return 1
    fi

    . ./.venv/bin/activate
    python ./watchdog/worker_watchdog.py ${1}
}

cmd=""

if [ -n "${USE_CONFIG}" ]; then
    run_app "${cmd} -c ${CONFIG_FILE:-/etc/pero/watchdog.ini}"
    exit
fi

# add zookeeper servers
cmd="${cmd} -z ${ZOOKEEPER_SERVERS:-127.0.0.1}"

# username, password
cmd="${cmd} -u ${USERNAME:-pero} -n ${PASSWORD:-pero_pass}"

# add certificate path
cmd="${cmd} -e ${CA_CERT:-/etc/pero/certificates/ca.pem}"

# debug
if [ -n "${DEBUG}" ]; then
    cmd="${cmd} -d"
fi

# dry run
if [ -n "${DRY_RUN}" ]; then
    cmd="${cmd} --dry-run"
fi

run_app "${cmd}"
