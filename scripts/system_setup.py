#!/usr/bin/env python3

# sets up the whole worker system

import argparse
import os
import sys
import string
import subprocess
import jinja2
import copy
import random
import shutil
import time

import libs.worker_functions.connection_aux_functions as cf

# path to the system setup script directory
file_dir_path = os.path.dirname(os.path.realpath(__file__))

def parse_args():
    parser = argparse.ArgumentParser(
        'Sets up message broker and zookeeper server instance.'
    )
    parser.add_argument(
        '-z', '--zookeeper',
        help='List of zookeeper server domain names and IPs.',
        nargs='+',
        required=True
    )
    parser.add_argument(
        '--zookeeper-secure-port',
        help='SSL/TLS enabled port for zookeeper (default = 2181).',
        default=2181,
        type=int
    )
    parser.add_argument(
        '--zookeeper-port',
        help='Unsecure port for zookeeper for local connection (default = 2180).',
        default=2180,
        type=int
    )
    parser.add_argument(
        '-s', '--mq-server',
        help='List of message queue server domain names and IPs.',
        nargs='+',
        required=True
    )
    parser.add_argument(
        '--mq-amqp-port',
        help='AMQP(S) protocol port for message broker (default = 5672).',
        default=5672,
        type=int
    )
    parser.add_argument(
        '--mq-management-port',
        help='HTTP(S) API and Management console port for message broker (default = 15672).',
        default=15672,
        type=int
    )
    parser.add_argument(
        '-f', '--sftp-server',
        help='List of SFTP server domain names and IPs.',
        nargs='+',
        required=True
    )
    parser.add_argument(
        '--sftp-port',
        help='SFTP server port (default = 22).',
        default=22,
        type=int
    )
    parser.add_argument(
        '-u', '--username',
        help='Username for authentication on server.',
        default='pero'
    )
    parser.add_argument(
        '-p', '--password',
        help='Password for user authentication. Will be generated if none is given.',
        default=None
    )
    parser.add_argument(
        '--root-account',
        help='Superuser/administrator user name.',
        default='pero_admin'
    )
    parser.add_argument(
        '--root-password',
        help='Superuser/administrator password, will be generated if none is given.',
        default=None
    )
    parser.add_argument(
        '-c', '--ca-key',
        help='CA key for signing certificate requests.',
        default=None
    )
    parser.add_argument(
        '-e', '--ca-cert',
        help='CA Certificate for SSL/TLS connection verification.',
        default=None
    )
    parser.add_argument(
        '-r', '--ca-key-password',
        help='CA key encription password. Will be generated if none is given.',
        default=None
    )
    parser.add_argument(
        '--zookeeper-keystore-password',
        help='Password to decrypt zookeeper keystore.',
        default='zk_keystore_pass'
    )
    parser.add_argument(
        '--zookeeper-truststore-password',
        help='Password to decrypt zookeeper truststore.',
        default='zk_truststore_pass'
    )
    parser.add_argument(
        '-o', '--output-directory',
        help='Directory where configuration will be stored.',
        default=os.path.join(file_dir_path, '../config')
    )
    parser.add_argument(
        '-d', '--data-directory',
        help='Data directory for the services.',
        default=os.path.join(file_dir_path, '../data')
    )
    return parser.parse_args()

def generate_certificates(
    output_dir=None,
    ca_cert=None,
    ca_key=None,
    ca_key_password=None,
    zookeeper_server=None,
    mq_server=None
):
    """
    Generates certificates to the output directory.
    :param output_dir: output directory to generate certificates to
    :param ca_cert: path to certificate auhority certificate to use
    :param ca_key: path to certificate authority key to use
    :param ca_key_password: password for ca_key if encrypted
    :param zookeeper_server: ip addresses and domain names of zookeeper server
    :param mq_server: ip addresses and domain names of message broker server
    """
    
    # build command
    zk_server_args = []
    for server in zookeeper_server:
        zk_server_args.append('--zookeeper')
        zk_server_args.append(server['host'])

    mq_server_args = []
    for server in mq_server:
        mq_server_args.append('--mq')
        mq_server_args.append(server['host'])
    
    command = ['sh', os.path.join(file_dir_path, 'certificate_setup.sh')]
    if ca_cert and ca_key:
        command += ['--ca-key', ca_key, '--ca-cert', ca_cert]
        if ca_key_password:
            command += ['--ca-key-password', ca_key_password]
    
    command += ['--output-dir', output_dir]

    command += zk_server_args
    command += mq_server_args

    # call script for certificate generation with correct params
    if subprocess.call(command):
        raise RuntimeError('Failed to generate certificates!')

def generate_password() -> str:
    """
    Generates 64 chars long password.
    :return: password
    """
    chars = string.ascii_letters + string.digits + '!@#$%^&*()'
    length = 64
    passwd = ''
    for i in range(0, length):
        passwd += chars[random.SystemRandom().randint(0, len(chars) - 1)]
    return passwd

def generate_mq_config(output_dir, amqp_port, management_port):
    """
    Generates MQ configuration.
    :param output_dir: mq configuration output directory
    :param amqp_port: amqp port number
    :param management_port: https management port number
    """
    env = jinja2.Environment(
        loader=jinja2.FileSystemLoader('templates'),
        autoescape=jinja2.select_autoescape()
    )
    mq_config_template = env.get_template('rabbitmq/rabbitmq.conf.jinja2')
    with open(os.path.join(output_dir, 'rabbitmq.conf'), 'w') as out_file:
        out_file.write(mq_config_template.render(
            amqp_port = amqp_port,
            management_port = management_port
        ))

def generate_zk_config(
    output_dir,
    username,
    password,
    super_password,
    zookeeper_port,
    zookeeper_secure_port,
    zookeeper_keystore_password,
    zookeeper_truststore_password
):
    """
    Generate zookeeper configuration.
    :param output_dir: zookeeper config output directory
    :param username: username for zookeeper access by other services
    :param password: password to use for user authentication
    :param super_password: password for the super user
    :param zookeeper_port: zookeeper port to use for local connection
    :param zookeeper_secure_port: zookeeper encrypted port for remote connection
    :param zookeeper_keystore_password: password for zookeeper keystore
    :param zookeeper_truststore_password: password for zookeeper truststore
    """
    env = jinja2.Environment(
        loader=jinja2.FileSystemLoader('templates'),
        autoescape=jinja2.select_autoescape()
    )
    auth_conf = env.get_template('zookeeper/auth.conf.jinja2')
    with open(os.path.join(output_dir, 'auth.conf'), 'w') as out_file:
        out_file.write(auth_conf.render(
            username = username,
            password = password,
            super_password = super_password
        ))
    zk_config = env.get_template('zookeeper/zoo.cfg.jinja2')
    with open(os.path.join(output_dir, 'zoo.cfg'), 'w') as out_file:
        out_file.write(zk_config.render(
            zookeeper_port = zookeeper_port,
            zookeeper_secure_port = zookeeper_secure_port,
            zookeeper_keystore_password = zookeeper_keystore_password,
            zookeeper_truststore_password = zookeeper_truststore_password
        ))
    env_config = env.get_template('zookeeper/java.env.jinja2')
    with open(os.path.join(output_dir, 'java.env'), 'w') as out_file:
        out_file.write(env_config.render())

def generate_sftp_config(output_dir, username, password):
    """
    Generates configuration for sftp server.
    :param output_dir: directory where configuration will be placed
    :param username: sftp user name
    :param password: sftp user password
    """
    env = jinja2.Environment(
        loader=jinja2.FileSystemLoader('templates'),
        autoescape=jinja2.select_autoescape()
    )
    user_configuration = env.get_template('sftp/users.conf.jinja2')
    with open(os.path.join(output_dir, 'users.conf'), 'w') as out_file:
        out_file.write(user_configuration.render(
            username = username,
            password = password
        ))

def generate_docker_env(
    output_dir,
    host_name,
    sftp_data_dir,
    sftp_config_dir,
    mq_config_dir,
    mq_data_dir,
    zookeeper_config_dir,
    zookeeper_data_dir,
    zookeeper_secure_port,
    mq_amqp_port,
    mq_management_port,
    sftp_port
):
    """
    Generates docker compose environment variables file.
    :param output_dir: output directory where to place env file
    :param host_name: host name to use in containers
    :param sftp_data_dir: path to directory for sftp data
    :param sftp_config_dir: path to directory with sftp configuration
    :param mq_config_dir: path to directory with mq configuration
    :param mq_data_dir: path to directory with mq data
    :param zookeeper_config_dir: path to directory with zookeeper configuration
    :param zookeeper_data_dir: path to directory with zookeeper data
    :param zookeeper_secure_port: zookeeper port to expose for remote connection
    :param mq_amqp_port: AMQP port to expose
    :param mq_management_port: message broker management port to expose
    :param sftp_port: SFTP port to expose for remote connection
    """
    env = jinja2.Environment(
        loader=jinja2.FileSystemLoader('templates'),
        autoescape=jinja2.select_autoescape()
    )
    docker_env = env.get_template('docker.env.jinja2')
    with open(os.path.join(output_dir, 'docker.env'), 'w') as out_file:
        out_file.write(docker_env.render(
            zookeeper_config_dir=zookeeper_config_dir,
            zookeeper_data_dir=zookeeper_data_dir,
            mq_config_dir=mq_config_dir,
            mq_data_dir=mq_data_dir,
            sftp_data_dir=sftp_data_dir,
            sftp_config_dir=sftp_config_dir,
            host_name=host_name,
            zookeeper_secure_port=zookeeper_secure_port,
            mq_amqp_port=mq_amqp_port,
            mq_management_port=mq_management_port,
            sftp_port=sftp_port
        ))
    
def generate_client_configs(
    output_dir,
    zookeeper_server,
    username,
    password,
    ca_cert,
    log_dir
):
    """
    Generates client configuration.
    :param output_dir: output directory where config will be placed.
    :param zookeeper_server: zookeeper server list to use for connection
    :param username: client user name
    :param password: client user's password
    :param ca_cert: CA certificate for server identity verification
    :param log_dir: output directory where log daemon will place collected logs
    """
    env = jinja2.Environment(
        loader=jinja2.FileSystemLoader('templates'),
        autoescape=jinja2.select_autoescape()
    )
    worker_config = env.get_template('worker.ini.jinja2')
    zk_servers = [
        '{host}:{port}'.format(host=node['host'], port=node['port']) for node in zookeeper_server
    ]
    with open(os.path.join(output_dir, 'worker.ini'), 'w') as out_file:
        out_file.write(worker_config.render(
            zookeeper_servers=cf.zk_server_list(zk_servers),
            username=username,
            password=password,
            ca_cert=ca_cert
        ))
    
    watchdog_config = env.get_template('watchdog.ini.jinja2')
    with open(os.path.join(output_dir, 'watchdog.ini'), 'w') as out_file:
        out_file.write(watchdog_config.render(
            zookeeper_servers=cf.zk_server_list(zk_servers),
            username=username,
            password=password,
            ca_cert=ca_cert
        ))
    
    logd_config = env.get_template('logd.ini.jinja2')
    with open(os.path.join(output_dir, 'logd.ini'), 'w') as out_file:
        out_file.write(logd_config.render(
            zookeeper_servers=cf.zk_server_list(zk_servers),
            username=username,
            password=password,
            ca_cert=ca_cert,
            output_dir=log_dir
        ))

def run_services(env):
    """
    Runs rabbitmq, zookeeper and sftp service as docker containers.
    :param env: docker environment file
    """
    rc = subprocess.call([
        'docker', 'compose',
            '-f', os.path.join(file_dir_path, 'services-docker-compose.yaml'),
            '--env-file', env,
            'up', '-d'
    ])
    if rc:
        raise RuntimeError('Failed to start services!')

def apply_runtime_config(
    zk_server,
    mq_server,
    mq_management_server,
    sftp_server,
    username,
    password,
    super_user,
    super_password,
    ca_cert
):
    """
    Configures services in runtime.
    :param zk_server: list of zookeeper server domain names and ips to use
    :param mq_server: list of message broker server domain names and ips to configure
    :param mq_management_server: list of mq management server domain names and ips to configure
    :param sftp_server: list of sftp server domain names and ips to configure
    :param username: user account name
    :param password: user's password
    :param super_user: super user account name
    :param super_password: super user's password
    :param ca_cert: certification authority certificate for server identity
                    verification and encrypted connection establishment
    """

    zookeeper_server_list = [ cf.host_port_to_string(node) for node in zk_server ]
    mq_server_list = [ cf.host_port_to_string(node) for node in mq_server ]
    mq_management_server_list = [ cf.host_port_to_string(node) for node in mq_management_server ]
    sftp_server_list = [ cf.host_port_to_string(node) for node in sftp_server ]

    # configure the servers in zookeeper
    print('Configuring available servers in zookeeper.')
    command = [
        'python', os.path.join(file_dir_path, 'server_config_manager.py'),
            '-e', ca_cert,
            '-u', username,
            '-p', password,
            '-z'] + zookeeper_server_list + [
            '--add-mq-servers'] + mq_server_list + [
            '--add-monitoring-servers'] + mq_management_server_list + [
            '--add-ftp-servers'] + sftp_server_list
    if subprocess.call(command):
        raise RuntimeError('Failed to add server configuration to zookeeper!')
    
    # configure mq user
    print('Adding user to MQ server.')
    command = [
        'python', os.path.join(file_dir_path, 'mq_server_config_manager.py'),
            '-e', ca_cert,
            '-u', username,
            '-p', password,
            '-z'] + zookeeper_server_list + [
            '--mq-user', 'guest',
            '--mq-password', 'guest',
            '-a', username, password,
            '-t', 'monitoring', 'management']
    if subprocess.call(command):
        raise RuntimeError('Failed to add user to MQ!')
    
    # configure mq super user
    print('Adding super user to MQ server.')
    command = [
        'python', os.path.join(file_dir_path, 'mq_server_config_manager.py'),
            '-e', ca_cert,
            '-u', username,
            '-p', password,
            '-z'] + zookeeper_server_list + [
            '--mq-user', 'guest',
            '--mq-password', 'guest',
            '-a', super_user, super_password,
            '-t', 'monitoring', 'management', 'administrator']
    if subprocess.call(command):
        raise RuntimeError('Failed to add super user to MQ!')
    
    # common command options
    command_base = [
        'python', os.path.join(file_dir_path, 'mq_server_config_manager.py'),
            '-e', ca_cert,
            '-u', username,
            '-p', password,
            '-z'] + zookeeper_server_list + [
            '--mq-user', super_user,
            '--mq-password', super_password]

    # remove default mq user
    print('Removing default MQ user.')
    command = copy.deepcopy(command_base) + [
        '-d',
        '-a', 'guest']
    if subprocess.call(command):
        raise RuntimeError('Failed to remove default MQ user!')
    
    # configure mq vhost
    print('Configuring MQ vhost.')
    command = copy.deepcopy(command_base) + [
        '-o', 'pero',
        '-r', 'PERO default vhost']
    if subprocess.call(command):
        raise RuntimeError('Failed to add default PERO vhost in MQ!')
    
    # configure vhost access
    print('Adding accest to MQ vhost.')
    command = copy.deepcopy(command_base) + [
        '-a', username,
        '-v', 'pero']
    if subprocess.call(command):
        raise RuntimeError('Failed to configure PERO vhost access!')
    
    # configure logging queue
    print('Adding log queue.')
    command = [
        'python', os.path.join(file_dir_path, 'stage_config_manager.py'),
            '-e', ca_cert,
            '-u', username,
            '-p', password,
            '-z'] + zookeeper_server_list + [
            '-n', 'log']
    if subprocess.call(command):
        raise RuntimeError('Failed to configure logging queue for workers!')

def main():
    args = parse_args()

    # create output directory
    output_dir = os.path.abspath(args.output_directory)
    data_dir = os.path.abspath(args.data_directory)
    os.mkdir(output_dir)
    os.mkdir(data_dir)

    # parse server ips and names
    mq_server = cf.server_list(args.mq_server)
    for host in mq_server:
        host['port'] = args.mq_amqp_port
    zk_server = cf.server_list(args.zookeeper)
    for host in zk_server:
        host['port'] = args.zookeeper_secure_port
    sftp_server = cf.server_list(args.sftp_server)
    for host in sftp_server:
        host['port'] = args.sftp_port
    mq_management_server = cf.server_list(args.mq_server)
    for host in mq_management_server:
        host['port'] = args.mq_management_port

    # generate username and password
    username = args.username
    password = args.password
    super_user = args.root_account
    super_password = args.root_password

    # generate superuser and password
    if not password:
        password = generate_password()
        with open(os.path.join(output_dir, f'{username}.pass'), 'w') as pf:
            pf.write(password)
    if not super_password:
        super_password = generate_password()
        with open(os.path.join(output_dir, f'{super_user}.pass'), 'w') as pf:
            pf.write(super_password)
    
    # DEBUG
    print(f'output dir: {output_dir}')
    print(f'data dir: {data_dir}')
    print(f'mq server: {mq_server}')
    print(f'zk server: {zk_server}')
    print(f'sftp server: {sftp_server}')
    print(f'mq management server: {mq_management_server}')
    print(f'username: {username}')
    print(f'password: {password}')
    print(f'super_user: {super_user}')
    print(f'super_password: {super_password}')

    # generate certificates for selected services
    certificate_dir = os.path.join(output_dir, 'certificates')
    generate_certificates(
        output_dir=certificate_dir,
        ca_cert=args.ca_cert if args.ca_cert else None,
        ca_key=args.ca_key if args.ca_key else None,
        ca_key_password=args.ca_key_password if args.ca_key_password else None,
        zookeeper_server=zk_server,
        mq_server=mq_server
    )

    # get CA certificate location
    ca_cert = args.ca_cert if args.ca_cert else os.path.join(certificate_dir, 'ca.pem')

    # generate configuration for mq
    mq_config_dir = os.path.join(output_dir, 'rabbtimq')
    mq_cert_dir = os.path.join(mq_config_dir, 'certs')
    os.mkdir(mq_config_dir)
    os.mkdir(mq_cert_dir)
    for f in ('ca.pem', 'rabbit-server-key.pem', 'rabbit-server-cert.pem'):
        shutil.copy(
            src=os.path.join(certificate_dir, f),
            dst=os.path.join(mq_cert_dir, f)
        )
    generate_mq_config(
        output_dir=mq_config_dir,
        amqp_port=args.mq_amqp_port,
        management_port=args.mq_management_port
    )

    # generate configuration for zookeeper
    zk_config_dir = os.path.join(output_dir, 'zookeeper')
    os.mkdir(zk_config_dir)
    for f in ('zk_keystore.jks', 'zk_truststore.jks'):
        shutil.copy(
            src=os.path.join(certificate_dir, f),
            dst=os.path.join(zk_config_dir, f)
        )
    generate_zk_config(
        output_dir=zk_config_dir,
        username=username,
        password=password,
        super_password=super_password,
        zookeeper_port=args.zookeeper_port,
        zookeeper_secure_port=args.zookeeper_secure_port,
        zookeeper_keystore_password=args.zookeeper_keystore_password,
        zookeeper_truststore_password=args.zookeeper_truststore_password
    )

    # generate configuration for sftp
    sftp_config_dir = os.path.join(output_dir, 'sftp')
    os.mkdir(sftp_config_dir)
    generate_sftp_config(
        output_dir=sftp_config_dir,
        username=username,
        password=password
    )

    # generate docker-compose environment file
    mq_data_dir = os.path.join(data_dir, 'rabbitmq')
    zk_data_dir = os.path.join(data_dir, 'zookeeper')
    sftp_data_dir = os.path.join(data_dir, 'sftp')
    os.mkdir(mq_data_dir)
    os.mkdir(zk_data_dir)
    os.mkdir(sftp_data_dir)
    generate_docker_env(
        output_dir=output_dir,
        host_name=os.uname().nodename,
        sftp_data_dir=sftp_data_dir,
        sftp_config_dir=sftp_config_dir,
        zookeeper_config_dir=zk_config_dir,
        zookeeper_data_dir=zk_data_dir,
        mq_config_dir=mq_config_dir,
        mq_data_dir=mq_data_dir,
        zookeeper_secure_port=args.zookeeper_secure_port,
        mq_amqp_port=args.mq_amqp_port,
        mq_management_port=args.mq_management_port,
        sftp_port=args.sftp_port
    )

    # generate configuration for worker, watchdog, log_daemon
    log_dir = os.path.join(output_dir, 'logs')
    os.mkdir(log_dir)
    generate_client_configs(
        output_dir=output_dir,
        zookeeper_server=zk_server,
        ca_cert=ca_cert,
        username=username,
        password=password,
        log_dir=log_dir
    )

    # run services
    run_services(env=os.path.join(output_dir, 'docker.env'))

    # wait for services to boot up
    time.sleep(10)

    # apply services runtime configuration
    apply_runtime_config(
        zk_server=zk_server,
        mq_server=mq_server,
        sftp_server=sftp_server,
        mq_management_server=mq_management_server,
        username=username,
        password=password,
        super_user=super_user,
        super_password=super_password,
        ca_cert=ca_cert
    )

    return 0


if __name__ == '__main__':
    sys.exit(main())
