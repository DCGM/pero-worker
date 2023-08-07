#!/usr/bin/env python3

import argparse
import requests
import sys
import copy
import logging
import time
import traceback
import json
import urllib3

import worker_functions.constants as constants
import worker_functions.connection_aux_functions as cf

from worker_functions.zk_client import ZkClient

# setup logging (required by kazoo)
log_formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')

# use UTC time in log
log_formatter.converter = time.gmtime

stderr_handler = logging.StreamHandler()
stderr_handler.setFormatter(log_formatter)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
# TODO - remove debug level
logger.setLevel(logging.DEBUG)
logger.addHandler(stderr_handler)

def parse_args():
    parser = argparse.ArgumentParser(
        'Configures RabbitMQ server using management plugin HTTP API.'
    )
    parser.add_argument(
        '-z', '--zookeeper',
        help='List of zookeeper servers to use.',
        nargs='+',
        default=['127.0.0.1:2181']
    )
    parser.add_argument(
        '-l', '--zookeeper-list',
        help='File with list of zookeeper servers. One server per line.',
        type=argparse.FileType('r')
    )
    parser.add_argument(
        '-u', '--username',
        help='Username for authentication on server.',
        default=None
    )
    parser.add_argument(
        '-p', '--password',
        help='Password for user authentication.',
        default=None
    )
    parser.add_argument(
        '-e', '--ca-cert',
        help='CA Certificate for SSL/TLS connection verification.',
        default=None
    )

    parser.add_argument(
        '--mq-user',
        help='MQ username if differs from the one given by \'--username\' option.'
    )
    parser.add_argument(
        '--mq-password',
        help='MQ user password to use with \'--mq-user\' option.'
    )

    parser.add_argument(
        '-d', '--delete',
        help='Inverts function of the options, delete instead of add.',
        default=False,
        action='store_true'
    )
    parser.add_argument(
        '-a', '--add-user',
        help='Adds new user. Enter both username and password.',
        nargs='+',
        default=None
    )
    parser.add_argument(
        '-t', '--tags',
        help='Add user tags. Use with \'-a\' option.',
        nargs='+',
        default=[]
    )
    parser.add_argument(
        '-v', '--virtual-host-access',
        help='Add virutal host access to user. Use with \'-a\' option.'
    )

    parser.add_argument(
        '-o', '--add-virtual-host',
        help='Add virtual host with given name.',
    )
    parser.add_argument(
        '-r', '--virtual-host-description',
        help='Description of virtual host, use with \'-o\'.',
        default=''
    )

    parser.add_argument(
        '-s', '--list-users',
        help='Lists all configured users.',
        default=False,
        action='store_true'
    )
    parser.add_argument(
        '-i', '--list-virtual-hosts',
        help='Get list of virtual hosts.',
        default=False,
        action='store_true'
    )
    parser.add_argument(
        '-g', '--get-user-info',
        help='Get details about given users.',
        nargs='+',
        default=[]
    )
    return parser.parse_args()

class ZKAuxConfigManager(ZkClient):
    """
    Client for obtaining configuration from zookeeper
    """

    def zk_get_mq_monitoring_servers(self):
        """
        Get list of mq servers from zookeeper
        :return: list of mq servers or None
        """
        mq_monitoring_servers = None
        
        # get server list
        try:
            mq_monitoring_servers = cf.server_list(self.zk.get_children(constants.WORKER_CONFIG_MQ_MONITORING_SERVERS))
        except Exception:
            self.logger.error('Failed to obtain list of MQ monitoring servers from zookeeper!')
            self.logger.error('Received error:\n{}'.format(traceback.format_exc()))
            mq_monitoring_servers = None
        
        return mq_monitoring_servers


class ManagementResponseError(ConnectionError):
    def __init__(self, message, servers=[]):
        super().__init__(message)
        self.servers = servers


class MQManagementClient:

    default_monitoring_port = 15672

    vhosts = 'https://{server}/api/vhosts'
    vhost_by_name = 'https://{server}/api/vhosts/{name}'

    users = 'https://{server}/api/users/'
    user_by_name = 'https://{server}/api/users/{name}'
    supported_tags = ['administrator', 'monitoring', 'management']

    user_vhost_permissions = 'https://{server}/api/permissions/{vhost}/{user}'
    user_vhost_permissions_msg = {'configure':'.*','write':'.*','read':'.*'}


    def __init__(self, mq_monitoring_servers, username, password, ca_cert, logger):
        self.mq_monitoring_servers = mq_monitoring_servers
        self.username = username
        self.password = password
        self.ca_cert = ca_cert
        self.logger = logger

    def request_to_all_servers(self, method, url, request=''):
        """
        Sends request to all MQ management/monitoring servers.
        :param method: HTTP method to use
        :param url: url to send request to
        :param request: request to send in the message
        :raise: ManagementResponseError if some servers does not respond
        """
        failed = []
        for server in self.mq_monitoring_servers:
            if not server['port']:
                server['port'] = self.default_monitoring_port
            try:
                response = requests.request(
                    method=method,
                    url=url.format(server=cf.host_port_to_string(server)),
                    auth=(self.username, self.password),
                    verify=self.ca_cert,
                    json=request
                )
            except requests.exceptions.RequestException:
                self.logger.error(
                    'Failed to connect to MQ monitoring api on server {}'
                    .format(cf.host_port_to_string(server))
                )
                self.logger.debug(
                    'Received error:\n{}'
                    .format(traceback.format_exc())
                )
                failed.append(server)
            else:
                if not response.ok:
                    self.logger.error(
                        'Failed to get response from server {}'
                        .format(cf.host_port_to_string(server))
                    )
                    self.logger.error(
                        'Status code: {status}, message: {reason}'
                        .format(
                            status = response.status_code,
                            reason = response.reason
                        )
                    )
                    failed.append(server)
        
        if failed:
            raise ManagementResponseError(
                    message='Following servers did not responded with OK response code: {servers}'
                            .format(servers = ', '.join(cf.host_port_to_string(f) for f in failed)),
                    servers = failed
                )
    
    def request_to_any_mq_server(self, method, url, request='', silent=False):
        """
        Sends request to MQ management/monitoring servers and returns response
        of the first server that responeded.
        :param method: HTTP method to use
        :param url: url to send request to
        :param request: request to send in the message
        :param silent: do not print error if status code is not 'ok'
        :raise: ConnectionError if no server responded with OK error code
        """
        response = None
        for server in self.mq_monitoring_servers:
            if not server['port']:
                server['port'] = self.default_monitoring_port
            try:
                response = requests.request(
                    method=method,
                    url=url.format(server=cf.host_port_to_string(server)),
                    auth=(self.username, self.password),
                    verify=self.ca_cert,
                    json=request
                )
            except requests.exceptions.RequestException:
                self.logger.error(
                    'Failed to connect to MQ monitoring api on server {}'
                    .format(cf.host_port_to_string(server))
                )
                self.logger.debug(
                    'Received error:\n{}'
                    .format(traceback.format_exc())
                )
            else:
                if not response.ok and not silent:
                    self.logger.error(
                        'Failed to get response from server {}'
                        .format(cf.host_port_to_string(server))
                    )
                    self.logger.error(
                        'Status code: {status}, message: {reason}'
                        .format(
                            status = response.status_code,
                            reason = response.reason
                        )
                    )
                else:
                    break

        if not response:
            raise ConnectionError('Failed to get response from all MQ servers!')
        
        return response.content

    def add_vhost(self, name: str, description: str):
        """
        Adds virtual host on all MQ servers
        :param name: name of the vhost
        :param description: Description of the vhost
        """
        message = dict()

        if description:
            message['description'] = description
        else:
            message['description'] = ''

        self.request_to_all_servers(
            'PUT',
            self.vhost_by_name.format(
                name = name,
                server = '{server}'
            ),
            message
        )

    def delete_vhost(self, name: str):
        """
        Deletes virtual host on all MQ servers
        :param name: name of the vhost
        """
        self.request_to_all_servers(
            'DELETE',
            self.vhost_by_name.format(
                name = name,
                server = '{server}'
            )
        )

    def list_vhosts(self):
        """
        List virtual hosts configured on MQ servers
        :return list of all vhosts
        """
        return self.request_to_any_mq_server('GET', self.vhosts)

    def list_users(self):
        """
        List users configured on MQ servers
        :return list of all users
        """
        return self.request_to_any_mq_server('GET', self.users)

    def get_user_details(self, user: str, silent: bool = False):
        """
        Get details of given user.
        :param user: user to show
        :param silent: do not print error if error code is not 'ok'
        :return user details serialized to JSON
        """
        return self.request_to_any_mq_server(
            'GET',
            self.user_by_name.format(
                name = user,
                server = '{server}'
            ),
            silent=silent
        )

    def add_user(self, name: str, password: str, tags: list):
        """
        Adds user to the MQ servers, or modifies existing user.
        :param name: name of the new user
        :param password: password of the new user
        :param tags: list of user tags
                     (available tags: administrator, monitoring, management)
        """
        for tag in tags:
            if tag not in self.supported_tags:
                raise ValueError(f'Unsupported user tag: {tag}!')
        
        # get user from MQ server
        try:
            message = json.loads(self.get_user_details(user=name, silent=True))
        except ConnectionError:
            message = dict()

        # set new user password and remove the old one
        if password:
            message['password'] = password
            if 'password_hash' in message:
                del message['password_hash']
            if 'hashing_algorithm' in message:
                del message['hashing_algorithm']
        
        # set new user tags
        if 'tags' not in message or tags:
            message['tags'] = tags

        self.request_to_all_servers(
            'PUT',
            self.user_by_name.format(
                name = name,
                server = '{server}'
            ),
            message
        )
    
    def delete_user(self, name: str):
        """
        Deletes existing user from all MQ servers.
        :param name: user name
        """
        self.request_to_all_servers(
            'DELETE',
            self.user_by_name.format(
                name = name,
                server = '{server}'
            )
        )

    def add_vhost_access(self, vhost, user):
        """
        Gives the user access to given virtual host.
        :param vhost: virtual host to be accessed by user 
        :param user: user to gain access
        """
        self.request_to_all_servers(
            'PUT',
            self.user_vhost_permissions.format(
                vhost = vhost,
                user = user,
                server = '{server}'
            ),
            self.user_vhost_permissions_msg
        )
    
    def delete_vhost_access(self, vhost, user):
        """
        Removes user's access to given virtual host.
        :param vhost: virtual host to remove from access list
        :param user: user to remove access to
        """
        self.request_to_all_servers(
            'DELETE',
            self.user_vhost_permissions.format(
                vhost = vhost,
                user = user,
                server = '{server}'
            )
        )

def main():
    args = parse_args()
    mq_monitoring_servers = None
    
    zookeeper_servers = cf.zk_server_list(args.zookeeper)
    if args.zookeeper_list:
        zookeeper_servers = cf.zk_server_list(args.zookeeper_list)

    # === Get MQ servers ===
    zk_client = ZKAuxConfigManager(
        zookeeper_servers = zookeeper_servers,
        username = args.username,
        password = args.password,
        ca_cert = args.ca_cert,
        logger = logger
    )
    zk_client.zk_connect()
    mq_monitoring_servers = zk_client.zk_get_mq_monitoring_servers()
    zk_client.zk_disconnect()

    if not mq_monitoring_servers:
        logger.error('Failed to get list of MQ servers!')
        return 1
    # === ===

    username = args.username
    password = args.password

    if args.mq_user:
        if not args.mq_password:
            raise ValueError('Password for mq user not given (\'--mq-password\')')
        username = args.mq_user
        password = args.mq_password

    mq_management_client = MQManagementClient(
        mq_monitoring_servers = mq_monitoring_servers,
        username = username,
        password = password,
        ca_cert = args.ca_cert,
        logger = logger
    )

    if args.delete:
        if args.add_virtual_host:
            mq_management_client.delete_vhost(args.add_virtual_host)
        
        if args.virtual_host_access:
            mq_management_client.delete_vhost_access(args.virtual_host_access, args.add_user[0])
        elif args.add_user:
            mq_management_client.delete_user(args.add_user[0])
    
    else:
        if args.add_virtual_host:
            mq_management_client.add_vhost(args.add_virtual_host, args.virtual_host_description)
        
        if args.add_user:
            if len(args.add_user) == 1:
                mq_management_client.add_user(args.add_user[0], None, args.tags)
            else:
                mq_management_client.add_user(args.add_user[0], args.add_user[1], args.tags)
        
        if args.virtual_host_access:
            mq_management_client.add_vhost_access(args.virtual_host_access, args.add_user[0])
        
    # TODO
    # add nice output formats

    if args.list_virtual_hosts:
        logger.info('List of vhosts:')
        for vhost in json.loads(mq_management_client.list_vhosts()):
            logger.info(f'{vhost}')
    
    if args.list_users:
        logger.info('List of users:')
        for user in json.loads(mq_management_client.list_users()):
            logger.info(f'{user}')
    
    if args.get_user_info:
        logger.info('User details:')
        for user in args.get_user_info:
            logger.info(json.loads(mq_management_client.get_user_details(user)))

    return 0


if __name__ == '__main__':
    sys.exit(main())
    

