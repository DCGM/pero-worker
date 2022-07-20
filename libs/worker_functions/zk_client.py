#!/usr/bin/env python3

# client for zookeeper

import logging
# zookeeper
from kazoo.client import KazooClient
from kazoo.handlers.threading import KazooTimeoutError
from kazoo.protocol.states import KazooState
import kazoo.exceptions

logger = logging.getLogger(__name__)

class ZkClient:
    def __init__(self, zookeeper_servers, username = '', password = '', ca_cert = None, logger = logging.getLogger(__name__)):
        # list of zookeeper servers
        self.zookeeper_servers = zookeeper_servers

        # use ssl/tls
        self.ca_cert = ca_cert

        # authentication credentials (SASL)
        if username:
            self.zk_auth = {
                'mechanism': 'DIGEST-MD5',
                'username': str(username),
                'password': str(password)
            }
        else:
            self.zk_auth = None

        # logger
        self.logger = logger

        # zookeeper connection
        self.zk = None

    def zk_connect(self):
        """
        Connects client to the zookeeper
        """
        # skip if connection is active
        if self.zk and self.zk.connected:
            return

        # create new connection
        self.zk = KazooClient(
            hosts=self.zookeeper_servers,
            ca=self.ca_cert,
            use_ssl=True if self.ca_cert else False,
            sasl_options=self.zk_auth
        )

        try:
            self.zk.start(timeout=20)
        except KazooTimeoutError:
            self.logger.error('Zookeeper connection timeout!')
            raise
    
    def zk_disconnect(self):
        """
        Disconnects from zookeeper
        """
        if self.zk and self.zk.connected:
            try:
                self.zk.stop()
                self.zk.close()
            except kazoo.exceptions.KazooException as e:
                self.logger.error('Failed to close zookeeper connection!')
                self.logger.error('Received error: {}'.format(traceback.format_exc()))

    def __del__(self):
        self.zk_disconnect()
