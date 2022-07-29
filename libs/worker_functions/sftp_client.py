#!/usr/bin/env python3

# SFTP client

import logging
import paramiko

import worker_functions.connection_aux_functions as cf

class SFTP_Client:
    """
    SFTP Client
    """

    def __init__(self, sftp_servers=[], username=None, password=None, logger = logging.getLogger(__name__)):
        self.sftp_servers = sftp_servers
        self.logger = logger

        self.sftp_connection = None
        self.ssh_connection = None

        if username:
            self.username = username
            self.password = password
        else:
            self.username = 'pero'
            self.password = 'pero'
    
    def sftp_connect(self):
        """
        Connect to server
        :raise: ConnectionError if connection to all SFTP servers fails
        """
        # setup client for connection
        self.ssh_connection = paramiko.SSHClient()
        self.ssh_connection.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        # connect to firs server avaliable
        for server in self.sftp_servers:
            try:
                self.logger.info('Connectiong to SFTP server {}'.format(cf.ip_port_to_string(server)))
                # get SSH connection
                self.ssh_connection.connect(
                    hostname=server['ip'],
                    port=server['port'],
                    username=self.username,
                    password=self.password,
                    allow_agent=False,
                    look_for_keys=False
                )
                self.logger.info('Opening SFTP channel')
                # get SFTP (SSH File Transfer Protocol) connection
                self.sftp_connection = self.ssh_connection.open_sftp()
            except paramiko.AuthenticationException:
                self.logger.error('Wrong authentication credentials!')
                continue
            except Exception as e:
                self.logger.error('Failed to connect to SFTP server {server}! Received error:\n{error}'.format(
                    server = cf.ip_port_to_string(server),
                    error = e
                ))
                continue
            else:
                self.logger.info('Connection established successfully!')
                return
        
        # failed to connect
        raise ConnectionError('Failed to connect to SFTP servers!')
    
    def sftp_get(self, remote_file, local_file):
        """
        Receive file from SFTP server
        :param remote_file: file to receive
        :param local_file: path to local file where received file will be stored
        """
        self.sftp_connection.get(remote_file, local_file)

    def sftp_disconnect(self):
        """
        Disconnect from server
        """
        if self.sftp_connection:
            self.sftp_connection.close()
        if self.ssh_connection:
            self.ssh_connection.close()
    
    def __del__(self):
        self.sftp_disconnect()
