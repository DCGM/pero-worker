#!/usr/bin/env python3

# auxiliary functions for zookeeper

import json # DEBUG
import ipaddress
import re
import logging

logger = logging.getLogger(__name__)

def ip_port_to_string(record: dict) -> str:
    """
    Generates string from dict containing ip and port as parameters.
    :param record: ip, port record as dict
    :return: given record as string
    """
    if not record['ip']:
        return ''
    
    ip = ipaddress.ip_address(record['ip'])

    if not record['port']:  # address without port
        return record['ip']

    if ip.version == 4:  # ipv4
        return '{ip}:{port}'.format(
                ip = record['ip'],
                port = record['port']
            )
    else:  # ipv6
        return '[{ip}]:{port}'.format(
                ip = record['ip'],
                port = record['port']
            )

def parse_ip_port(record: str) -> dict:
    """
    Parses string in format ipv4:port or [ivp6]:port, validates ip address and port
    and returns dictionary object with separate ip and port as attributes.
    :param record: ip and port record as string
    :return: validated dictionary object of the record
    """
    output_record = {
        'ip': None,
        'port': None
    }

    # detect ip type
    ip_port = record.split(':')

    # ipv4
    if len(ip_port) <= 2:
        output_record['ip'] = ip_port[0]
        # validate address
        try:
            ipaddress.ip_address(output_record['ip'])
        except ValueError:
            raise ValueError('Bad IP address format! Record: {}'.format(record))
        # validate port
        if len(ip_port) == 2:
            try:
                output_record['port'] = int(ip_port[1])
                if output_record['port'] not in range(0, 65535):
                    raise ValueError
            except ValueError:
                raise ValueError('Bad port number! Record: {}'.format(record))
    # ipv6
    else:
        output_record['ip'] = record
        # validate if record is ipv6
        try:
            ipaddress.ip_address(output_record['ip'])
        except ValueError:
            # record might contain port as well / ip might be in brackets
            ip_port = re.fullmatch(r'\[([\d\w{a,f}:]+)\]:?(\d*)', record)
            # check ip
            try:
                ipaddress.ip_address(ip_port.group(1))
            except ValueError:
                raise ValueError('Bad IP address format! Record: {}'.format(record))
            output_record['ip'] = ip_port.group(1)
            # check port
            if ip_port.group(2):
                try:
                    output_record['port'] = int(ip_port.group(2))
                    if output_record['port'] not in range(0, 65535):
                        raise ValueError
                except ValueError:
                    raise ValueError('Bad port number! Record: {}'.format(record))

    return output_record

def server_list(server_list) -> list:
    """
    Parses list of servers from file, list or csv string.
    Format is ip:port separated by commas for string or list.
    If read from file, format is ip:port, one record per line.
    :param server_list: list of servers / opened file
    :return: list of servers
    """
    servers = []
    if isinstance(server_list, str):
        server_list = server_list.split(',')
    for server in server_list:
        server = server.strip()
        try:
            server = parse_ip_port(server)
        except ValueError as e:
            logger.error(e)
            # skip bad server record
        else:
            servers.append(server)
    return servers

def zk_server_list(servers) -> str:
    """
    Returns list of zookeeper servers as string separated by commas.
    :param servers: list of zookeeper servers or file with server list
    :return: validated zookeeper server list as csv of ip:port values
    """
    # parse servers from file, list or csv string and validate them
    validated_servers = server_list(servers)
    # convert to ip:port format
    output_servers = []
    for server in validated_servers:
        output_servers.append(ip_port_to_string(server))
    # join to resulting csv
    return ','.join(output_servers)
