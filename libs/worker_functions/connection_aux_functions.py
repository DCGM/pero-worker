#!/usr/bin/env python3

# auxiliary functions for zookeeper

import json # DEBUG
import ipaddress
import re

output_record = {
    'host': None,
    'port': None
}

def host_port_to_string(record: dict) -> str:
    """
    Generates string from dict containing ip/hostname and port as parameters.
    :param record: host record as python dict
    :return: given record as string
    """
    if not record['host']:
        return ''
    
    try:  # IP
        host = ipaddress.ip_address(record['host'])
    except ValueError:  # hostname
        host = record['host']

    if not record['port']:  # host without port
        return record['host']
    
    if isinstance(host, str) or host.version == 4:  # hostname / ipv4
        return '{host}:{port}'.format(
                host = record['host'],
                port = record['port']
            )
    else:  # ipv6
        return '[{host}]:{port}'.format(
                host = record['host'],
                port = record['port']
            )

def is_valid_hostname(hostname: str) -> bool:
    """
    Checks if hostname is valid:
        - contains at least one character and a maximum of 63 characters
        - consists only of allowed characters
        - doesn't begin or end with a hyphen.
    Thanks to Tim Pietzcker - https://stackoverflow.com/questions/2532053/validate-a-hostname-string
    :param hostname: hostname to check
    :return: hostname validity
    """
    if len(hostname) > 255:
        return False
    if hostname[-1] == ".":
        hostname = hostname[:-1] # strip exactly one dot from the right, if present
    allowed = re.compile("(?!-)[A-Z\d-]{1,63}(?<!-)$", re.IGNORECASE)
    return all(allowed.match(x) for x in hostname.split("."))

def parse_host_port(record: str) -> dict:
    """
    Parses string in format ipv4:port or [ivp6]:port, validates ip address and port
    and returns dictionary object with separate ip and port as attributes.
    :param record: ip and port record as string
    :return: validated dictionary object of the record
    """
    output_record = {
        'host': None,
        'port': None
    }

    # detect ip type
    host_port = record.split(':')

    # ipv4 / hostname
    if len(host_port) <= 2:
        output_record['host'] = host_port[0]
        # validate address
        try:
            ipaddress.ip_address(output_record['host'])
        except ValueError:
            # validate hostname
            if not is_valid_hostname(output_record['host']):
                raise ValueError('Bad IP address or hostname! Record: {}'.format(record))
        # validate port
        if len(host_port) == 2:
            try:
                output_record['port'] = int(host_port[1])
                if output_record['port'] not in range(0, 65535):
                    raise ValueError
            except ValueError:
                raise ValueError('Bad port number! Record: {}'.format(record))
    # ipv6
    else:
        output_record['host'] = record
        # validate if record is ipv6
        try:
            ipaddress.ip_address(output_record['host'])
        except ValueError:
            # record might contain port as well / ip might be in brackets
            host_port = re.fullmatch(r'\[([\d\w{a,f}:]+)\]:?(\d*)', record)
            # check ip
            try:
                ipaddress.ip_address(host_port.group(1))
            except (ValueError, AttributeError):
                raise ValueError('Bad IP address format! Record: {}'.format(record))
            output_record['host'] = host_port.group(1)
            # check port
            if host_port.group(2):
                try:
                    output_record['port'] = int(host_port.group(2))
                    if output_record['port'] not in range(0, 65535):
                        raise ValueError
                except ValueError:
                    raise ValueError('Bad port number! Record: {}'.format(record))

    return output_record

def server_list(server_list, skip_bad=True) -> list:
    """
    Parses list of servers from file, list or csv string.
    Format is host:port separated by commas for string or list.
    If read from file, format is host:port, one record per line.
    :param server_list: list of servers / opened file
    :param skip_bad: skip records that cannot be parsed instead of rising exception (default=True)
    :return: list of servers
    """
    servers = []
    if isinstance(server_list, str):
        server_list = server_list.split(',')
    for server in server_list:
        server = server.strip()
        try:
            server = parse_host_port(server)
        except ValueError:
            if not skip_bad:
                raise
        else:
            servers.append(server)
    return servers

def zk_server_list(servers) -> str:
    """
    Returns list of zookeeper servers as string separated by commas.
    :param servers: list of zookeeper servers or file with server list
    :return: validated zookeeper server list as csv of host:port values
    """
    # parse servers from file, list or csv string and validate them
    validated_servers = server_list(servers)
    # convert to host:port format
    output_servers = []
    for server in validated_servers:
        output_servers.append(host_port_to_string(server))
    # join to resulting csv
    return ','.join(output_servers)
