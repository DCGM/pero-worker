#!/usr/bin/env python3

# auxiliary functions for zookeeper

def zk_server_list(servers) -> str:
    """
    Returns list of zookeeper servers as string separated by commas.
    :param servers: list of zookeeper servers or file with server list
    :return: zookeeper server list
    """
    server_list = []

    # read servers from file
    if not isinstance(servers, list):
        for line in servers:
            if line[-1] == '\n':
                line = line[:-1]
            server_list.append(line)
    # read servers from list
    else:
        server_list = servers
    
    return ','.join(server_list)
