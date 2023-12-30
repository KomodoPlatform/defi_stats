#!/usr/bin/env python3
from const import MM2_RPC_PORTS, MM2_NETID


def get_mm2_rpc_port(netid=MM2_NETID):
    return MM2_RPC_PORTS[str(netid)]


def get_netid_filename(filename, netid):
    parts = filename.split(".")
    return f"{'.'.join(parts[:-1])}_{netid}.{parts[-1]}"


def get_chunks(data, chunk_length):
    for i in range(0, len(data), chunk_length):
        yield data[i: i + chunk_length]
