#!/usr/bin/env python3
from decimal import Decimal
from const import MM2_RPC_PORTS, MM2_DB_PATHS, MM2_NETID


def format_10f(number: float) -> str:
    """
    Format a float to 10 decimal places.
    """
    return f"{number:.10f}"


def list_json_key(data: dict, key: str, filter_value: str) -> Decimal:
    """
    list of key values from dicts.
    """
    return [i for i in data if i[key] == filter_value]


def sum_json_key(data: dict, key: str) -> Decimal:
    """
    Sum a key from a list of dicts.
    """
    return sum(Decimal(d[key]) for d in data)


def sum_json_key_10f(data: dict, key: str) -> str:
    """
    Sum a key from a list of dicts and format to 10 decimal places.
    """
    return format_10f(sum_json_key(data, key))


def sort_dict_list(data: dict, key: str, reverse=False) -> dict:
    """
    Sort a list of dicts by the value of a key.
    """
    return sorted(data, key=lambda k: k[key], reverse=reverse)


def sort_dict(data: dict, reverse=False) -> dict:
    """
    Sort a list of dicts by the value of a key.
    """
    k = list(data.keys())
    k.sort()
    if reverse:
        k.reverse()
    resp = {}
    for i in k:
        resp.update({i: data[i]})
    return resp


def set_pair_as_tuple(pair):
    if isinstance(pair, list):
        pair = tuple(pair)
    if isinstance(pair, str):
        pair = tuple(map(str, pair.split("_")))
    if not isinstance(pair, tuple):
        raise TypeError("Pair should be a string, tuple or list")
    if len(pair) != 2:
        raise ValueError("Pair tuple should have two values only")
    return pair


def order_pair_by_market_cap(pair, gecko_source):
    if pair[0].split("-")[0] in gecko_source:
        if pair[1].split("-")[0] in gecko_source:
            if (
                gecko_source[pair[1].split("-")[0]]["usd_market_cap"]
                < gecko_source[pair[0].split("-")[0]]["usd_market_cap"]
            ):
                pair = (pair[1], pair[0])
        else:
            pair = (pair[1], pair[0])
    return pair


def get_mm2_rpc_port(netid=MM2_NETID):
    return MM2_RPC_PORTS[str(netid)]


def get_sqlite_db_paths(netid=MM2_NETID):
    return MM2_DB_PATHS[str(netid)]


def get_netid_filename(filename, netid):
    parts = filename.split(".")
    return f"{'.'.join(parts[:-1])}_{netid}.{parts[-1]}"


def get_all_coin_pairs(coin, coins):
    return [(i, coin) for i in coins if coin not in [i, f"{i}-segwit"]]
