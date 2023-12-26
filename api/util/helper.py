#!/usr/bin/env python3
import json
import time
import inspect
from decimal import Decimal
from util.enums import NetId
from const import MM2_RPC_PORTS, MM2_DB_PATHS, MM2_NETID
from util.logger import logger, get_trace, StopWatch

get_stopwatch = StopWatch


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


def sort_dict_list(data: list(), key: str, reverse=False) -> dict:
    """
    Sort a list of dicts by the value of a key.
    """
    return sorted(data, key=lambda k: k[key], reverse=reverse)


def sort_dict(data: dict, reverse=False) -> dict:
    """
    Sort a dict by the value the root key.
    """
    k = list(data.keys())
    k.sort()
    if reverse:
        k.reverse()
    resp = {}
    for i in k:
        resp.update({i: data[i]})
    return resp


def valid_coins(coins_config):
    return [
        i
        for i in list(coins_config.keys())
        if coins_config[i]["is_testnet"] is False
        and coins_config[i]["wallet_only"] is False
    ]


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


def is_7777(db_file: str) -> bool:
    if db_file.startswith("seed"):
        return True
    return False


def get_netid(db_file):
    for netid in NetId:
        if netid.value in db_file:
            return netid.value
    if is_7777(db_file):
        return "7777"
    elif is_source_db(db_file=db_file):
        return "8762"
    else:
        return "ALL"


def is_source_db(db_file: str) -> bool:
    if db_file.endswith("MM2.db"):
        return True
    return False


def is_pair_priced(pair: tuple, priced_coins: set()) -> bool:
    """
    Checks if both coins in a pair are priced.
    """
    try:
        base = pair[0].split("-")[0]
        rel = pair[1].split("-")[0]
        common = set((base, rel)).intersection(priced_coins)
        return len(common) == 2
    except Exception as e:
        err = {"error": f"{type(e)} Error checking if {pair} is priced: {e}"}
        logger.error(err)
        return False


def save_json(fn, data):
    start = int(time.time())
    stack = inspect.stack()[1]
    context = get_trace(stack)
    try:
        if len(data) > 0:
            with open(fn, "w+") as f:
                json.dump(data, f, indent=4)
                get_stopwatch(start, error=True, context=context)
                return data, len(data)
    except Exception as e:
        error = f"{type(e)}: {e}"
        context = get_trace(stack, error)
        get_stopwatch(start, error=True, context=context)
        return data, -1
    error = f"Not saving {fn}, data is empty"
    context = get_trace(stack, error)
    get_stopwatch(start, error=True, context=context)
    return data, -1