#!/usr/bin/env python3
from decimal import Decimal
from typing import Dict
from const import MM2_RPC_PORTS, MM2_NETID
import util.templates as template
from util.defaults import default_error


def get_mm2_rpc_port(netid=MM2_NETID):
    try:
        return MM2_RPC_PORTS[str(netid)]
    except Exception as e:
        return default_error(e)


def get_netid_filename(filename, netid):
    try:
        parts = filename.split(".")
        return f"{'.'.join(parts[:-1])}_{netid}.{parts[-1]}"
    except Exception as e:
        return default_error(e)


def get_chunks(data, chunk_length):
    try:
        for i in range(0, len(data), chunk_length):
            yield data[i: i + chunk_length]
    except Exception as e:
        return default_error(e)


def get_price_at_finish(swap):
    try:
        end_time = swap["finished_at"]
        taker_amount = Decimal(swap["taker_amount"])
        maker_amount = Decimal(swap["maker_amount"])
        return {end_time: taker_amount / maker_amount}
    except Exception as e:
        return default_error(e)


def get_pairs_info(pair_list: str, priced: bool = False) -> list:
    try:
        return [template.pair_info(i, priced) for i in pair_list]
    except Exception as e:
        return default_error(e)


def get_pair_info_sorted(pair_list: str, priced: bool = False) -> dict:
    try:
        return sorted(
            get_pairs_info(pair_list, priced),
            key=lambda d: d["ticker_id"],
        )
    except Exception as e:
        return default_error(e)


def get_last_trade_time(pair_str: str, last_traded_cache: Dict) -> int:
    try:
        if pair_str in last_traded_cache:
            return last_traded_cache[pair_str]["last_swap"]
        return 0
    except Exception as e:
        return default_error(e)
