#!/usr/bin/env python3
from decimal import Decimal
from typing import Dict
from const import MM2_RPC_PORTS, MM2_NETID
import util.templates as template


def get_mm2_rpc_port(netid=MM2_NETID):
    return MM2_RPC_PORTS[str(netid)]


def get_netid_filename(filename, netid):
    parts = filename.split(".")
    return f"{'.'.join(parts[:-1])}_{netid}.{parts[-1]}"


def get_chunks(data, chunk_length):
    for i in range(0, len(data), chunk_length):
        yield data[i: i + chunk_length]


def get_price_at_finish(swap):
    end_time = swap["finished_at"]
    taker_amount = Decimal(swap["taker_amount"])
    maker_amount = Decimal(swap["maker_amount"])
    return {end_time: taker_amount / maker_amount}


def get_pair_info_sorted(pair_list):
    return sorted(
        [template.pair_info(i) for i in pair_list], key=lambda d: d["ticker_id"]
    )


def get_last_trade_time(pair_str: str, last_traded_cache: Dict) -> int:
    if pair_str in last_traded_cache:
        return last_traded_cache[pair_str]["last_swap"]
    return 0
