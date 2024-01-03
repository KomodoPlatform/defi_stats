#!/usr/bin/env python3
from decimal import Decimal
from const import MM2_RPC_PORTS, MM2_NETID
from util.logger import logger
import lib


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


def is_pair_priced(base, quote) -> bool:
    """
    Checks if both coins in a pair are priced.
    """
    try:
        if base in lib.PRICED_COINS and quote in lib.PRICED_COINS:
            return True
    except Exception as e:  # pragma: no cover
        logger.warning(f"Pair {base}/{quote} is unpriced: {e}")
    return False
