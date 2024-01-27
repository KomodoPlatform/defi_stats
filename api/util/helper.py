#!/usr/bin/env python3
from datetime import timedelta
from decimal import Decimal
from typing import Dict
from const import MM2_RPC_PORTS, MM2_NETID
from util.logger import logger
import util.defaults as default
import util.memcache as memcache
import util.templates as template


def get_mm2_rpc_port(netid=MM2_NETID):
    try:
        return MM2_RPC_PORTS[str(netid)]
    except Exception as e:  # pragma: no cover
        return default.error(e)


def get_netid_filename(filename, netid):
    try:
        parts = filename.split(".")
        return f"{'.'.join(parts[:-1])}_{netid}.{parts[-1]}"
    except Exception as e:  # pragma: no cover
        return default.error(e)


def get_chunks(data, chunk_length):
    try:
        for i in range(0, len(data), chunk_length):
            yield data[i : i + chunk_length]
    except Exception as e:  # pragma: no cover
        return default.error(e)


def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)


def get_last_trade_info(pair_str: str, last_traded_cache: Dict, all=False):
    try:
        # TODO: cover 'all' case
        pair_str = pair_str.replace("-segwit", "")
        if pair_str in last_traded_cache:
            return last_traded_cache[pair_str]
        reverse_pair = base_quote_from_pair(pair_str, True)
        if reverse_pair in last_traded_cache:
            return last_traded_cache[reverse_pair]
    except Exception as e:  # pragma: no cover
        logger.warning(default.error(e))
    return template.last_trade_info()


def base_quote_from_pair(variant, reverse=False):
    # TODO: This workaround fixes the issue
    # but need to find root cause to avoid
    # unexpected related issues
    if variant == "OLD_USDC-PLG20_USDC-PLG20":
        variant = "USDC-PLG20_USDC-PLG20_OLD"
    split_variant = variant.split("_")
    try:
        if len(split_variant) == 2:
            base = split_variant[0]
            quote = split_variant[1]
        elif variant.startswith("IRIS_ATOM-IBC"):
            base = "IRIS_ATOM-IBC"
            quote = variant.replace(f"{base}_", "")
        elif variant.endswith("IRIS_ATOM-IBC"):
            quote = "IRIS_ATOM-IBC"
            base = variant.replace(f"_{quote}", "")
        elif len(split_variant) == 4 and "OLD" in split_variant:
            if split_variant[1] == "OLD":
                base = f"{split_variant[0]}_{split_variant[1]}"
            if split_variant[3] == "OLD":
                quote = f"{split_variant[2]}_{split_variant[3]}"
        elif len(split_variant) == 3 and "OLD" in split_variant:
            if split_variant[2] == "OLD":
                base = split_variant[0]
                quote = f"{split_variant[1]}_{split_variant[2]}"
            elif split_variant[1] == "OLD":
                base = f"{split_variant[0]}_{split_variant[1]}"
                quote = split_variant[2]

        if reverse:
            return quote, base
        return base, quote
    except Exception as e:  # pragma: no cover
        logger.warning(f"failed to parse {variant} into base/quote!")
        return default.error(e)


def get_price_at_finish(swap, is_reversed=False):
    try:
        end_time = swap["finished_at"]
        if (not is_reversed and swap["trade_type"] == "buy") or (
            is_reversed and swap["trade_type"] == "sell"
        ):
            # Base is maker on buy (unless inverted)
            base_amount = Decimal(swap["maker_amount"])
            quote_amount = Decimal(swap["taker_amount"])
        else:
            # Base is taker on sell (unless inverted)
            base_amount = Decimal(swap["taker_amount"])
            quote_amount = Decimal(swap["maker_amount"])
        return {end_time: base_amount / quote_amount}
    except Exception as e:  # pragma: no cover
        return default.error(e)


def get_swaps_volumes(swaps_for_pair, is_reversed=False):
    try:
        base_swaps = []
        quote_swaps = []
        for i in swaps_for_pair:
            if (not is_reversed and i["trade_type"] == "buy") or (
                is_reversed and i["trade_type"] == "sell"
            ):
                # Base is maker on buy (unless inverted)
                base_swaps.append(i["maker_amount"])
                quote_swaps.append(i["taker_amount"])
            else:
                # Base is taker on sell (unless inverted)
                base_swaps.append(i["taker_amount"])
                quote_swaps.append(i["maker_amount"])

        return [sum(base_swaps), sum(quote_swaps)]
    except Exception as e:  # pragma: no cover
        msg = "get_swaps_volumes failed!"
        return default.error(e, msg)
