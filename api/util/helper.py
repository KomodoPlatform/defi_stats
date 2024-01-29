#!/usr/bin/env python3
from datetime import timedelta
from decimal import Decimal
from typing import Dict
from const import MM2_RPC_PORTS, MM2_NETID
from util.logger import logger, timed
import util.defaults as default
import util.memcache as memcache
import util.templates as template
import util.transform as transform


def get_mm2_rpc_port(netid=MM2_NETID):
    try:
        return MM2_RPC_PORTS[str(netid)]
    except Exception as e:  # pragma: no cover
        return default.error(e)


def get_netid(filename):
    if "7777" in filename:
        return "7777"
    if filename.startswith("seed"):
        return "7777"
    else:
        return "8762"


def get_netid_filename(filename, netid):
    try:
        parts = filename.split(".")
        return f"{'.'.join(parts[:-1])}_{netid}.{parts[-1]}"
    except Exception as e:  # pragma: no cover
        return default.error(e)


def get_chunks(data, chunk_length):
    try:
        for i in range(0, len(data), chunk_length):
            yield data[i: i + chunk_length]
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
    try:
        if variant == "OLD_USDC-PLG20_USDC-PLG20":
            variant = "USDC-PLG20_USDC-PLG20_OLD"
        split_variant = variant.split("_")
        if len(split_variant) == 2:
            base = split_variant[0]
            quote = split_variant[1]
        elif variant.startswith("IRIS_ATOM-IBC"):
            base = "IRIS_ATOM-IBC"
            quote = variant.replace(f"{base}_", "")
        elif variant.endswith("IRIS_ATOM-IBC"):
            quote = "IRIS_ATOM-IBC"
            base = variant.replace(f"_{quote}", "")

        elif variant.startswith("IRIS_ATOM"):
            base = "IRIS_ATOM"
            quote = variant.replace(f"{base}_", "")
        elif variant.endswith("IRIS_ATOM"):
            quote = "IRIS_ATOM"
            base = variant.replace(f"_{quote}", "")

        elif variant.startswith("ATOM-IBC_IRIS"):
            base = "ATOM-IBC_IRIS"
            quote = variant.replace(f"{base}_", "")
        elif variant.endswith("ATOM-IBC_IRIS"):
            quote = "ATOM-IBC_IRIS"
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
        # failed to parse ATOM-IBC_IRIS_LTC into base/quote!
        if reverse:
            return quote, base
        return base, quote
    except Exception as e:  # pragma: no cover
        msg = f"failed to parse {variant} into base/quote! {e}"
        data = {"error": msg}
        return default.result(msg=msg, loglevel='warning', data=data)


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


def get_coin_variants(coin, segwit_only=False):
    coins_config = memcache.get_coins_config()
    coin = coin.split("-")[0]
    data = [
        i
        for i in coins_config
        if (i.replace(coin, "") == "" or i.replace(coin, "").startswith("-"))
        and (not segwit_only or i.endswith("segwit") or i.replace(coin, "") == "")
    ]
    return data


def get_pair_variants(pair, segwit_only=False):
    variants = []
    base, quote = base_quote_from_pair(pair)
    base_variants = get_coin_variants(base, segwit_only=segwit_only)
    quote_variants = get_coin_variants(quote, segwit_only=segwit_only)
    for i in base_variants:
        for j in quote_variants:
            if i != j:
                variants.append(f"{i}_{j}")
    return variants


# The lowest ask / highest bid needs to be inverted
# to result in conventional vaules like seen at
# https://api.binance.com/api/v1/ticker/24hr where
# askPrice > bidPrice
@timed
def find_lowest_ask(orderbook: dict) -> str:
    """Returns lowest ask from provided orderbook"""
    try:
        if len(orderbook["bids"]) > 0:
            return transform.format_10f(
                min([Decimal(bid["price"]) for bid in orderbook["bids"]])
            )
    except KeyError as e:  # pragma: no cover
        return default.error(e, data=transform.format_10f(0))
    except Exception as e:  # pragma: no cover
        return default.error(e, data=transform.format_10f(0))
    return transform.format_10f(0)


@timed
def find_highest_bid(orderbook: list) -> str:
    """Returns highest bid from provided orderbook"""
    try:
        if len(orderbook["asks"]) > 0:
            return transform.format_10f(
                max([Decimal(ask["price"]) for ask in orderbook["asks"]])
            )
    except KeyError as e:  # pragma: no cover
        return default.error(e, data=transform.format_10f(0))
    except Exception as e:  # pragma: no cover
        return default.error(e, data=transform.format_10f(0))
    return transform.format_10f(0)


def get_pairs_info(pair_list: str, priced: bool = False) -> list:
    try:
        return [template.pair_info(i, priced) for i in pair_list]
    except Exception as e:  # pragma: no cover
        return default.error(e)


def get_pair_info_sorted(pair_list: str, priced: bool = False) -> dict:
    try:
        return sorted(
            get_pairs_info(pair_list, priced),
            key=lambda d: d["ticker_id"],
        )
    except Exception as e:  # pragma: no cover
        return default.error(e)
