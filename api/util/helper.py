#!/usr/bin/env python3
from datetime import timedelta
from decimal import Decimal
from typing import Dict
from const import MM2_RPC_PORTS, MM2_NETID
import util.templates as template
from util.defaults import default_error
from util.logger import logger
import util.transform as transform


def get_mm2_rpc_port(netid=MM2_NETID):
    try:
        return MM2_RPC_PORTS[str(netid)]
    except Exception as e:  # pragma: no cover
        return default_error(e)


def get_netid_filename(filename, netid):
    try:
        parts = filename.split(".")
        return f"{'.'.join(parts[:-1])}_{netid}.{parts[-1]}"
    except Exception as e:  # pragma: no cover
        return default_error(e)


def get_chunks(data, chunk_length):
    try:
        for i in range(0, len(data), chunk_length):
            yield data[i: i + chunk_length]
    except Exception as e:  # pragma: no cover
        return default_error(e)


def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)


def get_pairs_info(pair_list: str, priced: bool = False) -> list:
    try:
        return [template.pair_info(i, priced) for i in pair_list]
    except Exception as e:  # pragma: no cover
        return default_error(e)


def get_pair_info_sorted(pair_list: str, priced: bool = False) -> dict:
    try:
        return sorted(
            get_pairs_info(pair_list, priced),
            key=lambda d: d["ticker_id"],
        )
    except Exception as e:  # pragma: no cover
        return default_error(e)


def get_last_trade_item(pair_str: str, last_traded_cache: Dict, item: str):
    try:
        pair_str = pair_str.replace("-segwit", "")
        reverse_pair = "_".join(pair_str.split("_")[::-1])
        if pair_str in last_traded_cache:
            v = last_traded_cache[pair_str][item]
            is_reversed = False
        elif reverse_pair in last_traded_cache:
            v = last_traded_cache[reverse_pair][item]
            is_reversed = True
        else:
            return template.first_last_swap()[item]

        if item in ["first_swap_uuid", "last_swap_uuid"]:
            return v
        if item in ["first_swap_time", "last_swap_time"]:
            return int(v)
        if item in ["first_swap_price", "last_swap_price"]:
            if is_reversed:
                return Decimal(1 / v)
            return Decimal(v)
    except Exception as e:  # pragma: no cover
        logger.warning(default_error(e))
    if item in ["last_swap_uuid"]:
        return ""
    return Decimal(0)


def get_last_trade_info(pair_str: str, last_traded_cache: Dict):
    try:
        pair_str = pair_str.replace("-segwit", "")
        if pair_str in last_traded_cache:
            return last_traded_cache[pair_str]
        reverse_pair = "_".join(pair_str.split("_")[::-1])
        if reverse_pair in last_traded_cache:
            return last_traded_cache[reverse_pair]
    except Exception as e:  # pragma: no cover
        logger.warning(default_error(e))
    return template.last_trade_info()


def pair_without_segwit_suffix(maker_coin, taker_coin):
    """Removes `-segwit` suffixes from the tickers of a pair"""
    if maker_coin.endswith("-"):
        maker_coin = maker_coin[:-1]
    if taker_coin.endswith("-"):
        taker_coin = taker_coin[:-1]
    return f'{maker_coin.replace("-segwit", "")}_{taker_coin.replace("-segwit", "")}'


def get_coin_variants(coin, coins_config, segwit_only=False):
    coin = coin.split("-")[0]
    return [
        i
        for i in coins_config
        if (i.replace(coin, "") == "" or i.replace(coin, "").startswith("-"))
        and (not segwit_only or i.endswith("segwit") or i.replace(coin, "") == "")
    ]


def get_pair_variants(pair, coins_config, segwit_only=False):
    base, quote = transform.base_quote_from_pair(pair)
    base_variants = get_coin_variants(base, coins_config, segwit_only=segwit_only)
    quote_variants = get_coin_variants(quote, coins_config, segwit_only=segwit_only)
    variants = []
    for i in base_variants:
        for j in quote_variants:
            if i != j:
                variants.append(f"{i}_{j}")
    return variants


def get_gecko_price(ticker, gecko_source) -> float:
    try:
        if ticker in gecko_source:
            return Decimal(gecko_source[ticker]["usd_price"])
    except KeyError as e:  # pragma: no cover
        logger.warning(f"Failed to get usd_price and mcap for {ticker}: [KeyError] {e}")
    except Exception as e:  # pragma: no cover
        logger.warning(f"Failed to get usd_price and mcap for {ticker}: {e}")
    return Decimal(0)  # pragma: no cover


def get_gecko_mcap(ticker, gecko_source) -> float:
    try:
        if ticker in gecko_source:
            return Decimal(gecko_source[ticker]["usd_market_cap"])
    except KeyError as e:  # pragma: no cover
        logger.warning(f"Failed to get usd_price and mcap for {ticker}: [KeyError] {e}")
    except Exception as e:  # pragma: no cover
        logger.warning(f"Failed to get usd_price and mcap for {ticker}: {e}")
    return Decimal(0)  # pragma: no cover


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
        return default_error(e)


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

        volumes = [sum(base_swaps), sum(quote_swaps)]
        return volumes
    except Exception as e:  # pragma: no cover
        msg = "get_swaps_volumes failed!"
        return default_error(e, msg)
