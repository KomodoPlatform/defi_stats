#!/usr/bin/env python3
from datetime import timedelta
from decimal import Decimal
from typing import Dict
from const import MM2_RPC_PORTS, MM2_NETID
import util.templates as template
from util.defaults import default_error
from util.logger import logger


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


def get_price_at_finish(swap):
    try:
        end_time = swap["finished_at"]
        taker_amount = Decimal(swap["taker_amount"])
        maker_amount = Decimal(swap["maker_amount"])
        return {end_time: taker_amount / maker_amount}
    except Exception as e:  # pragma: no cover
        return default_error(e)


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


def get_last_trade_time(pair_str: str, last_traded_cache: Dict) -> int:
    try:
        pair_str = pair_str.replace("-segwit", "")
        if pair_str in last_traded_cache:
            return int(last_traded_cache[pair_str]["last_swap"])
        reverse_pair = "_".join(pair_str.split("_")[::-1])
        if reverse_pair in last_traded_cache:
            return int(last_traded_cache[reverse_pair]["last_swap"])

    except Exception as e:  # pragma: no cover
        logger.warning(default_error(e))
    return 0


def get_last_trade_price(pair_str: str, last_traded_cache: Dict) -> int:
    try:
        pair_str = pair_str.replace("-segwit", "")
        if pair_str in last_traded_cache:
            return Decimal(last_traded_cache[pair_str]["last_price"])
        reverse_pair = "_".join(pair_str.split("_")[::-1])
        if reverse_pair in last_traded_cache:
            return Decimal(last_traded_cache[reverse_pair]["last_price"])
    except Exception as e:  # pragma: no cover
        logger.warning(default_error(e))
    return 0


def get_last_trade_uuid(pair_str: str, last_traded_cache: Dict) -> int:
    try:
        pair_str = pair_str.replace("-segwit", "")
        if pair_str in last_traded_cache:
            return last_traded_cache[pair_str]["last_swap_uuid"]
        reverse_pair = "_".join(pair_str.split("_")[::-1])
        if reverse_pair in last_traded_cache:
            return last_traded_cache[reverse_pair]["last_swap_uuid"]
    except Exception as e:  # pragma: no cover
        logger.warning(default_error(e))
    return ""


def pair_without_segwit_suffix(maker_coin, taker_coin):
    """Removes `-segwit` suffixes from the tickers of a pair"""
    if maker_coin.endswith('-'):
        maker_coin = maker_coin[:-1]
    if taker_coin.endswith('-'):
        taker_coin = taker_coin[:-1]
    return f'{maker_coin.replace("-segwit", "")}_{taker_coin.replace("-segwit", "")}'


def get_coin_variants(coin, coins_config):
    return [
        i for i in coins_config
        if i.replace(coin, "") == ""
        or i.replace(coin, "").startswith("-")
    ]