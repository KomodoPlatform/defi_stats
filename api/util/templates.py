#!/usr/bin/env python3
import util.cron as cron
import util.helper as helper


def last_price_for_pair():  # pragma: no cover
    return {"timestamp": 0, "price": 0}


def liquidity():  # pragma: no cover
    return {
        "rel_usd_price": 0,
        "quote_liquidity_coins": 0,
        "quote_liquidity_usd": 0,
        "base_usd_price": 0,
        "base_liquidity_coins": 0,
        "base_liquidity_usd": 0,
        "liquidity_in_usd": 0,
    }


def pair_info(pair_str: str, priced: bool = False) -> dict:
    base, quote = helper.base_quote_from_pair(pair_str)
    return {
        "ticker_id": pair_str,
        "base": base,
        "target": quote,
        "last_swap_time": 0,
        "last_swap_price": 0,
        "last_swap_uuid": "",
        "priced": priced,
    }


def orderbook(pair_str):
    base, quote = helper.base_quote_from_pair(pair_str)
    base = base.replace("-segwit", "")
    quote = quote.replace("-segwit", "")
    data = {
        "pair": f"{base}_{quote}",
        "base": base,
        "quote": quote,
        "variants": [],
        "asks": [],
        "bids": [],
        "highest_bid": 0,
        "lowest_ask": 0,
        "liquidity_in_usd": 0,
        "total_asks_base_vol": 0,
        "total_bids_base_vol": 0,
        "total_asks_quote_vol": 0,
        "total_bids_quote_vol": 0,
        "total_asks_base_usd": 0,
        "total_bids_quote_usd": 0,
        "base_liquidity_coins": 0,
        "base_liquidity_usd": 0,
        "quote_liquidity_coins": 0,
        "quote_liquidity_usd": 0,
        "liquidity_in_usd": 0,
        "timestamp": f"{int(cron.now_utc())}",
    }
    return data


def gecko_info(coin_id):
    return {"usd_market_cap": 0, "usd_price": 0, "coingecko_id": coin_id}


def volumes_and_prices(suffix, base, quote):
    return {
        f"trades_{suffix}": 0,
        "base": base,
        "base_volume": 0,
        "base_volume_usd": 0,
        "base_price_usd": 0,
        "quote": quote,
        "quote_volume": 0,
        "quote_volume_usd": 0,
        "quote_price_usd": 0,
        "oldest_price_time": 0,
        "newest_price_time": 0,
        "oldest_price": 0,
        "newest_price": 0,
        f"highest_price_{suffix}": 0,
        f"lowest_price_{suffix}": 0,
        f"price_change_pct_{suffix}": 0,
        f"price_change_{suffix}": 0,
        "last_swap_price": 0,
        "last_swap_uuid": "",
        "last_swap_time": 0,
        "combined_volume_usd": 0,
        "variants": [],
    }


def volumes_ticker():
    return {
        "taker_volume": 0,
        "maker_volume": 0,
        "trade_volume": 0,
        "swaps": 0,
        "taker_volume_usd": 0,
        "maker_volume_usd": 0,
        "trade_volume_usd": 0,
    }


def ticker_info(suffix, base, quote):
    return {
        "ticker_id": f"{base}_{quote}",
        "variants": [],
        f"trades_{suffix}": 0,
        "base_currency": base,
        "base_volume": 0,
        "base_volume_usd": 0,
        "base_liquidity_coins": 0,
        "base_liquidity_usd": 0,
        "base_usd_price": 0,
        "quote_currency": quote,
        "quote_volume": 0,
        "quote_volume_usd": 0,
        "quote_liquidity_coins": 0,
        "quote_liquidity_usd": 0,
        "quote_usd_price": 0,
        "combined_volume_usd": 0,
        "liquidity_in_usd": 0,
        "last_swap_price": 0,
        "last_swap_uuid": "",
        "last_swap_time": 0,
        "oldest_price": 0,
        "oldest_price_time": 0,
        "newest_price": 0,
        "newest_price_time": 0,
        f"highest_price_{suffix}": 0,
        f"lowest_price_{suffix}": 0,
        f"price_change_pct_{suffix}": 0,
        f"price_change_{suffix}": 0,
        "highest_bid": 0,
        "lowest_ask": 0,
    }


def coin_trade_vol_item():
    return {
        "taker_volume": 0,
        "maker_volume": 0,
        "trade_volume": 0,
        "swaps": 0,
    }


def first_last_swap():
    return {
        "last_swap_time": 0,
        "last_swap_price": 0,
        "last_swap_uuid": "",
    }


def pair_trade_vol_item():
    return {
        "base_volume": 0,
        "quote_volume": 0,
        "swaps": 0,
    }


def last_trade_info():
    return {
        "swap_count": 0,
        "sum_taker_traded": 0,
        "sum_maker_traded": 0,
        "last_swap": 0,
        "last_swap_price": 0,
        "last_swap_uuid": "",
        "last_taker_amount": 0,
        "last_maker_amount": 0,
    }


def last_traded_item():
    return {
        "total_num_swaps": 0,
        "maker_num_swaps": 0,
        "taker_num_swaps": 0,
        "maker_last_swap_uuid": 0,
        "maker_last_swap_time": 0,
        "taker_last_swap_uuid": 0,
        "taker_last_swap_time": 0,
    }
