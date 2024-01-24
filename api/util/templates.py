#!/usr/bin/env python3
import util.cron as cron


def last_price_for_pair():  # pragma: no cover
    return {"timestamp": 0, "price": 0}


def liquidity():  # pragma: no cover
    return {
        "rel_usd_price": 0,
        "rel_liquidity_coins": 0,
        "rel_liquidity_usd": 0,
        "base_usd_price": 0,
        "base_liquidity_coins": 0,
        "base_liquidity_usd": 0,
        "liquidity_usd": 0,
    }


def pair_info(pair_str: str, priced: bool = False) -> dict:
    return {
        "ticker_id": pair_str,
        "pool_id": pair_str,
        "base": pair_str.split("_")[0],
        "target": pair_str.split("_")[1],
        "last_trade": 0,
        "priced": priced,
    }


def orderbook(pair_str):
    x = pair_str.split("_")
    base = x[0].replace("-segwit", "")
    quote = x[1].replace("-segwit", "")
    return {
        "pair": f"{base}_{quote}",
        "base": base,
        "quote": quote,
        "timestamp": f"{int(cron.now_utc())}",
        "asks": [],
        "bids": [],
        "liquidity_usd": 0,
        "total_asks_base_vol": 0,
        "total_bids_base_vol": 0,
        "total_asks_quote_vol": 0,
        "total_bids_quote_vol": 0,
        "total_asks_base_usd": 0,
        "total_bids_quote_usd": 0,
        "trades_24hr": 0,
        "volume_usd_24hr": 0,
    }


def gecko_info(coin_id):
    return {"usd_market_cap": 0, "usd_price": 0, "coingecko_id": coin_id}


def volumes_and_prices(suffix):
    return {
        "base_volume": 0,
        "quote_volume": 0,
        f"highest_price_{suffix}": 0,
        f"lowest_price_{suffix}": 0,
        "trades_24hr": 0,
        f"price_change_percent_{suffix}": 0,
        f"price_change_{suffix}": 0,
        "last_swap_uuid": "",
        "last_swap_price": 0,
        "last_swap_time": 0,
        "first_swap_uuid": "",
        "first_swap_price": 0,
        "first_swap_time": 0,
        "oldest_price": 0,
        "newest_price": 0,
        "oldest_price_time": 0,
        "newest_price_time": 0,
        "variants": [],
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
        "first_swap_time": 0,
        "first_swap_price": 0,
        "first_swap_uuid": "",
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
        "last_price": 0,
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
        "maker_first_swap_uuid": 0,
        "maker_first_swap_time": 0,
        "taker_last_swap_uuid": 0,
        "taker_last_swap_time": 0,
        "taker_first_swap_uuid": 0,
        "taker_first_swap_time": 0,
    }
