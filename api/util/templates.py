#!/usr/bin/env python3
import time


def last_price_for_pair():  # pragma: no cover
    return {"timestamp": 0, "price": 0}


def swap_counts():  # pragma: no cover
    return {"swaps_all_time": 0, "swaps_30d": 0, "swaps_24hr": 0}


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
        "timestamp": f"{int(time.time())}",
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
        "last_price": 0,
        "last_trade": 0,
        "trades_24hr": 0,
        f"price_change_percent_{suffix}": 0,
        f"price_change_{suffix}": 0,
        "last_swap_uuid": "",
        "oldest_price": 0,
        "newest_price": 0,
        "oldest_price_time": 0,
        "newest_price_time": 0
    }
