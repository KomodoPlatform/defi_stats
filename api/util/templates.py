#!/usr/bin/env python3
import time


def last_price_for_pair():  # pragma: no cover
    return {"timestamp": 0, "price": 0}


def swap_counts():  # pragma: no cover
    return {"swaps_all_time": 0, "swaps_30d": 0, "swaps_24h": 0}


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


def pair_info(base, quote):
    return {
        "ticker_id": f"{base}_{quote}",
        "pool_id": f"{base}_{quote}",
        "base": base,
        "target": quote,
    }


def orderbook(pair_str):
    x = pair_str.split("_")
    return {
        "pair": f"{pair_str}",
        "base": x[0],
        "quote": x[1],
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
    }
