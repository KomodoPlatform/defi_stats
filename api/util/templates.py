#!/usr/bin/env python3


def last_price_for_pair():
    return {"timestamp": 0, "price": 0}


def swap_counts():
    return {"swaps_all_time": 0, "swaps_30d": 0, "swaps_24h": 0}


def liquidity():
    return {
        "rel_usd_price": 0,
        "rel_liquidity_coins": 0,
        "rel_liquidity_usd": 0,
        "base_usd_price": 0,
        "base_liquidity_coins": 0,
        "base_liquidity_usd": 0,
        "liquidity_usd": 0,
    }


def orderbook(base: str, quote: str, v2=False):
    data = {
        "pair": f"{base}_{quote}",
        "base": base,
        "quote": quote,
        "bids": [],
        "asks": [],
        "total_asks_base_vol": 0,
        "total_asks_quote_vol": 0,
        "total_bids_base_vol": 0,
        "total_bids_quote_vol": 0,
    }
    if v2:  # pragma: no cover
        data.update({"total_asks_base_vol": {"decimal": 0}})
        data.update({"total_asks_quote_vol": {"decimal": 0}})
        data.update({"total_bids_base_vol": {"decimal": 0}})
        data.update({"total_bids_quote_vol": {"decimal": 0}})
    return data


def pair_info(base, quote):
    return {
        "ticker_id": f"{base}_{quote}",
        "pool_id": f"{base}_{quote}",
        "base": base,
        "target": quote,
    }
