#!/usr/bin/env python3
import os
import time
import json
from pymemcache.client.base import PooledClient
from util.logger import logger, timed
from const import MEMCACHE_LIMIT
import util.defaults as default
from dotenv import load_dotenv

load_dotenv()


class JsonSerde(object):  # pragma: no cover
    def serialize(self, key, value):
        if isinstance(value, str):
            return value.encode("utf-8"), 1
        return json.dumps(value).encode("utf-8"), 2

    def deserialize(self, key, value, flags):
        if flags == 1:
            return value.decode("utf-8")
        if flags == 2:
            return json.loads(value.decode("utf-8"))
        raise Exception("Unknown serialization format")

try:  # pragma: no cover
    MEMCACHE = PooledClient(
        ("memcached", 11211),
        serde=JsonSerde(),
        timeout=10,
        max_pool_size=50,
        ignore_exc=True,
    )
    MEMCACHE.set("foo", "bar", 60)
    if os.getenv("IS_TESTING") == "True":
        MEMCACHE.set("testing", True, 3600)
    logger.info("Connected to memcached docker container")

except Exception as e:  # pragma: no cover
    logger.muted(e)
    MEMCACHE = PooledClient(
        ("localhost", 11211),
        serde=JsonSerde(),
        timeout=10,
        max_pool_size=50,
        ignore_exc=True,
    )
    logger.info("Connected to memcached on localhost")
    if os.getenv("IS_TESTING") == "True":
        MEMCACHE.set("testing", True, 3600)

MEMCACHE.cache_memlimit = MEMCACHE_LIMIT


def stats():  # pragma: no cover
    return MEMCACHE.stats()


def get(key):  # pragma: no cover
    i = 0
    if os.getenv("IS_TESTING") == "True" and key != "testing":
        key = f"{key}-testing"
    while i < 7:
        cached = None
        try:
            cached = MEMCACHE.get(key)
        except OSError:
            time.sleep(0.1)
        if cached is not None:
            return cached
        i += 1
    if (
        "orderbook" not in key
        and "ticker_info" not in key
        and "prices" not in key
        and key not in ["testing"]
    ):
        logger.warning(f"Failed to get '{key}' from memcache")
    return None


@timed
def update(key, value, expiry):
    try:
        if os.getenv("IS_TESTING") == "True" and key != "testing":
            key = f"{key}-testing"
        if value is not None:
            MEMCACHE.set(key, value, expiry)
            msg = f"{key} added to memcache"
            return default.result(data=key, msg=msg, loglevel="cached", ignore_until=5)
        msg = f"{key} memcache not updated, data is empty"
    except Exception as e:
        msg = f"{key} memcache not updated: {e}"
        logger.warning(f"Failed to cache {key}! {e}")
        logger.warning(f"Failed to cache {str(value)[:100]}!")
    return default.result(data=key, msg=msg, loglevel="warning", ignore_until=0)


# EXTERNAL SOURCES CACHE
def set_coins(data):  # pragma: no cover
    update("coins", data, 86400)


def get_coins():  # pragma: no cover
    return get("coins")


def set_coins_config(data):  # pragma: no cover
    update("coins_config", data, 86400)


def get_coins_config():  # pragma: no cover
    return get("coins_config")


def set_fixer_rates(data):  # pragma: no cover
    update("fixer_rates", data, 3600)


def get_fixer_rates():  # pragma: no cover
    return get("fixer_rates")


def set_gecko_source(data):  # pragma: no cover
    update("gecko_source", data, 3600)


def get_gecko_source():  # pragma: no cover
    return get("gecko_source")


# FOUNDATIONAL CACHE
def set_pairs_orderbook_extended(data):  # pragma: no cover
    update("pairs_orderbook_extended", data, 3600)


def get_pairs_orderbook_extended():  # pragma: no cover
    return get("pairs_orderbook_extended")


def set_coin_volumes_24hr(data):  # pragma: no cover
    update("coin_volumes_24hr", data, 3600)


def get_coin_volumes_24hr():  # pragma: no cover
    return get("coin_volumes_24hr")


def set_pair_volumes_24hr(data):  # pragma: no cover
    update("pair_volumes_24hr", data, 3600)


def get_pair_volumes_24hr():  # pragma: no cover
    return get("pair_volumes_24hr")


def set_pair_volumes_14d(data):  # pragma: no cover
    update("pair_volumes_14d", data, 3600)


def get_pair_volumes_14d():  # pragma: no cover
    return get("pair_volumes_14d")


def set_pairs_last_traded(data):  # pragma: no cover
    update("pairs_last_traded", data, 3600)


def get_pairs_last_traded():  # pragma: no cover
    return get("pairs_last_traded")


def set_pair_prices_24hr(data):  # pragma: no cover
    update("pair_prices_24hr", data, 3600)


def get_pair_prices_24hr():  # pragma: no cover
    return get("pair_prices_24hr")


def set_tickers(data):  # pragma: no cover
    update("tickers", data, 3600)


def get_tickers():  # pragma: no cover
    return get("tickers")


def set_gecko_pairs(data):  # pragma: no cover
    update("gecko_pairs", data, 3600)


def get_gecko_pairs():  # pragma: no cover
    return get("gecko_pairs")


def set_stats_api_summary(data):  # pragma: no cover
    update("stats_api_summary", data, 3600)


def get_stats_api_summary():  # pragma: no cover
    return get("stats_api_summary")


# MARKETS
def set_markets_summary(data):  # pragma: no cover
    update("markets_summary", data, 3600)


def get_markets_summary():  # pragma: no cover
    return get("markets_summary")


def set_adex_24hr(data):  # pragma: no cover
    update("adex_24hr", data, 3600)


def get_adex_24hr():  # pragma: no cover
    return get("adex_24hr")


def set_adex_fortnite(data):  # pragma: no cover
    update("adex_fortnite", data, 3600)


def get_adex_fortnite():  # pragma: no cover
    return get("adex_fortnite")


# REVIEW CACHE (TOO LARGE)
# def set_summary(data):  # pragma: no cover
# update("generic_summary", data, 3600)


# def get_summary():  # pragma: no cover
# data = get("generic_summary")
# return data

"""
def set_tickers(data):  # pragma: no cover
    update("generic_tickers", data, 3600)


def get_tickers():  # pragma: no cover
    data = get("generic_tickers")
    return data


def set_tickers_14d(data):  # pragma: no cover
    update("generic_tickers_14d", data, 3600)


def get_tickers_14d():  # pragma: no cover
    data = get("generic_tickers_14d")
    return data

"""
