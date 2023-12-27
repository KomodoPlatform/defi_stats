#!/usr/bin/env python3
import time
import inspect
from fastapi import APIRouter
from fastapi_utils.tasks import repeat_every
from util.logger import logger
from lib.cache import Cache
from lib.cache_item import CacheItem
from util.enums import NetId
from lib.external import FixerAPI, CoinGeckoAPI
from const import NODE_TYPE
from db.sqlitedb_merge import import_source_databases
from util.logger import logger, timed
from util.templates import default_error, default_result
from util.exceptions import DataStructureError

router = APIRouter()

# Pure Upstream Data Sourcing


@router.on_event("startup")
@repeat_every(seconds=60)
@timed
def coins():  # pragma: no cover
    try:
        coins_cache = CacheItem("coins")
        coins_cache.save()
        coins_config_cache = CacheItem("coins_config")
        coins_config_cache.save()
    except Exception as e:
        return default_error(e)
    return default_result("Coins update loop complete!", loglevel='loop')


@router.on_event("startup")
@repeat_every(seconds=60)
def gecko_data():  # pragma: no cover
    try:
        cache = Cache()
        gecko_cache = cache.get_item("gecko_source")
        data = CoinGeckoAPI().get_gecko_source()
        gecko_cache.save(data)
    except Exception as e:
        return


@router.on_event("startup")
@repeat_every(seconds=60)
def prices_service():  # pragma: no cover
    try:
        cache = Cache()
        prices_tickers_v1_cache = cache.get_item("prices_tickers_v1")
        prices_tickers_v1_cache.save()
    except Exception as e:
        return
    try:
        cache = Cache()
        prices_tickers_v2_cache = cache.get_item("prices_tickers_v2")
        prices_tickers_v2_cache.save()
    except Exception as e:
        return


@router.on_event("startup")
@repeat_every(seconds=600)
def fixer_rates():  # pragma: no cover
    try:
        cache = Cache()
        fixer = FixerAPI()
        fixer_rates_cache = cache.get_item("fixer_rates")
        fixer_rates_cache.save(fixer.latest())
    except Exception as e:
        return


# Derived Cache data for Gecko endpoints


@router.on_event("startup")
@repeat_every(seconds=10)
def gecko_pairs():
    try:
        cache = Cache(netid="ALL")
        gecko_pairs_cache = cache.get_item("gecko_pairs")
        data = cache.calc.traded_pairs(days=7)
        resp = gecko_pairs_cache.save(data)
    except Exception as e:
        return


@router.on_event("startup")
@repeat_every(seconds=10)
def gecko_tickers():
    try:
        cache = Cache(netid="ALL")
        gecko_tickers_cache = CacheItem(name="gecko_tickers", netid="ALL")
        data = cache.calc.traded_tickers(pairs_days=7)
        resp = gecko_tickers_cache.save(data)
    except Exception as e:
        return


# Stats-API Cache

"""
@router.on_event("startup")
@repeat_every(seconds=60)
def gecko_data():  # pragma: no cover
    try:
        cache.save.gecko_data()
    except Exception as e:
        logger.warning(f"{type(e)} Error in [gecko_data]: {e}")


@router.on_event("startup")
@repeat_every(seconds=60)
def summary():  # pragma: no cover
    try:
        cache.save.summary()
    except Exception as e:
        logger.warning(f"{type(e)} Error in [summary_data]: {e}")


@router.on_event("startup")
@repeat_every(seconds=60)
def ticker():  # pragma: no cover
    try:
        cache.save.ticker()
    except Exception as e:
        logger.warning(f"{type(e)} Error in [ticker_data]: {e}")


@router.on_event("startup")
@repeat_every(seconds=600)  # caching data every 10 minutes
def atomicdexio():  # pragma: no cover
    try:
        cache.save.atomicdexio()
    except Exception as e:
        logger.warning(f"{type(e)} Error in [atomicdex_io]: {e}")


@router.on_event("startup")
@repeat_every(seconds=600)  # caching data every 10 minutes
def atomicdex_fortnight():  # pragma: no cover
    try:
        cache.save.atomicdex_fortnight()
    except Exception as e:
        logger.warning(f"{type(e)} in [atomicdex_io_fortnight]: {e}")
"""

# Derived Cache data for Markets endpoints


@router.on_event("startup")
@repeat_every(seconds=10)
def markets_last_trade():
    # This one is fast, so can do all netids in seq in same func
    for netid in NetId:
        try:
            cache = Cache(netid=netid.value)
            cache_item = CacheItem("markets_last_trade")
            data = cache.calc.pairs_last_trade()
            logger.info(data[1])
            resp = cache_item.save(data)
        except Exception as e:
            pass

@timed
@router.on_event("startup")
@repeat_every(seconds=10)
def markets_pairs(netid):
    try:
        cache = Cache(netid=netid)
        cache_item = CacheItem(name="markets_pairs", netid=netid)
        data = cache.calc.traded_pairs(days=120)
        if "error" not in data:
            if len(data) > 0:
                resp = cache_item.save(data)
                return default_result(msg=f"Markets pairs updated ({netid})")
        else:
            raise DataStructureError("Unexpected data structure returned")
        return default_result(msg=f"Markets pairs not, updated because input data was empty ({netid})")
    except Exception as e:
        return default_result(msg=f"Markets pairs update failed! ({netid})")

@router.on_event("startup")
@repeat_every(seconds=10)
def markets_pairs_7777():
    markets_pairs("7777")


@router.on_event("startup")
@repeat_every(seconds=10)
def markets_pairs_8762():
    markets_pairs("8762")


@router.on_event("startup")
@repeat_every(seconds=10)
def markets_pairs_ALL():
    markets_pairs("ALL")

@timed
def markets_tickers(netid):
    try:
        cache = Cache(netid=netid)
        cache_item = CacheItem(name="markets_tickers", netid=netid)
        data = cache.calc.traded_tickers(pairs_days=120)
        resp = cache_item.save(data)
    except Exception as e:
        return


@router.on_event("startup")
@repeat_every(seconds=10)
def markets_tickers_7777():
    markets_tickers("7777")


@router.on_event("startup")
@repeat_every(seconds=10)
def markets_tickers_8762():
    markets_tickers("8762")


@router.on_event("startup")
@repeat_every(seconds=10)
def markets_tickers_all():
    markets_tickers("ALL")


# Processing Loops


@router.on_event("startup")
@repeat_every(seconds=60)
def import_dbs():
    if NODE_TYPE != "serve":
        try:
            import_source_databases()
        except Exception as e:
            pass
