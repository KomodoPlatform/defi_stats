#!/usr/bin/env python3
from fastapi import APIRouter
from fastapi_utils.tasks import repeat_every
from const import MARKETS_DAYS
from db.sqlitedb_merge import import_source_databases
from lib.cache import Cache
from lib.cache_item import CacheItem
from lib.external import FixerAPI, CoinGeckoAPI
from lib.generics import Generics
from lib.markets import Markets
from util.defaults import default_error, default_result
from util.enums import NetId
from util.logger import timed
from util.validate import validate_loop_data


router = APIRouter()

# Pure Upstream Data Sourcing


@router.on_event("startup")
@repeat_every(seconds=86400)
@timed
def coins():  # pragma: no cover
    try:
        for i in ["coins", "coins_config"]:
            cache_item = CacheItem("coins")
            cache_item.save()
    except Exception as e:
        return default_error(e)
    return default_result("Coins update loop complete!", loglevel="loop")


@router.on_event("startup")
@repeat_every(seconds=300)
@timed
def gecko_data():  # pragma: no cover
    try:
        cache = Cache()
        cache_item = cache.get_item("gecko_source")
        data = CoinGeckoAPI().get_gecko_source()
        cache_item.save(data)
    except Exception as e:
        return default_error(e)
    msg = "Gecko data update loop complete!"
    return default_result(msg=msg, loglevel="loop")


@router.on_event("startup")
@repeat_every(seconds=60)
@timed
def prices_service():  # pragma: no cover
    try:
        for i in ["prices_tickers_v1", "prices_tickers_v2"]:
            cache_item = CacheItem(i)
            cache_item.save()
    except Exception as e:
        return default_error(e)
    msg = "Prices update loop complete!"
    return default_result(msg=msg, loglevel="loop")


@router.on_event("startup")
@repeat_every(seconds=600)
@timed
def fixer_rates():  # pragma: no cover
    try:
        cache = Cache()
        fixer = FixerAPI()
        cache_item = cache.get_item("fixer_rates")
        cache_item.save(fixer.latest())
    except Exception as e:
        return default_error(e)
    msg = "Fixer rates update loop complete!"
    return default_result(msg=msg, loglevel="loop")


# Derived Cache data for Gecko endpoints


@router.on_event("startup")
@repeat_every(seconds=60)
@timed
def gecko_pairs():
    try:
        cache = Cache(netid="ALL")
        cache_item = cache.get_item(name="gecko_pairs")
        generics = Generics(netid="ALL")
        data = generics.traded_pairs(days=7)
        if validate_loop_data(data, cache_item, "ALL"):
            cache_item.save(data)
    except Exception as e:
        return default_error(e)
    msg = "Gecko pairs (ALL) loop complete!"
    return default_result(msg=msg, loglevel="loop")


@router.on_event("startup")
@repeat_every(seconds=60)
@timed
def gecko_tickers():
    try:
        cache = Cache(netid="ALL")
        cache_item = cache.get_item(name="gecko_tickers")
        generics = Generics(netid="ALL")
        data = generics.traded_tickers(pairs_days=7)
        if validate_loop_data(data, cache_item, "ALL"):
            cache_item.save(data)
    except Exception as e:
        return default_error(e)
    msg = "Gecko tickers (ALL) loop complete!"
    return default_result(msg=msg, loglevel="loop")


# Derived Cache data for Markets endpoints


@router.on_event("startup")
@repeat_every(seconds=60)
@timed
def markets_last_trade():
    # This one is fast, so can do all netids in seq in same func
    for netid in NetId:
        try:
            cache = Cache(netid=netid.value)
            cache_item = cache.get_item(name="markets_last_trade")
            markets = Markets(netid=netid.value)
            data = markets.last_trade()
            if validate_loop_data(data, cache_item, netid):
                cache_item.save(data)
        except Exception as e:
            return default_error(e)
    msg = "Markets last trade loop complete!"
    return default_result(msg=msg, loglevel="loop")


@timed
def markets_pairs(netid):
    try:
        cache = Cache(netid=netid)
        cache_item = cache.get_item(name="markets_pairs")
        markets = Markets(netid=netid)
        data = markets.pairs(days=MARKETS_DAYS)
        if validate_loop_data(data, cache_item, netid):
            cache_item.save(data)
    except Exception as e:
        msg = f"Markets pairs update failed! ({netid}): {e}"
        return default_error(e, msg)


@router.on_event("startup")
@repeat_every(seconds=60)
@timed
def markets_pairs_7777():
    markets_pairs("7777")
    msg = "Market pairs (7777) loop complete!"
    return default_result(msg=msg, loglevel="loop")


@router.on_event("startup")
@repeat_every(seconds=60)
@timed
def markets_pairs_8762():
    markets_pairs("8762")
    msg = "Market pairs (8762) loop complete!"
    return default_result(msg=msg, loglevel="loop")


@router.on_event("startup")
@repeat_every(seconds=60)
@timed
def markets_pairs_ALL():
    markets_pairs("ALL")
    msg = "Market pairs (ALL) loop complete!"
    return default_result(msg=msg, loglevel="loop")


@timed
def markets_tickers(netid):
    try:
        cache = Cache(netid=netid)
        cache_item = cache.get_item(name="markets_tickers")
        markets = Markets(netid=netid)
        data = markets.tickers(pairs_days=MARKETS_DAYS)
        if validate_loop_data(data, cache_item, netid):
            cache_item.save(data)
    except Exception as e:
        msg = f"Failed for netid {netid}!"
        return default_error(e, msg)
    return default_result(
        msg=f"Market Tickers for netid {netid} Updated!", loglevel="loop"
    )


@router.on_event("startup")
@repeat_every(seconds=60)
@timed
def markets_tickers_7777():
    markets_tickers("7777")
    msg = "Market tickers (7777) loop complete!"
    return default_result(msg=msg, loglevel="loop")


@router.on_event("startup")
@repeat_every(seconds=60)
@timed
def markets_tickers_8762():
    markets_tickers("8762")
    msg = "Market tickers (8762) loop complete!"
    return default_result(msg=msg, loglevel="loop")


@router.on_event("startup")
@repeat_every(seconds=60)
@timed
def markets_tickers_all():
    markets_tickers("ALL")
    msg = "Market tickers (all) loop complete!"
    return default_result(msg=msg, loglevel="loop")


# Processing Loops


@router.on_event("startup")
@repeat_every(seconds=600)
@timed
def import_dbs():
    NODE_TYPE = "noserve"
    if NODE_TYPE != "serve":
        try:
            import_source_databases()
        except Exception as e:
            return default_error(e)
        msg = "Import source databases loop complete!"
        return default_result(msg=msg, loglevel="loop")


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
