#!/usr/bin/env python3
import requests
from fastapi import APIRouter
from fastapi_utils.tasks import repeat_every
import db


from lib.cache import Cache
from util.defaults import default_error, default_result
from util.logger import timed, logger
import lib
from const import NODE_TYPE, RESET_TABLE


router = APIRouter()


@router.on_event("startup")
@repeat_every(seconds=300)
@timed
def check_cache():  # pragma: no cover
    try:
        cache = Cache()
        cache.updated_since(True)
    except Exception as e:
        return default_error(e)


# Pure Upstream Data Sourcing


@router.on_event("startup")
@repeat_every(seconds=14400)
@timed
def coins():  # pragma: no cover
    try:
        for i in ["coins", "coins_config"]:
            lib.CacheItem(i).save()
    except Exception as e:
        return default_error(e)
    return default_result("Coins update loop complete!", loglevel="loop")


@router.on_event("startup")
@repeat_every(seconds=600)
@timed
def gecko_data():  # pragma: no cover
    try:
        lib.CacheItem("gecko_source").save()
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
            lib.CacheItem(i).save()
    except Exception as e:
        return default_error(e)
    msg = "Prices update loop complete!"
    return default_result(msg=msg, loglevel="loop")


@router.on_event("startup")
@repeat_every(seconds=300)
@timed
def fixer_rates():  # pragma: no cover
    try:
        return requests.get("https://rates.komodo.earth/api/v1/usd_rates").json()
    except Exception as e:
        return default_error(e)
    msg = "Fixer rates update loop complete!"
    return default_result(msg=msg, loglevel="loop")


# Derived Cache data for Gecko endpoints


@router.on_event("startup")
@repeat_every(seconds=120)
@timed
def gecko_tickers():
    try:
        lib.CacheItem(name="gecko_tickers").save()
    except Exception as e:
        return default_error(e)
    msg = "Gecko tickers (ALL) loop complete!"
    return default_result(msg=msg, loglevel="loop")


# Derived Cache data for Markets endpoints


# Stats API Loops
@router.on_event("startup")
@repeat_every(seconds=120)
@timed
def statsapi_atomicdex_fortnight():
    try:
        lib.CacheItem(name="statsapi_adex_fortnite").save()
    except Exception as e:
        logger.warning(default_error(e))
    msg = "Stats API Adex fortnight loop complete!"
    return default_result(msg=msg, loglevel="loop")


@router.on_event("startup")
@repeat_every(seconds=120)
@timed
def statsapi_summary():
    try:
        lib.CacheItem(name="statsapi_summary").save()
    except Exception as e:
        return default_error(e)
    msg = "Stats API summary loop complete!"
    return default_result(msg=msg, loglevel="loop")


# Generic Loops
@router.on_event("startup")
@repeat_every(seconds=60)
@timed
def generic_last_traded():
    try:
        lib.CacheItem(name="generic_last_traded").save()
    except Exception as e:
        return default_error(e)
    msg = "generic_last_traded loop complete!"
    return default_result(msg=msg, loglevel="loop")


@router.on_event("startup")
@repeat_every(seconds=120)
@timed
def generic_pairs():
    try:
        lib.CacheItem(name="generic_pairs").save()
    except Exception as e:
        return default_error(e)
    msg = "Generic pairs (ALL) loop complete!"
    return default_result(msg=msg, loglevel="loop")


@router.on_event("startup")
@repeat_every(seconds=120)
@timed
def generic_tickers():
    try:
        lib.CacheItem(name="generic_tickers").save()
    except Exception as e:
        return default_error(e)
    msg = "Generic tickers (ALL) loop complete!"
    return default_result(msg=msg, loglevel="loop")


@router.on_event("startup")
@repeat_every(seconds=150)
@timed
def import_dbs():
    if NODE_TYPE != "serve":
        try:
            merge = db.SqliteMerge()
            merge.import_source_databases()
        except Exception as e:
            return default_error(e)
        msg = "Import source databases loop complete!"
        return default_result(msg=msg, loglevel="merge")
    msg = "Import source databases skipped, NodeType is 'serve'!"
    msg += " Masters will be updated from cron."
    return default_result(msg=msg, loglevel="merge")


@router.on_event("startup")
@repeat_every(seconds=300)
@timed
def populate_pgsqldb_loop():
    gecko_source = lib.load_gecko_source()
    coins_config = lib.load_coins_config()
    if RESET_TABLE:
        db.reset_defi_stats_table()
    # updates last 24 hours swaps
    db.populate_pgsqldb(coins_config, gecko_source)
