#!/usr/bin/env python3
from fastapi import APIRouter
from fastapi_utils.tasks import repeat_every
from db.sqlitedb_merge import SqliteMerge
from db.sqldb import populate_pgsqldb, reset_defi_stats_table


from lib.cache import Cache
from util.defaults import default_error, default_result
from util.logger import timed, logger
import lib
from const import NODE_TYPE

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
        logger.loop("Init coins source update")
        for i in ["coins", "coins_config"]:
            cache_item = lib.CacheItem("coins")
            cache_item.save()
    except Exception as e:
        return default_error(e)
    return default_result("Coins update loop complete!", loglevel="loop")


@router.on_event("startup")
@repeat_every(seconds=300)
@timed
def gecko_data():  # pragma: no cover
    try:
        logger.loop("Init gecko source update")
        cache_item = lib.CacheItem("gecko_source")
        cache_item.save()
    except Exception as e:
        return default_error(e)
    msg = "Gecko data update loop complete!"
    return default_result(msg=msg, loglevel="loop")


@router.on_event("startup")
@repeat_every(seconds=60)
@timed
def prices_service():  # pragma: no cover
    try:
        logger.loop("Init prices_service source update")
        for i in ["prices_tickers_v1", "prices_tickers_v2"]:
            cache_item = lib.CacheItem(i)
            cache_item.save()
    except Exception as e:
        return default_error(e)
    msg = "Prices update loop complete!"
    return default_result(msg=msg, loglevel="loop")


@router.on_event("startup")
@repeat_every(seconds=900)
@timed
def fixer_rates():  # pragma: no cover
    try:
        logger.loop("Init fixer_rates source update")
        cache_item = lib.CacheItem("fixer_rates")
        cache_item.save()
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
        logger.loop("Init gecko_tickers source update")
        cache_item = lib.CacheItem(name="gecko_tickers")
        cache_item.save()
    except Exception as e:
        return default_error(e)
    msg = "Gecko tickers (ALL) loop complete!"
    return default_result(msg=msg, loglevel="loop")


# Derived Cache data for Markets endpoints


@timed
def markets_pairs(netid):
    try:
        cache_item = lib.CacheItem(name="markets_pairs", netid=netid)
        cache_item.save()
    except Exception as e:
        msg = f"Markets pairs update failed! ({netid}): {e}"
        return default_error(e, msg)


@timed
def markets_tickers(netid):
    try:
        cache_item = lib.CacheItem(name="markets_tickers", netid=netid)
        cache_item.save()
    except Exception as e:
        msg = f"Failed for netid {netid}!"
        return default_error(e, msg)
    return default_result(
        msg=f"Market Tickers for netid {netid} Updated!", loglevel="loop"
    )


@router.on_event("startup")
@repeat_every(seconds=120)
@timed
def markets_tickers_7777():
    markets_tickers("7777")
    msg = "Market tickers (7777) loop complete!"
    return default_result(msg=msg, loglevel="loop")


@router.on_event("startup")
@repeat_every(seconds=120)
@timed
def markets_tickers_8762():
    markets_tickers("8762")
    msg = "Market tickers (8762) loop complete!"
    return default_result(msg=msg, loglevel="loop")


@router.on_event("startup")
@repeat_every(seconds=120)
@timed
def markets_tickers_all():
    markets_tickers("ALL")
    msg = "Market tickers (all) loop complete!"
    return default_result(msg=msg, loglevel="loop")


# Stats API Loops
@router.on_event("startup")
@repeat_every(seconds=120)
@timed
def statsapi_atomicdex_fortnight():
    try:
        cache_item = lib.CacheItem(name="statsapi_adex_fortnite")
        cache_item.save()
    except Exception as e:
        logger.info(default_error(e))
    msg = "Stats API Adex fortnight loop complete!"
    return default_result(msg=msg, loglevel="loop")


@router.on_event("startup")
@repeat_every(seconds=120)
@timed
def statsapi_summary():
    try:
        cache_item = lib.CacheItem(name="statsapi_summary")
        cache_item.save()
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
        cache_item = lib.CacheItem(name="generic_last_traded")
        cache_item.save()
    except Exception as e:
        return default_error(e)
    msg = "generic_last_traded loop complete!"
    return default_result(msg=msg, loglevel="loop")


@router.on_event("startup")
@repeat_every(seconds=120)
@timed
def generic_pairs():
    try:
        cache_item = lib.CacheItem(name="generic_pairs")
        cache_item.save()
    except Exception as e:
        return default_error(e)
    msg = "Generic pairs (ALL) loop complete!"
    return default_result(msg=msg, loglevel="loop")


@router.on_event("startup")
@repeat_every(seconds=120)
@timed
def generic_tickers():
    try:
        cache_item = lib.CacheItem(name="generic_tickers")
        cache_item.save()
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
            merge = SqliteMerge()
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
def truncate_wal():
    try:
        merge = SqliteMerge()
        merge.truncate_wal()
    except Exception as e:
        return default_error(e)
    msg = "Database wal truncation loop complete!"
    return default_result(msg=msg, loglevel="loop")


@router.on_event("startup")
@repeat_every(seconds=300)
@timed
def populate_pgsqldb_loop():
    # reset_defi_stats_table()
    # updates last 24 hours swaps
    populate_pgsqldb()
