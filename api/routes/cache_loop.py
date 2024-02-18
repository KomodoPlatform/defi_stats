#!/usr/bin/env python3
from fastapi import APIRouter
from fastapi_utils.tasks import repeat_every
from const import NODE_TYPE
import db.sqldb as db
import db.sqlitedb_merge as old_db_merge
import util.defaults as default
import util.memcache as memcache
from lib.cache import Cache, CacheItem
from lib.cache_calc import CacheCalc
from lib.coins import Coins
from util.logger import logger, timed

router = APIRouter()


@router.on_event("startup")
@repeat_every(seconds=300)
@timed
def check_cache():  # pragma: no cover
    """Checks when cache items last updated"""
    try:
        cache = Cache()
        cache.healthcheck(to_console=True)
        logger.info(memcache.stats())
    except Exception as e:
        return default.result(msg=e, loglevel="warning")


@router.on_event("startup")
@timed
def init_missing_cache():  # pragma: no cover
    memcache.set_coins(CacheItem(name="coins").data)
    memcache.set_coins_config(CacheItem(name="coins_config").data)
    memcache.set_fixer_rates(CacheItem(name="fixer_rates").data)
    memcache.set_gecko_source(CacheItem(name="gecko_source").data)
    memcache.set_gecko_pairs(CacheItem(name="gecko_pairs").data)
    memcache.set_adex_fortnite(CacheItem(name="adex_fortnite").data)
    memcache.set_adex_24hr(CacheItem(name="adex_24hr").data)
    memcache.set_markets_summary(CacheItem(name="markets_summary").data)
    memcache.set_pair_last_traded(CacheItem(name="pair_last_traded").data)
    memcache.set_pair_last_traded_24hr(CacheItem(name="pair_last_traded_24hr").data)
    memcache.set_pair_prices_24hr(CacheItem(name="pair_prices_24hr").data)
    memcache.set_pair_volumes_24hr(CacheItem(name="pair_volumes_24hr").data)
    memcache.set_pair_volumes_14d(CacheItem(name="pair_volumes_14d").data)
    memcache.set_coin_volumes_24hr(CacheItem(name="coin_volumes_24hr").data)
    memcache.set_pair_orderbook_extended(CacheItem(name="pair_orderbook_extended").data)
    memcache.set_tickers(CacheItem(name="tickers").data)
    memcache.set_stats_api_summary(CacheItem(name="stats_api_summary").data)

    # memcache.set_summary(CacheItem(name="generic_summary").data)
    # memcache.set_tickers(CacheItem(name="generic_tickers").data)
    # memcache.set_tickers_14d(CacheItem(name="generic_tickers_14d").data)
    memcache.update("coins_with_segwit", [i.coin for i in Coins().with_segwit], 86400)

    msg = "init missing cache loop complete!"
    return default.result(msg=msg, loglevel="loop", ignore_until=3)


# ORDERBOOKS CACHE
@router.on_event("startup")
@repeat_every(seconds=60)
@timed
def update_pair_orderbook_extended():
    if memcache.get("testing") is None:
        try:
            # builds from variant caches and saves to file
            CacheItem(name="pair_orderbook_extended").save()
        except Exception as e:
            return default.result(msg=e, loglevel="warning")
        msg = "pair_orderbook_extended refresh loop complete!"
        return default.result(msg=msg, loglevel="loop", ignore_until=5)


@router.on_event("startup")
@repeat_every(seconds=120)
@timed
def refresh_pair_orderbook_extended():
    if memcache.get("testing") is None:
        try:
            # threaded to populate variant caches
            CacheCalc().pair_orderbook_extended(refresh=True)
        except Exception as e:
            return default.result(msg=e, loglevel="warning")
        msg = "pair_orderbook_extended refresh loop complete!"
        return default.result(msg=msg, loglevel="loop", ignore_until=5)


# PRICES CACHE
@router.on_event("startup")
@repeat_every(seconds=60)
@timed
def refresh_prices_24hr():
    if memcache.get("testing") is None:
        try:
            CacheItem(name="pair_prices_24hr").save()
        except Exception as e:
            return default.result(msg=e, loglevel="warning")
        msg = "pair_prices_24hr refresh loop complete!"
        return default.result(msg=msg, loglevel="loop", ignore_until=5)


# VOLUMES CACHE
@router.on_event("startup")
@repeat_every(seconds=60)
@timed
def get_pair_volumes_24hr():
    if memcache.get("testing") is None:
        try:
            CacheItem(name="pair_volumes_24hr").save()
        except Exception as e:
            return default.result(msg=e, loglevel="warning")
        msg = "pair_volumes_24hr refresh loop complete!"
        return default.result(msg=msg, loglevel="loop", ignore_until=5)


# VOLUMES CACHE
@router.on_event("startup")
@repeat_every(seconds=60)
@timed
def get_pair_volumes_14d():
    if memcache.get("testing") is None:
        try:
            CacheItem(name="pair_volumes_14d").save()
        except Exception as e:
            return default.result(msg=e, loglevel="warning")
        msg = "pair_volumes_14d refresh loop complete!"
        return default.result(msg=msg, loglevel="loop", ignore_until=5)


@router.on_event("startup")
@repeat_every(seconds=60)
@timed
def get_coin_volumes_24hr():
    if memcache.get("testing") is None:
        try:
            CacheItem(name="coin_volumes_24hr").save()
        except Exception as e:
            return default.result(msg=e, loglevel="warning")
        msg = "coin_volumes_24hr refresh loop complete!"
        return default.result(msg=msg, loglevel="loop", ignore_until=5)


# LAST TRADE
@router.on_event("startup")
@repeat_every(seconds=60)
@timed
def pair_last_traded():
    if memcache.get("testing") is None:
        try:
            CacheItem(name="pair_last_traded").save()
        except Exception as e:
            return default.result(msg=e, loglevel="warning")
        msg = "pair_last_traded loop complete!"
        return default.result(msg=msg, loglevel="loop", ignore_until=5)


@router.on_event("startup")
@repeat_every(seconds=60)
@timed
def pair_last_traded_24hr():
    if memcache.get("testing") is None:
        try:
            CacheItem(name="pair_last_traded_24hr").save()
        except Exception as e:
            return default.result(msg=e, loglevel="warning")
        msg = "pair_last_traded_24hr loop complete!"
        return default.result(msg=msg, loglevel="loop", ignore_until=5)


# TICKERS
@router.on_event("startup")
@repeat_every(seconds=90)
@timed
def pair_tickers():
    if memcache.get("testing") is None:
        try:
            CacheCalc().tickers(refresh=True)
        except Exception as e:
            return default.result(msg=e, loglevel="warning")
        msg = "pair_last_traded loop complete!"
        return default.result(msg=msg, loglevel="loop", ignore_until=5)


# MARKETS CACHE
@router.on_event("startup")
@repeat_every(seconds=90)
@timed
def get_markets_summary():
    if memcache.get("testing") is None:
        try:
            CacheItem(name="markets_summary").save()
        except Exception as e:
            return default.result(msg=e, loglevel="warning")
        msg = "markets_summary loop for memcache complete!"
        return default.result(msg=msg, loglevel="loop", ignore_until=5)


@router.on_event("startup")
@repeat_every(seconds=60)
@timed
def prices_service():  # pragma: no cover
    if memcache.get("testing") is None:
        try:
            for i in ["prices_tickers_v1", "prices_tickers_v2"]:
                CacheItem(i).save()
        except Exception as e:
            return default.result(msg=e, loglevel="warning")
        msg = "Prices update loop complete!"
        return default.result(msg=msg, loglevel="loop", ignore_until=5)


# EXTERNAL SOURCES CACHE
@router.on_event("startup")
@repeat_every(seconds=14400)
@timed
def coins():  # pragma: no cover
    if memcache.get("testing") is None:
        try:
            for i in ["coins_config", "coins"]:
                CacheItem(i).save()
        except Exception as e:
            return default.result(msg=e, loglevel="warning")
        return default.result(
            msg="Coins update loop complete!", loglevel="loop", ignore_until=5
        )


@router.on_event("startup")
@repeat_every(seconds=450)
@timed
def gecko_data():  # pragma: no cover
    if memcache.get("testing") is None:
        try:
            CacheItem("gecko_source").save()
        except Exception as e:
            return default.result(msg=e, loglevel="warning")
        msg = "Gecko source data update loop complete!"
        return default.result(msg=msg, loglevel="loop", ignore_until=5)


@router.on_event("startup")
@repeat_every(seconds=60)
@timed
def gecko_pairs():  # pragma: no cover
    if memcache.get("testing") is None:
        try:
            CacheItem("gecko_pairs").save()
        except Exception as e:
            return default.result(msg=e, loglevel="warning")
        msg = "Gecko pairs data update loop complete!"
        return default.result(msg=msg, loglevel="loop", ignore_until=5)


@router.on_event("startup")
@repeat_every(seconds=90)
@timed
def stats_api_summary():  # pragma: no cover
    if memcache.get("testing") is None:
        try:
            CacheItem("stats_api_summary").save()
        except Exception as e:
            return default.result(msg=e, loglevel="warning")
        msg = "stats_api_summary data update loop complete!"
        return default.result(msg=msg, loglevel="loop", ignore_until=5)


@router.on_event("startup")
@repeat_every(seconds=300)
@timed
def fixer_rates():  # pragma: no cover
    if memcache.get("testing") is None:
        try:
            CacheItem(name="fixer_rates").save()
        except Exception as e:
            return default.result(msg=e, loglevel="warning")
        msg = "Fixer rates update loop complete!"
        return default.result(msg=msg, loglevel="loop", ignore_until=5)


# DATABASE SYNC
@router.on_event("startup")
@repeat_every(seconds=300)
@timed
def populate_pgsqldb_loop():
    if memcache.get("testing") is None:
        # updates last 24 hours swaps
        db.SqlSource().populate_pgsqldb()


@router.on_event("startup")
@repeat_every(seconds=180)
@timed
def import_dbs():
    if memcache.get("testing") is None:
        if NODE_TYPE != "serve":
            try:
                merge = old_db_merge.SqliteMerge()
                merge.import_source_databases()
            except Exception as e:
                return default.result(msg=e, loglevel="warning")
            msg = "Import source databases loop complete!"
            return default.result(msg=msg, loglevel="merge")
        msg = "Import source databases skipped, NodeType is 'serve'!"
        msg += " Masters will be updated from cron."
        return default.result(msg=msg, loglevel="merge")


@router.on_event("startup")
@repeat_every(seconds=120)
@timed
def adex_fortnite():
    if memcache.get("testing") is None:
        try:
            pass
            CacheItem(name="adex_fortnite").save()
        except Exception as e:
            logger.warning(default.result(msg=e, loglevel="warning"))
        msg = "Adex fortnight loop complete!"
        return default.result(msg=msg, loglevel="loop", ignore_until=5)


@router.on_event("startup")
@repeat_every(seconds=120)
@timed
def adex_24hr():
    if memcache.get("testing") is None:
        try:
            pass
            CacheItem(name="adex_24hr").save()
        except Exception as e:
            logger.warning(default.result(msg=e, loglevel="warning"))
        msg = "Adex 24hr loop complete!"
        return default.result(msg=msg, loglevel="loop", ignore_until=5)


# REVIEW
@router.on_event("startup")
@repeat_every(seconds=120)
@timed
def refresh_tickers():
    if memcache.get("testing") is None:
        try:
            CacheItem(name="tickers").save()
        except Exception as e:
            return default.result(msg=e, loglevel="warning")
        msg = "Tickers refresh loop complete!"
        return default.result(msg=msg, loglevel="loop", ignore_until=5)


"""

@router.on_event("startup")
@repeat_every(seconds=300)
@timed
def refresh_generic_tickers_14d():
    if memcache.get("testing") is None:
        try:
            CacheItem(name="generic_tickers_14d").save()
        except Exception as e:
            return default.result(msg=e, loglevel="warning")
        msg = "Generic tickers 14d refresh loop complete!"
        return default.result(msg=msg, loglevel="loop")


@router.on_event("startup")
@repeat_every(seconds=60)
@timed
def get_generic_tickers_cache_14d():
    if memcache.get("testing") is None:
        try:
            CacheItem(
                name="generic_tickers_14d",
                from_memcache=True
            ).save()
        except Exception as e:
            return default.result(msg=e, loglevel="warning")
        msg = "Generic tickers 14d loop for memcache complete!"
        return default.result(msg=msg, loglevel="loop")


@router.on_event("startup")
@repeat_every(seconds=120)
@timed
def generic_summary():
    if memcache.get("testing") is None:
        try:
            pass
            # CacheItem(name="generic_summary").save()
        except Exception as e:
            return default.result(msg=e, loglevel="warning")
        msg = "Summary loop complete!"
        return default.result(msg=msg, loglevel="loop")
"""
