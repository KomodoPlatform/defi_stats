#!/usr/bin/env python3
import time
from fastapi import APIRouter
from fastapi_utils.tasks import repeat_every
from logger import logger
from cache import Cache
from db import update_master_sqlite_dbs
from enums import NetId


router = APIRouter()
cache = Cache()


@router.on_event("startup")
@repeat_every(seconds=86400)
def update_coins():  # pragma: no cover
    try:
        cache.save.save_coins()
        cache.save.save_coins_config()
        return {"result": "Updated coins"}
    except IOError as e:
        err = f"Error in [update_coins]: {e}"
        logger.warning(err)
        return {"error": err}


@router.on_event("startup")
@repeat_every(seconds=120)
def cache_gecko_data():  # pragma: no cover
    try:
        cache.save.save_gecko_source()
    except IOError as e:
        logger.warning(f"{type(e)} Error in [cache_gecko_data]: {e}")


@router.on_event("startup")
@repeat_every(seconds=60)
def cache_prices_service():  # pragma: no cover
    try:
        cache.save.save_prices_tickers_v1()
    except IOError as e:
        logger.warning(f"{type(e)} Error in [cache_prices_service]: {e}")
    try:
        cache.save.save_prices_tickers_v2()
    except IOError as e:
        logger.warning(f"{type(e)} Error in [cache_prices_service]: {e}")


@router.on_event("startup")
@repeat_every(seconds=600)
def cache_fixer_rates():  # pragma: no cover
    try:
        cache.save.save_fixer_rates_source()
    except IOError as e:
        logger.warning(f"{type(e)} Error in [cache_fixer_rates]: {e}")


@router.on_event("startup")
@repeat_every(seconds=300)
def update_dbs():
    started = int(time.time())
    logger.info("Updating DBs...")
    try:
        update_master_sqlite_dbs()
    except Exception as e:
        logger.warning(f"{type(e)} Error in [update_master_sqlite_dbs]: {e}")

    for netid in NetId:
        try:
            cache.save.save_gecko_pairs(netid=netid.value)
        except Exception as e:
            logger.warning(f"{type(e)} Error in [save_gecko_pairs]: {e}")

        try:
            cache.save.save_gecko_tickers(netid=netid.value)
        except Exception as e:
            logger.warning(f"{type(e)} Error in [save_gecko_tickers]: {e}")
    logger.debug(f"Time to update_dbs: {int(time.time())-started}")


@router.on_event("startup")
@repeat_every(seconds=300)
def update_markets_8762():
    started = int(time.time())
    logger.info(f"Caching markets for netid {8762}...")

    try:
        cache.save.save_markets_last_trade(netid=8762)
    except Exception as e:
        logger.warning(f"{type(e)} Error in [save_markets_last_trade]: {e}")

    try:
        cache.save.save_markets_pairs(netid=8762)
    except Exception as e:
        logger.warning(f"{type(e)} Error in [save_markets_pairs]: {e}")

    try:
        cache.save.save_markets_tickers(netid=8762)
    except Exception as e:
        logger.warning(f"{type(e)} Error in [save_markets_tickers]: {e}")
    logger.debug(f"Time to process netid 8762: {int(time.time())-started}")


@router.on_event("startup")
@repeat_every(seconds=300)
def update_markets_7777():
    started = time.time()
    logger.info(f"Caching markets for netid {7777}...")

    try:
        cache.save.save_markets_last_trade(netid=7777)
    except Exception as e:
        logger.warning(f"{type(e)} Error in [save_markets_last_trade]: {e}")

    try:
        cache.save.save_markets_pairs(netid=7777)
    except Exception as e:
        logger.warning(f"{type(e)} Error in [save_markets_pairs]: {e}")

    try:
        cache.save.save_markets_tickers(netid=7777)
    except Exception as e:
        logger.warning(f"{type(e)} Error in [save_markets_tickers]: {e}")
    logger.debug(f"Time to process netid 7777: {int(time.time())-started}")


@router.on_event("startup")
@repeat_every(seconds=300)
def update_markets_all():
    started = time.time()
    logger.info("Caching markets for netid all...")

    try:
        cache.save.save_markets_last_trade(netid="all")
    except Exception as e:
        logger.warning(f"{type(e)} Error in [save_markets_last_trade]: {e}")

    try:
        cache.save.save_markets_pairs(netid="all")
    except Exception as e:
        logger.warning(f"{type(e)} Error in [save_markets_pairs]: {e}")

    try:
        cache.save.save_markets_tickers(netid="all")
    except Exception as e:
        logger.warning(f"{type(e)} Error in [save_markets_tickers]: {e}")
    logger.debug(f"Time to process netid {'all'}: {int(time.time())-started}")
