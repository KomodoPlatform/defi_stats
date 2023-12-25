#!/usr/bin/env python3
from fastapi import APIRouter
from fastapi.responses import JSONResponse
from decimal import Decimal
from datetime import datetime, timedelta
from util.logger import logger
from lib.models import (
    UsdVolume,
    CurrentLiquidity,
    Swaps24,
    PairTrades,
    AdexIo,
)

from util.helper import get_mm2_rpc_port, sort_dict_list
from lib.cache import Cache
from lib.pair import Pair
from util.utils import Utils
from lib.orderbook import Orderbook
from util.enums import TradeType, NetId
from util.transform import (
    gecko_ticker_to_market_ticker,
    gecko_ticker_to_market_ticker_summary,
)
from validate import validate_ticker_id
from const import MM2_DB_PATHS, MM2_RPC_PORTS
from db.sqlitedb import get_sqlite_db

router = APIRouter()
cache = Cache()

# Migrated from https://stats-api.atomicdex.io/docs
# https://github.com/KomodoPlatform/dexstats_sqlite_py/tree/stats-api-atomicdex-io



# //////////////////////////// #
# Routes retrieved from lib.cache  #
# //////////////////////////// #
@app.get('/api/v1/atomicdexio')
def atomicdexio():
    '''Simple Summary Statistics for last 24 hours'''
    try:
        return cache.atomicdexio()
    except Exception as e:  # pragma: no cover
        logger.warning(f"{type(e)} Error in [/api/v1/atomicdexio]: {e}")
        return {"error": f"{type(e)} Error in [/api/v1/atomicdexio]: {e}"}


@app.get('/api/v1/atomicdex_fortnight')
def atomicdex_fortnight():
    '''Extra Summary Statistics over last 2 weeks'''
    try:
        return cache.atomicdex_fortnight()
    except Exception as e:  # pragma: no cover
        logger.warning(f"{type(e)} Error in [/api/v1/atomicdex_fortnight]: {e}")
        return {"error": f"{type(e)} Error in [/api/v1/atomicdex_fortnight]: {e}"}


@app.get('/api/v1/summary')
def summary():
    '''
    Trade summary for the last 24 hours for all
    pairs traded in the last 7 days.
    '''
    try:
        return cache.summary()
    except Exception as e:  # pragma: no cover
        logger.warning(f"{type(e)} Error in [/api/v1/summary]: {e}")
        return {"error": f"{type(e)} Error in [/api/v1/atomicdexio]: {e}"}


@app.get('/api/v1/ticker')
def ticker():
    '''
    Orderbook summary for the last 24 hours
    for all pairs traded in the last 7 days.
    '''
    try:
        return cache.ticker()
    except Exception as e:  # pragma: no cover
        logger.warning(f"{type(e)} Error in [/api/v1/ticker]: {e}")
        return {"error": f"{type(e)} Error in [/api/v1/atomicdexio]: {e}"}


@app.get('/api/v2/ticker')
def ticker_v2():
    '''
    Orderbook summary for the last 24 hours
    for all pairs traded in the last 7 days.
    '''
    try:
        data = cache.ticker()
        cleaned = {}
        [cleaned.update(i) for i in data]
        return cleaned
    except Exception as e:  # pragma: no cover
        logger.warning(f"{type(e)} Error in [/api/v1/ticker]: {e}")
        return {"error": f"{type(e)} Error in [/api/v1/atomicdexio]: {e}"}


# ////////////////////////// #
# Routes retrieved from mm2  #
# ////////////////////////// #
@app.get('/api/v1/orderbook/{pair}')
def orderbook(pair: str = "KMD_LTC") -> dict:
    '''
    Live Orderbook for this pair
    Parameters:
        pair: str (e.g. KMD_LTC)
    '''
    try:
        if len(pair) > 32:
            raise HTTPException(
                status_code=400,
                detail="Pair cant be longer than 32 symbols"
            )  # pragma: no cover
        elif "_" not in pair:
            raise HTTPException(
                status_code=400,
                detail="Pair should be in format TICKER1_TICKER2"
            )  # pragma: no cover
        elif pair == "":
            raise HTTPException(
                status_code=400,
                detail="Pair can not be empty. Use the format TICKER1_TICKER2"
            )  # pragma: no cover
        pair = models.Pair(pair)
        orderbook = models.Orderbook(pair)
        return orderbook.for_pair(True)
    except Exception as e:  # pragma: no cover
        logger.warning(f"{type(e)} Error in [/api/v1/orderbook/{pair}]: {e}")
        return {"error": f"{type(e)} Error in [/api/v1/atomicdexio]: {e}"}


@app.get('/api/v1/trades/{pair}')
def trades(pair="KMD_LTC"):
    '''
    Swaps for this pair in the last 24 hours.
    Parameters:
        pair: str (e.g. KMD_LTC)
    '''
    try:
        if len(pair) > 32:
            raise HTTPException(
                status_code=400,
                detail="Pair cant be longer than 32 symbols"
            )  # pragma: no cover
        elif "_" not in pair:
            raise HTTPException(
                status_code=400,
                detail="Pair should be in format TICKER1_TICKER2"
            )  # pragma: no cover
        elif pair == "":
            raise HTTPException(
                status_code=400,
                detail="Pair can not be empty. Use the format TICKER1_TICKER2"
            )  # pragma: no cover
        DB = models.SqliteDB(const.MM2_DB_PATH, dict_format=True)
        pair = models.Pair(pair)
        trades_data = pair.trades(days=1)
        DB.close()
        return trades_data
    except Exception as e:  # pragma: no cover
        logger.warning(f"{type(e)} Error in [/api/v1/trades/{pair}]: {e}")
        return {"error": f"{type(e)} Error in [/api/v1/atomicdexio]: {e}"}


@app.get('/api/v1/last_price/{pair}')
def last_price_for_pair(pair="KMD_LTC"):
    '''Last trade price for a given pair.'''
    try:
        pair = models.Pair(pair)
        DB = models.SqliteDB(const.MM2_DB_PATH, dict_format=True)
        last_price = DB.get_last_price_for_pair(pair.base, pair.quote)
        DB.close()
        return last_price
    except Exception as e:  # pragma: no cover
        logger.warning(f"{type(e)} Error in [/api/v1/last_price/{pair}]: {e}")
        return {"error": f"{type(e)} Error in [/api/v1/atomicdexio]: {e}"}
