#!/usr/bin/env python3
from fastapi import APIRouter
from fastapi_utils.tasks import repeat_every
from pydantic import BaseModel
from typing import List
from logger import logger
import models
import const
import time
from helper import validate_ticker

router = APIRouter()
cache = models.Cache()
endpoints = models.Endpoints()

# Gecko caching functions


@router.on_event("startup")
@repeat_every(seconds=60)
def cache_gecko_data():  # pragma: no cover
    try:
        cache.save.gecko_source()
    except Exception as e:
        logger.warning(f"{type(e)} Error in [cache_gecko_data]: {e}")


@router.on_event("startup")
@repeat_every(seconds=180)
def cache_gecko_tickers():  # pragma: no cover
    try:
        cache.save.gecko_tickers()
    except Exception as e:
        logger.warning(f"{type(e)} Error in [cache_ticker_data]: {e}")


# Gecko Response Models


class PairsItem(BaseModel):
    ticker_id: str = "KMD_DGB"
    pool_id: str = "KMD_DGB"
    base: str = "KMD"
    target: str = "DGB"


class TickersItem(BaseModel):
    ticker_id: str = "KMD_LTC"
    base_currency: str = "KMD"
    target_currency: str = "LTC"
    last_price: str = "0.0034393809"
    last_trade: str = "1697525818"
    trades_24hr: str = "52"
    base_volume: str = "845.2425635969204"
    target_volume: str = "2.9121541628874037"
    bid: str = "0.00337853"
    ask: str = "0.00344367"
    high: str = "0.0034736790"
    low: str = "0.0034116503"
    volume_usd_24hr: str = "649642.1516335415"


class TickersSummary(BaseModel):
    last_update: int = 1697383557
    pairs_count: int = 173
    swaps_count: int = 120
    combined_liquidity_usd: str = "212249.7466552301"
    combined_volume_usd: str = "2901.4732791150"
    data: List[TickersItem]


class OrderbookItem(BaseModel):
    ticker_id: str = "KMD_DGB"
    timestamp: str = "1700050000"
    bids: List[List] = [["0.00337853", "11.00000000"]]
    asks: List[List] = [["0.00344367", "53.00000000"]]
    total_asks_base_vol: str = "55408.7479581332120"
    total_bids_rel_vol: str = "570.3474962785280"


class BuyItem(BaseModel):
    trade_id: str = "724a6418-c3a3-4510-8fd8-edf73933d219"
    price: str = "0.0034619483"
    base_volume: str = "200"
    target_volume: str = "0.69238966"
    timestamp: str = "1700050000"
    type: str = "buy"


class SellItem(BaseModel):
    trade_id: str = "990f3e78-e436-4405-808f-44832e97c65c"
    price: str = "50.1"
    base_volume: str = "195.9088880675"
    target_volume: str = "0.67380529"
    timestamp: str = "1697377776"
    type: str = "sell"


class HistoricalTradesItem(BaseModel):
    ticker_id: str = "KMD_LTC"
    start_time: str = "1697298233"
    end_time: str = "1697384633"
    limit: str = "100"
    trades_count: str = "5"
    sum_base_volume: str = "845.2425635969204"
    sum_target_volume: str = "2.912154162887404"
    average_price: str = "0.00344162344"
    buy: List[BuyItem]
    sell: List[SellItem]


# Gecko Format Endpoints
@router.get(
    '/pairs',
    response_model=List[PairsItem],
    description="Returns a list of all pairs traded within the last 'x' days."
)
async def gecko_pairs():
    try:
        return endpoints.gecko_pairs()
    except Exception as e:  # pragma: no cover
        logger.warning(f"{type(e)} Error in [/api/v3/gecko/pairs]: {e}")
        return {"error": f"{type(e)} Error in [/api/v3/gecko/pairs]: {e}"}


@router.get(
    '/tickers',
    response_model=TickersSummary,
    description="24-hour price & volume for each market pair traded in last 7 days."
)
def gecko_tickers():
    try:
        return cache.load.gecko_tickers()
    except Exception as e:  # pragma: no cover
        logger.warning(f"{type(e)} Error in [/api/v3/gecko/tickers]: {e}")
        return {"error": f"{type(e)} Error in [/api/v3/gecko/tickers]: {e}"}


@router.get(
    '/orderbook/{ticker_id}',
    description="Provides current order book information for the given market pair.",
    response_model=OrderbookItem
)
def gecko_orderbook(ticker_id: str = "KMD_LTC", depth: int = 100):
    try:
        return models.Orderbook(models.Pair(ticker_id)).for_pair(True)
    except Exception as e:  # pragma: no cover
        err = f"{type(e)} Error in /api/v3/gecko/orderbook [{ticker_id}] [depth: {depth}]]: {e}"
        logger.warning(err)
        return err


@router.get(
    '/historical_trades/{ticker_id}',
    description="Used to return data on historical completed trades for a given market pair.",
    response_model=HistoricalTradesItem
)
def gecko_historical_trades(
        trade_type: const.TradeType = 'all',
        ticker_id: str = "KMD_LTC",
        limit: int = 100,
        start_time: int = int(time.time()) - 86400,
        end_time: int = int(time.time())
):
    try:
        validate_ticker(ticker_id)
        pair = models.Pair(ticker_id)
        return pair.historical_trades(
            trade_type=trade_type,
            limit=limit,
            start_time=start_time,
            end_time=end_time
        )
    except Exception as e:  # pragma: no cover
        logger.warning(
            f"{type(e)} Error in /api/v3/gecko/historical_trades [{ticker_id}]: {e}")
        return {"error": f"{type(e)} Error in /api/v3/gecko/historical_trades [{ticker_id}]: {e}"}
