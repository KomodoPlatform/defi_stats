#!/usr/bin/env python3
from fastapi import APIRouter, Response
from fastapi_utils.tasks import repeat_every
from fastapi.responses import JSONResponse
from decimal import Decimal
from typing import List
from logger import logger
from models import (
    UsdVolume,
    CurrentLiquidity,
    GeckoOrderbookItem,
    GeckoHistoricalTradesItem,
    ErrorMessage,
)
from helper import get_mm2_rpc_port
from cache import Cache
from pair import Pair
from orderbook import Orderbook
from enums import TradeType, NetId
from external import CoinGeckoAPI
from transform import gecko_ticker_to_market_ticker

router = APIRouter()
cache = Cache()


# Migrated from https://stats.testchain.xyz/api/v1/usd_volume_24h
@router.get(
    '/usd_volume_24h',
    response_model=UsdVolume,
    description="24-hour price & volume for each market pair traded in last 7 days."
)
def usd_volume_24h(netid: NetId = NetId.NETID_7777):
    try:
        data = cache.load.load_gecko_tickers(netid=netid.value)
        return {"usd_volume_24h": data['combined_volume_usd']}
    except Exception as e:  # pragma: no cover
        logger.warning(f"{type(e)} Error in [/api/v3/markets/usd_volume_24h]: {e}")
        return {"error": f"{type(e)} Error in [/api/v3/markets/usd_volume_24h]: {e}"}

# New endpoint
@router.get(
    '/current_liquidity',
    response_model=CurrentLiquidity,
    description="24-hour price & volume for each market pair traded in last 7 days."
)
def current_liquidity(netid: NetId = NetId.NETID_7777):
    try:
        data = cache.load.load_gecko_tickers(netid=netid.value)
        return {"current_liquidity": data['combined_liquidity_usd']}
    
    except Exception as e:  # pragma: no cover
        logger.warning(f"{type(e)} Error in [/api/v3/markets/current_liquidity]: {e}")
        return {"error": f"{type(e)} Error in [/api/v3/markets/current_liquidity]: {e}"}

# Migrated from https://stats.testchain.xyz/api/v1/summary
@router.get(
    '/summary',
    description="24-hour price & volume for each market pair traded in last 7 days."
)
def summary(netid: NetId = NetId.NETID_7777):
    try:
        data = cache.load.load_gecko_tickers(netid=netid.value)
        resp = []
        for i in data['data']:
            resp.append(gecko_ticker_to_market_ticker(i))
        return resp            
    except Exception as e:  # pragma: no cover
        logger.warning(f"{type(e)} Error in [/api/v3/gecko/tickers]: {e}")
        return {"error": f"{type(e)} Error in [/api/v3/gecko/tickers]: {e}"}

# Migrated from https://stats.testchain.xyz/api/v1/summary_for_ticker/KMD
@router.get(
    '/summary_for_ticker/{ticker}',
    description="24-hour price & volume for each market pair involving a ticker, traded in last 7 days."
)
def summary_for_ticker(ticker: str, netid: NetId = NetId.NETID_7777):
    try:
        data = cache.load.load_gecko_tickers(netid=netid.value)
        resp = []
        for i in data['data']:
            if ticker in [i['base_currency'], i['target_currency']]:
                resp.append(gecko_ticker_to_market_ticker(i))
        return resp            
    except Exception as e:  # pragma: no cover
        logger.warning(f"{type(e)} Error in [/api/v3/gecko/tickers]: {e}")
        return {"error": f"{type(e)} Error in [/api/v3/gecko/tickers]: {e}"}


@router.get('/api/v1/ticker')
def ticker():
    available_pairs_ticker = get_availiable_pairs(path_to_db)
    ticker_data = []
    for pair in available_pairs_ticker:
        ticker_data.append(ticker_for_pair(pair, path_to_db, 1))
    return ticker_data


@router.get('/api/v1/ticker_for_ticker/{ticker_ticker}')
def ticker(ticker_ticker="KMD"):
    return ticker_for_ticker(ticker_ticker, path_to_db, 1)


@router.get('/api/v1/swaps24/{ticker}')
def ticker(ticker="KMD"):
    return swaps24h_for_ticker(ticker, path_to_db, 1)


@router.get('/api/v1/orderbook/{market_pair}')
def orderbook(market_pair="KMD_BTC"):
    orderbook_data = orderbook_for_pair(market_pair)
    return orderbook_data


@router.get('/api/v1/trades/{market_pair}/{days_in_past}')
def trades(market_pair="KMD_BTC", days_in_past=1):
    trades_data = trades_for_pair(market_pair, path_to_db, int(days_in_past))
    return trades_data


@router.get('/api/v1/atomicdexio')
def atomicdex_info_api():
    data = atomicdex_info(path_to_db)
    return data


@router.get('/api/v1/fiat_rates')
def fiat_rates():
    with open('gecko_cache.json', 'r') as json_file:
        gecko_cached_data = json.load(json_file)
    return gecko_cached_data


# TODO: get volumes for x days for ticker
@router.get("/api/v1/volumes_ticker/{ticker_vol}/{days_in_past}")
def volumes_history_ticker(ticker_vol="KMD", days_in_past=1):
    return volume_for_ticker(ticker_vol, path_to_db, int(days_in_past))


@router.get('/api/v1/tickers_summary')
def tickers_summary():
    return summary_ticker(path_to_db)
