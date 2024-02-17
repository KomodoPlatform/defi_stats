#!/usr/bin/env python3
from fastapi import APIRouter
from fastapi.responses import JSONResponse
from decimal import Decimal
from datetime import datetime, timedelta
from util.cron import cron
from typing import List, Dict
from const import MARKETS_PAIRS_DAYS
from models.generic import ErrorMessage
from util.exceptions import BadPairFormatError
from models.markets import (
    MarketsUsdVolume,
    MarketsFiatRatesItem,
    MarketsAtomicdexIo,
    MarketsOrderbookItem,
    MarketsSwaps24,
    PairTrades,
    MarketsSummaryItem,
    MarketsSummaryForTicker,
    MarketsTickerItem,
)
from lib.cache_calc import CacheCalc
from lib.pair import Pair
from lib.markets import Markets
from routes.metadata import markets_desc
from util.enums import TradeType
from util.logger import logger
from util.transform import (
    deplatform,
    derive,
    invert,
    sortdata
)
import util.memcache as memcache
from util.transform import template
import util.validate as validate
import db.sqldb as db

router = APIRouter()


@router.get(
    "/atomicdexio",
    description=markets_desc.adexio,
    response_model=MarketsAtomicdexIo,
    responses={406: {"model": ErrorMessage}},
    status_code=200,
)
def atomicdex_info_api():
    query = db.SqlQuery()
    return query.swap_counts()


@router.get(
    "/fiat_rates",
    description="Coin prices in USD (where available)",
    response_model=Dict[str, MarketsFiatRatesItem],
    responses={406: {"model": ErrorMessage}},
    status_code=200,
)
def fiat_rates():
    return memcache.get_gecko_source()


@router.get(
    "/orderbook/{pair_str}",
    description="Get Orderbook for a market pair in `KMD_LTC` format.",
    response_model=MarketsOrderbookItem,
    responses={406: {"model": ErrorMessage}},
    status_code=200,
)
def orderbook(pair_str: str = "KMD_LTC", depth: int = 100):
    try:
        depair = deplatform.pair(pair_str)
        is_reversed = pair_str != sortdata.pair_by_market_cap(pair_str)
        if is_reversed:
            cache_name = f"orderbook_{invert.pair(depair)}_ALL"
        else:
            cache_name = f"orderbook_{depair}_ALL"

        data = memcache.get(cache_name)
        if data is None:
            pair = Pair(pair_str=pair_str)
            data = pair.orderbook(pair_str=pair_str, depth=depth)
        if pair_str in data:
            data = data[pair_str]
        else:
            data = data["ALL"]
        if Decimal(data["liquidity_usd"]) > 0:
            if is_reversed:
                logger.calc("Returning inverted orderbook_extended cache")
                data = invert.pair_orderbook(data)
            data["volume_usd_24hr"] = data["trade_volume_usd"]
        return data
    except Exception as e:  # pragma: no cover
        err = {"error": f"{e}"}
        logger.warning(err)
        return JSONResponse(status_code=400, content=err)


# Migrated from https://stats.testchain.xyz/api/v1/summary
@router.get(
    "/summary",
    description=f"24-hour price & volume for each pair traded in last {MARKETS_PAIRS_DAYS} days.",
    response_model=List[MarketsSummaryItem],
    responses={406: {"model": ErrorMessage}},
    status_code=200,
)
def summary():
    try:
        data = memcache.get_markets_summary()
        # TODO: remove this when dashboard updates
        for i in data:
            i["trading_pair"] = i["pair"]
            i["price_change_percent_24hr"] = i["price_change_pct_24hr"]
        return data
    except Exception as e:  # pragma: no cover
        logger.warning(f"{type(e)} Error in [/api/v3/market/summary]: {e}")
        return {"error": f"{type(e)} Error in [/api/v3/market/summary]: {e}"}


# Migrated from https://stats.testchain.xyz/api/v1/summary_for_ticker/KMD
@router.get(
    "/summary_for_ticker/{coin}",
    description="24h price & volume for market pairs with a specific coin traded in last 7 days",
    response_model=MarketsSummaryForTicker,
    responses={406: {"model": ErrorMessage}},
    status_code=200,
)
def summary_for_ticker(coin: str = "KMD"):
    # TODO: Segwit not merged in this endpoint yet
    try:
        logger.calc(coin)
        if "_" in coin:
            return {"error": f"Coin value '{coin}' looks like a pair."}
        summary = memcache.get_markets_summary()
        data = []
        swaps_count = 0
        liquidity = 0
        volume = 0
        for i in summary:
            if coin in [i["base_currency"], i["quote_currency"]]:
                if i["last_swap"] > 0:

                    swaps_count += int(i["trades_24hr"])
                    liquidity += Decimal(i["liquidity_usd"])
                    volume += Decimal(i["volume_usd_24hr"])
                    i["last_trade"] = i["last_swap"]
                    i["price_change_percent_24hr"] = i["price_change_pct_24hr"]
                    i["quote_usd_price"] = i["quote_price_usd"]
                    i["base_usd_price"] = i["base_price_usd"]
                    i["base"] = i["base_currency"]
                    i["quote"] = i["quote_currency"]
                    data.append(i)

        resp = {
            "last_update": int(cron.now_utc()),
            "pairs_count": len(data),
            "swaps_count": int(swaps_count),
            "liquidity_usd": Decimal(liquidity),
            "volume_usd_24hr": Decimal(volume),
            "data": data,
        }
        return resp
    except Exception as e:  # pragma: no cover
        logger.warning(f"{type(e)} Error in [/api/v3/market/summary_for_ticker]: {e}")
        return {"error": f"{type(e)} Error in [/api/v3/market/summary_for_ticker]: {e}"}


@router.get(
    "/swaps24/{coin}",
    description=markets_desc.swaps24,
    response_model=MarketsSwaps24,
    responses={406: {"model": ErrorMessage}},
    status_code=200,
)
def swaps24(coin: str = "KMD") -> dict:
    try:
        data = memcache.get_coin_volumes_24hr()
        trades = 0
        volume = 0
        volume_usd = 0
        decoin = deplatform.coin(coin)
        if decoin in data["volumes"]:
            if coin.replace("-segwit", "") == decoin:
                if decoin in data["volumes"][decoin]:
                    trades += int(data["volumes"][decoin][decoin]["total_swaps"])
                    volume += Decimal(data["volumes"][decoin][decoin]["total_volume"])
                    volume_usd += Decimal(
                        data["volumes"][decoin][decoin]["trade_volume_usd"]
                    )
                if f"{decoin}-segwit" in data["volumes"][decoin]:
                    trades += int(
                        data["volumes"][decoin][f"{decoin}-segwit"]["total_swaps"]
                    )
                    volume += Decimal(
                        data["volumes"][decoin][f"{decoin}-segwit"]["total_volume"]
                    )
                    volume_usd += Decimal(
                        data["volumes"][decoin][f"{decoin}-segwit"]["trade_volume_usd"]
                    )
            elif coin in data["volumes"][decoin]:
                trades += int(data["volumes"][decoin][coin]["total_swaps"])
                volume += Decimal(data["volumes"][decoin][coin]["total_volume"])
                volume_usd += Decimal(data["volumes"][decoin][coin]["trade_volume_usd"])
        return {
            "ticker": coin,
            "volume": volume,
            "volume_usd": volume_usd,
            "swaps_amount_24hr": trades,
        }
    except Exception as e:  # pragma: no cover
        logger.warning(f"{type(e)} Error in [/api/v3/market/swaps24]: {e}")
        return {"error": f"{type(e)} Error in [/api/v3/market/swaps24]: {e}"}


@router.get(
    "/ticker",
    description="Simple last price and liquidity for each market pair, traded in last 7 days.",
    response_model=List[Dict[str, MarketsTickerItem]],
    responses={406: {"model": ErrorMessage}},
    status_code=200,
)
def ticker():
    try:
        c = CacheCalc()
        return c.tickers_lite()
    except Exception as e:  # pragma: no cover
        logger.warning(f"{type(e)} Error in [/api/v3/market/ticker]: {e}")
        return {"error": f"{type(e)} Error in [/api/v3/market/ticker]: {e}"}


@router.get(
    "/ticker_for_ticker",
    description="Simple last price and liquidity for each market pair for a specific coin , e.g. `KMD`, `KMD-BEP20`.",
)
def ticker_for_ticker(ticker):
    try:
        c = CacheCalc()
        return c.tickers_lite(coin=ticker)
    except Exception as e:  # pragma: no cover
        logger.warning(f"{type(e)} Error in [/api/v3/market/ticker_for_ticker]: {e}")
        return {"error": f"{type(e)} Error in [/api/v3/market/ticker_for_ticker]: {e}"}


@router.get(
    "/tickers_summary",
    description="Total swaps and volume involving for each active ticker in the last 24hrs.",
)
def tickers_summary():
    try:
        data = memcache.get_coin_volumes_24hr()
        resp = {}
        for depair in data["volumes"]:
            for variant in data["volumes"][depair]:
                if variant != "ALL":
                    v = variant.replace("-segwit", "")
                    if v not in resp:
                        resp.update(
                            {
                                v: {
                                    "trades_24hr": data["volumes"][depair][variant][
                                        "total_swaps"
                                    ],
                                    "volume_24hr": data["volumes"][depair][variant][
                                        "total_volume"
                                    ],
                                    "volume_usd_24hr": data["volumes"][depair][variant][
                                        "trade_volume_usd"
                                    ],
                                }
                            }
                        )
                    else:
                        resp[v]["trades_24hr"] += data["volumes"][depair][variant][
                            "total_swaps"
                        ]
                        resp[v]["volume_24hr"] += data["volumes"][depair][variant][
                            "total_volume"
                        ]
                        resp[v]["volume_usd_24hr"] += data["volumes"][depair][variant][
                            "trade_volume_usd"
                        ]
        return resp
    except Exception as e:  # pragma: no cover
        logger.warning(f"{type(e)} Error in [/api/v3/market/swaps24]: {e}")
        return {"error": f"{type(e)} Error in [/api/v3/market/swaps24]: {e}"}


@router.get(
    "/trades/{pair_str}/{days_in_past}",
    response_model=List[PairTrades],
    description="Trades for the last 'x' days for a pair in `KMD_LTC` format.",
)
def trades(pair_str: str = "KMD_LTC", days_in_past: int = 5):
    try:
        for value, name in [(days_in_past, "days_in_past")]:
            validate.positive_numeric(value, name)
        data = Markets().trades(
            pair_str=pair_str, days_in_past=days_in_past, all_variants=False
        )
        return data
    except BadPairFormatError as e:
        err = {"error": f"{type(e)}: {e}"}
        logger.warning(err)
    except Exception as e:  # pragma: no cover
        err = {"error": f"{type(e)}: {e}"}
        logger.warning(err)
    return JSONResponse(status_code=400, content=err)


# Migrated from https://stats.testchain.xyz/api/v1/usd_volume_24h
@router.get(
    "/usd_volume_24hr",
    response_model=MarketsUsdVolume,
    description="Volume (in USD) traded in last 24hrs.",
    responses={406: {"model": ErrorMessage}},
    status_code=200,
)
def usd_volume_24h():
    try:
        data = memcache.get_pair_volumes_24hr()
        return {"usd_volume_24hr": data["trade_volume_usd"]}
    except Exception as e:  # pragma: no cover
        logger.warning(f"{type(e)} Error in [/api/v3/markets/usd_volume_24h]: {e}")
        return {"error": f"{type(e)} Error in [/api/v3/markets/usd_volume_24h]: {e}"}


@router.get(
    "/volumes_ticker/{coin}/{days_in_past}",
    description="Daily coin volume (e.g. `KMD, KMD-BEP20, KMD-ALL`) traded last 'x' days.",
)
def volumes_ticker(coin="KMD", days_in_past=1, trade_type: TradeType = TradeType.ALL):
    try:
        volumes_dict = {}
        query = db.SqlQuery()
        gecko_source = memcache.get_gecko_source()
        # Individual tickers only, no merge except segwit
        decoin = deplatform.coin(coin)
        variants = derive.coin_variants(coin, segwit_only=True)
        for i in range(0, int(days_in_past)):
            d = datetime.today() - timedelta(days=i)
            d_str = d.strftime("%Y-%m-%d")
            day_ts = int(int(d.strftime("%s")) / 86400) * 86400
            start_time = int(day_ts)
            end_time = int(day_ts) + 86400
            volumes = query.coin_trade_volumes(start_time=start_time, end_time=end_time)
            data = query.coin_trade_volumes_usd(volumes, gecko_source)
            volumes_dict[d_str] = template.volumes_ticker()
            for variant in variants:
                if decoin in data["volumes"]:
                    if variant in data["volumes"][decoin]:
                        volumes_dict[d_str] = (
                            volumes_dict[d_str] | data["volumes"][decoin][variant]
                        )
            data = {
                d_str: volumes_dict[d_str]["total_volume"] for d_str in volumes_dict
            }
        return data
    except Exception as e:
        logger.warning(e)
