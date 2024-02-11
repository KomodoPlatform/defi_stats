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
    MarketsCurrentLiquidity,
    MarketsFiatRatesItem,
    MarketsAtomicdexIo,
    MarketsOrderbookItem,
    MarketsPairLastTradeItem,
    MarketsSwaps24,
    PairTrades,
    MarketsSummaryItem,
    MarketsSummaryForTicker,
)
from lib.pair import Pair
from lib.markets import Markets
from routes.metadata import markets_desc
from util.enums import TradeType
from util.logger import logger
from util.transform import (
    clean,
    convert,
    deplatform,
    sumdata,
    derive,
    invert,
    sortdata,
    merge,
)
import util.memcache as memcache
from util.transform import template
import util.transform as transform
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


"""
# New endpoint
@router.get(
    "/current_liquidity",
    response_model=MarketsCurrentLiquidity,
    description=markets_desc.liquidity,
    responses={406: {"model": ErrorMessage}},
    status_code=200,
)
def current_liquidity():
    try:
        data = memcache.get_tickers()
        return {"current_liquidity": data["combined_liquidity_usd"]}
    except Exception as e:  # pragma: no cover
        logger.warning(f"{type(e)} Error in [/api/v3/markets/current_liquidity]: {e}")
        return {"error": f"{type(e)} Error in [/api/v3/markets/current_liquidity]: {e}"}
"""


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
            data = pair.orderbook(pair_str=pair_str, depth=depth, no_thread=True)

        orderbook_data = template.orderbook(pair_str=pair_str)
        for variant in derive.pair_variants(pair_str=pair_str, segwit_only=True):
            if variant in data:
                variant_data = data[variant]
            elif invert.pair(variant) in data:
                variant_data = data[invert.pair(variant)]
            else:
                logger.warning(f"Unable to find orderbook for {variant}")
                variant_data = template.orderbook(variant)
            orderbook_data = merge.orderbooks(orderbook_data, variant_data)

        logger.merge(data.keys())
        if Decimal(orderbook_data["liquidity_in_usd"]) > 0:
            if is_reversed:
                logger.calc("Returning inverted cache")
                orderbook_data = invert.markets_orderbook(orderbook_data)
        logger.loop(orderbook_data.keys())
        orderbook_data.update({"liquidity_usd": orderbook_data["liquidity_in_usd"]})
        logger.pair(orderbook_data.keys())
        return orderbook_data
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
        return memcache.get_markets_summary()
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
        if "_" in coin:
            return {"error": f"Coin value '{coin}' looks like a pair."}
        resp = memcache.get_tickers()
        data = memcache.get_pair_last_traded()
        new_data = []
        for i in resp["data"]:
            if coin in [i["base_currency"], i["quote_currency"]]:
                if i["last_swap_time"] == 0:
                    if i["ticker_id"] in data:
                        i = i | data[i["ticker_id"]]
                new_data.append(convert.ticker_to_summary_for_ticker(i))
        resp.update(
            {
                "pairs_count": len(new_data),
                "swaps_count": int(sumdata.json_key(new_data, "trades_24hr")),
                "liquidity_usd": sumdata.json_key_10f(new_data, "liquidity_usd"),
                "volume_usd_24hr": sumdata.json_key_10f(new_data, "volume_usd_24hr"),
                "data": new_data,
            }
        )
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


"""
@router.get(
    "/ticker",
    description="Simple last price and liquidity for each market pair, traded in last 7 days.",
)
def ticker():
    try:
        data = memcache.get_tickers()
        resp = []
        for i in data["data"]:
            resp.append(transform.ticker_to_market_ticker(i))
        return resp
    except Exception as e:  # pragma: no cover
        logger.warning(f"{type(e)} Error in [/api/v3/market/ticker]: {e}")
        return {"error": f"{type(e)} Error in [/api/v3/market/ticker]: {e}"}


@router.get(
    "/ticker_for_ticker",
    description="Simple last price and liquidity for each market pair for a specific ticker.",
)
def ticker_for_ticker(ticker):
    try:
        data = memcache.get_tickers()
        resp = []
        for i in data["data"]:
            if ticker in [i["base_currency"], i["quote_currency"]]:
                resp.append(transform.ticker_to_market_ticker(i))
        return resp
    except Exception as e:  # pragma: no cover
        logger.warning(f"{type(e)} Error in [/api/v3/market/ticker_for_ticker]: {e}")
        return {"error": f"{type(e)} Error in [/api/v3/market/ticker_for_ticker]: {e}"}
"""


@router.get(
    "/tickers_summary",
    description="Total swaps and volume involving for each active ticker in the last 24hrs.",
)
def tickers_summary():
    try:
        data = memcache.get_tickers()
        resp = {}
        for i in data["data"]:
            base = i["base_currency"]
            rel = i["quote_currency"]
            for ticker in [base, rel]:
                if ticker not in resp:
                    resp.update({ticker: {"trades_24hr": 0, "volume_24hr": 0}})
                resp[ticker]["trades_24hr"] += int(i["trades_24hr"])
                if ticker == base:
                    resp[ticker]["volume_24hr"] += Decimal(i["base_volume"])
                elif ticker == rel:
                    resp[ticker]["volume_24hr"] += Decimal(i["quote_volume"])
        resp = clean.decimal_dicts(resp)
        with_action = {}
        tickers = list(resp.keys())
        tickers.sort()
        for ticker in tickers:
            if resp[ticker]["trades_24hr"] > 0:
                with_action.update({ticker: resp[ticker]})
        return with_action
    except Exception as e:  # pragma: no cover
        logger.warning(f"{type(e)} Error in [/api/v3/market/swaps24]: {e}")
        return {"error": f"{type(e)} Error in [/api/v3/market/swaps24]: {e}"}


@router.get(
    "/trades/{pair_str}/{days_in_past}",
    response_model=List[PairTrades],
    description="Trades for the last 'x' days for a pair in `KMD_LTC` format.",
)
def trades(
    pair_str: str = "KMD_LTC",
    days_in_past: int | None = None,
    all_variants: bool = False,
):
    try:
        for value, name in [(days_in_past, "days_in_past")]:
            validate.positive_numeric(value, name)
        data = Markets().trades(
            pair=pair_str, days_in_past=days_in_past, all_variants=all_variants
        )
        return data
    except BadPairFormatError as e:
        err = {"error": f"{e.msg}"}
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
        data = memcache.get_tickers()
        return {"usd_volume_24hr": data["combined_volume_usd"]}
    except Exception as e:  # pragma: no cover
        logger.warning(f"{type(e)} Error in [/api/v3/markets/usd_volume_24h]: {e}")
        return {"error": f"{type(e)} Error in [/api/v3/markets/usd_volume_24h]: {e}"}


@router.get(
    "/volumes_ticker/{coin}/{days_in_past}",
    description="Daily coin volume (e.g. `KMD, KMD-BEP20, KMD-ALL`) traded last 'x' days.",
)
def volumes_ticker(coin="KMD", days_in_past=1, trade_type: TradeType = TradeType.ALL):
    # TODO: Use new DB
    volumes_dict = {}
    query = db.SqlQuery()
    gecko_source = memcache.get_gecko_source()
    # Individual tickers only, no merge except segwit
    stripped_coin = deplatform.coin(coin)
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
            if stripped_coin in data["volumes"]:
                if variant in data["volumes"][stripped_coin]:
                    volumes_dict[d_str] = (
                        volumes_dict[d_str] | data["volumes"][stripped_coin][variant]
                    )
        data = {d_str: volumes_dict[d_str]["trade_volume"] for d_str in volumes_dict}
    return data
