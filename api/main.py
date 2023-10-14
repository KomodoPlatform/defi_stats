#!/usr/bin/env python3
import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi_utils.tasks import repeat_every
from logger import logger
import models
import const
import time

cache = models.Cache()

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
@repeat_every(seconds=60)
def cache_gecko_data():  # pragma: no cover
    try:
        cache.save.gecko_source()
    except Exception as e:
        logger.warning(f"{type(e)} Error in [cache_gecko_data]: {e}")


@app.on_event("startup")
@repeat_every(seconds=180)
def cache_gecko_tickers():  # pragma: no cover
    try:
        cache.save.gecko_tickers()
    except Exception as e:
        logger.warning(f"{type(e)} Error in [cache_ticker_data]: {e}")


@app.on_event("startup")
@repeat_every(seconds=60)
def cache_gecko_pairs():  # pragma: no cover
    try:
        cache.save.gecko_pairs()
    except Exception as e:
        logger.warning(f"{type(e)} Error in [cache_pairs_data]: {e}")


@app.on_event("startup")
@repeat_every(seconds=86400)
def update_coins_config():  # pragma: no cover
    try:
        cache.save.coins_config()
    except Exception as e:
        logger.warning(f"{type(e)} Error in [update_coins_config]: {e}")


@app.on_event("startup")
@repeat_every(seconds=86400)
def update_coins():  # pragma: no cover
    try:
        cache.save.coins()
    except Exception as e:
        logger.warning(f"{type(e)} Error in [update_coins]: {e}")


### Gecko Format Endpoints ###


@app.get('/api/v3/gecko/pairs')
def gecko_pairs(days: int = 7) -> list:
    '''
    Details for cryptoassets tradable/traded on the Komodo DeFi Framework, since a specific timestamp.
    Params: "days" (int) - Return all pairs traded in the last "x" days. A value of zero will return all tradable pairs.
    Output format:
    [
        {
            “ticker_id”: “KMD_DGB”,
            "base": "KMD",
            "target": "DGB"
        },
        {
            “ticker_id”: “KMD_DOGE”,
            "base": "KMD",
            "target": "DOGE"
        }
    ]
    
    TODO: Confirm if this applies to all tradable pairs, or only pairs traded in the last "x" days?
    If all tradable pairs, use permutation of tickers in coins_config.json file.
    Otherwise, use MM2.db to get pairs traded in the last "x" days.
    '''
    try:
        return cache.load.gecko_pairs()
    except Exception as e:  # pragma: no cover
        logger.warning(f"{type(e)} Error in [/api/v3/gecko/pairs]: {e}")
        return {"error": f"{type(e)} Error in [/api/v3/gecko/pairs]: {e}"}


@app.get('/api/v3/gecko/tickers')
def gecko_tickers() -> list:
    '''
    The /tickers endpoint provides 24-hour pricing and volume information on each market pair available on an exchange.
    Params: "since" (int) - Unix timestamp in seconds. A value of zero will return all tradable pairs.
    Output format:
    [
        {
            "ticker_id": "BTC_ETH",
            "base_currency": "BTC",
            "target_currency": "ETH",
            "last_price":"50.0",
            "base_volume":"10",
            "target_volume":"500", 
            "bid":"49.9",
            "ask":"50.1",
            "high":”51.3”,
            “low”:”49.2”,
            “liquidity_in_usd”:“100”
        },
        {
            "ticker_id": "KMD_DASH",
            "base_currency": "KMD",
            "target_currency": "DASH",
            "last_price":"5.0",
            "base_volume":"100",
            "target_volume":"400", 
            "bid":"45.9",
            "ask":"50.2",
            "high":”52.3”,
            “low”:”42.7”,
            “liquidity_in_usd”:“1000”
        }
    ]
    
    TODO: Confirm if this applies to all tradable pairs, or only pairs traded in the last "x" days?
    If all tradable pairs, use permutation of tickers in coins_config.json file.
    Otherwise, use MM2.db to get pairs traded in the last "x" days.
    '''
    return cache.load.gecko_tickers()

    
@app.get('/api/v3/gecko/orderbook')
def gecko_orderbook(ticker_id: str = "KMD_LTC", depth: int = 100) -> dict:
    '''
    The /orderbook endpoint is to provide order book information with at least depth = 100 (50 each side) returned for a given market pair/ticker. 
    Params:
        "ticker_id" (str) - tickers for pair in format BASE_TARGET
        "depth" (int) - number of orders to return. A value of 100 means 50 for each bid/ask side. A value of zero returns full orderbook.

    Output format:
    {  
        "ticker_id": "KMD_LTC",
        "timestamp":"1700050000",
        "bids":[  
            [  
                "49.8",
                "0.50000000"
            ],
            [  
                "49.9",
                "6.40000000"
            ]
        ],
        "asks":[  
            [  
                "50.1",
                "9.20000000"
            ],
            [  
                "50.2",
                "7.9000000"
            ]
        ]
    }
    '''
    try:
        if len(ticker_id) > 32:
            raise HTTPException(
                status_code=400,
                detail="Pair cant be longer than 32 symbols"
            )  # pragma: no cover
        elif "_" not in ticker_id:
            raise HTTPException(
                status_code=400,
                detail="Pair should be in format BASE_TARGET"
            )  # pragma: no cover
        elif ticker_id == "":
            raise HTTPException(
                status_code=400,
                detail="Pair can not be empty. Use the format BASE_TARGET"
            )  # pragma: no cover
        ticker_id = models.Pair(ticker_id)
        orderbook = models.Orderbook(ticker_id)
        return orderbook.for_pair(True)
    except Exception as e:  # pragma: no cover
        logger.warning(f"{type(e)} Error in /api/v3/orderbook [{ticker_id}] [depth: {depth}]]: {e}")
        return {"error": f"{type(e)} Error in /api/v3/orderbook [{ticker_id}] [depth: {depth}]]: {e}"}


@app.get('/api/v3/gecko/historical_trades')
def gecko_historical_trades(
        trade_type: const.TradeType,
        ticker_id: str = "KMD_LTC",
        limit: int = 100,
        start_time: int = int(time.time()) - 86400,
        end_time: int = int(time.time())
    ) -> dict:
    '''
    The /historical_trades endpoint is used to return data on historical completed trades for a given market pair.
    Params:
        "ticker_id" (str) - tickers for pair in format BASE_TARGET
        "type" (enum) - "buy", "sell", or "all"
        "start_time" - Unix timestamp in seconds. A value of zero will return all trades.
        "end_time" - Unix timestamp in seconds. A value of zero will assume the current time.
        "limit" - number of trades to return. Defaults to 100. A value of zero implies unlimited (will be paginated).
        "page_size" - number of trades to return per page. Defaults to 100.
        "page" - page number for pagination. Defaults to 1.

    Output format:
    {
        "ticker_id": "KMD_LTC",
        “buy”: [  
            {        
                "trade_id":1234567,
                "price":"50.1",
                "base_volume":"0.1",
                "target_volume":"1",
                "trade_timestamp":"1700050000",
                "type":"buy"
            }
        ],
        “sell”: [
            {        
                "trade_id":1234567,
                "price":"50.1",
                "base_volume":"0.1",
                "target_volume":"1",
                "trade_timestamp":"1700050000",
                "type":"sell"
            }
        ]
    }

    '''
    try:
        if len(ticker_id) > 32:
            raise HTTPException(
                status_code=400,
                detail="Pair cant be longer than 32 symbols"
            )  # pragma: no cover
        elif "_" not in ticker_id:
            raise HTTPException(
                status_code=400,
                detail="Pair should be in format BASE_TARGET"
            )  # pragma: no cover
        elif ticker_id == "":
            raise HTTPException(
                status_code=400,
                detail="Pair can not be empty. Use the format BASE_TARGET"
            )  # pragma: no cover
        pair = models.Pair(ticker_id)
        logger.info(ticker_id)
        return pair.historical_trades(trade_type=trade_type, limit=limit, start_time=start_time, end_time=end_time)
    except Exception as e:  # pragma: no cover
        logger.warning(f"{type(e)} Error in /api/v3/historical_trades [{ticker_id}]: {e}")
        return {"error": f"{type(e)} Error in /api/v3/historical_trades [{ticker_id}]: {e}"}






if __name__ == '__main__':  # pragma: no cover
    uvicorn.run("main:app", host=const.API_HOST, port=const.API_PORT)
