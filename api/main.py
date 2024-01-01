#!/usr/bin/env python3
import time
import uvicorn
from contextlib import asynccontextmanager
from fastapi import FastAPI
from const import API_HOST, API_PORT
from routes import gecko, cache_loop, swaps, tickers, rates, coins, markets, prices
from lib.cache_item import CacheItem
from lib.models import ErrorMessage, HealthCheck


@asynccontextmanager
async def lifespan(app: FastAPI):
    # star up functions
    for i in ["coins", "coins_config"]:
        cache_item = CacheItem("coins")
        cache_item.save()
    yield
    # shut down functions


app = FastAPI()

app.include_router(cache_loop.router)

app.include_router(
    gecko.router,
    prefix="/api/v3/gecko",
    tags=["CoinGecko"],
    dependencies=[],
    responses={418: {"description": "I'm a teapot"}},
)

app.include_router(
    coins.router,
    prefix="/api/v3/coins",
    tags=["Coins"],
    dependencies=[],
    responses={418: {"description": "I'm a teapot"}},
)

app.include_router(
    markets.router,
    prefix="/api/v3/markets",
    tags=["Markets"],
    dependencies=[],
    responses={418: {"description": "I'm a teapot"}},
)

app.include_router(
    rates.router,
    prefix="/api/v3/rates",
    tags=["Rates"],
    dependencies=[],
    responses={418: {"description": "I'm a teapot"}},
)

app.include_router(
    prices.router,
    prefix="/api/v3/prices",
    tags=["Prices"],
    dependencies=[],
    responses={418: {"description": "I'm a teapot"}},
)

app.include_router(
    swaps.router,
    prefix="/api/v3/swaps",
    tags=["Swaps"],
    dependencies=[],
    responses={418: {"description": "I'm a teapot"}},
)

app.include_router(
    tickers.router,
    prefix="/api/v3/tickers",
    tags=["Tickers"],
    dependencies=[],
    responses={418: {"description": "I'm a teapot"}},
)


@app.get(
    "/healthcheck",
    tags=["Status"],
    description="Simple service status check",
    responses={406: {"model": ErrorMessage}},
    response_model=HealthCheck,
    status_code=200,
)
def healthcheck():
    return {"timestamp": int(time.time()), "status": "ok"}


if __name__ == "__main__":  # pragma: no cover
    uvicorn.run("main:app", host=API_HOST, port=API_PORT)
