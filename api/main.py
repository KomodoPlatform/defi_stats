#!/usr/bin/env python3
import uvicorn
from fastapi import FastAPI
from const import API_HOST, API_PORT
from routes import gecko, cache, swaps, tickers, rates, coins

app = FastAPI()

app.include_router(cache.router)

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
    swaps.router,
    prefix="/api/v3/swaps",
    tags=["Swaps"],
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
    tickers.router,
    prefix="/api/v3/tickers",
    tags=["Tickers"],
    dependencies=[],
    responses={418: {"description": "I'm a teapot"}},
)


if __name__ == '__main__':  # pragma: no cover
    uvicorn.run("main:app", host=API_HOST, port=API_PORT)
