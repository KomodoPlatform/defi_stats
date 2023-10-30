#!/usr/bin/env python3
import uvicorn
from fastapi import FastAPI
import const
from routes import gecko, cache

app = FastAPI()

app.include_router(
    gecko.router,
    prefix="/api/v3/gecko",
    tags=["CoinGecko"],
    dependencies=[],
    responses={418: {"description": "I'm a teapot"}},
)

app.include_router(cache.router)


if __name__ == '__main__':  # pragma: no cover
    uvicorn.run("main:app", host=const.API_HOST, port=const.API_PORT)
