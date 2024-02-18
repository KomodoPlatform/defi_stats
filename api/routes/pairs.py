#!/usr/bin/env python3
from fastapi import APIRouter
from models.generic import ErrorMessage
import util.memcache as memcache

router = APIRouter()


@router.get(
    "/last_traded",
    description="Returns last trade info for all pairs matching the filter",
    responses={406: {"model": ErrorMessage}},
    status_code=200,
)
def last_traded():
    return memcache.get_pair_last_traded()


@router.get(
    "/orderbook_extended",
    description="",
    responses={406: {"model": ErrorMessage}},
    status_code=200,
)
def orderbook_extended():
    return memcache.get_pair_orderbook_extended()


@router.get(
    "/prices_24hr",
    description="",
    responses={406: {"model": ErrorMessage}},
    status_code=200,
)
def prices_24hr():
    return memcache.get_pair_prices_24hr()


@router.get(
    "/volumes_14d",
    description="",
    responses={406: {"model": ErrorMessage}},
    status_code=200,
)
def volumes_14d():
    return memcache.get_pair_volumes_14d()


@router.get(
    "/volumes_24hr",
    description="",
    responses={406: {"model": ErrorMessage}},
    status_code=200,
)
def volumes_24hr():
    return memcache.get_pair_volumes_24hr()
