#!/usr/bin/env python3
from fastapi import APIRouter
from lib.cache import Cache

router = APIRouter()
cache = Cache()
