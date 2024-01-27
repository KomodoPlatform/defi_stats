import os
import sys

API_ROOT_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(API_ROOT_PATH)
import util.memcache as memcache
from lib.cache import CacheItem

data = CacheItem("coins").data
memcache.set_coins(data)

data = CacheItem("coins_config").data
memcache.set_coins_config(data)

data = CacheItem("gecko_source").data
memcache.set_gecko_source(data)

data = CacheItem("generic_last_traded").data
memcache.set_last_traded(data)


data = CacheItem("generic_tickers").data
memcache.set_tickers(data)

data = CacheItem("adex_fortnite").data
memcache.set_adex_fortnite(data)

data = CacheItem("statsapi_summary").data
memcache.set_statsapi_summary(data)
