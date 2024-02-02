import os
import sys

API_ROOT_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(API_ROOT_PATH)
import util.memcache as memcache
from lib.cache import CacheItem
from lib.coins import Coins


memcache.set_coins(CacheItem(name="coins").data)
memcache.set_coins_config(CacheItem(name="coins_config").data)
memcache.set_fixer_rates(CacheItem(name="fixer_rates").data)
memcache.set_gecko_source(CacheItem(name="gecko_source").data)
memcache.set_adex_fortnite(CacheItem(name="generic_adex_fortnite").data)
memcache.set_last_traded(CacheItem(name="generic_last_traded").data)
memcache.set_summary(CacheItem(name="generic_summary").data)
memcache.set_tickers(CacheItem(name="generic_tickers").data)
memcache.update("coins_with_segwit", [i.coin for i in Coins().with_segwit], 86400)
