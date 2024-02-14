import os
import sys

API_ROOT_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(API_ROOT_PATH)

from lib.coins import Coins
from lib.cache import CacheItem
import util.memcache as memcache


# Initialize cache from files if available
memcache.set_coins(CacheItem(name="coins").data)
memcache.set_coins_config(CacheItem(name="coins_config").data)
memcache.set_fixer_rates(CacheItem(name="fixer_rates").data)
memcache.set_gecko_source(CacheItem(name="gecko_source").data)
memcache.set_gecko_pairs(CacheItem(name="gecko_pairs").data)
# memcache.set_adex_fortnite(CacheItem(name="adex_fortnite").data)
memcache.set_markets_summary(CacheItem(name="markets_summary").data)
memcache.set_pair_last_traded(CacheItem(name="pair_last_traded").data)
memcache.set_pair_prices_24hr(CacheItem(name="pair_prices_24hr").data)
memcache.set_pair_volumes_24hr(CacheItem(name="pair_volumes_24hr").data)
memcache.set_coin_volumes_24hr(CacheItem(name="coin_volumes_24hr").data)
memcache.set_pair_orderbook_extended(CacheItem(name="pair_orderbook_extended").data)
memcache.set_tickers(CacheItem(name="tickers").data)
memcache.set_stats_api_summary(CacheItem(name="stats_api_summary").data)

# memcache.set_summary(CacheItem(name="generic_summary").data)
# memcache.set_tickers(CacheItem(name="generic_tickers").data)
# memcache.set_tickers_14d(CacheItem(name="generic_tickers_14d").data)
memcache.update("coins_with_segwit", [i.coin for i in Coins().with_segwit], 86400)
