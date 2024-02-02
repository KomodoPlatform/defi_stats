from lib.coins import Coins, Coin
from lib.external import FixerAPI, CoinGeckoAPI
from lib.cache import Cache, CacheItem
from lib.generic import Generic
from lib.markets import Markets
from lib.stats_api import StatsAPI
from lib.pair import Pair, get_all_coin_pairs
from lib.dex_api import DexAPI
import util.memcache as memcache
from util.logger import logger


# Initialize cache from files if available
memcache.set_coins(CacheItem(name="coins").data)
memcache.set_coins_config(CacheItem(name="coins_config").data)
memcache.set_fixer_rates(CacheItem(name="fixer_rates").data)
memcache.set_gecko_source(CacheItem(name="gecko_source").data)
memcache.set_adex_fortnite(CacheItem(name="generic_adex_fortnite").data)
memcache.set_last_traded(CacheItem(name="generic_last_traded").data)
memcache.set_summary(CacheItem(name="generic_summary").data)
memcache.set_tickers(CacheItem(name="generic_tickers").data)
memcache.update("coins_with_segwit", [i.coin for i in Coins().with_segwit], 86400)
