from lib.coins import Coins
from lib.coin import Coin
from lib.external import FixerAPI, CoinGeckoAPI
from lib.cache import (
    Cache,
    CacheItem,
    load_coins_config,
    load_gecko_source,
    load_generic_last_traded,
    load_generic_pairs,
)
from lib.generic import Generic
from lib.orderbook import Orderbook
from lib.markets import Markets
from lib.stats_api import StatsAPI
from lib.pair import Pair, get_all_coin_pairs
from lib.dex_api import DexAPI, get_orderbook

COINS = Coins()
PRICED_COINS = list(set([i.coin.replace("-segwit", "") for i in COINS.with_price]))
KMD_PAIRS = get_all_coin_pairs("KMD", PRICED_COINS)
KMD_PAIRS.sort()
KMD_PAIRS_INFO = [
    i
    for i in KMD_PAIRS
    if i.split("_")[0] in PRICED_COINS and i.split("_")[1] in PRICED_COINS
]
