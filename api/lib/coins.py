#!/usr/bin/env python3
from dataclasses import dataclass
from util.logger import logger, timed
import util.defaults as default
import util.helper as helper
import util.memcache as memcache
from util.transform import sortdata


@dataclass
class Coins:  # pragma: no cover
    def __init__(self):
        coins_config = memcache.get_coins_config()
        self.coins = [Coin(coin=i) for i in coins_config]

    @property
    def with_mcap(self):
        return [i for i in self.coins if i.mcap > 0]

    @property
    def with_segwit(self):
        data = [i for i in self.coins if i.has_segwit]
        return data

    @property
    def wallet_only(self):
        return [i for i in self.coins if i.is_wallet_only]

    @property
    def testnet_only(self):
        return [i for i in self.coins if i.is_testnet]

    @property
    def by_type(self):
        for coin in self.coins:
            if coin.type not in self.by_type_data:
                self.by_type_data.update({coin.type: []})
            self.by_type_data[coin.type].append(coin)
        return self.by_type_data


class Coin:
    def __init__(self, coin: str = "KMD", **kwargs):
        try:
            # Set params
            self.kwargs = kwargs
            self.options = []
            default.params(self, self.kwargs, self.options)
            self.coin = coin
            self.ticker = self.coin.split("-")[0]
            coins_config = memcache.get_coins_config()

            # Designate coin
            if self.coin in coins_config:
                self.type = coins_config[self.coin]["type"]
                self.is_testnet = coins_config[self.coin]["is_testnet"]
                self.is_wallet_only = coins_config[self.coin]["wallet_only"]
            else:
                self.type = "Delisted"
                self.is_testnet = False
                self.is_wallet_only = True
        except Exception as e:  # pragma: no cover
            msg = f"Init Coin for {coin} failed!"
            logger.error(f"{type(e)} {msg}: {e}")

    @property
    def usd_price(self):
        return helper.get_gecko_price(ticker=self.coin)

    @property
    def mcap(self):
        return helper.get_gecko_mcap(ticker=self.coin)

    @property
    def is_priced(self):
        return self.usd_price > 0

    @property
    def is_tradable(self):
        coins_config = memcache.get_coins_config()
        if self.coin in coins_config:
            if self.is_wallet_only:
                return False
            return True
        return False

    @property
    def has_segwit(self):
        coins_config = memcache.get_coins_config()
        if self.coin.endswith("-segwit"):
            return True
        if f"{self.coin}-segwit" in coins_config:
            return True
        return False


def get_segwit_coins():
    data = memcache.get("coins_with_segwit")
    if data is None:
        coins = Coins()
        data = [i.coin for i in coins.with_segwit]
        memcache.update("coins_with_segwit", data, 86400)
    return data


def get_tradable_coins():
    coins = Coins()
    return coins.tradable_only


def get_kmd_pairs():
    coins = Coins()
    coins_with_price = [i.coin for i in coins.with_price]
    kmd_pairs = get_all_coin_pairs("KMD", coins_with_price)
    kmd_pairs.sort()
    return kmd_pairs


@timed
def get_all_coin_pairs(coin, priced_coins):
    try:
        pairs = [
            (f"{i}_{coin}") for i in priced_coins if coin not in [i, f"{i}-segwit"]
        ]
        data = list(set([sortdata.pair_by_market_cap(i) for i in pairs]))

    except Exception as e:  # pragma: no cover
        data = []
        msg = f"{coin} get_all_coin_pairs failed! {e}"
        return default.result(data=data, msg=msg, loglevel="warning")
    msg = f"Completed get_all_coin_pairs for {coin}"
    return default.result(data=data, msg=msg, loglevel="pair", ignore_until=2)
