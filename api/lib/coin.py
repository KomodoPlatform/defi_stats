#!/usr/bin/env python3
from lib.cache_load import (
    load_coins_config,
    get_gecko_price_and_mcap,
)


class Coin:
    def __init__(self, coin: str = "KMD"):
        self.coin = coin
        self.ticker = self.coin.split("-")[0]
        # Designate coin
        if self.coin in self.coins_config_cache:
            self.type = self.coins_config_cache[self.coin]["type"]
            self.is_testnet = self.coins_config_cache[self.coin]["is_testnet"]
            self.is_wallet_only = self.coins_config_cache[self.coin]["wallet_only"]
        else:
            self.type = "Delisted"
            self.is_testnet = False
            self.is_wallet_only = True

    @property
    def coins_config_cache(self):
        return load_coins_config()

    @property
    def usd_price(self):
        return get_gecko_price_and_mcap(self.coin)[0]

    @property
    def mcap(self):
        return get_gecko_price_and_mcap(self.coin)[1]

    @property
    def is_priced(self):
        return self.usd_price > 0

    @property
    def is_tradable(self):
        if self.coin in self.coins_config_cache:
            if self.is_wallet_only:
                return False
            return True
        return False

    @property
    def has_segwit(self):
        if self.coin.endswith("-segwit"):
            return True
        if f"{self.coin}-segwit" in self.coins_config_cache:
            return True
        return False

    @property
    def is_valid(self):
        if self.is_tradable:
            return True
        if self.has_segwit:  # pragma: no cover
            for i in [self.ticker, f"{self.ticker}-segwit"]:
                if i in self.coins_config_cache:
                    if not self.coins_config_cache[i]["wallet_only"]:
                        return True
        return False

    @property
    def related_coins(self):
        data = [
            Coin(coin=i)
            for i in self.coins_config_cache
            if i == self.coin or i.startswith(f"{self.ticker}-")
        ]
        return data
