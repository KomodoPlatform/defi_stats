#!/usr/bin/env python3
from lib.cache_load import (
    load_coins_config,
    get_gecko_price_and_mcap,
    load_gecko_source,
)
from util.defaults import set_params
from util.logger import logger


class Coin:
    def __init__(self, coin: str = "KMD", **kwargs):
        try:
            # Set params
            self.kwargs = kwargs
            self.options = ["testing"]
            set_params(self, self.kwargs, self.options)

            self.coin = coin
            self.ticker = self.coin.split("-")[0]
            if "gecko_source" in kwargs:
                self.gecko_source = kwargs["gecko_source"]
            else:
                logger.loop(f"Getting gecko_source for {coin}")
                self.gecko_source = load_gecko_source(testing=self.testing)
            if "coins_config" in kwargs:
                self.coins_config = kwargs["coins_config"]
            else:
                self.coins_config = load_coins_config(testing=self.testing)

            # Designate coin
            if self.coin in self.coins_config:
                self.type = self.coins_config[self.coin]["type"]
                self.is_testnet = self.coins_config[self.coin]["is_testnet"]
                self.is_wallet_only = self.coins_config[self.coin]["wallet_only"]
            else:
                self.type = "Delisted"
                self.is_testnet = False
                self.is_wallet_only = True
        except Exception as e:  # pragma: no cover
            msg = f"Init Coin for {coin} failed!"
            logger.error(f"{type(e)} {msg}: {e}")

    @property
    def usd_price(self):
        return get_gecko_price_and_mcap(
            ticker=self.coin, gecko_source=self.gecko_source
        )[0]

    @property
    def mcap(self):
        return get_gecko_price_and_mcap(
            ticker=self.coin, gecko_source=self.gecko_source
        )[1]

    @property
    def is_priced(self):
        return self.usd_price > 0

    @property
    def is_tradable(self):
        if self.coin in self.coins_config:
            if self.is_wallet_only:
                return False
            return True
        return False

    @property
    def has_segwit(self):
        if self.coin.endswith("-segwit"):
            return True
        if f"{self.coin}-segwit" in self.coins_config:
            return True
        return False

    @property
    def is_valid(self):
        if self.is_tradable:
            return True
        if self.has_segwit:  # pragma: no cover
            for i in [self.ticker, f"{self.ticker}-segwit"]:
                if i in self.coins_config:
                    if not self.coins_config[i]["wallet_only"]:
                        return True
        return False

    @property
    def related_coins(self):
        data = [
            Coin(coin=i, gecko_source=self.gecko_source)
            for i in self.coins_config.keys()
            if i == self.coin or i.startswith(f"{self.ticker}-")
        ]
        return data
