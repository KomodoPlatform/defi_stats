#!/usr/bin/env python3
from typing import Dict
from dataclasses import dataclass
from util.logger import logger
from util.cron import cron
import util.defaults as default
import util.memcache as memcache
from util.transform import derive


@dataclass
class Coins:  # pragma: no cover
    def __init__(self, coins_config=None, gecko_source=None):
        try:
            self.init_at = cron.now_utc()
            self._config = coins_config
            self._gecko_source = gecko_source
            self.coins = [
                Coin(coin=i, coins_config=self.config, gecko_source=self.gecko_source)
                for i in self.config
            ]
            self.tickers = sorted([j for j in self.config.keys()])
        except Exception as e:  # pragma: no cover
            logger.error(f"Failed to init Coins: {e}")

    @property
    def age(self):
        return cron.now_utc() - self.init_at

    @property
    def gecko_source(self):
        if self._gecko_source is None:
            logger.calc("sourcing gecko")
            self._gecko_source = memcache.get_gecko_source()
        return self._gecko_source

    @property
    def config(self):
        if self._config is None:
            self._config = memcache.get_coins_config()
        return self._config

    @property
    def with_mcap(self):
        return [i for i in self.coins if i.mcap > 0]

    @property
    def with_price(self):
        return [i for i in self.coins if i.is_priced]

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
    def __init__(self, coin, coins_config: Dict, gecko_source=None, **kwargs):
        try:
            # Set params
            self.kwargs = kwargs
            self.options = []
            default.params(self, self.kwargs, self.options)
            self.coin = coin
            self.ticker = self.coin.split("-")[0]
            self.coins_config = coins_config
            self._gecko_source = gecko_source

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
    def gecko_source(self):
        if self._gecko_source is None:
            self._gecko_source = memcache.get_gecko_source()
        return self._gecko_source

    @property
    def usd_price(self):
        return derive.gecko_price(ticker=self.coin, gecko_source=self.gecko_source)

    @property
    def mcap(self):
        return derive.gecko_mcap(ticker=self.coin, gecko_source=self.gecko_source)

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
