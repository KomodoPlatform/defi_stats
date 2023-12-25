from const import API_ROOT_PATH, templates


class Files:
    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.options = ["testing", "netid"]
        templates.set_params(self, self.kwargs, self.options)
        if self.testing:
            folder = f"{API_ROOT_PATH}/tests/fixtures"
        else:
            folder = f"{API_ROOT_PATH}/cache"
        # Coins repo data
        self.coins = f"{folder}/coins/coins.json"
        self.coins_config = f"{folder}/coins/coins_config.json"
        # For Markets endpoints
        self.markets_tickers = f"{folder}/markets/tickers_{self.netid}.json"
        self.markets_pairs = f"{folder}/markets/pairs_{self.netid}.json"
        self.markets_last_trade = f"{folder}/markets/last_trade_{self.netid}.json"
        # For CoinGecko endpoints
        self.gecko_source = f"{folder}/gecko/source.json"
        self.gecko_tickers = f"{folder}/gecko/tickers_{self.netid}.json"
        self.gecko_pairs = f"{folder}/gecko/pairs_{self.netid}.json"
        # For Rates endpoints
        self.fixer_rates = f"{folder}/rates/fixer_io.json"
        # For Prices endpoints
        self.prices_tickers_v1 = f"{folder}/prices/tickers_v1.json"
        self.prices_tickers_v2 = f"{folder}/prices/tickers_v2.json"

    def get_cache_fn(self, name):
        return getattr(self, name, None)
