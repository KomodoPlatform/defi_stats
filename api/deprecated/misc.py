
    def get_platform(self, coin):
        if len(coin.split("-")) == 2:
            return coin.split("-")[1]
        return None