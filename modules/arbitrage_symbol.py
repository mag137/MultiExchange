_version_ = "1.0"

from collections import deque

class SpreadWindow:
    def __init__(self, size):
        self.window = deque(maxlen=size)

    def append(self, value):
        self.window.append(value)

    def get_all(self):
        return list(self.window)

class EMA:
    def __init__(self, alpha):
        self.alpha = alpha
        self.value = None

    def update(self, price):
        if self.value is None:
            self.value = price
        else:
            self.value = self.alpha * price + (1 - self.alpha) * self.value
        return self.value

class ArbitrageSymbol:
    def __init__(self, exchange1, exchange2):
        self.exchange1 = exchange1
        self.exchange2 = exchange2
        self.average_price_dict = {}
        self.average_price_dict[exchange1] = {}
        self.average_price_dict[exchange2] = {}
        self.direct_spread = None # bid2 - ask1
        self.reverse_spread = None # bid1 - ask2

    # input_average_prices = {exchange:{average_ask, average_bid}}
    def spread_calc(self, input_average_prices):
        if self.exchange1 in input_average_prices:
            self.average_price_dict[self.exchange1]['average_ask'] = input_average_prices[self.exchange1]['average_ask']
            self.average_price_dict[self.exchange1]['average_bid'] = input_average_prices[self.exchange1]['average_bid']

        if self.exchange2 in input_average_prices:
            self.average_price_dict[self.exchange2]['average_ask'] = input_average_prices[self.exchange1]['average_ask']
            self.average_price_dict[self.exchange2]['average_bid'] = input_average_prices[self.exchange1]['average_bid']

        if self.average_price_dict[self.exchange1] is not None and self.average_price_dict[self.exchange2] is not None:


    pass