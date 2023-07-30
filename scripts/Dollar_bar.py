from typing import Dict

import numpy as np

from hummingbot import data_path
from hummingbot.client.hummingbot_application import HummingbotApplication
from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.data_feed.candles_feed.candles_factory import CandlesFactory
from hummingbot.strategy.script_strategy_base import ScriptStrategyBase


class DollarBar(ScriptStrategyBase):
    """
    This script download the data from binance and calculate the dollar bar and volume bar
    """

    strategy = {
        'exchange': "binance",
        'trading_pairs': ["BTC-TUSD", "BTC-USDT"],
        'intervals': ["1m"],
        'days_to_download': 1,
        'ref_interval': '1h',
        'num_ref_intervals': 24,  # number of reference intervals to use for calculating the volume of the dollar bar

    }

    exchange = strategy['exchange']
    trading_pairs = strategy["trading_pairs"]
    intervals = strategy["intervals"]
    days_to_download = strategy["days_to_download"]

    # The object needs the markets variable to be initialized
    markets = {"binance_paper_trade": {"BTC-USDT"}}

    @staticmethod
    def get_max_records(days_to_download: int, interval: str) -> int:
        conversion = {"m": 1, "h": 60, "d": 1440}
        unit = interval[-1]
        quantity = int(interval[:-1])
        return int(days_to_download * 24 * 60 * quantity / conversion[unit])

    def __init__(self, connectors: Dict[str, ConnectorBase]):
        super().__init__(connectors)
        combinations = [(trading_pair, interval) for trading_pair in self.trading_pairs for interval in self.intervals]

        self.candles = {f"{combinations[0]}_{combinations[1]}": {} for combinations in combinations}
        # we need to initialize the candles for each trading pair
        for combination in combinations:
            candle = CandlesFactory.get_candle(connector=self.exchange, trading_pair=combination[0],
                                               interval=combination[1],
                                               max_records=self.get_max_records(self.days_to_download, combination[1]))
            candle.start()
            # we are storing the candles object and the csv path to save the candles
            self.candles[f"{combination[0]}_{combination[1]}"]["candles"] = candle
            self.candles[f"{combination[0]}_{combination[1]}"][
                "csv_path"] = data_path() + f"/candles_{self.exchange}_{combination[0]}_{combination[1]}.csv"

    def on_tick(self):
        for trading_pair, candles_info in self.candles.items():
            if not candles_info["candles"].is_ready:
                self.logger().info(f"Candles not ready yet for {trading_pair}! Missing {candles_info['candles']._candles.maxlen - len(candles_info['candles']._candles)}")

        if self.all_candles_ready():
            self.logger().info("All candles are ready! Stopping the script")

            for trading_pair, candles_info in self.candles.items():
                df_path = candles_info["csv_path"][:-4] + "_dollar.csv"
                df = self.timebar2dollarbar(candles_info["candles"].candles_df)
                # df = candles_info["candles"].candles_df
                df.to_csv(df_path, index=False)

            HummingbotApplication.main_application().stop()

    def on_stop(self):
        for candles_info in self.candles.values():
            candles_info["candles"].stop()

    def timebar2dollarbar(self, df, target=1):
        """
        target is the target volume of dollars per bar in millions
        """
        target = target * 1e6
        # value is already implemented in the Binance API
        # it is not clear to me how this value is computed
        # so we do it with the median price in the candle, which I understand.
        # for trading_pair, candles_info in self.candles.items():
        # we compute the median price for each candle
        vwap = df[['open', 'close', 'low', 'high']].median(axis=1)
        vwap.name = 'vwap'

        vol = df['volume'] * vwap
        vol.name = 'vol'

        df = df.join(vwap).join(vol)

        self.logger().info("computing $Bar ")
        # we compute the dollar value of each candle

        dollar_bars = df.groupby(self.n_bar(np.cumsum(df['vwap']), target)).agg({'vwap': 'ohlc', 'vol': 'sum'})
        # groupped_df = groupped_df.agg({'vwap': 'ohlc', 'vol': 'sum'})
        # nbars = self.bar(np.cumsum(df['vwap']), target)

        dollar_bars_price = dollar_bars.loc[:, 'vwap']
        self.logger().info(f"Number of dollar bars: {len(dollar_bars)}")

        return dollar_bars_price

    def n_bar(self, x, y):
        "Helper bar function to construct / return a integer number of bars."
        return np.int64(np.round(x / y))  # return an integer number of bars

    def all_candles_ready(self):
        """
        Checks if the historical candlesticks are full.
        :return:
        """
        return all(candles_info["candles"].is_ready for candles_info in self.candles.values())
