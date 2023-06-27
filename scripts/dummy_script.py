from hummingbot.strategy.script_strategy_base import ScriptStrategyBase


class dummy_script(ScriptStrategyBase):
    markets = {"binance_paper_trade": {"BTC-USDT"}}

    def on_tick(self):
        self.logger().info("hello test")
