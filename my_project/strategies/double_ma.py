"""
改进的双均线策略
增强数据格式兼容性和错误处理
"""
from typing import Dict, Any, List
from strategies.base_strategy import BaseStrategy
from core.data_adapter import DataAdapter


class DoubleMa(BaseStrategy):
    """双均线策略（已修复数据兼容性问题）"""

    def __init__(self, name: str, config: Dict[str, Any] = None):
        super().__init__(name, config)

        # 策略参数
        self.short_period = config.get("short_period", 30)
        self.long_period = config.get("long_period", 60)
        self.symbol = config.get("symbol", "SHFE.cu2401")
        self.volume = config.get("volume", 1)

        # 策略状态
        self.klines = []
        self.position = 0
        self.adapter = DataAdapter()  # 数据适配器

        # 计算所需的最小数据量
        self.min_data_length = max(self.short_period, self.long_period) + 2

        self.write_log(f"双均线策略初始化完成: 短周期={self.short_period}, 长周期={self.long_period}")

    def on_init(self):
        """策略初始化"""
        super().on_init()
        self.write_log("双均线策略初始化开始")

        # 重置状态
        self.klines = []
        self.position = 0

        self.inited = True
        self.write_log("双均线策略初始化完成")

    def on_start(self):
        """策略启动"""
        super().on_start()
        self.write_log("双均线策略启动")
        self.trading = True

    def on_stop(self):
        """策略停止"""
        # 平掉所有持仓
        if self.position != 0 and self.klines:
            close_price = self.klines[-1].get('close', 0) if hasattr(self.klines[-1], 'get') else 0
            if not close_price and isinstance(self.klines[-1], dict):
                close_price = self.klines[-1].get('close', 0)

            if self.position > 0:
                self.sell(self.symbol, close_price, abs(self.position))
                self.write_log(f"策略停止，平多仓: {self.symbol}")
            elif self.position < 0:
                self.cover(self.symbol, close_price, abs(self.position))
                self.write_log(f"策略停止，平空仓: {self.symbol}")

        super().on_stop()
        self.write_log("双均线策略停止")

    def on_tick(self, tick_data: Dict[str, Any]):
        """Tick数据处理（增强兼容性）"""
        if not self.trading:
            return

        # 使用适配器处理数据格式
        processed_data = self.adapter.extract_core_data(tick_data)

        # 检查数据是否匹配策略品种
        data_symbol = processed_data.get('symbol', '')
        if data_symbol != self.symbol:
            return

        # 双均线策略主要基于K线，tick数据可选择性处理
        # 这里可以添加基于tick的交易逻辑
        pass

    def on_bar(self, bar_data: Dict[str, Any]):
        """K线数据处理（增强兼容性和错误处理）"""
        if not self.trading:
            return

        try:
            # 使用适配器确保数据格式正确
            processed_data = self.adapter.extract_core_data(bar_data)

            # 验证数据完整性
            if not self._validate_bar_data(processed_data):
                return

            # 检查品种匹配
            data_symbol = processed_data.get('symbol', '')
            if data_symbol != self.symbol:
                return

            # 添加K线数据
            self.klines.append(processed_data)

            # 保持数据长度
            if len(self.klines) > self.min_data_length + 100:
                self.klines = self.klines[-self.min_data_length - 100:]

            # 检查数据量是否足够
            if len(self.klines) < self.min_data_length:
                return

            # 提取收盘价序列
            closes = self._extract_closes()
            if len(closes) < self.min_data_length:
                return

            # 计算移动平均线
            short_ma = self._calculate_ma(closes, self.short_period)
            long_ma = self._calculate_ma(closes, self.long_period)

            if len(short_ma) < 2 or len(long_ma) < 2:
                return

            # 获取均线值
            prev_short, curr_short = short_ma[-2], short_ma[-1]
            prev_long, curr_long = long_ma[-2], long_ma[-1]

            # 生成交易信号
            self._generate_trading_signal(prev_short, prev_long, curr_short, curr_long, processed_data)

        except Exception as e:
            self.write_log(f"处理K线数据时发生异常: {e}")

    def _validate_bar_data(self, bar_data: Dict[str, Any]) -> bool:
        """验证Bar数据完整性"""
        required_fields = ['symbol', 'datetime', 'close']
        return all(field in bar_data for field in required_fields)

    def _extract_closes(self) -> List[float]:
        """从K线数据中提取收盘价序列"""
        closes = []
        for kline in self.klines:
            close_price = kline.get('close', 0)
            if close_price > 0:  # 有效的价格
                closes.append(close_price)
        return closes

    def _calculate_ma(self, data: List[float], period: int) -> List[float]:
        """计算移动平均线（增强容错）"""
        if len(data) < period:
            return []

        ma_values = []
        for i in range(period - 1, len(data)):
            try:
                ma_value = sum(data[i - period + 1:i + 1]) / period
                ma_values.append(ma_value)
            except (IndexError, ZeroDivisionError):
                # 处理计算错误
                continue

        return ma_values

    def _generate_trading_signal(self, prev_short: float, prev_long: float,
                                 curr_short: float, curr_long: float, bar_data: Dict[str, Any]):
        """生成交易信号（增强信号逻辑）"""
        close_price = bar_data.get('close', 0)

        # 死叉信号：短周期下穿长周期，做空
        if (prev_short > prev_long and curr_short < curr_long and
                self.position >= 0):  # 当前无持仓或多头持仓

            if self.position > 0:  # 如果持有多头，先平仓
                self.sell(self.symbol, close_price, self.position)
                self.write_log(f"死叉信号，平多仓: {self.symbol} @ {close_price}")

            # 开空仓
            self.short(self.symbol, close_price, self.volume)
            self.position = -self.volume
            self.write_log(f"死叉信号，做空: {self.symbol} @ {close_price}")

        # 金叉信号：短周期上穿长周期，做多
        elif (prev_short < prev_long and curr_short > curr_long and
              self.position <= 0):  # 当前无持仓或空头持仓

            if self.position < 0:  # 如果持有空头，先平仓
                self.cover(self.symbol, close_price, abs(self.position))
                self.write_log(f"金叉信号，平空仓: {self.symbol} @ {close_price}")

            # 开多仓
            self.buy(self.symbol, close_price, self.volume)
            self.position = self.volume
            self.write_log(f"金叉信号，做多: {self.symbol} @ {close_price}")

    def on_order(self, order_data: Dict[str, Any]):
        """订单更新回调"""
        order_status = order_data.get('status', '')
        order_id = order_data.get('order_id', '')
        self.write_log(f"订单更新: {order_id} - {order_status}")

    def on_trade(self, trade_data: Dict[str, Any]):
        """成交更新回调"""
        symbol = trade_data.get('symbol', '')
        volume = trade_data.get('volume', 0)
        price = trade_data.get('price', 0)
        self.write_log(f"成交: {symbol} {volume}手 @ {price}")


# 测试代码
if __name__ == "__main__":
    # 创建策略实例进行测试
    config = {
        "symbol": "SHFE.cu2401",
        "short_period": 30,
        "long_period": 60,
        "volume": 1
    }

    strategy = DoubleMa("test_strategy", config)

    # 测试数据
    test_bar = {
        'symbol': 'SHFE.cu2401',
        'datetime': 1704067200000000000,
        'open': 68020.0,
        'high': 68090.0,
        'low': 67970.0,
        'close': 68000.0,
        'volume': 1000
    }

    # 测试初始化
    strategy.on_init()
    strategy.on_start()

    # 测试数据处理
    strategy.on_bar(test_bar)

    strategy.on_stop()
    print("双均线策略测试完成")
