"""
改进的策略基类
提供统一的数据接口和增强的错误处理
"""
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
from datetime import datetime
from core.data_adapter import DataAdapter  # 修改处：修正导入路径，从core包导入


class BaseStrategy(ABC):
    """策略基类（增强数据接口和错误处理）"""

    def __init__(self, name: str, config: Dict[str, Any] = None):
        self.name = name
        self.config = config or {}
        self.inited = False
        self.trading = False
        self.adapter = DataAdapter()  # 数据适配器

        # 策略组件（由引擎注入）
        self.engine = None
        self.event_engine = None
        self.data_manager = None

    def set_engine(self, engine):
        """设置策略引擎"""
        self.engine = engine

    def set_event_engine(self, event_engine):
        """设置事件引擎"""
        self.event_engine = event_engine

    def set_data_manager(self, data_manager):
        """设置数据管理器"""
        self.data_manager = data_manager

    def on_init(self):
        """策略初始化回调（增强初始化逻辑）"""
        self.inited = True
        self.write_log(f"策略 {self.name} 初始化完成")

    def on_start(self):
        """策略启动回调"""
        self.trading = True
        self.write_log(f"策略 {self.name} 启动")

    def on_stop(self):
        """策略停止回调（增强资源清理）"""
        self.trading = False
        self.write_log(f"策略 {self.name} 停止")

    @abstractmethod
    def on_tick(self, tick_data: Dict[str, Any]):
        """Tick数据回调（必须实现）"""
        pass

    @abstractmethod
    def on_bar(self, bar_data: Dict[str, Any]):
        """K线数据回调（必须实现）"""
        pass

    def on_order(self, order_data: Dict[str, Any]):
        """订单更新回调（增强错误处理）"""
        try:
            order_status = order_data.get('status', '')
            order_id = order_data.get('order_id', '')
            self.write_log(f"订单状态更新: {order_id} - {order_status}")
        except Exception as e:
            self.write_log(f"处理订单更新时发生异常: {e}")

    def on_trade(self, trade_data: Dict[str, Any]):
        """成交更新回调（增强错误处理）"""
        try:
            symbol = trade_data.get('symbol', '')
            volume = trade_data.get('volume', 0)
            price = trade_data.get('price', 0)
            self.write_log(f"成交记录: {symbol} {volume}手 @ {price}")
        except Exception as e:
            self.write_log(f"处理成交记录时发生异常: {e}")

    # 交易接口（增强参数验证）
    def buy(self, symbol: str, price: float, volume: int) -> Optional[str]:
        """买入开仓（增强参数验证）"""
        if not self._validate_trade_params(symbol, price, volume):
            return None

        if self.engine:
            return self.engine.buy(symbol, price, volume, self.name)
        return None

    def sell(self, symbol: str, price: float, volume: int) -> Optional[str]:
        """卖出平仓（增强参数验证）"""
        if not self._validate_trade_params(symbol, price, volume):
            return None

        if self.engine:
            return self.engine.sell(symbol, price, volume, self.name)
        return None

    def short(self, symbol: str, price: float, volume: int) -> Optional[str]:
        """卖出开仓（增强参数验证）"""
        if not self._validate_trade_params(symbol, price, volume):
            return None

        if self.engine:
            return self.engine.short(symbol, price, volume, self.name)
        return None

    def cover(self, symbol: str, price: float, volume: int) -> Optional[str]:
        """买入平仓（增强参数验证）"""
        if not self._validate_trade_params(symbol, price, volume):
            return None

        if self.engine:
            return self.engine.cover(symbol, price, volume, self.name)
        return None

    def _validate_trade_params(self, symbol: str, price: float, volume: int) -> bool:
        """验证交易参数有效性"""
        if not symbol or not isinstance(symbol, str):
            self.write_log("错误: 交易品种不能为空")
            return False

        if price <= 0:
            self.write_log("错误: 交易价格必须大于0")
            return False

        if volume <= 0:
            self.write_log("错误: 交易数量必须大于0")
            return False

        return True

    # 数据工具方法
    def extract_symbol(self, data: Dict[str, Any]) -> str:
        """从数据中提取品种符号（兼容多种格式）"""
        return self.adapter.extract_core_data(data).get('symbol', '')

    def extract_price(self, data: Dict[str, Any]) -> float:
        """从数据中提取价格（兼容多种格式）"""
        core_data = self.adapter.extract_core_data(data)
        return core_data.get('close', core_data.get('last_price', 0.0))

    def validate_data(self, data: Dict[str, Any]) -> bool:
        """验证数据有效性"""
        return self.adapter.validate_data_format(data)

    def get_position(self, symbol: str = None) -> Dict[str, Any]:
        """获取持仓（增强错误处理）"""
        if self.engine:
            try:
                return self.engine.get_position(symbol, self.name)
            except Exception as e:
                self.write_log(f"获取持仓时发生异常: {e}")
        return {}

    def get_account(self) -> Dict[str, Any]:
        """获取账户（增强错误处理）"""
        if self.engine:
            try:
                return self.engine.get_account()
            except Exception as e:
                self.write_log(f"获取账户时发生异常: {e}")
        return {}

    def write_log(self, msg: str):
        """输出日志（增强日志格式）"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_msg = f"[{timestamp}] [{self.name}] {msg}"

        if self.engine:
            self.engine.write_log(log_msg)
        else:
            print(log_msg)

    def safe_execute(self, func, *args, **kwargs):
        """安全执行函数，捕获异常"""
        try:
            return func(*args, **kwargs)
        except Exception as e:
            self.write_log(f"执行函数 {func.__name__} 时发生异常: {e}")
            return None


# 测试代码
if __name__ == "__main__":
    # 测试基类功能
    class TestStrategy(BaseStrategy):
        def on_tick(self, tick_data):
            self.write_log(f"处理Tick数据: {tick_data}")

        def on_bar(self, bar_data):
            self.write_log(f"处理Bar数据: {bar_data}")

    # 创建测试策略
    strategy = TestStrategy("test_strategy")

    # 测试方法
    strategy.on_init()
    strategy.on_start()

    # 测试数据验证
    test_data = {'symbol': 'TEST', 'close': 100.0}
    print("数据验证结果:", strategy.validate_data(test_data))

    # 测试交易参数验证
    print("参数验证结果:", strategy._validate_trade_params("SYMBOL", 100.0, 1))

    strategy.on_stop()
    print("策略基类测试完成")
