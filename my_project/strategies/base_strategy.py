"""
策略基类定义
所有具体策略都应继承自此基类
"""
from abc import ABC, abstractmethod
from typing import Dict, Any, List
from datetime import datetime


class BaseStrategy(ABC):
    """策略基类"""

    def __init__(self, name: str, config: Dict[str, Any] = None):
        """
        初始化策略

        Args:
            name: 策略名称
            config: 策略配置参数
        """
        self.name = name
        self.config = config or {}
        self.inited = False
        self.trading = False

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
        """策略初始化回调"""
        self.inited = True
        self.write_log(f"策略 {self.name} 初始化完成")

    def on_start(self):
        """策略启动回调"""
        self.trading = True
        self.write_log(f"策略 {self.name} 启动")

    def on_stop(self):
        """策略停止回调"""
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
        """订单更新回调"""
        pass

    def on_trade(self, trade_data: Dict[str, Any]):
        """成交更新回调"""
        pass

    def buy(self, symbol: str, price: float, volume: int):
        """买入开仓"""
        if self.engine:
            return self.engine.buy(symbol, price, volume, self.name)
        return None

    def sell(self, symbol: str, price: float, volume: int):
        """卖出平仓"""
        if self.engine:
            return self.engine.sell(symbol, price, volume, self.name)
        return None

    def short(self, symbol: str, price: float, volume: int):
        """卖出开仓"""
        if self.engine:
            return self.engine.short(symbol, price, volume, self.name)
        return None

    def cover(self, symbol: str, price: float, volume: int):
        """买入平仓"""
        if self.engine:
            return self.engine.cover(symbol, price, volume, self.name)
        return None

    def write_log(self, msg: str):
        """输出日志"""
        if self.engine:
            self.engine.write_log(f"[{self.name}] {msg}")
        else:
            print(f"[{datetime.now()}] [{self.name}] {msg}")

    def get_position(self, symbol: str = None):
        """获取持仓"""
        if self.engine:
            return self.engine.get_position(symbol, self.name)
        return None

    def get_account(self):
        """获取账户"""
        if self.engine:
            return self.engine.get_account()
        return None
