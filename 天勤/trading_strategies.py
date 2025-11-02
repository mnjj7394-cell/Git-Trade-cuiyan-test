#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
修复版交易策略模块
修复问题：super().start()调用错误
修改内容：修复策略启动方法，避免调用不存在的父类方法
"""

import asyncio
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

class BaseTradingStrategy(ABC):
    """交易策略基类 - 修复版本"""

    def __init__(self, name: str, config: Dict[str, Any] = None):
        self.name = name
        self.data_manager = None
        self.is_running = False
        self.config = config or {}
        self.orders_created = 0
        self.trades_executed = 0

    def set_data_manager(self, data_manager):
        """设置数据管理器引用"""
        self.data_manager = data_manager

    async def start(self):
        """启动策略 - 修复：不再调用不存在的super().start()"""
        self.is_running = True
        self._log(f"策略 {self.name} 已启动")
        # 策略特定的启动逻辑由子类实现
        await self.on_start()

    async def stop(self):
        """停止策略"""
        self.is_running = False
        self._log(f"策略 {self.name} 已停止")
        # 策略特定的停止逻辑由子类实现
        await self.on_stop()

    async def on_start(self):
        """策略启动时的自定义逻辑 - 子类可重写"""
        pass

    async def on_stop(self):
        """策略停止时的自定义逻辑 - 子类可重写"""
        pass

    @abstractmethod
    async def on_market_data(self, symbol: str, quote_data: Any):
        """行情数据回调 - 子类必须实现"""
        pass

    @abstractmethod
    async def on_order_update(self, order_data: Any):
        """订单更新回调 - 子类必须实现"""
        pass

    @abstractmethod
    async def on_trade_update(self, trade_data: Any):
        """成交更新回调 - 子类必须实现"""
        pass

    async def place_order(self, symbol: str, direction: str, volume: int, 
                         price_type: str = "LIMIT", price: float = None) -> Optional[str]:
        """下单接口"""
        if not self.data_manager:
            self._log("错误: 未设置数据管理器，无法下单", level="ERROR")
            return None

        try:
            order_id = await self.data_manager.place_order(
                symbol, direction, volume, price_type, price
            )
            return order_id
        except Exception as e:
            self._log(f"下单失败: {e}", level="ERROR")
            return None

    def _log(self, message: str, level: str = "INFO"):
        """日志记录"""
        if hasattr(self, 'data_manager') and self.data_manager and hasattr(self.data_manager, 'logger'):
            getattr(self.data_manager.logger, level.lower())(message)
        else:
            print(f"{level}: {message}")

class DoubleMaStrategy(BaseTradingStrategy):
    """双均线策略 - 修复版本"""

    def __init__(self, config: Dict[str, Any] = None):
        super().__init__("DoubleMaStrategy", config)
        self.short_period = self.config.get("short_period", 30)
        self.long_period = self.config.get("long_period", 60)
        self.symbol = self.config.get("symbol", "SHFE.bu2012")
        self.volume = self.config.get("volume", 1)

    async def on_market_data(self, symbol: str, quote_data: Any):
        """行情数据回调 - 实现双均线逻辑"""
        if not self.is_running or symbol != self.symbol:
            return

        # 简化的双均线逻辑
        try:
            # 这里应该计算MA值，但为简化测试，直接模拟
            if hasattr(quote_data, 'close'):
                # 模拟交易信号
                await self._check_trading_signal()
        except Exception as e:
            self._log(f"处理行情数据时出错: {e}", level="ERROR")

    async def on_order_update(self, order_data: Any):
        """订单更新回调"""
        if order_data.status == "FINISHED":
            self.orders_created += 1
            self._log(f"订单完成: {order_data.order_id}")

    async def on_trade_update(self, trade_data: Any):
        """成交更新回调"""
        self.trades_executed += 1
        self._log(f"成交更新: {trade_data.trade_id}")

    async def _check_trading_signal(self):
        """检查交易信号 - 简化版本"""
        # 模拟交易逻辑：每5次调用产生一次信号
        if self.orders_created % 5 == 0 and self.orders_created < 10:
            direction = "BUY" if self.orders_created % 2 == 0 else "SELL"
            await self.place_order(self.symbol, direction, self.volume)
            self._log(f"产生交易信号: {self.symbol} {direction} {self.volume}手")

    async def on_start(self):
        """策略启动自定义逻辑"""
        self._log(f"双均线策略启动: 短周期{self.short_period}, 长周期{self.long_period}")

    async def on_stop(self):
        """策略停止自定义逻辑"""
        self._log(f"双均线策略停止，共创建{self.orders_created}个订单")

# 策略工厂函数
def create_strategy(strategy_name: str, config: Dict[str, Any] = None) -> BaseTradingStrategy:
    """创建策略实例"""
    strategy_map = {
        "double_ma": DoubleMaStrategy,
    }
    
    strategy_class = strategy_map.get(strategy_name, DoubleMaStrategy)
    return strategy_class(config)

# 策略配置
STRATEGY_CONFIGS = {
    "double_ma": {
        "short_period": 5,
        "long_period": 10,
        "volume": 1
    }
}

if __name__ == "__main__":
    # 测试策略
    async def test_strategy():
        strategy = DoubleMaStrategy({"short_period": 5, "long_period": 10})
        await strategy.start()
        await asyncio.sleep(1)
        await strategy.stop()
    
    asyncio.run(test_strategy())
