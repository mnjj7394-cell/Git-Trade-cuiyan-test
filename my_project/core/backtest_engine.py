"""
修改后的回测引擎
支持动态加载策略
"""
import importlib
import time
from typing import Dict, Any, List, Optional
from datetime import datetime
from strategies.base_strategy import BaseStrategy


class BacktestEngine:
    """回测引擎（支持策略动态加载）"""

    def __init__(self, event_engine, data_manager):
        self.event_engine = event_engine
        self.data_manager = data_manager
        self.strategy = None
        self.history_data = []
        self.running = False

    def load_strategy(self, strategy_name: str, strategy_config: Dict[str, Any] = None) -> bool:
        """
        动态加载策略

        Args:
            strategy_name: 策略类名（如"MovingAverageStrategy"）
            strategy_config: 策略配置参数

        Returns:
            bool: 加载是否成功
        """
        try:
            # 动态导入策略模块
            module_name = f"strategies.{strategy_name.lower()}"
            strategy_module = importlib.import_module(module_name)

            # 获取策略类（类名与文件名一致，但首字母大写）
            class_name = ''.join(word.capitalize() for word in strategy_name.split('_'))
            strategy_class = getattr(strategy_module, class_name)

            # 创建策略实例
            self.strategy = strategy_class(strategy_name, strategy_config)

            # 设置策略组件
            self.strategy.set_engine(self)
            self.strategy.set_event_engine(self.event_engine)
            self.strategy.set_data_manager(self.data_manager)

            self.write_log(f"策略 {strategy_name} 加载成功")
            return True

        except ImportError as e:
            self.write_log(f"策略模块加载失败: {e}")
            return False
        except AttributeError as e:
            self.write_log(f"策略类不存在: {e}")
            return False
        except Exception as e:
            self.write_log(f"策略加载异常: {e}")
            return False

    def set_history_data(self, data: List[Dict[str, Any]]):
        """设置历史数据"""
        self.history_data = data
        self.write_log(f"已加载 {len(data)} 条历史数据")

    def run_backtest(self, strategy_name: str, strategy_config: Dict[str, Any] = None):
        """
        运行回测

        Args:
            strategy_name: 策略名称
            strategy_config: 策略配置
        """
        # 加载策略
        if not self.load_strategy(strategy_name, strategy_config):
            self.write_log("策略加载失败，回测终止")
            return

        # 初始化策略
        self.strategy.on_init()
        if not self.strategy.inited:
            self.write_log("策略初始化失败")
            return

        # 启动策略
        self.strategy.on_start()
        self.running = True

        self.write_log("开始回测...")
        start_time = time.time()

        # 回放历史数据
        for i, data in enumerate(self.history_data):
            if not self.running:
                break

            # 模拟数据推送
            self._push_data(data)

            # 更新进度
            if i % 100 == 0:
                progress = (i + 1) / len(self.history_data) * 100
                self.write_log(f"回测进度: {progress:.1f}%")

        # 停止策略
        self.strategy.on_stop()
        self.running = False

        end_time = time.time()
        duration = end_time - start_time
        self.write_log(f"回测完成，耗时: {duration:.2f}秒")

    def _push_data(self, data: Dict[str, Any]):
        """推送数据到策略"""
        if not self.strategy or not self.strategy.trading:
            return

        data_type = data.get("data_type", "")

        if data_type == "tick":
            self.strategy.on_tick(data)
        elif data_type == "bar":
            self.strategy.on_bar(data)

    def buy(self, symbol: str, price: float, volume: int, strategy_name: str) -> Optional[str]:
        """买入委托（模拟）"""
        order_id = f"ORDER_{int(time.time() * 1000)}_{symbol}"
        self.write_log(f"[{strategy_name}] 买入委托: {symbol} {volume}手 @ {price}")

        # 模拟订单处理
        order_data = {
            "order_id": order_id,
            "symbol": symbol,
            "direction": "BUY",
            "price": price,
            "volume": volume,
            "strategy": strategy_name,
            "status": "ALLTRADED",
            "trade_time": datetime.now()
        }

        # 推送订单事件
        self.event_engine.put({
            "type": "order_update",
            "data": order_data
        })

        return order_id

    def sell(self, symbol: str, price: float, volume: int, strategy_name: str) -> Optional[str]:
        """卖出委托（模拟）"""
        order_id = f"ORDER_{int(time.time() * 1000)}_{symbol}"
        self.write_log(f"[{strategy_name}] 卖出委托: {symbol} {volume}手 @ {price}")

        order_data = {
            "order_id": order_id,
            "symbol": symbol,
            "direction": "SELL",
            "price": price,
            "volume": volume,
            "strategy": strategy_name,
            "status": "ALLTRADED",
            "trade_time": datetime.now()
        }

        self.event_engine.put({
            "type": "order_update",
            "data": order_data
        })

        return order_id

    def short(self, symbol: str, price: float, volume: int, strategy_name: str) -> Optional[str]:
        """卖空委托（模拟）"""
        order_id = f"ORDER_{int(time.time() * 1000)}_{symbol}"
        self.write_log(f"[{strategy_name}] 卖空委托: {symbol} {volume}手 @ {price}")

        order_data = {
            "order_id": order_id,
            "symbol": symbol,
            "direction": "SHORT",
            "price": price,
            "volume": volume,
            "strategy": strategy_name,
            "status": "ALLTRADED",
            "trade_time": datetime.now()
        }

        self.event_engine.put({
            "type": "order_update",
            "data": order_data
        })

        return order_id

    def cover(self, symbol: str, price: float, volume: int, strategy_name: str) -> Optional[str]:
        """平空委托（模拟）"""
        order_id = f"ORDER_{int(time.time() * 1000)}_{symbol}"
        self.write_log(f"[{strategy_name}] 平空委托: {symbol} {volume}手 @ {price}")

        order_data = {
            "order_id": order_id,
            "symbol": symbol,
            "direction": "COVER",
            "price": price,
            "volume": volume,
            "strategy": strategy_name,
            "status": "ALLTRADED",
            "trade_time": datetime.now()
        }

        self.event_engine.put({
            "type": "order_update",
            "data": order_data
        })

        return order_id

    def get_position(self, symbol: str = None, strategy_name: str = None) -> Dict[str, Any]:
        """获取持仓（模拟）"""
        # 返回模拟持仓数据
        return {
            "symbol": symbol or "ALL",
            "strategy": strategy_name or "ALL",
            "volume": 0,
            "frozen": 0,
            "price": 0.0,
            "pnl": 0.0
        }

    def get_account(self) -> Dict[str, Any]:
        """获取账户（模拟）"""
        # 返回模拟账户数据
        return {
            "balance": 1000000.0,
            "available": 1000000.0,
            "commission": 0.0,
            "margin": 0.0,
            "close_profit": 0.0,
            "position_profit": 0.0
        }

    def write_log(self, msg: str):
        """输出日志"""
        print(f"[{datetime.now()}] [BacktestEngine] {msg}")

    def stop(self):
        """停止回测"""
        self.running = False
        if self.strategy:
            self.strategy.on_stop()
        self.write_log("回测已停止")
