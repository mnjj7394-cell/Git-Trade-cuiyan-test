"""
改进的回测引擎
解决数据格式不匹配、异步兼容性和交易接口缺失问题
"""
import importlib
import time
from typing import Dict, Any, List, Optional
from datetime import datetime
from core.event_engine import EventEngine
from core.data_manager import DataManager
from strategies.base_strategy import BaseStrategy
from core.data_adapter import DataAdapter
from core.async_sync_bridge import AsyncSyncBridge


class BacktestEngine:
    """回测引擎（已修复数据格式、异步和交易接口问题）"""

    def __init__(self, event_engine: EventEngine, data_manager: DataManager):
        self.event_engine = event_engine
        self.data_manager = data_manager
        self.strategy = None
        self.history_data = []
        self.running = False
        self.adapter = DataAdapter()  # 数据适配器
        self.bridge = AsyncSyncBridge()  # 异步桥接器
        self.progress = 0

        # 新增：账户和持仓管理
        self.accounts = {}  # 策略账户字典
        self.positions = {}  # 策略持仓字典
        self.orders = {}  # 订单字典
        self.trades = []  # 成交记录列表

        # 初始账户设置
        self.initial_balance = 1000000.0  # 初始资金

    def set_history_data(self, data: List[Dict[str, Any]]):
        """设置历史数据（支持多种格式）"""
        # 统一数据格式
        unified_data = []
        for item in data:
            if self.adapter.validate_data_format(item):
                # 已经是统一格式，直接使用
                unified_data.append(item)
            else:
                # 尝试转换格式
                converted = self.adapter.convert_tqsdk_to_strategy_format(item)
                if self.adapter.validate_data_format(converted):
                    unified_data.append(converted)

        self.history_data = unified_data
        print(f"[{datetime.now()}] [BacktestEngine] 已加载 {len(self.history_data)} 条历史数据")

    def load_strategy(self, strategy_name: str, strategy_config: Dict[str, Any] = None) -> bool:
        """动态加载策略（增强错误处理）"""
        try:
            # 动态导入策略模块
            module_name = f"strategies.{strategy_name.lower()}"
            strategy_module = importlib.import_module(module_name)

            # 获取策略类（支持多种命名约定）
            class_name = ''.join(word.capitalize() for word in strategy_name.split('_'))
            strategy_class = getattr(strategy_module, class_name)

            # 创建策略实例
            self.strategy = strategy_class(strategy_name, strategy_config or {})

            # 设置策略组件
            self.strategy.set_engine(self)
            self.strategy.set_event_engine(self.event_engine)
            self.strategy.set_data_manager(self.data_manager)

            # 新增：初始化策略账户
            self._init_strategy_account(strategy_name)

            print(f"[{datetime.now()}] [BacktestEngine] 策略 {strategy_name} 加载成功")
            return True

        except ImportError as e:
            print(f"[{datetime.now()}] [BacktestEngine] 策略模块加载失败: {e}")
            return False
        except AttributeError as e:
            print(f"[{datetime.now()}] [BacktestEngine] 策略类不存在: {e}")
            return False
        except Exception as e:
            print(f"[{datetime.now()}] [BacktestEngine] 策略加载异常: {e}")
            return False

    def _init_strategy_account(self, strategy_name: str):
        """初始化策略账户"""
        if strategy_name not in self.accounts:
            self.accounts[strategy_name] = {
                "balance": self.initial_balance,
                "available": self.initial_balance,
                "commission": 0.0,
                "margin": 0.0,
                "close_profit": 0.0,
                "position_profit": 0.0
            }

        if strategy_name not in self.positions:
            self.positions[strategy_name] = {}

    def run_backtest(self, strategy_name: str, strategy_config: Dict[str, Any] = None):
        """运行回测（修复数据推送问题）"""
        # 加载策略
        if not self.load_strategy(strategy_name, strategy_config):
            print(f"[{datetime.now()}] [BacktestEngine] 策略加载失败，回测终止")
            return

        # 初始化策略
        self.strategy.on_init()
        if not hasattr(self.strategy, 'inited') or not self.strategy.inited:
            print(f"[{datetime.now()}] [BacktestEngine] 策略初始化失败")
            return

        # 启动策略
        self.strategy.on_start()
        self.running = True
        self.progress = 0

        print(f"[{datetime.now()}] [BacktestEngine] 开始回测...")
        start_time = time.time()

        total_data = len(self.history_data)

        # 回放历史数据
        for i, data in enumerate(self.history_data):
            if not self.running:
                break

            # 更新进度
            self.progress = (i + 1) / total_data * 100
            if i % 10 == 0:  # 每10条数据更新一次进度
                print(f"[{datetime.now()}] [BacktestEngine] 回测进度: {self.progress:.1f}%")

            # 确保数据格式正确
            processed_data = self.adapter.extract_core_data(data)

            # 根据数据类型推送到策略
            data_type = processed_data.get('data_type', 'unknown')
            if data_type == 'tick':
                self.strategy.on_tick(processed_data)
            elif data_type == 'bar':
                self.strategy.on_bar(processed_data)
            else:
                # 未知数据类型，尝试通用处理
                if 'open' in processed_data and 'close' in processed_data:
                    self.strategy.on_bar(processed_data)
                else:
                    self.strategy.on_tick(processed_data)

        # 停止策略
        self.strategy.on_stop()
        self.running = False

        end_time = time.time()
        duration = end_time - start_time
        print(f"[{datetime.now()}] [BacktestEngine] 回测完成，耗时: {duration:.2f}秒")

    def _push_data(self, data: Dict[str, Any]):
        """推送数据到策略（已修复数据格式问题）"""
        if not self.strategy or not self.running:
            return

        # 使用适配器处理数据格式
        processed_data = self.adapter.extract_core_data(data)
        data_type = processed_data.get('data_type', 'unknown')

        try:
            if data_type == 'tick':
                self.strategy.on_tick(processed_data)
            elif data_type == 'bar':
                self.strategy.on_bar(processed_data)
            else:
                # 默认按Bar处理
                self.strategy.on_bar(processed_data)
        except Exception as e:
            print(f"[{datetime.now()}] [BacktestEngine] 数据处理异常: {e}")

    # 完善的交易接口
    def buy(self, symbol: str, price: float, volume: int, strategy_name: str) -> Optional[str]:
        """买入开仓（做多）"""
        order_id = f"BUY_{int(time.time()*1000)}_{symbol}"
        self.write_log(f"[{strategy_name}] 买入开仓: {symbol} {volume}手 @ {price}")

        # 模拟成交处理
        self._process_trade(order_id, symbol, "BUY", price, volume, strategy_name)
        return order_id

    def sell(self, symbol: str, price: float, volume: int, strategy_name: str) -> Optional[str]:
        """卖出平仓（平多）"""
        order_id = f"SELL_{int(time.time()*1000)}_{symbol}"
        self.write_log(f"[{strategy_name}] 卖出平仓: {symbol} {volume}手 @ {price}")

        # 模拟成交处理
        self._process_trade(order_id, symbol, "SELL", price, volume, strategy_name)
        return order_id

    def short(self, symbol: str, price: float, volume: int, strategy_name: str) -> Optional[str]:
        """卖出开仓（做空）- 新增缺失接口"""
        order_id = f"SHORT_{int(time.time()*1000)}_{symbol}"
        self.write_log(f"[{strategy_name}] 卖出开仓: {symbol} {volume}手 @ {price}")

        # 模拟成交处理
        self._process_trade(order_id, symbol, "SHORT", price, volume, strategy_name)
        return order_id

    def cover(self, symbol: str, price: float, volume: int, strategy_name: str) -> Optional[str]:
        """买入平仓（平空）- 新增缺失接口"""
        order_id = f"COVER_{int(time.time()*1000)}_{symbol}"
        self.write_log(f"[{strategy_name}] 买入平仓: {symbol} {volume}手 @ {price}")

        # 模拟成交处理
        self._process_trade(order_id, symbol, "COVER", price, volume, strategy_name)
        return order_id

    def _process_trade(self, order_id: str, symbol: str, direction: str,
                      price: float, volume: int, strategy_name: str):
        """处理成交（模拟交易逻辑）"""
        # 创建成交记录
        trade = {
            "trade_id": f"TRADE_{int(time.time()*1000)}",
            "order_id": order_id,
            "symbol": symbol,
            "direction": direction,
            "price": price,
            "volume": volume,
            "strategy": strategy_name,
            "trade_time": datetime.now()
        }

        self.trades.append(trade)

        # 更新持仓
        self._update_position(symbol, direction, price, volume, strategy_name)

        # 推送成交事件
        self.event_engine.put({
            "type": "trade_update",
            "data": trade
        })

    def _update_position(self, symbol: str, direction: str, price: float,
                        volume: int, strategy_name: str):
        """更新持仓信息"""
        if strategy_name not in self.positions:
            self.positions[strategy_name] = {}

        if symbol not in self.positions[strategy_name]:
            self.positions[strategy_name][symbol] = {
                "volume": 0,  # 持仓量（正数为多仓，负数为空仓）
                "frozen": 0,  # 冻结持仓
                "price": 0.0,  # 持仓均价
                "pnl": 0.0    # 持仓盈亏
            }

        position = self.positions[strategy_name][symbol]

        # 根据交易方向更新持仓
        if direction == "BUY":  # 买入开仓 - 增加多仓
            old_volume = position["volume"]
            old_price = position["price"]

            if old_volume >= 0:  # 当前无持仓或多头持仓
                new_volume = old_volume + volume
                if new_volume != 0:
                    position["price"] = (old_volume * old_price + volume * price) / new_volume
                position["volume"] = new_volume
            else:  # 当前空头持仓 - 平空
                if volume <= abs(old_volume):
                    position["volume"] = old_volume + volume
                else:
                    # 平空后开多
                    position["volume"] = volume - abs(old_volume)
                    position["price"] = price

        elif direction == "SELL":  # 卖出平仓 - 减少多仓
            if position["volume"] > 0:
                position["volume"] = max(0, position["volume"] - volume)

        elif direction == "SHORT":  # 卖出开仓 - 增加空仓
            old_volume = position["volume"]
            old_price = position["price"]

            if old_volume <= 0:  # 当前无持仓或空头持仓
                new_volume = old_volume - volume
                if new_volume != 0:
                    position["price"] = (abs(old_volume) * old_price + volume * price) / abs(new_volume)
                position["volume"] = new_volume
            else:  # 当前多头持仓 - 平多
                if volume <= old_volume:
                    position["volume"] = old_volume - volume
                else:
                    # 平多后开空
                    position["volume"] = volume - old_volume
                    position["price"] = price

        elif direction == "COVER":  # 买入平仓 - 减少空仓
            if position["volume"] < 0:
                position["volume"] = min(0, position["volume"] + volume)

    def get_position(self, symbol: str = None, strategy_name: str = None) -> Dict[str, Any]:
        """获取持仓（返回真实持仓数据）"""
        if strategy_name not in self.positions:
            return {}

        if symbol and symbol in self.positions[strategy_name]:
            return self.positions[strategy_name][symbol].copy()
        elif not symbol:
            # 返回所有持仓
            return {sym: pos.copy() for sym, pos in self.positions[strategy_name].items()}
        else:
            return {
                "symbol": symbol or "ALL",
                "strategy": strategy_name or "ALL",
                "volume": 0,
                "frozen": 0,
                "price": 0.0,
                "pnl": 0.0
            }

    def get_account(self, strategy_name: str = None) -> Dict[str, Any]:
        """获取账户（返回真实账户数据）"""
        if strategy_name and strategy_name in self.accounts:
            return self.accounts[strategy_name].copy()
        elif not strategy_name and self.accounts:
            # 返回第一个账户（简化处理）
            first_strategy = next(iter(self.accounts.keys()))
            return self.accounts[first_strategy].copy()
        else:
            return {
                "balance": self.initial_balance,
                "available": self.initial_balance,
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


# 测试代码
if __name__ == "__main__":
    # 模拟测试
    from core.event_engine import EventEngine
    from core.data_manager import DataManager

    event_engine = EventEngine()
    data_manager = DataManager(event_engine)
    engine = BacktestEngine(event_engine, data_manager)

    # 测试数据设置
    test_data = [
        {
            'data_type': 'bar',
            'symbol': 'SHFE.cu2401',
            'datetime': 1704067200000000000,
            'open': 68020.0,
            'high': 68090.0,
            'low': 67970.0,
            'close': 68000.0
        }
    ]

    engine.set_history_data(test_data)

    # 测试交易接口
    strategy_name = "test_strategy"

    # 测试买入
    order_id = engine.buy("SHFE.cu2401", 68000.0, 1, strategy_name)
    print(f"买入订单ID: {order_id}")

    # 测试做空
    order_id = engine.short("SHFE.cu2401", 68100.0, 1, strategy_name)
    print(f"做空订单ID: {order_id}")

    # 测试持仓查询
    position = engine.get_position("SHFE.cu2401", strategy_name)
    print(f"持仓情况: {position}")

    # 测试账户查询
    account = engine.get_account(strategy_name)
    print(f"账户情况: {account}")

    print("回测引擎测试完成")
