"""
改进的回测引擎
集成财务计算引擎和订单生命周期管理
"""
import asyncio
import time
from typing import Dict, Any, List, Optional
from datetime import datetime
from core.event_engine import EventEngine
from core.data_manager import DataManager
from core.thread_safe_manager import thread_safe_manager
from core.accounting_engine import AccountingEngine  # 新增财务计算引擎
from core.order_lifecycle_manager import OrderLifecycleManager  # 新增订单生命周期管理
from core.data_sync_service import DataSyncService  # 新增数据同步服务


class BacktestEngine:
    """回测引擎（已集成核心服务）"""

    def __init__(self, event_engine: EventEngine, data_manager: DataManager):
        self.event_engine = event_engine
        self.data_manager = data_manager
        self.strategy = None
        self.history_data = []
        self.running = False
        self.progress = 0
        self.current_prices = {}  # 存储当前价格

        # 集成核心服务
        self.accounting_engine = AccountingEngine()  # 财务计算引擎
        self.order_manager = OrderLifecycleManager()  # 订单生命周期管理
        self.sync_service = DataSyncService()  # 数据同步服务
        self.thread_safe_manager = thread_safe_manager

        # 回测统计
        self.backtest_stats = {
            "start_time": None,
            "end_time": None,
            "total_trades": 0,
            "total_orders": 0,
            "total_errors": 0
        }

    def set_history_data(self, data: List[Dict[str, Any]]):
        """设置历史数据（线程安全）"""
        with self.thread_safe_manager.locked_resource("history_data_setup"):
            self.history_data = data
            print(f"[{datetime.now()}] [BacktestEngine] 已加载 {len(self.history_data)} 条历史数据")

    def load_strategy(self, strategy_name: str, strategy_config: Dict[str, Any] = None) -> bool:
        """加载策略（集成服务初始化）"""
        with self.thread_safe_manager.locked_resource("strategy_loading"):
            try:
                import importlib
                strategy_module = importlib.import_module(f"strategies.{strategy_name}")
                class_name = ''.join(word.capitalize() for word in strategy_name.split('_'))
                strategy_class = getattr(strategy_module, class_name)

                self.strategy = strategy_class(strategy_name, strategy_config or {})
                self.strategy.set_engine(self)
                self.strategy.set_event_engine(self.event_engine)
                self.strategy.set_data_manager(self.data_manager)

                # 初始化策略账户
                self._initialize_strategy_account(strategy_name)

                print(f"[{datetime.now()}] [BacktestEngine] 策略 {strategy_name} 加载成功")
                return True

            except Exception as e:
                print(f"[{datetime.now()}] [BacktestEngine] 策略加载失败: {e}")
                return False

    def _initialize_strategy_account(self, strategy_name: str):
        """初始化策略账户（集成财务引擎）"""
        initial_balance = 1000000.0  # 初始资金

        # 通过财务引擎初始化账户
        account_data = {
            "balance": initial_balance,
            "available": initial_balance,
            "commission": 0.0,
            "margin": 0.0,
            "close_profit": 0.0,
            "position_profit": 0.0,
            "initial_balance": initial_balance
        }

        # 更新账户表
        account_table = self.data_manager.get_table("account")
        if account_table:
            account_table.update(account_data)

        print(f"[{datetime.now()}] [BacktestEngine] 策略账户初始化完成: {strategy_name}")

    def run_backtest(self, strategy_name: str, strategy_config: Dict[str, Any] = None):
        """运行回测（集成核心服务）"""
        with self.thread_safe_manager.locked_resource("backtest_execution"):
            # 记录开始时间
            self.backtest_stats["start_time"] = datetime.now()

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
                if i % 10 == 0:
                    print(f"[{datetime.now()}] [BacktestEngine] 回测进度: {self.progress:.1f}%")

                # 处理数据
                self._process_data_point(data, i)

                # 定期执行数据同步检查
                if i % 50 == 0:
                    self._perform_data_sync()

            # 停止策略
            self.strategy.on_stop()
            self.running = False

            # 记录结束时间
            self.backtest_stats["end_time"] = datetime.now()

            # 最终数据同步
            self._perform_final_sync()

            end_time = time.time()
            duration = end_time - start_time
            print(f"[{datetime.now()}] [BacktestEngine] 回测完成，耗时: {duration:.2f}秒")

            # 生成回测报告
            self._generate_backtest_report()

    def _process_data_point(self, data: Dict[str, Any], index: int):
        """处理单个数据点（集成订单和财务处理）"""
        try:
            # 更新当前价格
            symbol = data.get('symbol')
            if symbol:
                self.current_prices[symbol] = data.get('close', data.get('price', 0))

            # 推送到策略
            processed_data = self.data_manager.adapter.extract_core_data(data)
            data_type = processed_data.get('data_type', 'unknown')

            if data_type == 'tick':
                self.strategy.on_tick(processed_data)
            elif data_type == 'bar':
                self.strategy.on_bar(processed_data)
            else:
                if 'open' in processed_data and 'close' in processed_data:
                    self.strategy.on_bar(processed_data)
                else:
                    self.strategy.on_tick(processed_data)

            # 更新回测统计
            self.backtest_stats["total_orders"] = self.order_manager.get_order_statistics().get("total_orders", 0)

        except Exception as e:
            self.backtest_stats["total_errors"] += 1
            print(f"[{datetime.now()}] [BacktestEngine] 数据处理异常: {e}")

    def buy(self, symbol: str, price: float, volume: int, order_type: str = "LIMIT") -> str:
        """买入开仓（策略接口）"""
        with self.thread_safe_manager.locked_resource("order_execution"):
            try:
                # 创建订单
                order_id = self.order_manager.create_order(
                    symbol, "BUY", price, volume,
                    self.strategy.name if self.strategy else "unknown",
                    order_type
                )

                # 计算手续费
                commission = self.accounting_engine.calculate_commission(symbol, price, volume, "BUY")

                # 创建成交记录
                trade_id = self._create_trade(symbol, "BUY", price, volume, order_id, commission)

                # 更新持仓
                self._update_position(symbol, "BUY", price, volume, trade_id)

                # 更新账户
                self._update_account("BUY", price, volume, commission)

                self.backtest_stats["total_trades"] += 1
                self.write_log(f"买入开仓: {symbol} {volume}手 @ {price}")

                return order_id

            except Exception as e:
                self.write_log(f"买入开仓失败: {e}")
                return ""

    def sell(self, symbol: str, price: float, volume: int, order_type: str = "LIMIT") -> str:
        """卖出平仓（策略接口）"""
        with self.thread_safe_manager.locked_resource("order_execution"):
            try:
                # 创建订单
                order_id = self.order_manager.create_order(
                    symbol, "SELL", price, volume,
                    self.strategy.name if self.strategy else "unknown",
                    order_type
                )

                # 计算手续费
                commission = self.accounting_engine.calculate_commission(symbol, price, volume, "SELL")

                # 创建成交记录
                trade_id = self._create_trade(symbol, "SELL", price, volume, order_id, commission)

                # 更新持仓
                self._update_position(symbol, "SELL", price, volume, trade_id)

                # 更新账户
                self._update_account("SELL", price, volume, commission)

                self.backtest_stats["total_trades"] += 1
                self.write_log(f"卖出平仓: {symbol} {volume}手 @ {price}")

                return order_id

            except Exception as e:
                self.write_log(f"卖出平仓失败: {e}")
                return ""

    def short(self, symbol: str, price: float, volume: int, order_type: str = "LIMIT") -> str:
        """卖出开仓（策略接口）"""
        with self.thread_safe_manager.locked_resource("order_execution"):
            try:
                # 创建订单
                order_id = self.order_manager.create_order(
                    symbol, "SHORT", price, volume,
                    self.strategy.name if self.strategy else "unknown",
                    order_type
                )

                # 计算手续费
                commission = self.accounting_engine.calculate_commission(symbol, price, volume, "SHORT")

                # 创建成交记录
                trade_id = self._create_trade(symbol, "SHORT", price, volume, order_id, commission)

                # 更新持仓
                self._update_position(symbol, "SHORT", price, volume, trade_id)

                # 更新账户
                self._update_account("SHORT", price, volume, commission)

                self.backtest_stats["total_trades"] += 1
                self.write_log(f"卖出开仓: {symbol} {volume}手 @ {price}")

                return order_id

            except Exception as e:
                self.write_log(f"卖出开仓失败: {e}")
                return ""

    def cover(self, symbol: str, price: float, volume: int, order_type: str = "LIMIT") -> str:
        """买入平仓（策略接口）"""
        with self.thread_safe_manager.locked_resource("order_execution"):
            try:
                # 创建订单
                order_id = self.order_manager.create_order(
                    symbol, "COVER", price, volume,
                    self.strategy.name if self.strategy else "unknown",
                    order_type
                )

                # 计算手续费
                commission = self.accounting_engine.calculate_commission(symbol, price, volume, "COVER")

                # 创建成交记录
                trade_id = self._create_trade(symbol, "COVER", price, volume, order_id, commission)

                # 更新持仓
                self._update_position(symbol, "COVER", price, volume, trade_id)

                # 更新账户
                self._update_account("COVER", price, volume, commission)

                self.backtest_stats["total_trades"] += 1
                self.write_log(f"买入平仓: {symbol} {volume}手 @ {price}")

                return order_id

            except Exception as e:
                self.write_log(f"买入平仓失败: {e}")
                return ""

    def _create_trade(self, symbol: str, direction: str, price: float, volume: int,
                     order_id: str, commission: float) -> str:
        """创建成交记录"""
        trade_table = self.data_manager.get_table("trade")
        if trade_table:
            return trade_table.add_trade(
                symbol, direction, price, volume,
                self.strategy.name if self.strategy else "unknown",
                order_id, commission
            )
        return ""

    def _update_position(self, symbol: str, direction: str, price: float, volume: int, trade_id: str):
        """更新持仓"""
        position_table = self.data_manager.get_table("position")
        if position_table:
            position_table.update_position(
                symbol,
                self.strategy.name if self.strategy else "unknown",
                direction, price, volume, trade_id
            )

    def _update_account(self, direction: str, price: float, volume: int, commission: float):
        """更新账户"""
        account_table = self.data_manager.get_table("account")
        if account_table:
            # 计算保证金
            margin = self.accounting_engine.calculate_margin("SHFE.cu2401", price, volume)

            # 更新账户余额
            account_table.update_balance(-commission, f"{direction}手续费")

            # 更新保证金
            if direction in ["BUY", "SHORT"]:  # 开仓需要占用保证金
                account_table.update({"margin": account_table.get_account().get("margin", 0) + margin})
            else:  # 平仓释放保证金
                account_table.update({"margin": account_table.get_account().get("margin", 0) - margin})

    def _perform_data_sync(self):
        """执行数据同步检查"""
        try:
            account_table = self.data_manager.get_table("account")
            order_table = self.data_manager.get_table("order")
            position_table = self.data_manager.get_table("position")
            trade_table = self.data_manager.get_table("trade")

            if all([account_table, order_table, position_table, trade_table]):
                sync_success = self.sync_service.sync_data_tables(
                    account_table, order_table, position_table, trade_table
                )

                if not sync_success:
                    print(f"[{datetime.now()}] [BacktestEngine] 数据同步检查发现不一致")

        except Exception as e:
            print(f"[{datetime.now()}] [BacktestEngine] 数据同步失败: {e}")

    def _perform_final_sync(self):
        """执行最终数据同步"""
        try:
            print(f"[{datetime.now()}] [BacktestEngine] 执行最终数据同步...")
            self._perform_data_sync()

            # 生成最终同步报告
            sync_report = self.sync_service.generate_sync_report()
            if sync_report and 'summary' in sync_report:
                print(f"[{datetime.now()}] [BacktestEngine] 最终同步报告: {sync_report['summary']}")
            else:
                print(f"[{datetime.now()}] [BacktestEngine] 最终同步报告生成失败")

        except Exception as e:
            print(f"[{datetime.now()}] [BacktestEngine] 最终数据同步失败: {e}")

    def _generate_backtest_report(self):
        """生成回测报告（集成财务统计）"""
        try:
            # 获取账户信息
            account_table = self.data_manager.get_table("account")
            account_data = account_table.get_account() if account_table else {}

            # 获取订单统计
            order_stats = self.order_manager.get_order_statistics()

            # 获取交易统计
            trade_table = self.data_manager.get_table("trade")
            trades = trade_table.get_all_trades() if trade_table else []

            # 使用财务引擎生成财务报表
            financial_report = self.accounting_engine.generate_financial_report(
                account_data, trades, []
            )

            # 合并回测统计
            report = {
                "backtest_info": {
                    "strategy_name": self.strategy.name if self.strategy else "Unknown",
                    "start_time": self.backtest_stats["start_time"],
                    "end_time": self.backtest_stats["end_time"],
                    "data_points": len(self.history_data),
                    "progress": self.progress
                },
                "performance_metrics": financial_report.get("trading_statistics", {}),
                "account_summary": financial_report.get("account_summary", {}),
                "order_statistics": order_stats,
                "error_statistics": {
                    "total_errors": self.backtest_stats["total_errors"]
                }
            }

            print(f"[{datetime.now()}] [BacktestEngine] 回测报告生成完成")
            self._print_report_summary(report)

            return report

        except Exception as e:
            print(f"[{datetime.now()}] [BacktestEngine] 回测报告生成失败: {e}")
            return {}

    def _print_report_summary(self, report: Dict[str, Any]):
        """打印回测报告摘要"""
        print("\n" + "="*60)
        print("回测报告摘要")
        print("="*60)

        # 基本信息
        info = report.get("backtest_info", {})
        print(f"策略名称: {info.get('strategy_name', 'N/A')}")
        print(f"回测期间: {info.get('start_time')} 到 {info.get('end_time')}")
        print(f"数据点数: {info.get('data_points', 0)}")

        # 账户信息
        account = report.get("account_summary", {})
        print(f"初始资金: {account.get('initial_balance', 0):,.2f}")
        print(f"最终权益: {account.get('current_balance', 0):,.2f}")
        print(f"总盈亏: {account.get('current_balance', 0) - account.get('initial_balance', 0):,.2f}")

        # 交易统计
        metrics = report.get("performance_metrics", {})
        print(f"总交易次数: {metrics.get('total_trades', 0)}")
        print(f"胜率: {metrics.get('win_rate', 0):.2%}")
        print(f"盈亏比: {metrics.get('profit_factor', 0):.2f}")

        print("="*60)

    def write_log(self, msg: str):
        """写入日志（策略调用的接口方法）"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_msg = f"[{timestamp}] [BacktestEngine] {msg}"
        print(log_msg)

    def stop(self):
        """停止回测（集成资源清理）"""
        with self.thread_safe_manager.locked_resource("backtest_stop"):
            self.running = False
            if self.strategy:
                self.strategy.on_stop()

            # 执行最终同步
            self._perform_final_sync()

            print(f"[{datetime.now()}] [BacktestEngine] 回测已停止")

    def get_backtest_status(self) -> Dict[str, Any]:
        """获取回测状态（集成服务状态）"""
        return {
            "running": self.running,
            "progress": self.progress,
            "backtest_stats": self.backtest_stats,
            "sync_status": self.sync_service.get_sync_status(),
            "order_stats": self.order_manager.get_order_statistics()
        }


# 测试代码
if __name__ == "__main__":
    from core.event_engine import EventEngine
    from core.data_manager import DataManager

    # 创建测试实例
    event_engine = EventEngine()
    data_manager = DataManager(event_engine)
    engine = BacktestEngine(event_engine, data_manager)

    # 测试交易接口
    test_symbol = "SHFE.cu2401"
    test_price = 68000.0
    test_volume = 2

    # 测试买入开仓
    order_id = engine.buy(test_symbol, test_price, test_volume)
    print(f"测试买入开仓订单ID: {order_id}")

    # 测试卖出平仓
    order_id = engine.sell(test_symbol, test_price + 500, test_volume)
    print(f"测试卖出平仓订单ID: {order_id}")

    # 测试卖出开仓
    order_id = engine.short(test_symbol, test_price, test_volume)
    print(f"测试卖出开仓订单ID: {order_id}")

    # 测试买入平仓
    order_id = engine.cover(test_symbol, test_price - 300, test_volume)
    print(f"测试买入平仓订单ID: {order_id}")

    # 测试回测状态
    status = engine.get_backtest_status()
    print("回测引擎状态:", status)

    # 测试数据同步
    engine._perform_data_sync()

    print("回测引擎测试完成")
