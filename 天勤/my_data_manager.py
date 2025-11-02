#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
数据管理器 - 天勤SDK自定义数据表项目
功能：作为数据中枢，连接天勤API并维护自定义数据表的同步更新
创建日期：2024年
"""

import asyncio
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime, time

from tqsdk import TqApi, TqAuth
from tqsdk.objs import Account, Position, Order, Trade, Quote

from config import CurrentConfig
from 天勤.account import MyAccount
from 天勤.position import MyPosition
from 天勤.order import MyOrder
from 天勤.trade import MyTrade

from typing import List, Dict, Any, Optional
from trading_strategies import BaseTradingStrategy  # 导入策略基类

# 在配置中添加
import sys
sys.stdout.reconfigure(encoding='utf-8')  # Python 3.7+
class MyDataManager:
    """
    自定义数据管理器
    核心功能：连接天勤API，实时同步数据到自定义数据表，并维护数据一致性
    """

    async def _wait_for_update(self, timeout: float = 5.0) -> bool:
        """
        等待数据更新（用于测试）- 修复版本

        Args:
            timeout: 超时时间（秒）

        Returns:
            bool: 是否收到更新
        """
        if not self.api:
            self.logger.warning("API未连接，无法等待更新")
            return False

        try:
            # 使用更可靠的方式等待更新
            start_time = time.time()
            initial_update_count = self.update_count

            while time.time() - start_time < timeout:
                # 尝试等待短期更新
                try:
                    await asyncio.wait_for(self.api.wait_update(), timeout=1.0)

                    # 检查是否有实际更新
                    if self.update_count > initial_update_count:
                        self.logger.debug(f"检测到数据更新，计数: {self.update_count}")
                        return True

                except asyncio.TimeoutError:
                    # 短期超时是正常的，继续循环
                    continue
                except Exception as e:
                    self.logger.warning(f"等待更新时发生错误: {e}")
                    return False

            self.logger.debug(f"等待更新超时，未收到新数据")
            return False

        except Exception as e:
            self.logger.error(f"等待更新过程异常: {e}")
            return False




    def __init__(self):
        """初始化数据管理器"""
        self.config = CurrentConfig
        self.api: Optional[TqApi] = None
        self.is_connected: bool = False
        self.logger = self._setup_logger()

        # 天勤原始数据对象
        self.tq_account: Optional[Account] = None
        self.tq_positions: Dict[str, Position] = {}  # key: symbol
        self.tq_orders: Dict[str, Order] = {}  # key: order_id
        self.tq_trades: Dict[str, Trade] = {}  # key: trade_id
        self.tq_quotes: Dict[str, Quote] = {}  # key: symbol

        # 自定义数据表对象
        self.my_account: Optional[MyAccount] = None
        self.my_positions: Dict[str, MyPosition] = {}  # key: vt_symbol.direction
        self.my_orders: Dict[str, MyOrder] = {}  # key: vt_orderid
        self.my_trades: Dict[str, MyTrade] = {}  # key: vt_tradeid

        # 状态跟踪
        self.update_count: int = 0
        self.last_update_time: Optional[datetime] = None
        self.error_count: int = 0

        self.trading_strategies: List[BaseTradingStrategy] = []  # 策略列表
        self.is_trading: bool = False  # 交易状态标志
        self.strategy_config: Dict[str, Any] = {}  # 策略配置

    # 在 __init__ 方法后添加
    def get_running_status(self):
        """获取当前运行状态"""
        return {
            'is_connected': self.is_connected,
            'update_count': self.update_count,
            'last_update_time': self.last_update_time,
            'error_count': self.error_count
        }

    def _setup_logger(self) -> logging.Logger:
        """设置日志记录器"""
        logger = logging.getLogger("MyDataManager")
        logger.setLevel(getattr(logging, self.config.LOG_LEVEL))

        if not logger.handlers:
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )

            # 控制台处理器
            if self.config.LOG_TO_CONSOLE:
                console_handler = logging.StreamHandler()
                console_handler.setFormatter(formatter)
                logger.addHandler(console_handler)

            # 文件处理器
            if self.config.LOG_TO_FILE:
                file_handler = logging.FileHandler(self.config.get_log_filepath())
                file_handler.setFormatter(formatter)
                logger.addHandler(file_handler)

        return logger


    async def connect(self) -> bool:
        """
        连接天勤API

        Returns:
            bool: 连接是否成功
        """
        try:
            self.logger.info("开始连接天勤API...")
            print(f"[{datetime.now().strftime('%H:%M:%S')}] 步骤1: 开始连接天勤API")  # 添加

            # 验证配置
            if not self.config.validate_config():
                self.logger.error("配置验证失败，无法连接API")
                return False

            auth = TqAuth(self.config.TQ_USERNAME, self.config.TQ_PASSWORD)
            print(f"[{datetime.now().strftime('%H:%M:%S')}] 步骤2: 创建TqApi实例")  # 添加
            self.api = TqApi(auth=auth)

            print(f"[{datetime.now().strftime('%H:%M:%S')}] 步骤3: 获取初始数据对象")  # 添加
            self.tq_account = self.api.get_account()
            self.tq_positions[self.config.TEST_SYMBOL] = self.api.get_position(
                self.config.TEST_SYMBOL
            )

            print(f"[{datetime.now().strftime('%H:%M:%S')}] 步骤4: 等待初始数据就绪")  # 添加
            self.api.wait_update()

            print(f"[{datetime.now().strftime('%H:%M:%S')}] 步骤5: 初始化自定义数据表")  # 添加
            self._initialize_custom_tables()

            self.is_connected = True
            self.logger.info("✓ 天勤API连接成功，自定义数据表初始化完成")
            print(f"[{datetime.now().strftime('%H:%M:%S')}] 连接成功，初始化完成")  # 添加
            return True

        except Exception as e:
            self.logger.error(f"连接天勤API失败: {e}")
            print(f"[{datetime.now().strftime('%H:%M:%S')}] 连接失败: {e}")  # 添加
            self.error_count += 1
            return False


    def _initialize_custom_tables(self):
        """初始化自定义数据表对象"""
        # 初始化账户表
        self.my_account = MyAccount(gateway_name=self.config.GATEWAY_NAME)
        self.my_account.update_from_tqsdk(self.tq_account)

        # 初始化持仓表（多头和空头分别创建）
        symbol = self.config.TEST_SYMBOL
        exchange = symbol.split(".")[0] if "." in symbol else "UNKNOWN"

        # 多头持仓
        long_position = MyPosition(
            gateway_name=self.config.GATEWAY_NAME,
            symbol=symbol,
            exchange=exchange,
            direction="LONG"
        )
        long_position.update_from_tqsdk(self.tq_positions[symbol])
        self.my_positions[f"{symbol}.LONG"] = long_position

        # 空头持仓
        short_position = MyPosition(
            gateway_name=self.config.GATEWAY_NAME,
            symbol=symbol,
            exchange=exchange,
            direction="SHORT"
        )
        short_position.update_from_tqsdk(self.tq_positions[symbol])
        self.my_positions[f"{symbol}.SHORT"] = short_position

        self.logger.info("自定义数据表初始化完成")

    async def start_data_sync(self) -> None:
        """
        启动数据同步循环
        持续监听天勤API的数据更新，并同步到自定义数据表
        """
        if not self.is_connected or not self.api:
            self.logger.error("未连接到天勤API，无法启动数据同步")
            return

        self.logger.info("启动数据同步循环...")

        try:
            async with self.api.register_update_notify() as update_chan:
                while self.is_connected:
                    try:
                        # 等待数据更新
                        await asyncio.wait_for(
                            update_chan.receive(),
                            timeout=self.config.DATA_UPDATE_INTERVAL
                        )

                        # 同步所有数据表
                        await self._sync_all_tables()

                        # 记录更新状态
                        self.update_count += 1
                        self.last_update_time = datetime.now()

                        # 定期日志输出
                        if self.update_count % 100 == 0:
                            self.logger.debug(f"数据同步次数: {self.update_count}")

                    except asyncio.TimeoutError:
                        # 超时是正常的，继续循环
                        continue
                    except Exception as e:
                        self.logger.error(f"数据同步过程中发生错误: {e}")
                        self.error_count += 1

        except Exception as e:
            self.logger.error(f"数据同步循环异常终止: {e}")
            self.is_connected = False

    async def _sync_all_tables(self):
        """同步所有数据表"""
        try:
            # 同步账户数据
            self._sync_account_data()

            # 同步持仓数据
            self._sync_position_data()

            # 同步订单数据
            self._sync_order_data()

            # 同步成交数据
            self._sync_trade_data()

            # 同步行情数据（可选）
            self._sync_quote_data()

        except Exception as e:
            self.logger.error(f"同步数据表时发生错误: {e}")
            self.error_count += 1

    def _sync_account_data(self):
        """同步账户数据"""
        if self.tq_account and self.my_account:
            self.my_account.update_from_tqsdk(self.tq_account)

            # 详细日志记录
            if self.config.ENABLE_DETAILED_LOGGING and self.update_count % 10 == 0:
                self.logger.debug(
                    f"账户同步 - 余额: {self.my_account.balance:.2f}, "
                    f"可用: {self.my_account.available:.2f}"
                )

    def _sync_position_data(self):
        """同步持仓数据"""
        for symbol, tq_position in self.tq_positions.items():
            if symbol not in self.tq_positions:
                continue

            # 更新多头持仓
            long_key = f"{symbol}.LONG"
            if long_key in self.my_positions:
                self.my_positions[long_key].update_from_tqsdk(tq_position)

            # 更新空头持仓
            short_key = f"{symbol}.SHORT"
            if short_key in self.my_positions:
                self.my_positions[short_key].update_from_tqsdk(tq_position)

            # 详细日志记录
            if (self.config.ENABLE_DETAILED_LOGGING and
                    self.update_count % 10 == 0 and
                    (self.my_positions[long_key].volume > 0 or
                     self.my_positions[short_key].volume > 0)):
                self.logger.debug(
                    f"持仓同步 - {symbol}: "
                    f"多头{self.my_positions[long_key].volume}手, "
                    f"空头{self.my_positions[short_key].volume}手"
                )

    def _sync_order_data(self):
        """同步订单数据"""
        # 获取天勤的所有订单
        orders_dict = self._get_tq_orders_dict()

        for order_id, tq_order in orders_dict.items():
            if order_id.startswith("_"):
                continue

            vt_orderid = f"{self.config.GATEWAY_NAME}.{order_id}"

            # 创建或更新自定义订单对象
            if vt_orderid not in self.my_orders:
                self.my_orders[vt_orderid] = MyOrder(
                    gateway_name=self.config.GATEWAY_NAME,
                    orderid=order_id
                )

            self.my_orders[vt_orderid].update_from_tqsdk(tq_order)

            # 记录新订单
            if self.update_count % 50 == 0 and self.my_orders[vt_orderid].status == "SUBMITTING":
                self.logger.info(f"新订单: {vt_orderid} - {tq_order.instrument_id}")

    def _sync_trade_data(self):
        """同步成交数据"""
        # 获取天勤的所有成交
        trades_dict = self._get_tq_trades_dict()

        for trade_id, tq_trade in trades_dict.items():
            if trade_id.startswith("_"):
                continue

            vt_tradeid = f"{self.config.GATEWAY_NAME}.{trade_id}"

            # 创建或更新自定义成交对象
            if vt_tradeid not in self.my_trades:
                self.my_trades[vt_tradeid] = MyTrade(
                    gateway_name=self.config.GATEWAY_NAME,
                    tradeid=trade_id
                )
                self.logger.info(f"新成交: {vt_tradeid}")

            self.my_trades[vt_tradeid].update_from_tqsdk(tq_trade)

    def _sync_quote_data(self):
        """同步行情数据（可选功能）"""
        # 这里可以添加行情数据的同步逻辑
        # 目前主要关注账户、持仓、订单、成交四大核心数据
        pass

    def _get_tq_orders_dict(self) -> Dict[str, Order]:
        """获取天勤订单字典"""
        if not self.api:
            return {}

        try:
            # 从天勤数据结构中获取订单字典
            trade_data = self.api._data.get("trade", {})
            gateway_data = trade_data.get(self.config.GATEWAY_NAME, {})
            return gateway_data.get("orders", {})
        except Exception as e:
            self.logger.warning(f"获取天勤订单字典失败: {e}")
            return {}

    def _get_tq_trades_dict(self) -> Dict[str, Trade]:
        """获取天勤成交字典"""
        if not self.api:
            return {}

        try:
            # 从天勤数据结构中获取成交字典
            trade_data = self.api._data.get("trade", {})
            gateway_data = trade_data.get(self.config.GATEWAY_NAME, {})
            return gateway_data.get("trades", {})
        except Exception as e:
            self.logger.warning(f"获取天勤成交字典失败: {e}")
            return {}

    def get_account(self) -> Optional[MyAccount]:
        """获取自定义账户对象"""
        return self.my_account

    def get_position(self, symbol: str, direction: str) -> Optional[MyPosition]:
        """获取指定合约和方向的持仓对象"""
        key = f"{symbol}.{direction}"
        return self.my_positions.get(key)

    def get_order(self, vt_orderid: str) -> Optional[MyOrder]:
        """获取指定订单ID的订单对象"""
        return self.my_orders.get(vt_orderid)

    def get_trade(self, vt_tradeid: str) -> Optional[MyTrade]:
        """获取指定成交ID的成交对象"""
        return self.my_trades.get(vt_tradeid)

    def get_all_positions(self) -> List[MyPosition]:
        """获取所有持仓对象"""
        return list(self.my_positions.values())

    def get_all_orders(self) -> List[MyOrder]:
        """获取所有订单对象"""
        return list(self.my_orders.values())

    def get_all_trades(self) -> List[MyTrade]:
        """获取所有成交对象"""
        return list(self.my_trades.values())

    def get_active_orders(self) -> List[MyOrder]:
        """获取所有活跃订单（未完全成交的订单）"""
        return [order for order in self.my_orders.values() if order.is_active()]

    async def disconnect(self):
        """断开天勤API连接 - 修复版本"""
        if self.api:  # 修复：添加None检查
            try:
                # 修复：检查close方法是否是协程函数
                if asyncio.iscoroutinefunction(self.api.close):
                    await self.api.close()  # 异步关闭
                else:
                    self.api.close()  # 同步关闭
            except Exception as e:
                self.logger.warning(f"关闭API连接时发生警告: {e}")
            finally:
                self.is_connected = False
                self.logger.info("已断开天勤API连接")
        else:
            self.logger.info("API连接未建立，无需断开")  # 新增：处理未连接情况


    def get_status_report(self) -> Dict[str, Any]:
        """获取数据管理器状态报告"""
        return {
            "is_connected": self.is_connected,
            "update_count": self.update_count,
            "last_update_time": self.last_update_time.isoformat() if self.last_update_time else None,
            "error_count": self.error_count,
            "account_count": 1 if self.my_account else 0,
            "position_count": len(self.my_positions),
            "order_count": len(self.my_orders),
            "trade_count": len(self.my_trades),
            "active_order_count": len(self.get_active_orders()),
        }

    def validate_data_consistency(self) -> Dict[str, bool]:
        """
        验证自定义数据表与天勤原始数据的一致性

        Returns:
            Dict[str, bool]: 各数据表的一致性验证结果
        """
        results = {}

        try:
            # 验证账户数据一致性
            if self.tq_account and self.my_account:
                results["account"] = self._validate_account_consistency()
            else:
                results["account"] = False

            # 验证持仓数据一致性
            results["position"] = self._validate_position_consistency()

            # 验证订单数据一致性（抽样验证）
            results["order"] = self._validate_order_consistency()

            # 验证成交数据一致性（抽样验证）
            results["trade"] = self._validate_trade_consistency()

        except Exception as e:
            self.logger.error(f"数据一致性验证失败: {e}")
            results["error"] = False

        return results

    def _validate_account_consistency(self) -> bool:
        """验证账户数据一致性"""
        try:
            # 比较关键字段
            checks = [
                abs(self.tq_account.balance - self.my_account.balance) < self.config.FLOAT_PRECISION,
                abs(self.tq_account.available - self.my_account.available) < self.config.FLOAT_PRECISION,
            ]

            return all(checks)
        except Exception as e:
            self.logger.warning(f"账户一致性验证错误: {e}")
            return False

    def _validate_position_consistency(self) -> bool:
        """验证持仓数据一致性"""
        try:
            for symbol, tq_position in self.tq_positions.items():
                long_position = self.get_position(symbol, "LONG")
                short_position = self.get_position(symbol, "SHORT")

                if not long_position or not short_position:
                    return False

                # 检查总持仓量
                tq_long_volume = tq_position.pos_long_his + tq_position.pos_long_today
                tq_short_volume = tq_position.pos_short_his + tq_position.pos_short_today

                if (tq_long_volume != long_position.volume or
                        tq_short_volume != short_position.volume):
                    return False

            return True
        except Exception as e:
            self.logger.warning(f"持仓一致性验证错误: {e}")
            return False

    def _validate_order_consistency(self) -> bool:
        """验证订单数据一致性（抽样验证）"""
        try:
            tq_orders = self._get_tq_orders_dict()

            # 抽样验证前3个订单
            sample_orders = list(self.my_orders.values())[:3]
            for my_order in sample_orders:
                order_id = my_order.orderid
                if order_id in tq_orders:
                    tq_order = tq_orders[order_id]
                    if tq_order.volume_orign != my_order.volume:
                        return False

            return True
        except Exception as e:
            self.logger.warning(f"订单一致性验证错误: {e}")
            return False

    def _validate_trade_consistency(self) -> bool:
        """验证成交数据一致性（抽样验证）"""
        try:
            tq_trades = self._get_tq_trades_dict()

            # 抽样验证前3个成交
            sample_trades = list(self.my_trades.values())[:3]
            for my_trade in sample_trades:
                trade_id = my_trade.tradeid
                if trade_id in tq_trades:
                    tq_trade = tq_trades[trade_id]
                    if tq_trade.volume != my_trade.volume:
                        return False

            return True
        except Exception as e:
            self.logger.warning(f"成交一致性验证错误: {e}")
            return False


# 3. 添加策略管理方法
def add_trading_strategy(self, strategy: BaseTradingStrategy):
    """添加交易策略"""
    strategy.set_data_manager(self)  # 设置策略的数据管理器引用
    self.trading_strategies.append(strategy)
    self.logger.info(f"已添加策略: {strategy.name}")


async def start_trading(self):
    """启动所有交易策略"""
    self.is_trading = True
    for strategy in self.trading_strategies:
        await strategy.start()
    self.logger.info("所有交易策略已启动")


async def stop_trading(self):
    """停止所有交易策略"""
    self.is_trading = False
    for strategy in self.trading_strategies:
        await strategy.stop()
    self.logger.info("所有交易策略已停止")


# 4. 添加策略回调分发方法
async def dispatch_market_data_to_strategies(self, symbol: str, quote_data: Any):
    """将行情数据分发给所有策略"""
    for strategy in self.trading_strategies:
        if strategy.is_running:
            await strategy.on_market_data(symbol, quote_data)


async def dispatch_order_update_to_strategies(self, order_data: Any):
    """将订单更新分发给所有策略"""
    for strategy in self.trading_strategies:
        if strategy.is_running:
            await strategy.on_order_update(order_data)


async def dispatch_trade_update_to_strategies(self, trade_data: Any):
    """将成交更新分发给所有策略"""
    for strategy in self.trading_strategies:
        if strategy.is_running:
            await strategy.on_trade_update(trade_data)


# 5. 添加策略下单接口
async def place_order_for_strategy(self, symbol: str, direction: str, volume: int,
                                   price_type: str = "LIMIT", price: float = None) -> Optional[str]:
    """为策略提供下单接口"""
    if not self.api:
        self.logger.error("API未连接，无法下单")
        return None

    try:
        # 使用天勤API下单
        order = self.api.insert_order(
            symbol=symbol,
            direction="BUY" if direction == "BUY" else "SELL",
            offset="OPEN",
            volume=volume,
            price_type=price_type,
            limit_price=price
        )
        return order.order_id
    except Exception as e:
        self.logger.error(f"策略下单失败: {e}")
        return None


# 6. 在数据同步方法中添加策略通知
async def _sync_all_tables(self):
    """同步所有数据表并通知策略"""
    try:
        # 同步数据
        self._sync_account_data()
        self._sync_position_data()
        self._sync_order_data()
        self._sync_trade_data()

        # 如果有行情数据，通知策略
        if self.tq_quotes:
            for symbol, quote in self.tq_quotes.items():
                await self.dispatch_market_data_to_strategies(symbol, quote)

    except Exception as e:
        self.logger.error(f"同步数据表时发生错误: {e}")


# 简易使用示例
async def main():
    """数据管理器使用示例"""
    # 创建数据管理器
    data_manager = MyDataManager()

    try:
        # 连接天勤API
        if await data_manager.connect():
            print("✓ 连接成功")

            # 启动数据同步（在后台运行）
            import asyncio
            sync_task = asyncio.create_task(data_manager.start_data_sync())

            # 运行一段时间后检查状态
            await asyncio.sleep(5)

            # 获取状态报告
            status = data_manager.get_status_report()
            print("状态报告:", status)

            # 验证数据一致性
            consistency = data_manager.validate_data_consistency()
            print("数据一致性:", consistency)

            # 等待同步任务完成
            await sync_task

            # 修复：正确取消同步任务
            sync_task.cancel()
            try:
                await sync_task
            except asyncio.CancelledError:
                pass  # 任务取消是预期的

    except KeyboardInterrupt:
        print("程序被用户中断")
    except Exception as e:  # 新增：捕获其他异常
        print(f"程序运行出错: {e}")
    finally:
        # 确保断开连接
        await data_manager.disconnect()


# 修复：添加顶层异常处理
if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("程序被用户终止")
    except Exception as e:
        print(f"程序运行失败: {e}")
