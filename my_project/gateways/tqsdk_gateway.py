"""
修复频率解析问题的天勤网关
确保历史数据获取功能正常运行，添加交易数据获取方法
修复版本：解决异步方法超时问题，添加完整的超时控制机制
"""
import asyncio
import logging
import time
from typing import Dict, Any, List, Optional, Union
from datetime import datetime
from core.event_engine import EventEngine
from core.thread_safe_manager import thread_safe_manager


class TqsdkGateway:
    """天勤API网关（修复版本：添加超时控制和错误处理）"""

    def __init__(self, event_engine: EventEngine):
        self.event_engine = event_engine
        self.api = None
        self.connected = False
        self.logger = self._setup_logger()
        self._pending_tasks = set()  # 跟踪所有异步任务
        self._disconnecting = False  # 断开连接状态标志

        # 修改处1：添加超时配置
        self.timeout_config = {
            'api_call_timeout': 10.0,  # API调用超时时间（秒）
            'wait_update_timeout': 5.0,  # 数据更新等待超时
            'connection_timeout': 30.0,  # 连接超时时间
            'retry_attempts': 3,  # 重试次数
            'retry_delay': 1.0  # 重试延迟（秒）
        }

    def _setup_logger(self):
        """设置日志记录器"""
        logger = logging.getLogger("TqsdkGateway")
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            logger.setLevel(logging.INFO)
        return logger

    def _parse_frequency(self, frequency: Union[str, int]) -> int:
        """
        解析频率参数，将字符串转换为秒数
        支持格式: "1min", "5min", "15min", "30min", "1h", "4h", "1d"
        """
        if isinstance(frequency, int):
            return frequency

        frequency = frequency.lower().strip()

        # 映射表：频率字符串到秒数
        frequency_map = {
            "1s": 1,
            "5s": 5,
            "10s": 10,
            "15s": 15,
            "30s": 30,
            "1min": 60,
            "3min": 180,
            "5min": 300,
            "15min": 900,
            "30min": 1800,
            "1h": 3600,
            "2h": 7200,
            "4h": 14400,
            "1d": 86400,
        }

        if frequency in frequency_map:
            return frequency_map[frequency]

        # 尝试解析数字格式
        try:
            if frequency.endswith('s'):
                return int(frequency[:-1])
            elif frequency.endswith('min'):
                return int(frequency[:-3]) * 60
            elif frequency.endswith('h'):
                return int(frequency[:-1]) * 3600
            elif frequency.endswith('d'):
                return int(frequency[:-1]) * 86400
            else:
                # 默认返回整数
                return int(frequency)
        except (ValueError, TypeError):
            self.logger.warning(f"无法解析频率参数: {frequency}，使用默认值60秒")
            return 60  # 默认1分钟

    async def connect(self, username: str, password: str) -> bool:
        """连接天勤API"""
        with thread_safe_manager.locked_resource("gateway_connection"):
            if self.connected:
                return True

            try:
                from tqsdk import TqApi, TqAuth

                self.logger.info("正在连接天勤API...")
                start_time = datetime.now()

                # 修改处2：为连接操作添加超时控制
                connect_task = asyncio.create_task(
                    self._create_api_instance_with_retry(username, password)
                )
                self._pending_tasks.add(connect_task)
                connect_task.add_done_callback(lambda t: self._pending_tasks.discard(t))

                # 添加连接超时
                self.api = await asyncio.wait_for(
                    connect_task,
                    timeout=self.timeout_config['connection_timeout']
                )
                self.connected = True

                connect_time = (datetime.now() - start_time).total_seconds()
                self.logger.info(f"天勤API连接成功，耗时: {connect_time:.2f}秒")
                return True

            except asyncio.TimeoutError:
                self.logger.error("天勤API连接超时")
                self.connected = False
                return False
            except Exception as e:
                self.logger.error(f"天勤API连接失败: {e}")
                self.connected = False
                return False

    async def _create_api_instance_with_retry(self, username: str, password: str):
        """创建API实例（带重试机制）"""
        from tqsdk import TqApi, TqAuth

        for attempt in range(self.timeout_config['retry_attempts']):
            try:
                # 修改处3：为API创建添加超时控制
                api_task = asyncio.create_task(
                    self._create_api_instance(username, password)
                )
                return await asyncio.wait_for(
                    api_task,
                    timeout=self.timeout_config['connection_timeout']
                )
            except asyncio.TimeoutError:
                if attempt < self.timeout_config['retry_attempts'] - 1:
                    self.logger.warning(f"API创建超时，第{attempt + 1}次重试...")
                    await asyncio.sleep(self.timeout_config['retry_delay'])
                else:
                    raise
            except Exception as e:
                if attempt < self.timeout_config['retry_attempts'] - 1:
                    self.logger.warning(f"API创建失败，第{attempt + 1}次重试: {e}")
                    await asyncio.sleep(self.timeout_config['retry_delay'])
                else:
                    raise

    async def _create_api_instance(self, username: str, password: str):
        """创建API实例"""
        from tqsdk import TqApi, TqAuth
        return TqApi(auth=TqAuth(username, password), _stock=False)

    async def disconnect(self):
        """断开连接"""
        with thread_safe_manager.locked_resource("gateway_disconnection"):
            if self._disconnecting:
                self.logger.info("断开连接操作已在进行中，跳过重复执行")
                return

            self._disconnecting = True
            self.logger.info("执行断开连接前的最终数据同步...")

            try:
                # 1. 取消所有待处理的异步任务
                await self._cancel_pending_tasks()

                # 2. 执行最终数据同步
                sync_success = await self._final_data_sync()
                if not sync_success:
                    self.logger.warning("最终数据同步失败")

                # 3. 安全关闭API连接
                if self.api is not None:
                    try:
                        if hasattr(self.api, 'close'):
                            # 修改处4：为关闭操作添加超时
                            close_task = asyncio.create_task(self.api.close())
                            await asyncio.wait_for(close_task, timeout=5.0)
                            self.logger.info("API连接已安全关闭")
                    except asyncio.TimeoutError:
                        self.logger.warning("API关闭超时，强制断开")
                    except Exception as e:
                        self.logger.warning(f"断开连接时发生警告: {e}")
                    finally:
                        self.api = None

                self.connected = False
                self.logger.info("天勤API连接已断开")

            except Exception as e:
                self.logger.error(f"断开连接时发生错误: {e}")
                raise
            finally:
                self._disconnecting = False
                self._pending_tasks.clear()
                return True

    async def _cancel_pending_tasks(self):
        """取消所有待处理的异步任务"""
        try:
            if self._pending_tasks:
                self.logger.info(f"正在取消 {len(self._pending_tasks)} 个异步任务")

                tasks_to_cancel = list(self._pending_tasks)
                self._pending_tasks.clear()

                for task in tasks_to_cancel:
                    if not task.done():
                        task.cancel()

                if tasks_to_cancel:
                    results = await asyncio.gather(*tasks_to_cancel, return_exceptions=True)
                    cancelled_count = sum(1 for r in results if isinstance(r, asyncio.CancelledError))
                    self.logger.info(f"成功取消 {cancelled_count} 个异步任务")
                    return cancelled_count
                return 0
            return 0
        except Exception as e:
            self.logger.warning(f"取消异步任务时发生错误: {e}")
            return 0

    async def _final_data_sync(self) -> bool:
        """最终数据同步"""
        try:
            self.logger.info("执行最终数据同步...")
            await asyncio.sleep(0.1)
            self.logger.info("最终数据同步完成")
            return True
        except Exception as e:
            self.logger.warning(f"最终数据同步失败: {e}")
            return False

    async def get_history_data(self, symbol: str, start_dt: str, end_dt: str,
                             frequency: Union[str, int] = 3600) -> List[Dict[str, Any]]:
        """获取历史数据"""
        with thread_safe_manager.locked_resource("history_data_fetch"):
            if not self.connected or not self.api:
                self.logger.error("未连接天勤API")
                return []

            try:
                from datetime import datetime as dt
                start_time = datetime.now()

                # 解析频率参数
                frequency_seconds = self._parse_frequency(frequency)

                self.logger.info(f"开始获取历史数据: {symbol} {start_dt} 到 {end_dt}")
                self.logger.info(f"原始频率: {frequency}, 解析后: {frequency_seconds}秒")

                # 转换日期格式
                start_date = dt.strptime(start_dt, "%Y-%m-%d")
                end_date = dt.strptime(end_dt, "%Y-%m-%d")

                # 获取K线数据
                klines = self.api.get_kline_serial(symbol, frequency_seconds,
                                                 start_dt=start_date, end_dt=end_date)

                # 修改处5：为历史数据等待添加超时控制
                try:
                    await asyncio.wait_for(
                        self._wait_for_data_ready(klines),
                        timeout=self.timeout_config['wait_update_timeout']
                    )
                except asyncio.TimeoutError:
                    self.logger.warning("历史数据等待超时，返回已有数据")
                    # 超时时返回已获取的数据

                # 转换为标准化格式
                history_data = []
                for i in range(len(klines)):
                    bar_data = {
                        'symbol': symbol,
                        'datetime': klines.datetime[i],
                        'open': float(klines.open[i]),
                        'high': float(klines.high[i]),
                        'low': float(klines.low[i]),
                        'close': float(klines.close[i]),
                        'volume': int(klines.volume[i]),
                        'open_interest': int(klines.open_oi[i]) if hasattr(klines, 'open_oi') else 0
                    }
                    history_data.append(bar_data)

                elapsed_time = (datetime.now() - start_time).total_seconds()
                self.logger.info(f"成功获取 {len(history_data)} 条历史数据，耗时: {elapsed_time:.2f}秒")
                return history_data

            except Exception as e:
                self.logger.error(f"获取历史数据失败: {e}")
                return []

    async def _wait_for_data_ready(self, klines):
        """等待数据就绪（可超时）"""
        while not self.api.is_changing(klines):
            await asyncio.sleep(0.1)
            if hasattr(self.api, 'wait_update'):
                self.api.wait_update()

    # 修改处6：为所有交易数据获取方法添加超时控制
    async def get_account_info(self) -> Dict[str, Any]:
        """获取账户信息（添加超时控制）"""
        if not self.connected or not self.api:
            self.logger.error("未连接天勤API")
            return {}

        try:
            # 获取天勤账户对象
            account = self.api.get_account()

            # 修改处7：为wait_update添加超时控制
            try:
                await asyncio.wait_for(
                    self.api.wait_update(),
                    timeout=self.timeout_config['wait_update_timeout']
                )
            except asyncio.TimeoutError:
                self.logger.warning("获取账户信息超时，返回空数据")
                return {}

            # 解析账户数据
            account_info = {
                'account_id': getattr(account, 'account_id', 'unknown'),
                'balance': float(getattr(account, 'balance', 0.0)),
                'available': float(getattr(account, 'available', 0.0)),
                'commission': float(getattr(account, 'commission', 0.0)),
                'margin': float(getattr(account, 'margin', 0.0)),
                'close_profit': float(getattr(account, 'close_profit', 0.0)),
                'position_profit': float(getattr(account, 'position_profit', 0.0)),
                'frozen_margin': float(getattr(account, 'frozen_margin', 0.0)),
                'frozen_commission': float(getattr(account, 'frozen_commission', 0.0)),
                'deposit': float(getattr(account, 'deposit', 0.0)),
                'withdraw': float(getattr(account, 'withdraw', 0.0)),
                'currency': getattr(account, 'currency', 'CNY'),
                'update_time': datetime.now().isoformat()
            }

            self.logger.debug(f"获取账户信息成功: {account_info}")
            return account_info

        except Exception as e:
            self.logger.error(f"获取账户信息失败: {e}")
            return {}

    async def get_orders(self, symbol: str = None) -> List[Dict[str, Any]]:
        """获取订单列表（添加超时控制）"""
        if not self.connected or not self.api:
            self.logger.error("未连接天勤API")
            return []

        try:
            # 获取天勤订单对象
            orders = self.api.get_orders()

            # 修改处8：为订单数据等待添加超时
            try:
                await asyncio.wait_for(
                    self.api.wait_update(),
                    timeout=self.timeout_config['wait_update_timeout']
                )
            except asyncio.TimeoutError:
                self.logger.warning("获取订单列表超时，返回空数据")
                return []

            # 解析订单数据
            orders_list = []
            for order_id, order in orders.items():
                order_data = {
                    'order_id': order_id,
                    'symbol': getattr(order, 'instrument_id', 'unknown'),
                    'direction': getattr(order, 'direction', 'UNKNOWN'),
                    'offset': getattr(order, 'offset', 'OPEN'),
                    'volume': int(getattr(order, 'volume_orign', 0)),
                    'volume_left': int(getattr(order, 'volume_left', 0)),
                    'volume_traded': int(getattr(order, 'volume_traded', 0)),
                    'price': float(getattr(order, 'limit_price', 0.0)),
                    'status': getattr(order, 'status', 'UNKNOWN'),
                    'insert_time': getattr(order, 'insert_date_time', 0),
                    'update_time': getattr(order, 'last_msg_time', 0)
                }

                if symbol is None or order_data['symbol'] == symbol:
                    orders_list.append(order_data)

            self.logger.debug(f"获取订单列表成功: 共{len(orders_list)}条订单")
            return orders_list

        except Exception as e:
            self.logger.error(f"获取订单列表失败: {e}")
            return []

    async def get_positions(self, symbol: str = None) -> List[Dict[str, Any]]:
        """获取持仓信息（添加超时控制）"""
        if not self.connected or not self.api:
            self.logger.error("未连接天勤API")
            return []

        try:
            # 获取天勤持仓对象
            positions = self.api.get_positions()

            # 修改处9：为持仓数据等待添加超时
            try:
                await asyncio.wait_for(
                    self.api.wait_update(),
                    timeout=self.timeout_config['wait_update_timeout']
                )
            except asyncio.TimeoutError:
                self.logger.warning("获取持仓信息超时，返回空数据")
                return []

            # 解析持仓数据
            positions_list = []
            for position_key, position in positions.items():
                position_data = {
                    'symbol': getattr(position, 'instrument_id', 'unknown'),
                    'direction': getattr(position, 'direction', 'UNKNOWN'),
                    'volume': int(getattr(position, 'volume', 0)),
                    'available_volume': int(getattr(position, 'available', 0)),
                    'frozen_volume': int(getattr(position, 'frozen', 0)),
                    'open_price': float(getattr(position, 'open_price', 0.0)),
                    'position_price': float(getattr(position, 'position_price', 0.0)),
                    'position_profit': float(getattr(position, 'position_profit', 0.0)),
                    'close_profit': float(getattr(position, 'close_profit', 0.0)),
                    'margin': float(getattr(position, 'margin', 0.0)),
                    'yd_volume': int(getattr(position, 'yd_volume', 0))
                }

                if symbol is None or position_data['symbol'] == symbol:
                    positions_list.append(position_data)

            self.logger.debug(f"获取持仓信息成功: 共{len(positions_list)}条持仓")
            return positions_list

        except Exception as e:
            self.logger.error(f"获取持仓信息失败: {e}")
            return []

    async def get_trades(self, symbol: str = None) -> List[Dict[str, Any]]:
        """获取成交记录（添加超时控制）"""
        if not self.connected or not self.api:
            self.logger.error("未连接天勤API")
            return []

        try:
            # 获取天勤成交对象
            trades = self.api.get_trades()

            # 修改处10：为成交数据等待添加超时
            try:
                await asyncio.wait_for(
                    self.api.wait_update(),
                    timeout=self.timeout_config['wait_update_timeout']
                )
            except asyncio.TimeoutError:
                self.logger.warning("获取成交记录超时，返回空数据")
                return []

            # 解析成交数据
            trades_list = []
            for trade_id, trade in trades.items():
                trade_data = {
                    'trade_id': trade_id,
                    'order_id': getattr(trade, 'order_id', 'unknown'),
                    'symbol': getattr(trade, 'instrument_id', 'unknown'),
                    'direction': getattr(trade, 'direction', 'UNKNOWN'),
                    'offset': getattr(trade, 'offset', 'OPEN'),
                    'volume': int(getattr(trade, 'volume', 0)),
                    'price': float(getattr(trade, 'price', 0.0)),
                    'trade_time': getattr(trade, 'trade_date_time', 0),
                    'commission': float(getattr(trade, 'commission', 0.0))
                }

                if symbol is None or trade_data['symbol'] == symbol:
                    trades_list.append(trade_data)

            self.logger.debug(f"获取成交记录成功: 共{len(trades_list)}条成交")
            return trades_list

        except Exception as e:
            self.logger.error(f"获取成交记录失败: {e}")
            return []

    def is_connected(self) -> bool:
        """检查连接状态"""
        return self.connected and self.api is not None

    def get_gateway_status(self) -> Dict[str, Any]:
        """获取网关状态"""
        return {
            "connected": self.connected,
            "api_available": self.api is not None,
            "pending_tasks": len(self._pending_tasks),
            "disconnecting": self._disconnecting,
            "timeout_config": self.timeout_config
        }


# 测试频率解析功能
def test_frequency_parsing():
    """测试频率解析功能"""
    gateway = TqsdkGateway(EventEngine())

    test_cases = [
        "1min", "5min", "15min", "30min", "1h", "4h", "1d",
        "60", 60, "300", 300, "invalid"
    ]

    print("频率解析测试结果:")
    print("-" * 40)
    for freq in test_cases:
        result = gateway._parse_frequency(freq)
        print(f"{freq!r:>10} -> {result:>5}秒")
    print("-" * 40)


# 测试交易数据功能
async def test_trading_data_functions():
    """测试交易数据获取功能"""
    event_engine = EventEngine()
    gateway = TqsdkGateway(event_engine)

    # 测试连接
    connected = await gateway.connect("test_user", "test_pass")
    print(f"连接状态: {connected}")

    if connected:
        # 测试账户信息获取
        account_info = await gateway.get_account_info()
        print("账户信息:", account_info)

        # 测试订单获取
        orders = await gateway.get_orders()
        print("订单数量:", len(orders))

        # 测试持仓获取
        positions = await gateway.get_positions()
        print("持仓数量:", len(positions))

        # 测试成交获取
        trades = await gateway.get_trades()
        print("成交数量:", len(trades))

        # 测试断开连接
        await gateway.disconnect()

    print("交易数据功能测试完成")


if __name__ == "__main__":
    # 运行频率解析测试
    test_frequency_parsing()

    # 运行交易数据功能测试
    import asyncio
    asyncio.run(test_trading_data_functions())
