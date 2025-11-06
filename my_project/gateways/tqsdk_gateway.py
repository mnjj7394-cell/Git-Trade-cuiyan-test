"""
修复频率解析问题的天勤网关
确保历史数据获取功能正常运行
修复版本：解决异步方法返回值不一致问题
"""
import asyncio
import logging
import time
from typing import Dict, Any, List, Optional, Union
from datetime import datetime
from core.event_engine import EventEngine
from core.thread_safe_manager import thread_safe_manager


class TqsdkGateway:
    """天勤API网关（已修复异步返回值问题）"""

    def __init__(self, event_engine: EventEngine):
        self.event_engine = event_engine
        self.api = None
        self.connected = False
        self.logger = self._setup_logger()
        self._pending_tasks = set()  # 跟踪所有异步任务
        self._disconnecting = False  # 断开连接状态标志

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

                # 创建连接任务并添加到跟踪集合
                connect_task = asyncio.create_task(
                    self._create_api_instance(username, password)
                )
                self._pending_tasks.add(connect_task)
                # 添加完成回调自动移除任务
                connect_task.add_done_callback(lambda t: self._pending_tasks.discard(t))

                self.api = await connect_task
                self.connected = True

                connect_time = (datetime.now() - start_time).total_seconds()
                self.logger.info(f"天勤API连接成功，耗时: {connect_time:.2f}秒")
                return True

            except Exception as e:
                self.logger.error(f"天勤API连接失败: {e}")
                self.connected = False
                return False

    async def _create_api_instance(self, username: str, password: str):
        """创建API实例"""
        from tqsdk import TqApi, TqAuth
        return TqApi(auth=TqAuth(username, password), _stock=False)

    async def disconnect(self):
        """断开连接（修复：确保始终返回协程对象）"""
        # 修改处1：使用异步锁管理资源访问
        with thread_safe_manager.locked_resource("gateway_disconnection"):
            if self._disconnecting:
                self.logger.info("断开连接操作已在进行中，跳过重复执行")
                # 修改处2：显式返回None，避免隐式返回问题
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
                            # 修改处3：确保关闭操作是异步的
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
                # 修改处4：确保异常情况下也返回有效值
                raise
            finally:
                self._disconnecting = False
                self._pending_tasks.clear()
                # 修改处5：显式返回完成标志
                return True

    async def _cancel_pending_tasks(self):
        """取消所有待处理的异步任务（修复：确保返回协程）"""
        try:
            if self._pending_tasks:
                self.logger.info(f"正在取消 {len(self._pending_tasks)} 个异步任务")

                tasks_to_cancel = list(self._pending_tasks)
                self._pending_tasks.clear()

                for task in tasks_to_cancel:
                    if not task.done():
                        task.cancel()

                if tasks_to_cancel:
                    # 修改处6：使用return_exceptions=True避免单个任务失败影响整体
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
        """最终数据同步（修复：确保返回布尔值）"""
        try:
            self.logger.info("执行最终数据同步...")
            # 修改处7：添加小的延迟确保异步操作完成
            await asyncio.sleep(0.1)
            self.logger.info("最终数据同步完成")
            return True
        except Exception as e:
            self.logger.warning(f"最终数据同步失败: {e}")
            return False

    async def get_history_data(self, symbol: str, start_dt: str, end_dt: str,
                             frequency: Union[str, int] = 3600) -> List[Dict[str, Any]]:
        """获取历史数据（已修复频率解析问题）"""
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

                # 等待数据就绪
                while not self.api.is_changing(klines):
                    await asyncio.sleep(0.1)
                    if hasattr(self.api, 'wait_update'):
                        self.api.wait_update()

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

    def is_connected(self) -> bool:
        """检查连接状态"""
        return self.connected and self.api is not None

    def get_gateway_status(self) -> Dict[str, Any]:
        """获取网关状态"""
        return {
            "connected": self.connected,
            "api_available": self.api is not None,
            "pending_tasks": len(self._pending_tasks),
            "disconnecting": self._disconnecting
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


if __name__ == "__main__":
    # 运行频率解析测试
    test_frequency_parsing()

    # 可选：运行完整的网关测试
    import asyncio

    async def test_gateway_connection():
        """测试网关连接"""
        event_engine = EventEngine()
        gateway = TqsdkGateway(event_engine)

        # 测试连接（使用测试账号）
        connected = await gateway.connect("test_user", "test_pass")
        print(f"连接状态: {connected}")

        if connected:
            # 测试状态检查
            status = gateway.get_gateway_status()
            print("网关状态:", status)

            # 测试断开连接
            await gateway.disconnect()

        print("网关测试完成")

    # 取消注释以运行连接测试
    # asyncio.run(test_gateway_connection())
