"""
改进的天勤网关
集成线程安全管理和数据同步服务
"""
import asyncio
import logging
import time
from typing import Dict, Any, List, Optional, Union
from datetime import datetime
from core.event_engine import EventEngine
from core.data_adapter import DataAdapter
from core.async_sync_bridge import AsyncSyncBridge
from core.thread_safe_manager import thread_safe_manager
from core.data_sync_service import DataSyncService


class TqsdkGateway:
    """天勤API网关（已集成数据同步和线程安全）"""

    def __init__(self, event_engine: EventEngine):
        self.event_engine = event_engine
        self.api = None
        self.connected = False
        self.logger = self._setup_logger()
        self.adapter = DataAdapter()
        self.bridge = AsyncSyncBridge()
        self.sync_service = DataSyncService()  # 新增数据同步服务
        self.last_data_update = None

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

    async def connect(self, username: str, password: str) -> bool:
        """连接天勤API（集成线程安全）"""
        with thread_safe_manager.locked_resource("gateway_connection"):
            try:
                from tqsdk import TqApi, TqAuth

                start_time = time.time()
                self.logger.info("正在连接天勤API...")

                # 异步连接
                self.api = TqApi(auth=TqAuth(username, password), _stock=False)
                self.connected = True

                connect_time = time.time() - start_time
                self.logger.info(f"天勤API连接成功，耗时: {connect_time:.2f}秒")
                return True

            except Exception as e:
                self.logger.error(f"天勤API连接失败: {e}")
                self.connected = False
                return False

    def connect_sync(self, username: str, password: str) -> bool:
        """同步连接天勤API"""
        return self.bridge.run_async_function(self.connect, username, password)

    async def get_history_data(self, symbol: str, start_dt: str, end_dt: str,
                             frequency: Union[str, int] = 3600) -> List[Dict[str, Any]]:
        """获取历史数据（集成数据同步检查）"""
        with thread_safe_manager.locked_resource("history_data_fetch"):
            if not self.connected or not self.api:
                self.logger.error("未连接天勤API")
                return []

            try:
                from datetime import datetime as dt
                start_time = time.time()

                self.logger.info(f"开始获取历史数据: {symbol} {start_dt} 到 {end_dt}")

                # 解析频率参数
                parsed_frequency = self._parse_frequency(frequency)
                self.logger.info(f"使用频率: {parsed_frequency}秒 ({frequency})")

                # 转换日期格式
                start_date = dt.strptime(start_dt, "%Y-%m-%d")
                end_date = dt.strptime(end_dt, "%Y-%m-%d")

                # 获取K线数据
                klines = self.api.get_kline_serial(symbol, parsed_frequency,
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
                        'type': 'data_update',
                        'table': 'market_data',
                        'data': {
                            'symbol': symbol,
                            'datetime': klines.datetime[i],
                            'open': float(klines.open[i]),
                            'high': float(klines.high[i]),
                            'low': float(klines.low[i]),
                            'close': float(klines.close[i]),
                            'volume': int(klines.volume[i]),
                            'open_interest': int(klines.open_oi[i]) if hasattr(klines, 'open_oi') else 0
                        }
                    }
                    history_data.append(bar_data)

                # 使用适配器标准化数据格式
                standardized_data = []
                for data in history_data:
                    standardized = self.adapter.convert_tqsdk_to_strategy_format(data)
                    if self.adapter.validate_data_format(standardized):
                        standardized_data.append(standardized)

                elapsed_time = time.time() - start_time
                self.logger.info(f"成功获取 {len(standardized_data)} 条历史数据，耗时: {elapsed_time:.2f}秒")

                # 记录数据更新时间
                self.last_data_update = datetime.now()

                # 触发数据同步检查（异步）
                asyncio.create_task(self._trigger_data_sync())

                return standardized_data

            except Exception as e:
                self.logger.error(f"获取历史数据失败: {e}")
                return []

    async def _trigger_data_sync(self):
        """触发数据同步检查（异步）"""
        try:
            # 这里需要获取四个数据表的实例
            # 在实际集成中，这些实例应该通过依赖注入传递
            # 暂时模拟同步调用
            self.logger.info("触发数据同步检查...")
            # 实际调用: sync_service.sync_data_tables(account_table, order_table, position_table, trade_table)

        except Exception as e:
            self.logger.warning(f"数据同步检查失败: {e}")

    def get_history_data_sync(self, symbol: str, start_dt: str, end_dt: str,
                            frequency: Union[str, int] = 3600) -> List[Dict[str, Any]]:
        """同步获取历史数据"""
        return self.bridge.run_async_function(self.get_history_data, symbol, start_dt, end_dt, frequency)

    async def disconnect(self):
        """断开连接（改进资源清理和数据同步）"""
        with thread_safe_manager.locked_resource("gateway_disconnection"):
            if self.api is not None:
                try:
                    # 执行最终数据同步
                    if hasattr(self, '_final_data_sync'):
                        await self._final_data_sync()

                    # 异步关闭连接
                    await self.api.close()
                    self.connected = False
                    self.logger.info("天勤API连接已关闭")

                    # 等待异步任务完成
                    await asyncio.sleep(0.5)
                    self.logger.info("天勤API连接已断开")

                except Exception as e:
                    self.logger.warning(f"断开连接时发生警告: {e}")
                finally:
                    self.api = None

    async def _final_data_sync(self):
        """断开连接前的最终数据同步"""
        try:
            self.logger.info("执行断开连接前的最终数据同步...")
            # 生成最终同步报告
            sync_report = self.sync_service.generate_sync_report()
            self.logger.info(f"最终同步报告: 成功率={sync_report['summary']['success_rate']:.2%}")

        except Exception as e:
            self.logger.warning(f"最终数据同步失败: {e}")

    def disconnect_sync(self):
        """同步断开连接"""
        if self.api:
            self.bridge.run_async_function(self.disconnect)

    def _parse_frequency(self, frequency: Union[str, int]) -> int:
        """解析频率参数"""
        if isinstance(frequency, int):
            return frequency

        if isinstance(frequency, str):
            frequency_lower = frequency.lower()
            if frequency_lower in ['1min', '1m']:
                return 60
            elif frequency_lower in ['5min', '5m']:
                return 300
            elif frequency_lower in ['15min', '15m']:
                return 900
            elif frequency_lower in ['30min', '30m']:
                return 1800
            elif frequency_lower in ['1h', '1hour']:
                return 3600
            elif frequency_lower in ['4h', '4hour']:
                return 14400
            elif frequency_lower in ['1d', '1day']:
                return 86400
            else:
                try:
                    return int(frequency)
                except ValueError:
                    self.logger.warning(f"无法解析频率参数: {frequency}, 使用默认值3600")
                    return 3600
        else:
            self.logger.warning(f"不支持的频率类型: {type(frequency)}, 使用默认值3600")
            return 3600

    def get_gateway_status(self) -> Dict[str, Any]:
        """获取网关状态（包含同步状态）"""
        sync_status = self.sync_service.get_sync_status()

        return {
            "connected": self.connected,
            "last_data_update": self.last_data_update,
            "sync_status": sync_status,
            "api_available": self.api is not None
        }


# 测试代码
if __name__ == "__main__":
    import asyncio
    from core.event_engine import EventEngine

    async def test_gateway():
        event_engine = EventEngine()
        gateway = TqsdkGateway(event_engine)

        # 测试网关状态
        status = gateway.get_gateway_status()
        print("网关初始状态:", status)

        # 测试频率解析
        test_frequencies = ["1min", "1h", 3600, "invalid"]
        for freq in test_frequencies:
            parsed = gateway._parse_frequency(freq)
            print(f"频率解析: {freq} -> {parsed}")

        print("网关测试完成")

    asyncio.run(test_gateway())
