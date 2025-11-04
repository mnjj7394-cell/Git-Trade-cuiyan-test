"""
改进的天勤网关
解决异步兼容性和数据格式标准化问题
"""
import asyncio
import logging
import time  # 修改处：导入标准库的time模块
from typing import Dict, Any, List, Optional
from datetime import datetime  # 修改处：移除time的导入，只导入datetime
from core.event_engine import EventEngine
from core.data_adapter import DataAdapter
from core.async_sync_bridge import AsyncSyncBridge



class TqsdkGateway:
    """天勤API网关（已修复异步和数据格式问题）"""

    def __init__(self, event_engine: EventEngine):
        self.event_engine = event_engine
        self.api = None
        self.connected = False
        self.logger = self._setup_logger()
        self.adapter = DataAdapter()  # 数据适配器
        self.bridge = AsyncSyncBridge()  # 异步桥接器

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
        """连接天勤API（异步版本）"""
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
                             frequency: int = 3600) -> List[Dict[str, Any]]:
        """获取历史数据（异步版本，返回标准化格式）"""
        if not self.connected or not self.api:
            self.logger.error("未连接天勤API")
            return []

        try:
            from datetime import datetime as dt
            start_time = time.time()

            self.logger.info(f"开始获取历史数据: {symbol} {start_dt} 到 {end_dt}")

            # 转换日期格式
            start_date = dt.strptime(start_dt, "%Y-%m-%d")
            end_date = dt.strptime(end_dt, "%Y-%m-%d")

            # 获取K线数据
            klines = self.api.get_kline_serial(symbol, frequency,
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

            return standardized_data

        except Exception as e:
            self.logger.error(f"获取历史数据失败: {e}")
            return []

    def get_history_data_sync(self, symbol: str, start_dt: str, end_dt: str,
                            frequency: int = 3600) -> List[Dict[str, Any]]:
        """同步获取历史数据"""
        return self.bridge.run_async_function(self.get_history_data, symbol, start_dt, end_dt, frequency)

    async def disconnect(self):
        """断开连接（改进资源清理）"""
        if self.api:
            try:
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

    def disconnect_sync(self):
        """同步断开连接"""
        if self.api:
            self.bridge.run_async_function(self.disconnect)


# 测试代码
if __name__ == "__main__":
    import time
    from core.event_engine import EventEngine

    # 测试同步接口
    async def test_async():
        event_engine = EventEngine()
        gateway = TqsdkGateway(event_engine)

        # 测试连接（需要有效的天勤账号）
        # connected = await gateway.connect("username", "password")
        # if connected:
        #     # 测试数据获取
        #     data = await gateway.get_history_data("SHFE.cu2401", "2024-01-01", "2024-01-05")
        #     print(f"获取到 {len(data)} 条数据")
        #
        #     await gateway.disconnect()

        print("异步测试完成（注释了实际连接代码）")

    # 运行测试
    asyncio.run(test_async())

    # 测试同步接口
    def test_sync():
        event_engine = EventEngine()
        gateway = TqsdkGateway(event_engine)

        # 测试同步方法
        # connected = gateway.connect_sync("username", "password")
        # if connected:
        #     data = gateway.get_history_data_sync("SHFE.cu2401", "2024-01-01", "2024-01-05")
        #     print(f"同步获取到 {len(data)} 条数据")
        #
        #     gateway.disconnect_sync()

        print("同步测试完成（注释了实际连接代码）")

    test_sync()
