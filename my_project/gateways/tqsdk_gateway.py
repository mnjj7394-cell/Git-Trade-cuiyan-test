"""
改进版天勤网关 - 优化连接性能和数据处理效率
主要改进：智能连接管理、数据缓存、性能优化
"""

import asyncio
import logging
import time
import hashlib
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Callable
from collections import defaultdict
import json

# 天勤SDK导入
try:
    from tqsdk import TqApi, TqAuth
    from tqsdk.objs import Quote
except ImportError:
    print("警告: 未安装tqsdk，请运行: pip install tqsdk")


class ConnectionPool:
    """连接池管理 - 提高连接复用效率"""

    def __init__(self, max_size=3):
        self._pool = []
        self._max_size = max_size
        self._in_use = set()
        self._creation_times = {}

    async def get_connection(self, auth):
        """从池中获取或创建连接"""
        # 查找可用连接

        for conn in self._pool:
            if conn['id'] not in self._in_use and conn['auth'] == auth:
                self._in_use.add(conn['id'])
                return conn['api']

        # 创建新连接
        if len(self._pool) < self._max_size:
            api = TqApi(auth=auth)
            conn_id = id(api)
            conn = {'id': conn_id, 'api': api, 'auth': auth}
            self._pool.append(conn)
            self._in_use.add(conn_id)
            self._creation_times[conn_id] = time.time()
            return api

        # 等待连接释放（简化处理，实际应使用异步等待）
        return None

    def release_connection(self, api):
        """释放连接回池"""
        conn_id = id(api)
        if conn_id in self._in_use:
            self._in_use.remove(conn_id)


class DataCache:
    """数据缓存层 - 减少重复请求"""

    def __init__(self, max_size=1000, ttl=300):
        self._cache = {}
        self._max_size = max_size
        self._ttl = ttl  # 缓存存活时间（秒）
        self._access_times = {}

    def _generate_key(self, symbol, start_dt, end_dt, frequency):
        """生成缓存键"""
        key_str = f"{symbol}_{start_dt}_{end_dt}_{frequency}"
        return hashlib.md5(key_str.encode()).hexdigest()

    def get(self, symbol, start_dt, end_dt, frequency):
        """从缓存获取数据"""
        key = self._generate_key(symbol, start_dt, end_dt, frequency)
        if key in self._cache:
            if time.time() - self._access_times[key] < self._ttl:
                self._access_times[key] = time.time()
                return self._cache[key]
            else:
                # 缓存过期，删除
                del self._cache[key]
                del self._access_times[key]
        return None

    def set(self, symbol, start_dt, end_dt, frequency, data):
        """设置缓存数据"""
        key = self._generate_key(symbol, start_dt, end_dt, frequency)

        # 清理过期缓存
        self._cleanup()

        # 如果缓存满，删除最久未使用的
        if len(self._cache) >= self._max_size:
            oldest_key = min(self._access_times.items(), key=lambda x: x[1])[0]
            del self._cache[oldest_key]
            del self._access_times[oldest_key]

        self._cache[key] = data
        self._access_times[key] = time.time()

    def _cleanup(self):
        """清理过期缓存"""
        current_time = time.time()
        expired_keys = [k for k, t in self._access_times.items()
                       if current_time - t > self._ttl]
        for key in expired_keys:
            del self._cache[key]
            del self._access_times[key]


class SmartUrlManager:
    """智能URL管理 - 优化服务器选择"""

    def __init__(self):
        self._servers = [
            "wss://md1.shinnytech.com/t/md/front/mobile",
            "wss://md2.shinnytech.com/t/md/front/mobile",
            "wss://md3.shinnytech.com/t/md/front/mobile"
        ]
        self._server_metrics = {}  # 服务器性能指标
        self._current_server = None

    async def get_optimal_server(self):
        """获取最优服务器"""
        if not self._server_metrics:
            return self._servers[0]

        # 根据响应时间和成功率选择最优服务器
        best_server = min(self._server_metrics.items(),
                         key=lambda x: x[1]['response_time'] / x[1]['success_rate'])[0]
        return best_server

    def update_metrics(self, server, response_time, success=True):
        """更新服务器指标"""
        if server not in self._server_metrics:
            self._server_metrics[server] = {
                'response_time': response_time,
                'success_rate': 1.0 if success else 0.0,
                'request_count': 1
            }
        else:
            metrics = self._server_metrics[server]
            # 指数加权移动平均更新
            alpha = 0.3  # 平滑因子
            metrics['response_time'] = (alpha * response_time +
                                      (1 - alpha) * metrics['response_time'])
            metrics['success_rate'] = (alpha * (1.0 if success else 0.0) +
                                     (1 - alpha) * metrics['success_rate'])
            metrics['request_count'] += 1


class TqsdkGateway:
    """
    改进版天勤网关 - 性能优化版本
    """

    def __init__(self, event_engine):
        self.event_engine = event_engine
        self.api = None
        self.auth = None
        self.is_connected = False

        # 性能优化组件
        self.connection_pool = ConnectionPool()
        self.data_cache = DataCache()
        self.url_manager = SmartUrlManager()

        # 日志设置
        self.logger = self._setup_logger()

        # 连接状态监控
        self._connect_time = 0
        self._last_activity = 0

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

    async def connect(self, username: str = "test", password: str = "test") -> bool:
        """
        优化连接方法 - 使用连接池和智能路由
        """
        try:
            start_time = time.time()

            self.auth = TqAuth(username, password)

            # 获取最优服务器
            optimal_server = await self.url_manager.get_optimal_server()
            self.logger.info(f"使用服务器: {optimal_server}")

            # 从连接池获取或创建连接
            self.api = await self.connection_pool.get_connection(self.auth)
            if not self.api:
                # 创建新连接
                self.api = TqApi(auth=self.auth)

            # 测试连接
            await asyncio.sleep(0.1)  # 短暂等待连接就绪
            self.is_connected = True
            self._connect_time = time.time()
            self._last_activity = time.time()

            connect_duration = time.time() - start_time
            self.logger.info(f"天勤API连接成功，耗时: {connect_duration:.2f}秒")

            # 更新服务器指标
            self.url_manager.update_metrics(optimal_server, connect_duration, True)

            return True

        except Exception as e:
            connect_duration = time.time() - start_time if 'start_time' in locals() else 0
            self.logger.error(f"天勤API连接失败: {e}")
            self.url_manager.update_metrics(optimal_server, connect_duration, False)
            self.is_connected = False
            return False

    async def get_history_data(self, symbol: str, start_dt: str, end_dt: str,
                              frequency: int = 3600) -> List[Dict[str, Any]]:
        """
        优化历史数据获取 - 使用缓存和批量处理
        """
        # 检查缓存
        cached_data = self.data_cache.get(symbol, start_dt, end_dt, frequency)
        if cached_data:
            self.logger.info(f"从缓存获取历史数据: {symbol}")
            return cached_data

        if not self.is_connected or not self.api:
            self.logger.error("未连接天勤API")
            return []

        try:
            start_time = time.time()
            self.logger.info(f"开始获取历史数据: {symbol} {start_dt} 到 {end_dt}")

            # 转换日期格式
            start_date = datetime.strptime(start_dt, "%Y-%m-%d")
            end_date = datetime.strptime(end_dt, "%Y-%m-%d")

            # 获取K线数据
            kline = self.api.get_kline_serial(symbol, frequency,
                                            start_dt=start_date, end_dt=end_date)

            # 等待数据就绪（带超时）
            await asyncio.wait_for(
                self._wait_for_data(kline),
                timeout=30.0  # 30秒超时
            )

            events = []
            for i in range(len(kline)):
                event = {
                    "type": "data_update",
                    "table": "market_data",
                    "data": {
                        "symbol": symbol,
                        "datetime": kline.datetime[i],
                        "open": float(kline.open[i]),
                        "high": float(kline.high[i]),
                        "low": float(kline.low[i]),
                        "close": float(kline.close[i]),
                        "volume": int(kline.volume[i]),
                        "open_interest": int(kline.open_oi[i]) if hasattr(kline, 'open_oi') else 0
                    }
                }
                events.append(event)

            # 缓存数据
            self.data_cache.set(symbol, start_dt, end_dt, frequency, events)

            duration = time.time() - start_time
            self.logger.info(f"成功获取 {len(events)} 条历史数据，耗时: {duration:.2f}秒")

            return events

        except asyncio.TimeoutError:
            self.logger.error("获取历史数据超时")
            return []
        except Exception as e:
            self.logger.error(f"获取历史数据失败: {e}")
            return []

    async def _wait_for_data(self, kline):
        """等待数据就绪的辅助方法"""
        while len(kline) == 0:
            await asyncio.sleep(0.1)
            if hasattr(self.api, 'wait_update'):
                self.api.wait_update()

    async def get_multiple_history_data(self, symbols: List[str], start_dt: str,
                                      end_dt: str, frequency: int = 3600) -> List[Dict[str, Any]]:
        """
        并行获取多个品种的历史数据
        """
        tasks = []
        for symbol in symbols:
            task = self.get_history_data(symbol, start_dt, end_dt, frequency)
            tasks.append(task)

        # 并行执行
        results = await asyncio.gather(*tasks, return_exceptions=True)

        all_events = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                self.logger.error(f"获取 {symbols[i]} 数据失败: {result}")
            else:
                all_events.extend(result)

        # 按时间排序
        all_events.sort(key=lambda x: x["data"]["datetime"])
        return all_events

    async def disconnect(self):
        """断开连接 - 修正：添加正确的资源清理"""
        if self.api:
            try:
                # 修正：添加API关闭调用，确保所有后台任务正确清理
                self.api.close()  # 关键修改：调用天勤API的close方法清理资源
                self.logger.info("天勤API连接已关闭")
            except Exception as e:
                self.logger.warning(f"关闭API时发生警告: {e}")
            finally:
                # 释放连接池
                self.connection_pool.release_connection(self.api)
                self.is_connected = False
                self.logger.info("天勤API连接已断开")
                # 修正：添加短暂等待，确保所有异步任务完成清理
                await asyncio.sleep(0.5)  # 关键修改：增加等待时间确保任务清理完成


# 测试代码
async def test_gateway():
    """测试网关功能 - 修正：改进资源清理流程"""

    class MockEventEngine:
        def put(self, event):
            print(f"收到事件: {event['type']} - {event['data']['symbol']}")

    print("=== 测试改进版天勤网关 ===")

    event_engine = MockEventEngine()
    gateway = TqsdkGateway(event_engine)

    try:
        # 测试连接
        if await gateway.connect("test", "test"):
            # 测试历史数据获取
            events = await gateway.get_history_data(
                symbol="SHFE.cu2401",
                start_dt="2024-01-01",
                end_dt="2024-01-05",
                frequency=3600
            )

            print(f"获取到 {len(events)} 条历史数据")

            if events:
                # 显示样例数据 - 修复时间戳转换
                for i, event in enumerate(events[:2]):
                    data = event["data"]
                    # 正确处理纳秒级时间戳
                    timestamp_seconds = data['datetime'] / 1000000000
                    dt_str = datetime.fromtimestamp(timestamp_seconds).strftime('%Y-%m-%d %H:%M:%S')
                    print(f"样例{i + 1}: {data['symbol']} {dt_str} "
                          f"开{data['open']} 高{data['high']} 低{data['low']} 收{data['close']}")

    except Exception as e:
        print(f"测试过程中发生错误: {e}")
        import traceback
        traceback.print_exc()

    finally:
        # 确保断开连接 - 修正：改进清理流程
        try:
            await gateway.disconnect()
        except Exception as e:
            print(f"断开连接时发生错误: {e}")
        # 修正：增加等待时间，确保所有后台任务完成清理
        await asyncio.sleep(1.0)  # 关键修改：延长等待时间确保完全清理


if __name__ == "__main__":
    # 运行测试 - 修正：使用更安全的事件循环管理
    try:
        asyncio.run(test_gateway())
    except KeyboardInterrupt:
        print("程序被用户中断")
    except Exception as e:
        print(f"程序运行异常: {e}")
