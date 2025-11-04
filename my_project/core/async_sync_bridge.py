"""
异步同步桥接模块
提供异步方法到同步方法的转换，确保兼容性
"""
import asyncio
import functools
from typing import Any, Callable, Coroutine
import sys
import threading


class AsyncSyncBridge:
    """异步同步桥接器，管理事件循环和异步任务"""

    def __init__(self):
        self._loop = None
        self._thread = None

    def get_event_loop(self):
        """获取或创建事件循环"""
        try:
            return asyncio.get_event_loop()
        except RuntimeError:
            # 如果没有事件循环，创建新的
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            return loop

    def run_async_function(self, async_func: Callable[..., Coroutine], *args, **kwargs) -> Any:
        """
        运行异步函数并返回同步结果

        Args:
            async_func: 异步函数
            *args: 函数参数
            **kwargs: 函数关键字参数

        Returns:
            异步函数的执行结果
        """
        loop = self.get_event_loop()

        # 如果已经在事件循环中，直接运行
        if loop.is_running():
            # 创建新任务并等待完成
            future = asyncio.ensure_future(async_func(*args, **kwargs))
            return asyncio.run_coroutine_threadsafe(future, loop).result()
        else:
            # 直接运行异步函数
            return loop.run_until_complete(async_func(*args, **kwargs))

    def async_to_sync(self, async_func: Callable[..., Coroutine]) -> Callable[..., Any]:
        """
        装饰器：将异步函数转换为同步函数

        Args:
            async_func: 要转换的异步函数

        Returns:
            同步版本的函数
        """

        @functools.wraps(async_func)
        def sync_wrapper(*args, **kwargs):
            return self.run_async_function(async_func, *args, **kwargs)

        return sync_wrapper

    def create_async_context(self):
        """创建异步上下文，用于在同步代码中运行异步任务"""

        class AsyncContext:
            def __init__(self, bridge):
                self.bridge = bridge
                self.loop = bridge.get_event_loop()

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc_val, exc_tb):
                # 清理事件循环
                if self.loop and not self.loop.is_closed():
                    self.loop.close()

        return AsyncContext(self)


# 同步函数装饰器
def async_to_sync(async_func):
    """便捷装饰器，将异步函数转换为同步函数"""
    bridge = AsyncSyncBridge()
    return bridge.async_to_sync(async_func)


# 测试代码
if __name__ == "__main__":
    bridge = AsyncSyncBridge()


    # 测试异步函数
    async def sample_async_function(x, y):
        await asyncio.sleep(0.1)  # 模拟异步操作
        return x + y


    # 测试同步调用
    result = bridge.run_async_function(sample_async_function, 5, 3)
    print(f"异步函数同步调用结果: {result}")


    # 测试装饰器
    @async_to_sync
    async def decorated_async_function(name):
        await asyncio.sleep(0.1)
        return f"Hello, {name}"


    result2 = decorated_async_function("World")
    print(f"装饰器测试结果: {result2}")

    # 测试上下文管理
    with bridge.create_async_context() as context:
        result3 = bridge.run_async_function(sample_async_function, 10, 20)
        print(f"上下文内调用结果: {result3}")
