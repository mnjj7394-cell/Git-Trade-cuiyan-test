"""
线程安全管理器
提供多线程环境下的数据安全访问机制
"""
import threading
from typing import Any, Dict, Callable
from contextlib import contextmanager


class ThreadSafeManager:
    """线程安全管理器，确保多线程环境下的数据一致性"""

    def __init__(self):
        self._locks: Dict[str, threading.RLock] = {}
        self._global_lock = threading.RLock()

    def get_lock(self, resource_name: str) -> threading.RLock:
        """获取指定资源的锁"""
        with self._global_lock:
            if resource_name not in self._locks:
                self._locks[resource_name] = threading.RLock()
            return self._locks[resource_name]

    @contextmanager
    def locked_resource(self, resource_name: str):
        """上下文管理器，用于安全访问资源"""
        lock = self.get_lock(resource_name)
        lock.acquire()
        try:
            yield
        finally:
            lock.release()

    def safe_execute(self, resource_name: str, func: Callable, *args, **kwargs) -> Any:
        """安全执行函数，自动加锁"""
        with self.locked_resource(resource_name):
            return func(*args, **kwargs)

    def clear_locks(self):
        """清理所有锁（用于资源回收）"""
        with self._global_lock:
            self._locks.clear()


# 全局线程安全管理器实例
thread_safe_manager = ThreadSafeManager()

# 测试代码
if __name__ == "__main__":
    import time
    import concurrent.futures

    # 测试共享资源
    shared_data = {"counter": 0}


    def increment_counter(thread_id):
        """线程安全地增加计数器"""
        with thread_safe_manager.locked_resource("test_counter"):
            current = shared_data["counter"]
            time.sleep(0.01)  # 模拟处理时间
            shared_data["counter"] = current + 1
            print(f"线程 {thread_id} 增加计数器: {current} -> {shared_data['counter']}")


    # 多线程测试
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(increment_counter, i) for i in range(10)]
        concurrent.futures.wait(futures)

    print(f"最终计数器值: {shared_data['counter']} (应为10)")
