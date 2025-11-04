"""
改进的事件引擎
增强事件队列的线程安全性和处理效率
"""
import asyncio
import queue
import threading
import time
from typing import Dict, Any, Callable, List, Optional
from datetime import datetime
from enum import Enum
from core.thread_safe_manager import thread_safe_manager


class EventPriority(Enum):
    """事件优先级枚举"""
    HIGH = 1  # 高优先级：交易成交、订单状态变更
    NORMAL = 2  # 普通优先级：行情数据、账户更新
    LOW = 3  # 低优先级：日志、统计信息


class EventEngine:
    """事件引擎（已增强线程安全和优先级处理）"""

    def __init__(self):
        self._handlers: Dict[str, List[Callable]] = {}
        self._active = False
        self._thread = None
        self._queue = queue.PriorityQueue()
        self._timer = 0
        self._lock = threading.RLock()
        self._stats = {
            "total_events": 0,
            "processed_events": 0,
            "failed_events": 0,
            "start_time": None,
            "last_event_time": None
        }

    def start(self):
        """启动事件引擎（线程安全）"""
        with thread_safe_manager.locked_resource("event_engine_start"):
            if self._active:
                return

            self._active = True
            self._thread = threading.Thread(target=self._run, daemon=True)
            self._thread.start()
            self._stats["start_time"] = datetime.now()
            print(f"[{datetime.now()}] [EventEngine] 事件引擎启动")

    def stop(self):
        """停止事件引擎（线程安全）"""
        with thread_safe_manager.locked_resource("event_engine_stop"):
            self._active = False
            if self._thread and self._thread.is_alive():
                self._thread.join(timeout=5.0)
            self._stats["last_event_time"] = datetime.now()
            print(f"[{datetime.now()}] [EventEngine] 事件引擎停止")

    def register(self, event_type: str, handler: Callable):
        """注册事件处理器（线程安全）"""
        with thread_safe_manager.locked_resource("event_handler_registration"):
            if event_type not in self._handlers:
                self._handlers[event_type] = []

            if handler not in self._handlers[event_type]:
                self._handlers[event_type].append(handler)
                print(f"[{datetime.now()}] [EventEngine] 注册事件处理器: {event_type}")

    def unregister(self, event_type: str, handler: Callable):
        """注销事件处理器（线程安全）"""
        with thread_safe_manager.locked_resource("event_handler_unregistration"):
            if event_type in self._handlers and handler in self._handlers[event_type]:
                self._handlers[event_type].remove(handler)
                print(f"[{datetime.now()}] [EventEngine] 注销事件处理器: {event_type}")

    def put(self, event: Dict[str, Any], priority: EventPriority = EventPriority.NORMAL):
        """放入事件（线程安全，支持优先级）"""
        with thread_safe_manager.locked_resource("event_put"):
            if not self._active:
                raise RuntimeError("事件引擎未启动")

            # 添加事件元数据
            event["_metadata"] = {
                "event_id": f"EVENT_{int(time.time() * 1000)}_{self._stats['total_events']}",
                "timestamp": datetime.now(),
                "priority": priority.value
            }

            # 根据优先级放入队列
            self._queue.put((priority.value, self._timer, event))
            self._timer += 1
            self._stats["total_events"] += 1
            self._stats["last_event_time"] = datetime.now()

    def put_high_priority(self, event: Dict[str, Any]):
        """放入高优先级事件"""
        self.put(event, EventPriority.HIGH)

    def put_low_priority(self, event: Dict[str, Any]):
        """放入低优先级事件"""
        self.put(event, EventPriority.LOW)

    def _run(self):
        """事件处理主循环（线程安全）"""
        print(f"[{datetime.now()}] [EventEngine] 事件处理循环开始")

        while self._active:
            try:
                # 获取事件（带超时以避免无限阻塞）
                try:
                    priority, timer, event = self._queue.get(timeout=0.1)
                except queue.Empty:
                    continue

                # 处理事件
                self._process_event(event)
                self._queue.task_done()

            except Exception as e:
                self._stats["failed_events"] += 1
                print(f"[{datetime.now()}] [EventEngine] 事件处理异常: {e}")

    def _process_event(self, event: Dict[str, Any]):
        """处理单个事件（线程安全）"""
        with thread_safe_manager.locked_resource("event_processing"):
            try:
                event_type = event.get("type")
                metadata = event.get("_metadata", {})

                if event_type not in self._handlers:
                    return

                # 调用所有注册的处理器
                for handler in self._handlers[event_type]:
                    try:
                        handler(event)
                        self._stats["processed_events"] += 1
                    except Exception as e:
                        self._stats["failed_events"] += 1
                        print(f"[{datetime.now()}] [EventEngine] 事件处理器异常: {e}")

                # 记录处理成功
                if self._stats["processed_events"] % 100 == 0:
                    print(f"[{datetime.now()}] [EventEngine] 已处理 {self._stats['processed_events']} 个事件")

            except Exception as e:
                self._stats["failed_events"] += 1
                print(f"[{datetime.now()}] [EventEngine] 事件处理失败: {e}")

    def wait(self, timeout: Optional[float] = None):
        """等待所有事件处理完成（线程安全）"""
        start_time = time.time()

        while not self._queue.empty():
            if timeout and (time.time() - start_time) > timeout:
                break
            time.sleep(0.01)

        # 额外等待一小段时间确保处理完成
        time.sleep(0.1)

    def get_stats(self) -> Dict[str, Any]:
        """获取事件引擎统计信息（线程安全）"""
        with thread_safe_manager.locked_resource("event_stats"):
            current_time = datetime.now()
            start_time = self._stats.get("start_time")

            if start_time:
                uptime = (current_time - start_time).total_seconds()
            else:
                uptime = 0.0

            queue_size = self._queue.qsize()

            stats = self._stats.copy()
            stats.update({
                "uptime_seconds": uptime,
                "queue_size": queue_size,
                "current_time": current_time,
                "is_active": self._active,
                "thread_alive": self._thread.is_alive() if self._thread else False
            })

            return stats

    def clear_handlers(self, event_type: str = None):
        """清空事件处理器（线程安全）"""
        with thread_safe_manager.locked_resource("event_handler_clear"):
            if event_type:
                if event_type in self._handlers:
                    self._handlers[event_type].clear()
                    print(f"[{datetime.now()}] [EventEngine] 清空事件处理器: {event_type}")
            else:
                self._handlers.clear()
                print(f"[{datetime.now()}] [EventEngine] 清空所有事件处理器")

    def clear_queue(self):
        """清空事件队列（线程安全）"""
        with thread_safe_manager.locked_resource("event_queue_clear"):
            while not self._queue.empty():
                try:
                    self._queue.get_nowait()
                    self._queue.task_done()
                except queue.Empty:
                    break
            print(f"[{datetime.now()}] [EventEngine] 事件队列已清空")


# 测试代码
if __name__ == "__main__":
    # 创建事件引擎实例
    engine = EventEngine()


    # 测试事件处理器
    def test_handler(event):
        print(f"处理事件: {event.get('type')} - {event.get('data', {})}")


    def error_handler(event):
        raise ValueError("测试异常处理")


    # 注册处理器
    engine.register("test_event", test_handler)
    engine.register("error_event", error_handler)

    # 启动引擎
    engine.start()

    # 发送测试事件
    engine.put({"type": "test_event", "data": {"message": "Hello"}})
    engine.put_high_priority({"type": "test_event", "data": {"message": "High Priority"}})
    engine.put_low_priority({"type": "error_event", "data": {"message": "Error Test"}})

    # 等待处理完成
    engine.wait(timeout=2.0)

    # 获取统计信息
    stats = engine.get_stats()
    print("事件引擎统计:", stats)

    # 停止引擎
    engine.stop()

    print("事件引擎测试完成")
