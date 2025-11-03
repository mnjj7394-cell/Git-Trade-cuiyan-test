class EventEngine:
    """
    同步事件引擎，管理事件发布和订阅，采用观察者模式。
    """
    def __init__(self):
        self._handlers = {}  # 存储事件类型对应的处理器列表

    def register(self, event_type, handler):
        """
        注册事件处理器。
        :param event_type: 事件类型字符串，如 "market_data"
        :param handler: 处理函数，接收事件字典作为参数
        """
        if event_type not in self._handlers:
            self._handlers[event_type] = []
        self._handlers[event_type].append(handler)

    def put(self, event):
        """
        发布事件，同步调用所有注册的处理器。
        :param event: 事件字典，必须包含 "type" 键表示事件类型
        """
        event_type = event.get('type')
        if event_type in self._handlers:
            for handler in self._handlers[event_type]:
                handler(event)

    def unregister(self, event_type, handler):
        """注销事件处理器"""
        if event_type in self._handlers:
            if handler in self._handlers[event_type]:
                self._handlers[event_type].remove(handler)
'''
# 测试代码
if __name__ == "__main__":
    def test_handler(event):
        print(f"处理事件: {event}")

    engine = EventEngine()
    engine.register("test", test_handler)
    engine.put({"type": "test", "data": "Hello"})  # 应输出 "处理事件: {...}"
'''