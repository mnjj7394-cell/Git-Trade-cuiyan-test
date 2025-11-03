
from .event_engine import EventEngine


class DataManager:
    """
    数据管理器，集中管理数据表实例并处理事件更新。
    """
    def __init__(self, event_engine):
        self.event_engine = event_engine
        self.tables = {}  # 存储数据表实例
        self._register_event_handlers()

    def add_table(self, table_name, table_instance):
        """
        添加数据表实例。
        :param table_name: 表名，如 "account"
        :param table_instance: 数据表类实例
        """
        self.tables[table_name] = table_instance

    def get_table(self, table_name):
        """获取数据表实例"""
        return self.tables.get(table_name)

    def _register_event_handlers(self):
        """注册事件处理器，根据事件类型更新对应数据表"""
        def generic_handler(event):
            table_name = event.get('table')
            if table_name in self.tables:
                self.tables[table_name].update(event.get('data'))

        # 注册通用处理器，监听所有表事件
        self.event_engine.register("data_update", generic_handler)

    def trigger_update(self, table_name, data):
        """触发数据更新事件"""
        self.event_engine.put({"type": "data_update", "table": table_name, "data": data})


'''
# 测试代码
if __name__ == "__main__":
    from ..tables.account_table import AccountTable

    engine = EventEngine()
    manager = DataManager(engine)
    account_table = AccountTable()
    manager.add_table("account", account_table)

    # 测试触发更新
    manager.trigger_update("account", {"balance": 100000})
    print("账户表数据:", account_table.data)  # 应显示更新后的数据
'''