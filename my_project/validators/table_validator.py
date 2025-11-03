class TableValidator:
    """
    数据表验证器，检查所有数据表的一致性和完整性。
    """
    def __init__(self, data_manager):
        self.data_manager = data_manager

    def validate_all_tables(self):
        """
        验证所有数据表，返回验证报告。
        :return: 字典，键为表名，值为 (是否有效, 错误信息) 元组
        """
        report = {}
        for table_name, table_instance in self.data_manager.tables.items():
            if hasattr(table_instance, 'validate'):
                is_valid, message = table_instance.validate()
                report[table_name] = (is_valid, message)
            else:
                report[table_name] = (False, "表缺少验证方法")
        return report

    def validate_cross_table(self):
        """
        跨表验证，检查表间数据一致性（如订单与成交匹配）。
        :return: 跨表验证结果
        """
        # 示例验证：订单总成交量应等于成交总成交量
        order_table = self.data_manager.get_table("order")
        trade_table = self.data_manager.get_table("trade")

        if order_table and trade_table:
            total_order_volume = sum(order.get("volume", 0) for order in order_table.orders.values())
            total_trade_volume = sum(trade.get("volume", 0) for trade in trade_table.trades)
            if total_order_volume != total_trade_volume:
                return False, f"订单总量 {total_order_volume} 与成交总量 {total_trade_volume} 不匹配"
            return True, "跨表验证通过"
        return False, "缺少订单或成交表"

'''
# 测试代码
if __name__ == "__main__":
    from data_manager import DataManager
    from event_engine import EventEngine
    from account_table import AccountTable
    from order_table import OrderTable
    from trade_table import TradeTable

    engine = EventEngine()
    manager = DataManager(engine)
    manager.add_table("account", AccountTable())
    manager.add_table("order", OrderTable())
    manager.add_table("trade", TradeTable())

    validator = TableValidator(manager)
    print("单表验证:", validator.validate_all_tables())
    print("跨表验证:", validator.validate_cross_table())
'''