class OrderTable:
    """
    订单表，管理委托订单的创建、成交和状态更新。
    """
    def __init__(self):
        self.orders = {}  # 格式: {order_id: {"symbol": str, "direction": str, "volume": int, "status": str}}

    def update(self, order_data):
        """
        更新订单数据。
        :param order_data: 字典，需包含 order_id, symbol, direction, volume, status
        """
        order_id = order_data.get("order_id")
        if order_id not in self.orders:
            self.orders[order_id] = {}
        self.orders[order_id].update(order_data)

    def get_order(self, order_id):
        """获取指定订单信息"""
        return self.orders.get(order_id)

    def get_active_orders(self):
        """获取所有活跃订单（状态为 pending 或 partial）"""
        active = {}
        for order_id, order in self.orders.items():
            if order.get("status") in ["pending", "partial"]:
                active[order_id] = order
        return active

    def validate(self):
        """验证订单数据，例如订单状态必须为预设值"""
        valid_statuses = ["pending", "partial", "filled", "cancelled"]
        for order_id, order in self.orders.items():
            if order.get("status") not in valid_statuses:
                return False, f"订单 {order_id} 状态无效"
        return True, "数据有效"

# 测试代码
if __name__ == "__main__":
    order_table = OrderTable()
    order_table.update({"order_id": "001", "symbol": "SHFE.cu2401", "direction": "buy", "volume": 5, "status": "pending"})
    print("订单数据:", order_table.orders)
    print("活跃订单:", order_table.get_active_orders())
    is_valid, msg = order_table.validate()
    print("验证结果:", msg)
