"""
改进的订单表
融合订单状态机和线程安全保护
"""
from typing import Dict, Any, List, Optional
from datetime import datetime
from core.thread_safe_manager import thread_safe_manager


class OrderTable:
    """订单表（已增强订单生命周期管理和线程安全）"""

    # 订单状态定义（融合第一个架构的状态机）
    STATUS_NOTTRADED = "未成交"
    STATUS_PARTTRADED = "部分成交"
    STATUS_ALLTRADED = "全部成交"
    STATUS_CANCELLED = "已撤销"
    STATUS_REJECTED = "已拒绝"

    def __init__(self):
        self.data: List[Dict[str, Any]] = []
        self._orders: Dict[str, Dict[str, Any]] = {}  # order_id -> order_data
        self._next_order_id = 1

    def _generate_order_id(self) -> str:
        """生成唯一订单ID"""
        order_id = f"ORDER_{self._next_order_id}_{int(datetime.now().timestamp() * 1000)}"
        self._next_order_id += 1
        return order_id

    def create_order(self, symbol: str, direction: str, price: float,
                     volume: int, strategy: str) -> str:
        """创建新订单（线程安全）"""
        with thread_safe_manager.locked_resource("order_table"):
            order_id = self._generate_order_id()

            order_data = {
                "order_id": order_id,
                "symbol": symbol,
                "direction": direction,  # BUY/SELL/SHORT/COVER
                "price": price,
                "volume": volume,
                "traded_volume": 0,  # 已成交数量
                "status": self.STATUS_NOTTRADED,
                "strategy": strategy,
                "create_time": datetime.now(),
                "update_time": datetime.now(),
                "trade_records": []  # 成交记录
            }

            self._orders[order_id] = order_data
            self.data.append(order_data.copy())

            print(f"创建订单: {order_id} {direction} {symbol} {volume}手 @ {price}")
            return order_id

    def update_order_status(self, order_id: str, status: str,
                            traded_volume: int = 0, trade_data: Dict[str, Any] = None):
        """更新订单状态（线程安全）"""
        with thread_safe_manager.locked_resource("order_table"):
            if order_id not in self._orders:
                raise ValueError(f"订单不存在: {order_id}")

            order = self._orders[order_id]
            old_status = order["status"]

            # 更新订单状态
            order["status"] = status
            order["update_time"] = datetime.now()

            if traded_volume > 0:
                order["traded_volume"] += traded_volume

            if trade_data:
                order["trade_records"].append(trade_data)

            # 检查订单是否完成
            if status in [self.STATUS_ALLTRADED, self.STATUS_CANCELLED, self.STATUS_REJECTED]:
                order["complete_time"] = datetime.now()

            print(f"订单状态更新: {order_id} {old_status} -> {status}")

    def cancel_order(self, order_id: str) -> bool:
        """撤销订单（线程安全）"""
        with thread_safe_manager.locked_resource("order_table"):
            if order_id not in self._orders:
                return False

            order = self._orders[order_id]
            if order["status"] in [self.STATUS_ALLTRADED, self.STATUS_CANCELLED]:
                return False

            self.update_order_status(order_id, self.STATUS_CANCELLED)
            return True

    def add_trade(self, order_id: str, trade_id: str, price: float,
                  volume: int, trade_time: datetime = None):
        """添加成交记录（线程安全）"""
        with thread_safe_manager.locked_resource("order_table"):
            if order_id not in self._orders:
                raise ValueError(f"订单不存在: {order_id}")

            order = self._orders[order_id]
            trade_data = {
                "trade_id": trade_id,
                "price": price,
                "volume": volume,
                "trade_time": trade_time or datetime.now()
            }

            # 更新订单状态
            new_traded_volume = order["traded_volume"] + volume
            total_volume = order["volume"]

            if new_traded_volume == 0:
                status = self.STATUS_NOTTRADED
            elif new_traded_volume < total_volume:
                status = self.STATUS_PARTTRADED
            else:
                status = self.STATUS_ALLTRADED

            self.update_order_status(order_id, status, volume, trade_data)

    def get_order(self, order_id: str) -> Optional[Dict[str, Any]]:
        """获取订单信息（线程安全）"""
        with thread_safe_manager.locked_resource("order_table"):
            return self._orders.get(order_id, {}).copy()

    def get_orders_by_strategy(self, strategy: str) -> List[Dict[str, Any]]:
        """根据策略获取订单列表（线程安全）"""
        with thread_safe_manager.locked_resource("order_table"):
            return [order.copy() for order in self._orders.values()
                    if order.get("strategy") == strategy]

    def get_active_orders(self) -> List[Dict[str, Any]]:
        """获取活跃订单（未成交/部分成交）（线程安全）"""
        with thread_safe_manager.locked_resource("order_table"):
            active_statuses = [self.STATUS_NOTTRADED, self.STATUS_PARTTRADED]
            return [order.copy() for order in self._orders.values()
                    if order.get("status") in active_statuses]

    def get_all_orders(self) -> List[Dict[str, Any]]:
        """获取所有订单（线程安全）"""
        with thread_safe_manager.locked_resource("order_table"):
            return [order.copy() for order in self._orders.values()]

    def reset(self):
        """重置订单表（线程安全）"""
        with thread_safe_manager.locked_resource("order_table"):
            self.data.clear()
            self._orders.clear()
            self._next_order_id = 1
            print("订单表已重置")


# 测试代码
if __name__ == "__main__":
    # 创建订单表实例
    order_table = OrderTable()

    # 测试创建订单
    order_id1 = order_table.create_order("SHFE.cu2401", "BUY", 68000.0, 2, "double_ma")
    order_id2 = order_table.create_order("SHFE.cu2401", "SELL", 68500.0, 1, "double_ma")

    # 测试成交
    order_table.add_trade(order_id1, "TRADE_001", 67950.0, 1)
    order_table.add_trade(order_id1, "TRADE_002", 67900.0, 1)
    order_table.add_trade(order_id2, "TRADE_003", 68450.0, 1)

    # 获取订单信息
    order1 = order_table.get_order(order_id1)
    order2 = order_table.get_order(order_id2)

    print("订单1状态:", order1["status"])
    print("订单2状态:", order2["status"])

    # 测试撤销订单
    order_id3 = order_table.create_order("SHFE.cu2401", "BUY", 67500.0, 1, "test")
    order_table.cancel_order(order_id3)

    # 测试线程安全
    import concurrent.futures


    def create_test_order(thread_id):
        symbol = f"TEST{thread_id}"
        order_id = order_table.create_order(symbol, "BUY", 100.0, 1, f"thread_{thread_id}")
        order_table.add_trade(order_id, f"TRADE_T{thread_id}", 99.0, 1)


    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        futures = [executor.submit(create_test_order, i) for i in range(5)]
        concurrent.futures.wait(futures)

    active_orders = order_table.get_active_orders()
    print(f"活跃订单数量: {len(active_orders)}")
