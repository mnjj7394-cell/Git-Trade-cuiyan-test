"""
订单生命周期管理器
管理订单状态流转、成交匹配和执行队列
"""
from typing import Dict, Any, List, Optional, Set
from datetime import datetime, timedelta
from enum import Enum
from core.thread_safe_manager import thread_safe_manager


class OrderStatus(Enum):
    """订单状态枚举"""
    SUBMITTING = "提交中"  # 订单已提交，等待确认
    NOTTRADED = "未成交"  # 订单已确认，等待成交
    PARTTRADED = "部分成交"  # 订单部分成交
    ALLTRADED = "全部成交"  # 订单完全成交
    CANCELLING = "撤销中"  # 订单撤销中
    CANCELLED = "已撤销"  # 订单已撤销
    REJECTED = "已拒绝"  # 订单被拒绝


class OrderLifecycleManager:
    """订单生命周期管理器（已实现完整状态机）"""

    def __init__(self):
        self.orders: Dict[str, Dict[str, Any]] = {}  # order_id -> order_data
        self.order_queue: List[str] = []  # 订单执行队列
        self.trade_matching: Dict[str, List[str]] = {}  # symbol -> [order_ids]
        self.max_queue_size = 1000
        self.order_timeout = timedelta(minutes=5)  # 订单超时时间

    def create_order(self, symbol: str, direction: str, price: float,
                     volume: int, strategy: str, order_type: str = "LIMIT") -> str:
        """创建新订单（线程安全）"""
        with thread_safe_manager.locked_resource("order_creation"):
            order_id = self._generate_order_id()

            order_data = {
                "order_id": order_id,
                "symbol": symbol,
                "direction": direction,  # BUY/SELL/SHORT/COVER
                "price": price,
                "volume": volume,
                "traded_volume": 0,
                "order_type": order_type,  # LIMIT/MARKET
                "status": OrderStatus.SUBMITTING.value,
                "strategy": strategy,
                "create_time": datetime.now(),
                "update_time": datetime.now(),
                "timeout_time": datetime.now() + self.order_timeout,
                "trade_records": [],
                "cancel_requests": 0
            }

            self.orders[order_id] = order_data
            self._add_to_queue(order_id)
            self._add_to_matching(symbol, order_id)

            print(f"订单创建: {order_id} {direction} {symbol} {volume}手 @ {price}")
            return order_id

    def update_order_status(self, order_id: str, status: OrderStatus,
                            traded_volume: int = 0, trade_data: Dict[str, Any] = None):
        """更新订单状态（线程安全）"""
        with thread_safe_manager.locked_resource("order_status_update"):
            if order_id not in self.orders:
                raise ValueError(f"订单不存在: {order_id}")

            order = self.orders[order_id]
            old_status = order["status"]

            # 验证状态转换合法性
            if not self._is_valid_status_transition(old_status, status.value):
                raise ValueError(f"无效状态转换: {old_status} -> {status.value}")

            # 更新订单状态
            order["status"] = status.value
            order["update_time"] = datetime.now()

            if traded_volume > 0:
                order["traded_volume"] += traded_volume

            if trade_data:
                order["trade_records"].append(trade_data)

            # 处理完成订单
            if status in [OrderStatus.ALLTRADED, OrderStatus.CANCELLED, OrderStatus.REJECTED]:
                order["complete_time"] = datetime.now()
                self._remove_from_queue(order_id)
                self._remove_from_matching(order["symbol"], order_id)

            print(f"订单状态更新: {order_id} {old_status} -> {status.value}")

    def cancel_order(self, order_id: str, reason: str = "") -> bool:
        """撤销订单（线程安全）"""
        with thread_safe_manager.locked_resource("order_cancellation"):
            if order_id not in self.orders:
                return False

            order = self.orders[order_id]
            current_status = order["status"]

            # 检查是否可以撤销
            if current_status in [OrderStatus.ALLTRADED.value, OrderStatus.CANCELLED.value,
                                  OrderStatus.REJECTED.value]:
                return False

            # 增加撤销请求计数
            order["cancel_requests"] += 1
            order["cancel_reason"] = reason

            if current_status == OrderStatus.SUBMITTING.value:
                # 直接撤销提交中的订单
                self.update_order_status(order_id, OrderStatus.CANCELLED)
            else:
                # 其他状态先转为撤销中
                self.update_order_status(order_id, OrderStatus.CANCELLING)

            print(f"订单撤销请求: {order_id} - {reason}")
            return True

    def match_trade(self, symbol: str, price: float, volume: int,
                    trade_time: datetime = None) -> List[Dict[str, Any]]:
        """匹配成交（线程安全）"""
        with thread_safe_manager.locked_resource("trade_matching"):
            matched_orders = []
            remaining_volume = volume

            # 获取该品种的待匹配订单
            order_ids = self.trade_matching.get(symbol, [])

            for order_id in order_ids:
                if remaining_volume <= 0:
                    break

                order = self.orders[order_id]
                if self._can_match_order(order, price):
                    matched_volume = min(remaining_volume,
                                         order["volume"] - order["traded_volume"])

                    if matched_volume > 0:
                        # 创建成交记录
                        trade_id = self._generate_trade_id()
                        trade_data = {
                            "trade_id": trade_id,
                            "order_id": order_id,
                            "symbol": symbol,
                            "price": price,
                            "volume": matched_volume,
                            "trade_time": trade_time or datetime.now(),
                            "match_type": "AUTO"
                        }

                        # 更新订单状态
                        new_traded_volume = order["traded_volume"] + matched_volume
                        if new_traded_volume == order["volume"]:
                            new_status = OrderStatus.ALLTRADED
                        else:
                            new_status = OrderStatus.PARTTRADED

                        self.update_order_status(order_id, new_status, matched_volume, trade_data)
                        matched_orders.append(trade_data)
                        remaining_volume -= matched_volume

            return matched_orders

    def get_order(self, order_id: str) -> Optional[Dict[str, Any]]:
        """获取订单信息（线程安全）"""
        with thread_safe_manager.locked_resource("order_query"):
            return self.orders.get(order_id, {}).copy()

    def get_orders_by_strategy(self, strategy: str) -> List[Dict[str, Any]]:
        """根据策略获取订单列表（线程安全）"""
        with thread_safe_manager.locked_resource("order_query"):
            return [order.copy() for order in self.orders.values()
                    if order.get("strategy") == strategy]

    def get_active_orders(self) -> List[Dict[str, Any]]:
        """获取活跃订单（线程安全）"""
        with thread_safe_manager.locked_resource("order_query"):
            active_statuses = {OrderStatus.SUBMITTING.value, OrderStatus.NOTTRADED.value,
                               OrderStatus.PARTTRADED.value, OrderStatus.CANCELLING.value}
            return [order.copy() for order in self.orders.values()
                    if order.get("status") in active_statuses]

    def cleanup_expired_orders(self) -> List[str]:
        """清理过期订单（线程安全）"""
        with thread_safe_manager.locked_resource("order_cleanup"):
            expired_orders = []
            current_time = datetime.now()

            for order_id, order in self.orders.items():
                if (order["status"] in [OrderStatus.SUBMITTING.value, OrderStatus.NOTTRADED.value] and
                        order.get("timeout_time") and order["timeout_time"] < current_time):
                    self.update_order_status(order_id, OrderStatus.CANCELLED)
                    expired_orders.append(order_id)
                    print(f"订单过期取消: {order_id}")

            return expired_orders

    def get_order_statistics(self) -> Dict[str, Any]:
        """获取订单统计（线程安全）"""
        with thread_safe_manager.locked_resource("order_statistics"):
            total_orders = len(self.orders)
            status_count = {status.value: 0 for status in OrderStatus}

            for order in self.orders.values():
                status = order["status"]
                if status in status_count:
                    status_count[status] += 1

            return {
                "total_orders": total_orders,
                "status_distribution": status_count,
                "queue_size": len(self.order_queue),
                "active_symbols": list(self.trade_matching.keys())
            }

    def _is_valid_status_transition(self, from_status: str, to_status: str) -> bool:
        """验证状态转换是否合法"""
        valid_transitions = {
            OrderStatus.SUBMITTING.value: [OrderStatus.NOTTRADED.value, OrderStatus.CANCELLED.value,
                                           OrderStatus.REJECTED.value],
            OrderStatus.NOTTRADED.value: [OrderStatus.PARTTRADED.value, OrderStatus.ALLTRADED.value,
                                          OrderStatus.CANCELLING.value, OrderStatus.CANCELLED.value],
            OrderStatus.PARTTRADED.value: [OrderStatus.ALLTRADED.value, OrderStatus.CANCELLING.value,
                                           OrderStatus.CANCELLED.value],
            OrderStatus.CANCELLING.value: [OrderStatus.CANCELLED.value],
            OrderStatus.CANCELLED.value: [],
            OrderStatus.ALLTRADED.value: [],
            OrderStatus.REJECTED.value: []
        }

        return to_status in valid_transitions.get(from_status, [])

    def _can_match_order(self, order: Dict[str, Any], price: float) -> bool:
        """检查订单是否可以匹配成交"""
        if order["status"] not in [OrderStatus.NOTTRADED.value, OrderStatus.PARTTRADED.value]:
            return False

        if order["order_type"] == "LIMIT":
            if order["direction"] in ["BUY", "COVER"]:
                return price <= order["price"]  # 买入：当前价格<=限价
            else:
                return price >= order["price"]  # 卖出：当前价格>=限价
        else:  # MARKET
            return True

    def _add_to_queue(self, order_id: str):
        """添加订单到执行队列"""
        if len(self.order_queue) < self.max_queue_size:
            self.order_queue.append(order_id)

    def _remove_from_queue(self, order_id: str):
        """从执行队列移除订单"""
        if order_id in self.order_queue:
            self.order_queue.remove(order_id)

    def _add_to_matching(self, symbol: str, order_id: str):
        """添加订单到匹配池"""
        if symbol not in self.trade_matching:
            self.trade_matching[symbol] = []
        self.trade_matching[symbol].append(order_id)

    def _remove_from_matching(self, symbol: str, order_id: str):
        """从匹配池移除订单"""
        if symbol in self.trade_matching and order_id in self.trade_matching[symbol]:
            self.trade_matching[symbol].remove(order_id)

    def _generate_order_id(self) -> str:
        """生成唯一订单ID"""
        timestamp = int(datetime.now().timestamp() * 1000)
        return f"ORDER_{timestamp}_{len(self.orders)}"

    def _generate_trade_id(self) -> str:
        """生成唯一成交ID"""
        timestamp = int(datetime.now().timestamp() * 1000)
        return f"TRADE_{timestamp}"


# 测试代码
if __name__ == "__main__":
    # 创建订单生命周期管理器实例
    order_manager = OrderLifecycleManager()

    # 测试创建订单
    order_id1 = order_manager.create_order("SHFE.cu2401", "BUY", 68000.0, 2, "double_ma")
    order_id2 = order_manager.create_order("SHFE.cu2401", "SELL", 68500.0, 1, "double_ma")

    # 测试订单状态更新
    order_manager.update_order_status(order_id1, OrderStatus.NOTTRADED)

    # 测试成交匹配
    matched_trades = order_manager.match_trade("SHFE.cu2401", 67900.0, 3)
    print("匹配成交:", matched_trades)

    # 测试订单撤销
    order_manager.cancel_order(order_id2, "测试撤销")

    # 测试获取订单信息
    order1 = order_manager.get_order(order_id1)
    print("订单1信息:", order1)

    # 测试订单统计
    stats = order_manager.get_order_statistics()
    print("订单统计:", stats)

    # 测试清理过期订单
    expired = order_manager.cleanup_expired_orders()
    print("过期订单:", expired)

    # 测试线程安全
    import concurrent.futures


    def create_test_orders(thread_id):
        symbol = f"TEST{thread_id}"
        for i in range(3):
            order_manager.create_order(symbol, "BUY", 100.0, 1, f"thread_{thread_id}")


    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        futures = [executor.submit(create_test_orders, i) for i in range(5)]
        concurrent.futures.wait(futures)

    final_stats = order_manager.get_order_statistics()
    print(f"总订单数: {final_stats['total_orders']}")
