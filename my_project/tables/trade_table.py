"""
改进的成交表
融合成交记录逻辑和线程安全保护
"""
from typing import Dict, Any, List, Optional
from datetime import datetime
from core.thread_safe_manager import thread_safe_manager


class TradeTable:
    """成交表（已增强成交统计分析和线程安全）"""

    def __init__(self):
        self.data: List[Dict[str, Any]] = []
        self._trades: Dict[str, Dict[str, Any]] = {}  # trade_id -> trade_data
        self._next_trade_id = 1

    def _generate_trade_id(self) -> str:
        """生成唯一成交ID"""
        trade_id = f"TRADE_{self._next_trade_id}_{int(datetime.now().timestamp() * 1000)}"
        self._next_trade_id += 1
        return trade_id

    def add_trade(self, symbol: str, direction: str, price: float, volume: int,
                  strategy: str, order_id: str = None, commission: float = 0.0) -> str:
        """添加成交记录（线程安全）"""
        with thread_safe_manager.locked_resource("trade_table"):
            trade_id = self._generate_trade_id()

            trade_data = {
                "trade_id": trade_id,
                "order_id": order_id,
                "symbol": symbol,
                "direction": direction,  # BUY/SELL/SHORT/COVER
                "price": price,
                "volume": volume,
                "amount": price * volume,  # 成交金额
                "commission": commission,
                "strategy": strategy,
                "trade_time": datetime.now(),
                "timestamp": datetime.now()
            }

            self._trades[trade_id] = trade_data
            self.data.append(trade_data.copy())

            print(f"成交记录: {trade_id} {direction} {symbol} {volume}手 @ {price}, 手续费: {commission:.2f}")
            return trade_id

    def get_trade(self, trade_id: str) -> Optional[Dict[str, Any]]:
        """获取成交信息（线程安全）"""
        with thread_safe_manager.locked_resource("trade_table"):
            return self._trades.get(trade_id, {}).copy()

    def get_trades_by_symbol(self, symbol: str) -> List[Dict[str, Any]]:
        """根据品种获取成交列表（线程安全）"""
        with thread_safe_manager.locked_resource("trade_table"):
            return [trade.copy() for trade in self._trades.values()
                    if trade.get("symbol") == symbol]

    def get_trades_by_strategy(self, strategy: str) -> List[Dict[str, Any]]:
        """根据策略获取成交列表（线程安全）"""
        with thread_safe_manager.locked_resource("trade_table"):
            return [trade.copy() for trade in self._trades.values()
                    if trade.get("strategy") == strategy]

    def get_trades_by_time_range(self, start_time: datetime, end_time: datetime) -> List[Dict[str, Any]]:
        """根据时间范围获取成交列表（线程安全）"""
        with thread_safe_manager.locked_resource("trade_table"):
            return [trade.copy() for trade in self._trades.values()
                    if start_time <= trade.get("trade_time") <= end_time]

    def calculate_statistics(self, strategy: str = None) -> Dict[str, Any]:
        """计算成交统计（线程安全）"""
        with thread_safe_manager.locked_resource("trade_table"):
            trades = self.get_trades_by_strategy(strategy) if strategy else list(self._trades.values())

            if not trades:
                return {
                    "total_trades": 0,
                    "total_volume": 0,
                    "total_amount": 0.0,
                    "total_commission": 0.0,
                    "avg_price": 0.0
                }

            total_trades = len(trades)
            total_volume = sum(trade.get("volume", 0) for trade in trades)
            total_amount = sum(trade.get("amount", 0.0) for trade in trades)
            total_commission = sum(trade.get("commission", 0.0) for trade in trades)
            avg_price = total_amount / total_volume if total_volume > 0 else 0.0

            # 按方向统计
            buy_trades = [t for t in trades if t.get("direction") in ["BUY", "COVER"]]
            sell_trades = [t for t in trades if t.get("direction") in ["SELL", "SHORT"]]

            statistics = {
                "total_trades": total_trades,
                "total_volume": total_volume,
                "total_amount": total_amount,
                "total_commission": total_commission,
                "avg_price": avg_price,
                "buy_trades": len(buy_trades),
                "sell_trades": len(sell_trades),
                "buy_volume": sum(t.get("volume", 0) for t in buy_trades),
                "sell_volume": sum(t.get("volume", 0) for t in sell_trades)
            }

            return statistics

    def get_all_trades(self) -> List[Dict[str, Any]]:
        """获取所有成交记录（线程安全）"""
        with thread_safe_manager.locked_resource("trade_table"):
            return [trade.copy() for trade in self._trades.values()]

    def reset(self):
        """重置成交表（线程安全）"""
        with thread_safe_manager.locked_resource("trade_table"):
            self.data.clear()
            self._trades.clear()
            self._next_trade_id = 1
            print("成交表已重置")


# 测试代码
if __name__ == "__main__":
    # 创建成交表实例
    trade_table = TradeTable()

    # 测试添加成交
    trade_id1 = trade_table.add_trade("SHFE.cu2401", "BUY", 68000.0, 2, "double_ma", "ORDER_001", 12.5)
    trade_id2 = trade_table.add_trade("SHFE.cu2401", "SELL", 68500.0, 1, "double_ma", "ORDER_002", 6.25)
    trade_id3 = trade_table.add_trade("SHFE.cu2401", "BUY", 67800.0, 1, "double_ma", "ORDER_003", 6.25)

    # 获取成交信息
    trade1 = trade_table.get_trade(trade_id1)
    print("成交1信息:", trade1)

    # 计算统计
    stats = trade_table.calculate_statistics("double_ma")
    print("成交统计:", stats)

    # 测试线程安全
    import concurrent.futures


    def add_test_trade(thread_id):
        symbol = f"TEST{thread_id}"
        trade_table.add_trade(symbol, "BUY", 100.0, 1, "test_strategy", commission=2.0)


    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        futures = [executor.submit(add_test_trade, i) for i in range(5)]
        concurrent.futures.wait(futures)

    all_stats = trade_table.calculate_statistics()
    print(f"总成交数: {all_stats['total_trades']}")
