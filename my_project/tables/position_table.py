"""
改进的持仓表
融合持仓计算逻辑和线程安全保护
"""
from typing import Dict, Any, List, Optional
from datetime import datetime
from core.thread_safe_manager import thread_safe_manager


class PositionTable:
    """持仓表（已增强浮动盈亏计算和线程安全）"""

    def __init__(self):
        self.data: List[Dict[str, Any]] = []
        self._positions: Dict[str, Dict[str, Any]] = {}  # symbol+strategy -> position_data

    def _get_position_key(self, symbol: str, strategy: str) -> str:
        """生成持仓唯一键"""
        return f"{symbol}_{strategy}"

    def update_position(self, symbol: str, strategy: str, direction: str,
                        price: float, volume: int, trade_id: str = None):
        """更新持仓（线程安全）"""
        with thread_safe_manager.locked_resource("position_table"):
            position_key = self._get_position_key(symbol, strategy)

            if position_key not in self._positions:
                # 创建新持仓
                self._positions[position_key] = {
                    "symbol": symbol,
                    "strategy": strategy,
                    "volume": 0,
                    "frozen": 0,
                    "price": 0.0,
                    "pnl": 0.0,
                    "float_pnl": 0.0,
                    "update_time": datetime.now(),
                    "trade_history": []
                }

            position = self._positions[position_key]
            old_volume = position["volume"]
            old_price = position["price"]

            # 计算新持仓
            if direction in ["BUY", "COVER"]:
                # 增加多头持仓或减少空头持仓
                new_volume = old_volume + volume
                if new_volume != 0:
                    position["price"] = (abs(old_volume) * old_price + volume * price) / abs(new_volume)
                position["volume"] = new_volume

            elif direction in ["SELL", "SHORT"]:
                # 减少多头持仓或增加空头持仓
                new_volume = old_volume - volume
                if new_volume != 0:
                    position["price"] = (abs(old_volume) * old_price + volume * price) / abs(new_volume)
                position["volume"] = new_volume

            # 记录交易历史
            trade_record = {
                "trade_id": trade_id or f"TRADE_{int(datetime.now().timestamp() * 1000)}",
                "direction": direction,
                "price": price,
                "volume": volume,
                "timestamp": datetime.now()
            }
            position["trade_history"].append(trade_record)

            position["update_time"] = datetime.now()

            # 添加到数据列表
            self.data.append(position.copy())

            print(
                f"持仓更新: {symbol} {strategy} {direction} {volume}手 @ {price}, 持仓量: {old_volume} -> {new_volume}")

    def calculate_float_pnl(self, symbol: str, strategy: str, current_price: float) -> float:
        """计算浮动盈亏（线程安全）"""
        with thread_safe_manager.locked_resource("position_table"):
            position_key = self._get_position_key(symbol, strategy)
            if position_key not in self._positions:
                return 0.0

            position = self._positions[position_key]
            volume = position["volume"]
            cost_price = position["price"]

            if volume > 0:  # 多头持仓
                float_pnl = (current_price - cost_price) * volume
            elif volume < 0:  # 空头持仓
                float_pnl = (cost_price - current_price) * abs(volume)
            else:
                float_pnl = 0.0

            position["float_pnl"] = float_pnl
            return float_pnl

    def update_pnl(self, symbol: str, strategy: str, pnl: float):
        """更新已实现盈亏（线程安全）"""
        with thread_safe_manager.locked_resource("position_table"):
            position_key = self._get_position_key(symbol, strategy)
            if position_key not in self._positions:
                return

            position = self._positions[position_key]
            position["pnl"] += pnl
            position["update_time"] = datetime.now()

            print(f"盈亏更新: {symbol} {strategy} 盈亏={pnl:.2f}, 累计盈亏={position['pnl']:.2f}")

    def get_position(self, symbol: str, strategy: str) -> Optional[Dict[str, Any]]:
        """获取持仓信息（线程安全）"""
        with thread_safe_manager.locked_resource("position_table"):
            position_key = self._get_position_key(symbol, strategy)
            return self._positions.get(position_key, {}).copy()

    def get_all_positions(self) -> List[Dict[str, Any]]:
        """获取所有持仓（线程安全）"""
        with thread_safe_manager.locked_resource("position_table"):
            return [position.copy() for position in self._positions.values()]

    def get_positions_by_strategy(self, strategy: str) -> List[Dict[str, Any]]:
        """根据策略获取持仓（线程安全）"""
        with thread_safe_manager.locked_resource("position_table"):
            return [position.copy() for position in self._positions.values()
                    if position.get("strategy") == strategy]

    def reset(self):
        """重置持仓表（线程安全）"""
        with thread_safe_manager.locked_resource("position_table"):
            self.data.clear()
            self._positions.clear()
            print("持仓表已重置")


# 测试代码
if __name__ == "__main__":
    # 创建持仓表实例
    position_table = PositionTable()

    # 测试持仓更新
    position_table.update_position("SHFE.cu2401", "double_ma", "BUY", 68000.0, 2, "TRADE_001")
    position_table.update_position("SHFE.cu2401", "double_ma", "SELL", 68500.0, 1, "TRADE_002")

    # 计算浮动盈亏
    current_price = 68200.0
    float_pnl = position_table.calculate_float_pnl("SHFE.cu2401", "double_ma", current_price)
    print(f"浮动盈亏 @ {current_price}: {float_pnl:.2f}")

    # 更新已实现盈亏
    position_table.update_pnl("SHFE.cu2401", "double_ma", 500.0)

    # 获取持仓信息
    position = position_table.get_position("SHFE.cu2401", "double_ma")
    print("持仓信息:", position)

    # 测试线程安全
    import concurrent.futures


    def update_test_position(thread_id):
        symbol = f"TEST{thread_id}"
        position_table.update_position(symbol, "test_strategy", "BUY", 100.0, 1)
        position_table.calculate_float_pnl(symbol, "test_strategy", 105.0)


    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        futures = [executor.submit(update_test_position, i) for i in range(5)]
        concurrent.futures.wait(futures)

    all_positions = position_table.get_all_positions()
    print(f"总持仓数量: {len(all_positions)}")
