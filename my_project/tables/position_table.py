class PositionTable:
    """
    持仓表，管理合约的多空持仓量和成本价。
    """
    def __init__(self):
        self.positions = {}  # 格式: {symbol: {"long": volume, "short": volume, "price": float}}

    def update(self, position_data):
        """
        更新持仓数据。
        :param position_data: 字典，需包含 symbol, direction, volume, price
        """
        symbol = position_data.get("symbol")
        direction = position_data.get("direction")  # "long" 或 "short"
        volume = position_data.get("volume", 0)
        price = position_data.get("price", 0.0)

        if symbol not in self.positions:
            self.positions[symbol] = {"long": 0, "short": 0, "price": 0.0}

        if direction == "long":
            self.positions[symbol]["long"] = volume
        elif direction == "short":
            self.positions[symbol]["short"] = volume
        self.positions[symbol]["price"] = price

    def get_position(self, symbol, direction):
        """获取指定合约和方向的持仓量"""
        if symbol in self.positions:
            return self.positions[symbol].get(direction, 0)
        return 0

    def validate(self):
        """验证持仓数据，例如持仓量不应为负"""
        for symbol, pos in self.positions.items():
            if pos["long"] < 0 or pos["short"] < 0:
                return False, f"{symbol} 持仓量不能为负"
        return True, "数据有效"

# 测试代码
if __name__ == "__main__":
    position_table = PositionTable()
    position_table.update({"symbol": "SHFE.cu2401", "direction": "long", "volume": 10, "price": 55000})
    print("持仓数据:", position_table.positions)
    print("铜多头持仓:", position_table.get_position("SHFE.cu2401", "long"))
    is_valid, msg = position_table.validate()
    print("验证结果:", msg)
