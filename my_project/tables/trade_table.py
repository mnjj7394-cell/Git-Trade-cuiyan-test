class TradeTable:
    """
    成交表，记录每笔成交的明细信息。
    """
    def __init__(self):
        self.trades = []  # 列表格式，每个元素为成交字典

    def update(self, trade_data):
        """
        添加成交记录。
        :param trade_data: 字典，需包含 trade_id, order_id, symbol, volume, price
        """
        self.trades.append(trade_data)

    def get_trades_by_symbol(self, symbol):
        """获取指定合约的所有成交"""
        return [trade for trade in self.trades if trade.get("symbol") == symbol]

    def get_total_volume(self, symbol):
        """获取指定合约的总成交手数"""
        total = 0
        for trade in self.trades:
            if trade.get("symbol") == symbol:
                total += trade.get("volume", 0)
        return total

    def validate(self):
        """验证成交数据，例如成交价和成交量应为正数"""
        for trade in self.trades:
            if trade.get("price", 0) <= 0:
                return False, f"成交 {trade.get('trade_id')} 价格无效"
            if trade.get("volume", 0) <= 0:
                return False, f"成交 {trade.get('trade_id')} 手数无效"
        return True, "数据有效"

# 测试代码
if __name__ == "__main__":
    trade_table = TradeTable()
    trade_table.update({"trade_id": "T001", "order_id": "001", "symbol": "SHFE.cu2401", "volume": 3, "price": 55000})
    print("成交记录:", trade_table.trades)
    print("铜成交总量:", trade_table.get_total_volume("SHFE.cu2401"))
    is_valid, msg = trade_table.validate()
    print("验证结果:", msg)
