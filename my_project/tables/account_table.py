class AccountTable:
    """
    资金账户表，存储账户余额、可用资金等数据。
    """
    def __init__(self):
        self.data = {
            "balance": 0.0,      # 账户余额
            "available": 0.0,    # 可用资金
            "commission": 0.0    # 手续费
        }

    def update(self, new_data):
        """
        更新账户数据。
        :param new_data: 新数据字典，键值对形式
        """
        self.data.update(new_data)

    def get_balance(self):
        """获取账户余额"""
        return self.data["balance"]

    def validate(self):
        """验证数据一致性，例如余额不应为负"""
        if self.data["balance"] < 0:
            return False, "余额不能为负"
        if self.data["available"] > self.data["balance"]:
            return False, "可用资金不能大于余额"
        return True, "数据有效"

# 测试代码
if __name__ == "__main__":
    account = AccountTable()
    account.update({"balance": 100000, "available": 100000})
    print("初始数据:", account.data)
    account.update({"balance": 95000})  # 模拟交易扣费
    print("更新后数据:", account.data)
    is_valid, msg = account.validate()
    print("验证结果:", msg)
