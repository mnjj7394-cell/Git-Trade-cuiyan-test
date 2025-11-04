"""
改进的账户表
融合财务计算逻辑和线程安全保护
"""
from typing import Dict, Any, List
from datetime import datetime
from core.thread_safe_manager import thread_safe_manager


class AccountTable:
    """账户表（已增强财务计算和线程安全）"""

    def __init__(self):
        self.data: List[Dict[str, Any]] = []
        self._current_account: Dict[str, Any] = self._create_initial_account()

    def _create_initial_account(self) -> Dict[str, Any]:
        """创建初始账户"""
        return {
            "account_id": "default_account",
            "balance": 1000000.0,  # 初始资金
            "available": 1000000.0,  # 可用资金
            "commission": 0.0,  # 手续费
            "margin": 0.0,  # 保证金
            "close_profit": 0.0,  # 平仓盈亏
            "position_profit": 0.0,  # 持仓盈亏
            "update_time": datetime.now(),
            "history": []  # 资金变动历史
        }

    def update(self, account_data: Dict[str, Any]):
        """更新账户数据（线程安全）"""
        with thread_safe_manager.locked_resource("account_table"):
            # 记录历史变更
            change_record = {
                "timestamp": datetime.now(),
                "before": self._current_account.copy(),
                "after": account_data
            }

            # 更新当前账户
            self._current_account.update(account_data)
            self._current_account["update_time"] = datetime.now()

            # 添加到历史记录
            self._current_account["history"].append(change_record)

            # 添加到数据列表
            self.data.append(self._current_account.copy())

    def update_balance(self, amount: float, description: str = ""):
        """更新资金余额（线程安全）"""
        with thread_safe_manager.locked_resource("account_balance"):
            old_balance = self._current_account["balance"]
            new_balance = old_balance + amount

            update_data = {
                "balance": new_balance,
                "available": self._current_account["available"] + amount
            }

            self.update(update_data)
            print(f"账户余额更新: {old_balance:.2f} -> {new_balance:.2f} ({description})")

    def update_commission(self, commission: float):
        """更新手续费（线程安全）"""
        with thread_safe_manager.locked_resource("account_commission"):
            old_commission = self._current_account["commission"]
            new_commission = old_commission + commission

            update_data = {
                "commission": new_commission,
                "available": self._current_account["available"] - commission
            }

            self.update(update_data)
            print(f"手续费更新: {old_commission:.2f} -> {new_commission:.2f}")

    def update_profit(self, close_profit: float = 0, position_profit: float = 0):
        """更新盈亏（线程安全）"""
        with thread_safe_manager.locked_resource("account_profit"):
            old_close_profit = self._current_account["close_profit"]
            old_position_profit = self._current_account["position_profit"]

            new_close_profit = old_close_profit + close_profit
            new_position_profit = old_position_profit + position_profit

            total_profit = close_profit + position_profit

            update_data = {
                "close_profit": new_close_profit,
                "position_profit": new_position_profit,
                "balance": self._current_account["balance"] + total_profit,
                "available": self._current_account["available"] + total_profit
            }

            self.update(update_data)
            print(f"盈亏更新: 平仓盈亏={close_profit:.2f}, 持仓盈亏={position_profit:.2f}")

    def get_account(self) -> Dict[str, Any]:
        """获取当前账户信息（线程安全）"""
        with thread_safe_manager.locked_resource("account_table"):
            return self._current_account.copy()

    def get_history(self) -> List[Dict[str, Any]]:
        """获取账户变更历史（线程安全）"""
        with thread_safe_manager.locked_resource("account_table"):
            return self._current_account["history"].copy()

    def reset(self):
        """重置账户表（线程安全）"""
        with thread_safe_manager.locked_resource("account_table"):
            self.data.clear()
            self._current_account = self._create_initial_account()
            print("账户表已重置")


# 测试代码
if __name__ == "__main__":
    # 创建账户表实例
    account_table = AccountTable()

    # 测试资金更新
    account_table.update_balance(50000.0, "初始入金")
    account_table.update_commission(120.5)
    account_table.update_profit(close_profit=1500.0, position_profit=800.0)

    # 获取当前账户信息
    current_account = account_table.get_account()
    print("当前账户状态:")
    for key, value in current_account.items():
        if key != "history":
            print(f"  {key}: {value}")

    # 测试线程安全
    import concurrent.futures


    def concurrent_update(thread_id):
        account_table.update_balance(thread_id * 100, f"线程{thread_id}操作")


    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        futures = [executor.submit(concurrent_update, i) for i in range(5)]
        concurrent.futures.wait(futures)

    final_account = account_table.get_account()
    print(f"最终余额: {final_account['balance']:.2f}")
