# my_tqsdk_tables/account.py

from typing import Optional

class MyAccount:
    """
    自定义账户资金数据表。
    基于天勤(TQSdk)的Account对象，但移除协程依赖，改为纯数据对象，风格更接近vn.py。
    """

    def __init__(self, gateway_name: str = "", accountid: str = ""):
        """
        初始化账户对象。

        Args:
            gateway_name: 网关名称，如 "CTP", "TTS" 等。
            accountid: 账户ID，如 "123456".
        """
        # 账户标识信息
        self.gateway_name: str = gateway_name
        self.accountid: str = accountid
        self.vt_accountid: str = f"{gateway_name}.{accountid}"  # 虚拟账户ID，全局唯一

        # 资金数据
        self.currency: str = "CNY"  # 币种
        self.balance: float = 0.0    # 账户权益（动态权益）
        self.available: float = 0.0  # 可用资金
        self.frozen: float = 0.0     # 冻结资金（含冻结保证金、手续费等）
        self.close_profit: float = 0.0  # 平仓盈亏
        self.position_profit: float = 0.0  # 持仓盈亏
        self.commission: float = 0.0    # 手续费
        self.risk_ratio: float = 0.0    # 风险度

    def update_from_tqsdk(self, tqsdk_account) -> None:
        """
        同步更新方法：从一个天勤的Account对象更新数据。

        注意：这是一个**同步**方法，调用者需负责提供最新的天勤数据。
        此方法不涉及任何网络请求或等待，完全符合“去协程化”要求。

        Args:
            tqsdk_account: 从天勤API获取的Account对象。
        """
        # 1. 更新基础资金信息
        self.balance = tqsdk_account.balance
        self.available = tqsdk_account.available
        self.frozen = tqsdk_account.frozen_margin  # 使用冻结保证金代表总冻结资金

        # 2. 更新盈亏与费用信息
        self.close_profit = tqsdk_account.close_profit
        self.position_profit = tqsdk_account.position_profit
        self.commission = tqsdk_account.commission

        # 3. 更新风险指标
        self.risk_ratio = tqsdk_account.risk_ratio

        # 注意：天勤的 currency, accountid 等字段通常在对象创建后不变，
        # 所以这里没有更新它们。如果会变，则需要添加相应更新逻辑。

    def __repr__(self) -> str:
        """用于打印对象的易读信息，便于调试。"""
        return (f"MyAccount(accountid={self.accountid}, "
                f"balance={self.balance}, available={self.available}, "
                f"frozen={self.frozen}, close_profit={self.close_profit})")