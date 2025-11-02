# my_tqsdk_tables/position.py

from typing import Optional

class MyPosition:
    """
    自定义持仓数据表。
    基于天勤(TQSdk)的Position对象，但将其拆分为独立的多头（LONG）和空头（SHORT）对象。
    移除协程依赖，改为纯数据对象，风格更接近vn.py。
    """

    def __init__(self, gateway_name: str = "", symbol: str = "", exchange: str = "", direction: str = ""):
        """
        初始化持仓对象。

        Args:
            gateway_name: 网关名称，如 "CTP"。
            symbol: 合约代码，如 "rb2410"。
            exchange: 交易所代码，如 "SHFE"。
            direction: 持仓方向，"LONG" 或 "SHORT"。
        """
        # 持仓标识信息
        self.gateway_name: str = gateway_name
        self.symbol: str = symbol
        self.exchange: str = exchange
        self.direction: str = direction  # "LONG" 或 "SHORT"
        self.vt_symbol: str = f"{symbol}.{exchange}"  # 虚拟合约代码
        self.vt_positionid: str = f"{gateway_name}.{self.vt_symbol}.{direction}"  # 虚拟持仓ID，全局唯一

        # 持仓数据
        self.volume: int = 0           # 总持仓量
        self.yd_volume: int = 0        # 昨仓数量
        self.frozen: int = 0           # 冻结持仓量（已委托平仓未成交）
        self.price: float = float("nan")  # 持仓均价
        self.pnl: float = float("nan")    # 持仓浮动盈亏
        # 以下为可选风控字段
        self.margin: float = float("nan") # 保证金占用

    def update_from_tqsdk(self, tqsdk_position) -> None:
        """
        同步更新方法：从一个天勤的Position对象更新数据。

        重要：此方法应根据当前对象的 direction（方向），只更新对应的半边持仓数据。
        调用者需负责创建两个MyPosition对象（LONG和SHORT）并分别调用此方法。

        Args:
            tqsdk_position: 从天勤API获取的Position对象。
        """
        if self.direction == "LONG":
            # 更新多头持仓数据
            self.volume = tqsdk_position.pos_long_his + tqsdk_position.pos_long_today
            self.yd_volume = tqsdk_position.pos_long_his
            self.frozen = tqsdk_position.volume_long_frozen  # 使用推荐字段
            self.price = tqsdk_position.position_price_long
            self.pnl = tqsdk_position.float_profit_long
            self.margin = tqsdk_position.margin_long

        elif self.direction == "SHORT":
            # 更新空头持仓数据
            self.volume = tqsdk_position.pos_short_his + tqsdk_position.pos_short_today
            self.yd_volume = tqsdk_position.pos_short_his
            self.frozen = tqsdk_position.volume_short_frozen  # 使用推荐字段
            self.price = tqsdk_position.position_price_short
            self.pnl = tqsdk_position.float_profit_short
            self.margin = tqsdk_position.margin_short

        else:
            raise ValueError(f"Invalid direction: {self.direction}. Must be 'LONG' or 'SHORT'.")

    def __repr__(self) -> str:
        """用于打印对象的易读信息，便于调试。"""
        return (f"MyPosition(symbol={self.symbol}, direction={self.direction}, "
                f"volume={self.volume}, yd_volume={self.yd_volume}, "
                f"frozen={self.frozen}, price={self.price:.2f}, pnl={self.pnl:.2f})")