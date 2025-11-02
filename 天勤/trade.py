# my_tqsdk_tables/trade.py

from datetime import datetime
from typing import Optional


class MyTrade:
    """
    自定义成交记录数据表。
    基于天勤(TQSdk)的Trade对象，但移除协程依赖，改为纯数据对象。
    成交记录是交易系统中不可变的原子事实，结构相对简单。
    """

    def __init__(self, gateway_name: str = "", tradeid: str = ""):
        """
        初始化成交记录对象。

        Args:
            gateway_name: 网关名称，如 "CTP"。
            tradeid: 成交ID，网关内唯一。
        """
        # 成交标识信息
        self.gateway_name: str = gateway_name
        self.tradeid: str = tradeid
        self.vt_tradeid: str = f"{gateway_name}.{tradeid}"  # 虚拟成交ID，全局唯一

        # 关联信息
        self.orderid: str = ""  # 对应的订单ID
        self.vt_orderid: str = ""  # 虚拟订单ID，格式: "gateway_name.orderid"
        self.exchange_tradeid: str = ""  # 交易所成交编号

        # 合约信息
        self.symbol: str = ""
        self.exchange: str = ""
        self.vt_symbol: str = ""  # 格式: "symbol.exchange"

        # 成交详情
        self.direction: str = ""  # 方向: "LONG" / "SHORT"
        self.offset: str = "NONE"  # 开平: "OPEN" / "CLOSE" / "CLOSETODAY"
        self.price: float = float("nan")  # 成交价格
        self.volume: int = 0  # 成交数量
        self.datetime: Optional[datetime] = None  # 成交时间

    def update_from_tqsdk(self, tqsdk_trade) -> None:
        """
        同步更新方法：从一个天勤的Trade对象更新数据。

        Args:
            tqsdk_trade: 从天勤API获取的Trade对象。
        """
        # 1. 更新基础信息
        self.orderid = tqsdk_trade.order_id
        self.vt_orderid = f"{self.gateway_name}.{self.orderid}"
        self.exchange_tradeid = tqsdk_trade.exchange_trade_id

        self.symbol = tqsdk_trade.instrument_id
        self.exchange = tqsdk_trade.exchange_id
        self.vt_symbol = f"{self.symbol}.{self.exchange}"

        # 2. 更新成交详情
        self.direction = "LONG" if tqsdk_trade.direction == "BUY" else "SHORT"
        self.offset = tqsdk_trade.offset
        self.price = tqsdk_trade.price
        self.volume = tqsdk_trade.volume

        # 3. 更新时间（将纳秒时间戳转换为datetime）
        if tqsdk_trade.trade_date_time > 0:
            # 将纳秒时间戳转换为秒，然后创建datetime对象
            self.datetime = datetime.fromtimestamp(tqsdk_trade.trade_date_time / 1e9)

    def __repr__(self) -> str:
        """用于打印对象的易读信息，便于调试。"""
        return (f"MyTrade(vt_tradeid={self.vt_tradeid}, symbol={self.symbol}, "
                f"direction={self.direction}, offset={self.offset}, "
                f"price={self.price}, volume={self.volume}, "
                f"time={self.datetime})")