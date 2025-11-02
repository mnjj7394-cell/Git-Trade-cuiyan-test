# my_tqsdk_tables/order.py

from datetime import datetime
from typing import Optional

class MyOrder:
    """
    自定义委托订单数据表。
    基于天勤(TQSdk)的Order对象，但移除协程依赖，改为纯数据对象。
    核心在于将天勤的简单订单状态(ALIVE/FINISHED)映射为更精细的vn.py风格状态。
    """

    def __init__(self, gateway_name: str = "", orderid: str = ""):
        """
        初始化委托单对象。

        Args:
            gateway_name: 网关名称，如 "CTP"。
            orderid: 订单ID，网关内唯一。
        """
        # 订单标识信息
        self.gateway_name: str = gateway_name
        self.orderid: str = orderid
        self.vt_orderid: str = f"{gateway_name}.{orderid}"  # 虚拟订单ID，全局唯一
        self.exchange_orderid: str = ""  # 交易所订单号

        # 合约信息
        self.symbol: str = ""
        self.exchange: str = ""
        self.vt_symbol: str = ""  # 格式: "symbol.exchange"

        # 订单属性
        self.direction: str = ""  # 方向: "LONG" / "SHORT"
        self.offset: str = "NONE"  # 开平: "OPEN" / "CLOSE" / "CLOSETODAY"
        self.type: str = "LIMIT"   # 类型: "LIMIT" / "MARKET"
        self.volume: int = 0       # 总委托数量
        self.traded: int = 0       # 已成交数量
        self.price: float = float("nan")  # 委托价格（限价单）
        self.trade_price: float = float("nan")  # 平均成交价

        # 订单设置（从天勤映射，按需使用）
        self.volume_condition: str = "ANY"  # 手数条件
        self.time_condition: str = "GFD"    # 时间条件

        # 状态与时间
        self.status: str = "SUBMITTING"  # 订单状态（核心字段，使用vn.py风格状态码）
        self.last_msg: str = ""          # 状态信息
        self.datetime: Optional[datetime] = None  # 下单时间（Python datetime对象）

    def update_from_tqsdk(self, tqsdk_order) -> None:
        """
        同步更新方法：从一个天勤的Order对象更新数据。

        此方法执行关键的状态映射逻辑，将天勤的简单状态转换为更精细的状态。

        Args:
            tqsdk_order: 从天勤API获取的Order对象。
        """
        # 1. 更新基础信息
        self.symbol = tqsdk_order.instrument_id
        self.exchange = tqsdk_order.exchange_id
        self.vt_symbol = f"{self.symbol}.{self.exchange}"
        self.exchange_orderid = tqsdk_order.exchange_order_id

        # 2. 更新订单属性
        self.direction = "LONG" if tqsdk_order.direction == "BUY" else "SHORT"
        self.offset = tqsdk_order.offset
        self.type = "LIMIT" if tqsdk_order.price_type == "LIMIT" else "MARKET"
        self.volume = tqsdk_order.volume_orign
        self.traded = tqsdk_order.volume_orign - tqsdk_order.volume_left  # 计算已成交量
        self.price = tqsdk_order.limit_price
        self.trade_price = tqsdk_order.trade_price

        self.volume_condition = tqsdk_order.volume_condition
        self.time_condition = tqsdk_order.time_condition
        self.last_msg = tqsdk_order.last_msg

        # 3. 关键步骤：映射并更新订单状态
        self._update_status(tqsdk_order)

        # 4. 更新时间（将纳秒时间戳转换为datetime）
        if tqsdk_order.insert_date_time > 0:
            # 将纳秒时间戳转换为秒，然后创建datetime对象
            self.datetime = datetime.fromtimestamp(tqsdk_order.insert_date_time / 1e9)

    def _update_status(self, tqsdk_order) -> None:
        """
        内部方法：根据天勤订单状态和成交情况，推断并设置vn.py风格的精细状态。
        这是此类最核心的逻辑。
        """
        # 计算已成交数量
        traded_volume = tqsdk_order.volume_orign - tqsdk_order.volume_left

        if tqsdk_order.status == "FINISHED":
            # 订单已结束
            if traded_volume == 0:
                self.status = "CANCELLED"  # 已撤销
            elif traded_volume == tqsdk_order.volume_orign:
                self.status = "ALLTRADED"  # 全部成交
            else:
                # 部分成交后剩余部分被撤销
                self.status = "CANCELLED"  # 天勤的FINISHED可能包含这种情况
        else:
            # 订单仍活跃 (ALIVE)
            if traded_volume == 0:
                self.status = "NOTTRADED"  # 未成交
            elif traded_volume < tqsdk_order.volume_orign:
                self.status = "PARTTRADED" # 部分成交
            else:
                self.status = "ALLTRADED"  # 全部成交（保护性逻辑）

    def is_active(self) -> bool:
        """
        判断订单是否仍处于活跃状态（可成交或可撤销）。
        对应于vn.py的Status.ACTIVE_STATUSES。
        """
        return self.status in ["SUBMITTING", "NOTTRADED", "PARTTRADED"]

    def __repr__(self) -> str:
        """用于打印对象的易读信息，便于调试。"""
        return (f"MyOrder(vt_orderid={self.vt_orderid}, symbol={self.symbol}, "
                f"direction={self.direction}, offset={self.offset}, "
                f"status={self.status}, volume={self.volume}, traded={self.traded}, "
                f"price={self.price})")