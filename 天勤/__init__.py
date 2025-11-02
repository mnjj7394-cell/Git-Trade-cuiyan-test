"""
My TQSdk Tables
一套基于天勤(TQSdk)数据模型、参考vn.py风格构建的简易数据表。
"""

# 导入即将创建的五个核心类，使其可以从包外部直接引用
# 例如：from my_tqsdk_tables import MyAccount, MyOrder
from .account import MyAccount
#from .sub_account import MySubAccount（考虑是否需要）
from .order import MyOrder
from .trade import MyTrade
from .position import MyPosition

__version__ = "0.1.0"  # 版本号