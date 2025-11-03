#!/usr/bin/env python
#  -*- coding: utf-8 -*-
"""
双均线策略 (Double Moving Average Strategy)
基于短周期和长周期移动平均线的交叉信号进行交易
"""
import pandas as pd
from strategies.base_strategy import BaseStrategy


class DoubleMa(BaseStrategy):
    """双均线策略"""

    def __init__(self, name: str, config: dict = None):
        """
        初始化双均线策略

        Args:
            name: 策略名称
            config: 策略配置参数
        """
        super().__init__(name, config)

        # 策略参数
        self.short_period = config.get("short_period", 30)  # 短周期，默认30
        self.long_period = config.get("long_period", 60)  # 长周期，默认60
        self.symbol = config.get("symbol", "SHFE.bu2012")  # 交易品种
        self.volume = config.get("volume", 1)  # 交易手数

        # 策略状态变量
        self.klines = []  # K线数据列表
        self.position = 0  # 当前持仓方向：0-无持仓，1-多仓，-1-空仓

        # 计算所需的最小K线数量
        self.min_data_length = self.long_period + 2

    def on_init(self):
        """策略初始化回调"""
        super().on_init()
        self.write_log(f"双均线策略初始化完成: 短周期={self.short_period}, 长周期={self.long_period}")

    def on_tick(self, tick_data):
        """Tick数据处理（双均线策略主要使用K线，此方法可留空）"""
        # 双均线策略通常基于K线数据，tick数据可选择性处理
        pass

    def on_bar(self, bar_data):
        """K线数据处理"""
        # 检查K线数据是否匹配策略品种
        if bar_data.get("symbol") != self.symbol:
            return

        # 添加新K线到序列
        self.klines.append(bar_data)

        # 保持K线序列长度不超过最小值
        if len(self.klines) > self.min_data_length + 100:  # 保留一些额外数据
            self.klines = self.klines[-self.min_data_length - 100:]

        # 检查是否有足够的数据计算均线
        if len(self.klines) < self.min_data_length:
            return

        # 提取收盘价序列
        closes = [kline.get("close", 0) for kline in self.klines]

        # 计算移动平均线
        short_ma = self._calculate_ma(closes, self.short_period)
        long_ma = self._calculate_ma(closes, self.long_period)

        # 检查是否有足够的数据点
        if len(short_ma) < 2 or len(long_ma) < 2:
            return

        # 获取前一个和当前周期的均线值
        prev_short = short_ma[-2]
        prev_long = long_ma[-2]
        curr_short = short_ma[-1]
        curr_long = long_ma[-1]

        # 生成交易信号
        self._generate_signal(prev_short, prev_long, curr_short, curr_long, bar_data)

    def _calculate_ma(self, data, period):
        """计算移动平均线"""
        if len(data) < period:
            return []

        ma_values = []
        for i in range(period - 1, len(data)):
            ma_value = sum(data[i - period + 1:i + 1]) / period
            ma_values.append(ma_value)

        return ma_values

    def _generate_signal(self, prev_short, prev_long, curr_short, curr_long, bar_data):
        """生成交易信号"""
        close_price = bar_data.get("close", 0)

        # 死叉信号：短周期下穿长周期，做空
        if (prev_short > prev_long and curr_short < curr_long and
                self.position >= 0):  # 当前无持仓或多头持仓

            if self.position == 1:  # 如果持有多头，先平仓
                self.sell(self.symbol, close_price, self.volume)
                self.write_log(f"平多仓: {self.symbol} @ {close_price}")

            # 开空仓
            self.short(self.symbol, close_price, self.volume)
            self.position = -1
            self.write_log(f"死叉信号，做空: {self.symbol} @ {close_price}")

        # 金叉信号：短周期上穿长周期，做多
        elif (prev_short < prev_long and curr_short > curr_long and
              self.position <= 0):  # 当前无持仓或空头持仓

            if self.position == -1:  # 如果持有空头，先平仓
                self.cover(self.symbol, close_price, self.volume)
                self.write_log(f"平空仓: {self.symbol} @ {close_price}")

            # 开多仓
            self.buy(self.symbol, close_price, self.volume)
            self.position = 1
            self.write_log(f"金叉信号，做多: {self.symbol} @ {close_price}")

    def on_order(self, order_data):
        """订单更新回调"""
        order_status = order_data.get("status", "")
        if order_status in ["ALLTRADED", "CANCELLED", "REJECTED"]:
            self.write_log(f"订单更新: {order_data.get('order_id')} - {order_status}")

    def on_trade(self, trade_data):
        """成交更新回调"""
        self.write_log(f"成交: {trade_data.get('symbol')} {trade_data.get('volume')}手 @ {trade_data.get('price')}")

    def on_stop(self):
        """策略停止回调"""
        # 平掉所有持仓
        if self.position != 0:
            close_price = self.klines[-1].get("close", 0) if self.klines else 0
            if self.position == 1:
                self.sell(self.symbol, close_price, self.volume)
            elif self.position == -1:
                self.cover(self.symbol, close_price, self.volume)

        super().on_stop()
