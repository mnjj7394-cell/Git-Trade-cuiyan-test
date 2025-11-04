"""
财务计算引擎
负责盈亏计算、手续费、保证金、账户权益等财务逻辑
"""
from typing import Dict, Any, List, Optional
from decimal import Decimal, ROUND_HALF_UP
from datetime import datetime
from core.thread_safe_manager import thread_safe_manager


class AccountingEngine:
    """财务计算引擎（已实现完整财务计算逻辑）"""

    def __init__(self):
        self.commission_rates = {
            "SHFE": {"open": 0.0001, "close": 0.0001, "close_today": 0.0001},  # 上期所
            "DCE": {"open": 0.0001, "close": 0.0001},  # 大商所
            "CZCE": {"open": 0.0001, "close": 0.0001},  # 郑商所
            "INE": {"open": 0.0001, "close": 0.0001},  # 能源中心
            "CFFEX": {"open": 0.0001, "close": 0.0001}  # 中金所
        }
        self.margin_rates = {
            "SHFE": 0.1,  # 10%保证金
            "DCE": 0.08,
            "CZCE": 0.08,
            "INE": 0.1,
            "CFFEX": 0.12
        }

    def calculate_commission(self, symbol: str, price: float, volume: int,
                             direction: str, offset: str = "OPEN") -> float:
        """计算手续费（线程安全）"""
        with thread_safe_manager.locked_resource("commission_calculation"):
            exchange = self._get_exchange_from_symbol(symbol)
            rate_config = self.commission_rates.get(exchange, {"open": 0.0001, "close": 0.0001})

            if offset == "CLOSE" and "close" in rate_config:
                rate = rate_config["close"]
            elif offset == "CLOSE_TODAY" and "close_today" in rate_config:
                rate = rate_config["close_today"]
            else:
                rate = rate_config["open"]

            commission = price * volume * rate
            return float(Decimal(str(commission)).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP))

    def calculate_margin(self, symbol: str, price: float, volume: int) -> float:
        """计算保证金（线程安全）"""
        with thread_safe_manager.locked_resource("margin_calculation"):
            exchange = self._get_exchange_from_symbol(symbol)
            rate = self.margin_rates.get(exchange, 0.1)
            margin = price * volume * rate
            return float(Decimal(str(margin)).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP))

    def calculate_trade_pnl(self, symbol: str, entry_price: float, exit_price: float,
                            volume: int, direction: str, commission: float = 0) -> Dict[str, float]:
        """计算交易盈亏（线程安全）"""
        with thread_safe_manager.locked_resource("pnl_calculation"):
            # 计算毛盈亏
            if direction in ["BUY", "COVER"]:  # 多头平仓
                gross_pnl = (exit_price - entry_price) * volume
            else:  # 空头平仓
                gross_pnl = (entry_price - exit_price) * volume

            # 计算净盈亏
            net_pnl = gross_pnl - commission

            # 计算盈亏比例
            investment = entry_price * volume
            if investment > 0:
                pnl_ratio = net_pnl / investment
            else:
                pnl_ratio = 0.0

            return {
                "gross_pnl": float(Decimal(str(gross_pnl)).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)),
                "net_pnl": float(Decimal(str(net_pnl)).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)),
                "commission": commission,
                "pnl_ratio": float(Decimal(str(pnl_ratio)).quantize(Decimal('0.0001'), rounding=ROUND_HALF_UP)),
                "entry_price": entry_price,
                "exit_price": exit_price,
                "volume": volume,
                "direction": direction
            }

    def calculate_position_pnl(self, symbol: str, position_data: Dict[str, Any],
                               current_price: float) -> Dict[str, float]:
        """计算持仓浮动盈亏（线程安全）"""
        with thread_safe_manager.locked_resource("position_pnl_calculation"):
            volume = position_data.get("volume", 0)
            cost_price = position_data.get("price", 0)

            if volume > 0:  # 多头持仓
                float_pnl = (current_price - cost_price) * volume
            elif volume < 0:  # 空头持仓
                float_pnl = (cost_price - current_price) * abs(volume)
            else:
                float_pnl = 0.0

            market_value = current_price * abs(volume)
            cost_value = cost_price * abs(volume)

            if cost_value > 0:
                float_pnl_ratio = float_pnl / cost_value
            else:
                float_pnl_ratio = 0.0

            return {
                "float_pnl": float(Decimal(str(float_pnl)).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)),
                "float_pnl_ratio": float(
                    Decimal(str(float_pnl_ratio)).quantize(Decimal('0.0001'), rounding=ROUND_HALF_UP)),
                "market_value": market_value,
                "cost_value": cost_value,
                "current_price": current_price
            }

    def update_account_equity(self, account_data: Dict[str, Any],
                              close_pnl: float = 0, position_pnl: float = 0,
                              commission: float = 0) -> Dict[str, Any]:
        """更新账户权益（线程安全）"""
        with thread_safe_manager.locked_resource("account_equity_calculation"):
            # 深拷贝账户数据
            updated_account = account_data.copy()

            # 更新手续费
            updated_account["commission"] = account_data.get("commission", 0) + commission

            # 更新平仓盈亏
            updated_account["close_profit"] = account_data.get("close_profit", 0) + close_pnl

            # 更新持仓盈亏
            updated_account["position_profit"] = position_pnl

            # 计算总权益
            balance = account_data.get("balance", 0)
            new_balance = balance + close_pnl - commission
            updated_account["balance"] = new_balance

            # 更新可用资金（考虑保证金占用）
            margin = account_data.get("margin", 0)
            updated_account["available"] = new_balance - margin

            updated_account["update_time"] = datetime.now()

            return updated_account

    def generate_financial_report(self, account_data: Dict[str, Any],
                                  trades: List[Dict[str, Any]],
                                  positions: List[Dict[str, Any]]) -> Dict[str, Any]:
        """生成财务报表（线程安全）"""
        with thread_safe_manager.locked_resource("financial_report"):
            # 计算交易统计
            total_trades = len(trades)
            winning_trades = len([t for t in trades if t.get("net_pnl", 0) > 0])
            losing_trades = total_trades - winning_trades
            win_rate = winning_trades / total_trades if total_trades > 0 else 0

            # 计算盈亏统计
            total_profit = sum(t.get("net_pnl", 0) for t in trades if t.get("net_pnl", 0) > 0)
            total_loss = abs(sum(t.get("net_pnl", 0) for t in trades if t.get("net_pnl", 0) < 0))
            profit_factor = total_profit / total_loss if total_loss > 0 else float('inf')

            # 计算持仓统计
            total_positions = len(positions)
            long_positions = len([p for p in positions if p.get("volume", 0) > 0])
            short_positions = len([p for p in positions if p.get("volume", 0) < 0])

            report = {
                "report_time": datetime.now(),
                "account_summary": {
                    "initial_balance": account_data.get("initial_balance", 0),
                    "current_balance": account_data.get("balance", 0),
                    "total_equity": account_data.get("balance", 0) + account_data.get("position_profit", 0),
                    "available": account_data.get("available", 0),
                    "margin": account_data.get("margin", 0),
                    "commission": account_data.get("commission", 0),
                    "close_profit": account_data.get("close_profit", 0),
                    "position_profit": account_data.get("position_profit", 0)
                },
                "trading_statistics": {
                    "total_trades": total_trades,
                    "winning_trades": winning_trades,
                    "losing_trades": losing_trades,
                    "win_rate": win_rate,
                    "total_profit": total_profit,
                    "total_loss": total_loss,
                    "profit_factor": profit_factor,
                    "largest_winning_trade": max([t.get("net_pnl", 0) for t in trades], default=0),
                    "largest_losing_trade": min([t.get("net_pnl", 0) for t in trades], default=0)
                },
                "position_statistics": {
                    "total_positions": total_positions,
                    "long_positions": long_positions,
                    "short_positions": short_positions,
                    "total_market_value": sum(self.calculate_position_pnl(
                        p.get("symbol", ""), p, p.get("current_price", 0)
                    ).get("market_value", 0) for p in positions)
                }
            }

            return report

    def _get_exchange_from_symbol(self, symbol: str) -> str:
        """从品种代码提取交易所"""
        if "." in symbol:
            return symbol.split(".")[0]
        return "SHFE"  # 默认上期所


# 测试代码
if __name__ == "__main__":
    # 创建财务计算引擎实例
    accounting_engine = AccountingEngine()

    # 测试手续费计算
    commission = accounting_engine.calculate_commission("SHFE.cu2401", 68000.0, 2, "BUY")
    print(f"手续费计算: {commission:.2f}")

    # 测试保证金计算
    margin = accounting_engine.calculate_margin("SHFE.cu2401", 68000.0, 2)
    print(f"保证金计算: {margin:.2f}")

    # 测试交易盈亏计算
    trade_pnl = accounting_engine.calculate_trade_pnl("SHFE.cu2401", 68000.0, 68500.0, 2, "BUY", commission)
    print("交易盈亏计算:", trade_pnl)

    # 测试持仓盈亏计算
    position_data = {"volume": 2, "price": 68000.0}
    position_pnl = accounting_engine.calculate_position_pnl("SHFE.cu2401", position_data, 68500.0)
    print("持仓盈亏计算:", position_pnl)

    # 测试账户权益更新
    account_data = {
        "balance": 1000000.0,
        "available": 1000000.0,
        "commission": 0.0,
        "margin": 0.0,
        "close_profit": 0.0,
        "position_profit": 0.0
    }
    updated_account = accounting_engine.update_account_equity(account_data, 1000.0, 500.0, 12.5)
    print("账户权益更新:", updated_account)

    # 测试财务报表生成
    trades = [{"net_pnl": 1000.0}, {"net_pnl": -500.0}, {"net_pnl": 800.0}]
    positions = [{"volume": 2, "symbol": "SHFE.cu2401", "current_price": 68500.0}]
    report = accounting_engine.generate_financial_report(updated_account, trades, positions)
    print("财务报表生成完成")
