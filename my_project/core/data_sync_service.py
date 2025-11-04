"""
数据同步服务
确保四个核心数据表（Account、Order、Position、Trade）的一致性
"""
from typing import Dict, Any, List, Set, Optional
from datetime import datetime
from core.thread_safe_manager import thread_safe_manager


class DataSyncService:
    """数据同步服务（确保多表数据一致性）"""

    def __init__(self):
        self.sync_log = []
        self.last_sync_time = None
        self.sync_interval = 5  # 同步间隔（秒）

    def validate_data_consistency(self, account_data: Dict[str, Any],
                                  order_data: List[Dict[str, Any]],
                                  position_data: List[Dict[str, Any]],
                                  trade_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """验证数据一致性（线程安全）"""
        with thread_safe_manager.locked_resource("data_consistency_check"):
            inconsistencies = []
            warnings = []

            # 1. 检查账户资金与成交记录的一致性
            account_balance = account_data.get("balance", 0)
            total_trade_amount = sum(trade.get("amount", 0) for trade in trade_data)
            total_commission = sum(trade.get("commission", 0) for trade in trade_data)

            expected_balance = account_data.get("initial_balance", 0) + \
                               account_data.get("close_profit", 0) - total_commission

            if abs(account_balance - expected_balance) > 0.01:
                inconsistencies.append({
                    "type": "account_balance_mismatch",
                    "expected": expected_balance,
                    "actual": account_balance,
                    "difference": account_balance - expected_balance
                })

            # 2. 检查持仓与成交记录的一致性
            position_volumes = {}
            for position in position_data:
                symbol = position.get("symbol")
                volume = position.get("volume", 0)
                position_volumes[symbol] = position_volumes.get(symbol, 0) + volume

            trade_volumes = {}
            for trade in trade_data:
                symbol = trade.get("symbol")
                direction = trade.get("direction")
                volume = trade.get("volume", 0)

                if direction in ["BUY", "COVER"]:
                    trade_volumes[symbol] = trade_volumes.get(symbol, 0) + volume
                else:  # SELL, SHORT
                    trade_volumes[symbol] = trade_volumes.get(symbol, 0) - volume

            for symbol, trade_volume in trade_volumes.items():
                position_volume = position_volumes.get(symbol, 0)
                if abs(trade_volume - position_volume) > 0.001:
                    inconsistencies.append({
                        "type": "position_volume_mismatch",
                        "symbol": symbol,
                        "expected_volume": trade_volume,
                        "actual_volume": position_volume,
                        "difference": position_volume - trade_volume
                    })

            # 3. 检查订单与成交的匹配
            order_status_check = self._validate_order_trade_match(order_data, trade_data)
            inconsistencies.extend(order_status_check)

            # 4. 检查时间序列一致性
            time_check = self._validate_timestamp_consistency(trade_data, order_data)
            warnings.extend(time_check)

            result = {
                "check_time": datetime.now(),
                "is_consistent": len(inconsistencies) == 0,
                "inconsistencies": inconsistencies,
                "warnings": warnings,
                "summary": {
                    "total_checks": 4,
                    "passed_checks": 4 - len(inconsistencies),
                    "failed_checks": len(inconsistencies),
                    "warnings_count": len(warnings)
                }
            }

            self._log_sync_result(result)
            return result

    def _validate_order_trade_match(self, orders: List[Dict[str, Any]],
                                    trades: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """验证订单与成交的匹配一致性"""
        inconsistencies = []

        # 构建订单索引
        order_index = {}
        for order in orders:
            order_id = order.get("order_id")
            order_index[order_id] = {
                "volume": order.get("volume", 0),
                "traded_volume": order.get("traded_volume", 0),
                "status": order.get("status", "")
            }

        # 构建成交索引
        trade_index = {}
        for trade in trades:
            order_id = trade.get("order_id")
            if order_id not in trade_index:
                trade_index[order_id] = []
            trade_index[order_id].append(trade)

        # 检查每个订单的成交匹配
        for order_id, order_info in order_index.items():
            order_trades = trade_index.get(order_id, [])
            total_traded_volume = sum(trade.get("volume", 0) for trade in order_trades)

            if abs(total_traded_volume - order_info["traded_volume"]) > 0.001:
                inconsistencies.append({
                    "type": "order_trade_volume_mismatch",
                    "order_id": order_id,
                    "expected_traded_volume": total_traded_volume,
                    "actual_traded_volume": order_info["traded_volume"],
                    "difference": order_info["traded_volume"] - total_traded_volume
                })

            # 检查订单状态是否合理
            if order_info["traded_volume"] > order_info["volume"]:
                inconsistencies.append({
                    "type": "order_volume_exceeded",
                    "order_id": order_id,
                    "order_volume": order_info["volume"],
                    "traded_volume": order_info["traded_volume"]
                })

        return inconsistencies

    def _validate_timestamp_consistency(self, trades: List[Dict[str, Any]],
                                        orders: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """验证时间戳一致性"""
        warnings = []

        # 检查成交时间是否在订单创建时间之后
        order_create_times = {}
        for order in orders:
            order_id = order.get("order_id")
            create_time = order.get("create_time")
            if create_time:
                order_create_times[order_id] = create_time

        for trade in trades:
            order_id = trade.get("order_id")
            trade_time = trade.get("trade_time")
            create_time = order_create_times.get(order_id)

            if create_time and trade_time and trade_time < create_time:
                warnings.append({
                    "type": "timestamp_anomaly",
                    "trade_id": trade.get("trade_id"),
                    "order_id": order_id,
                    "trade_time": trade_time,
                    "order_create_time": create_time,
                    "message": "成交时间早于订单创建时间"
                })

        return warnings

    def sync_data_tables(self, account_table, order_table, position_table, trade_table) -> bool:
        """同步四个数据表（线程安全）"""
        with thread_safe_manager.locked_resource("data_sync"):
            try:
                current_time = datetime.now()

                # 获取当前数据快照
                account_data = account_table.get_account()
                orders = order_table.get_all_orders()
                positions = position_table.get_all_positions()
                trades = trade_table.get_all_trades()

                # 验证一致性
                consistency_result = self.validate_data_consistency(
                    account_data, orders, positions, trades
                )

                # 如果发现不一致，尝试修复
                if not consistency_result["is_consistent"]:
                    repair_success = self._attempt_data_repair(
                        account_table, order_table, position_table, trade_table,
                        consistency_result["inconsistencies"]
                    )
                    consistency_result["repair_attempted"] = repair_success
                    consistency_result["repair_success"] = repair_success

                # 记录同步结果
                self.last_sync_time = current_time
                sync_record = {
                    "sync_time": current_time,
                    "consistency_result": consistency_result,
                    "data_counts": {
                        "accounts": 1,
                        "orders": len(orders),
                        "positions": len(positions),
                        "trades": len(trades)
                    }
                }
                self.sync_log.append(sync_record)

                print(f"数据同步完成: 一致性={consistency_result['is_consistent']}, " +
                      f"订单数={len(orders)}, 成交数={len(trades)}")

                return consistency_result["is_consistent"]

            except Exception as e:
                print(f"数据同步失败: {e}")
                return False

    def _attempt_data_repair(self, account_table, order_table, position_table,
                             trade_table, inconsistencies: List[Dict[str, Any]]) -> bool:
        """尝试修复数据不一致性"""
        repair_success = True

        for issue in inconsistencies:
            issue_type = issue.get("type")

            if issue_type == "account_balance_mismatch":
                # 修复账户余额
                expected_balance = issue.get("expected", 0)
                current_account = account_table.get_account()
                correction = expected_balance - current_account.get("balance", 0)

                if abs(correction) > 0.01:
                    account_table.update_balance(correction, "数据同步修复")
                    print(f"账户余额修复: 调整{correction:.2f}")

            elif issue_type == "position_volume_mismatch":
                # 修复持仓量
                symbol = issue.get("symbol")
                expected_volume = issue.get("expected_volume", 0)
                # 这里需要更复杂的修复逻辑，暂时记录警告
                print(f"警告: 持仓量不一致 {symbol}, 需要手动修复")
                repair_success = False

            elif issue_type == "order_trade_volume_mismatch":
                # 修复订单成交量
                order_id = issue.get("order_id")
                expected_volume = issue.get("expected_traded_volume", 0)
                # 需要更新订单的traded_volume字段
                print(f"警告: 订单成交量不一致 {order_id}, 需要手动修复")
                repair_success = False

        return repair_success

    def _log_sync_result(self, result: Dict[str, Any]):
        """记录同步结果"""
        log_entry = {
            "timestamp": datetime.now(),
            "result": result,
            "sync_id": f"SYNC_{int(datetime.now().timestamp() * 1000)}"
        }
        self.sync_log.append(log_entry)

        # 保持日志大小
        if len(self.sync_log) > 1000:
            self.sync_log = self.sync_log[-1000:]

    def get_sync_status(self) -> Dict[str, Any]:
        """获取同步状态"""
        return {
            "last_sync_time": self.last_sync_time,
            "total_syncs": len(self.sync_log),
            "recent_results": self.sync_log[-10:] if self.sync_log else []
        }

    def generate_sync_report(self) -> Dict[str, Any]:
        """生成同步报告"""
        if not self.sync_log:
            return {"message": "无同步记录"}

        recent_logs = self.sync_log[-100:]  # 最近100次同步

        successful_syncs = sum(1 for log in recent_logs
                               if log.get("result", {}).get("is_consistent", False))
        total_syncs = len(recent_logs)
        success_rate = successful_syncs / total_syncs if total_syncs > 0 else 0

        common_issues = {}
        for log in recent_logs:
            inconsistencies = log.get("result", {}).get("inconsistencies", [])
            for issue in inconsistencies:
                issue_type = issue.get("type")
                common_issues[issue_type] = common_issues.get(issue_type, 0) + 1

        return {
            "report_time": datetime.now(),
            "summary": {
                "total_syncs_analyzed": total_syncs,
                "success_rate": success_rate,
                "successful_syncs": successful_syncs,
                "failed_syncs": total_syncs - successful_syncs
            },
            "common_issues": common_issues,
            "recommendations": self._generate_recommendations(common_issues)
        }

    def _generate_recommendations(self, issues: Dict[str, int]) -> List[str]:
        """根据常见问题生成建议"""
        recommendations = []

        if issues.get("account_balance_mismatch", 0) > 0:
            recommendations.append("检查手续费计算逻辑是否正确")

        if issues.get("position_volume_mismatch", 0) > 0:
            recommendations.append("验证持仓更新是否在成交后立即执行")

        if issues.get("order_trade_volume_mismatch", 0) > 0:
            recommendations.append("检查订单状态机更新逻辑")

        if not recommendations:
            recommendations.append("数据一致性良好，继续保持")

        return recommendations


# 测试代码
if __name__ == "__main__":
    # 创建数据同步服务实例
    sync_service = DataSyncService()

    # 模拟测试数据
    test_account = {"balance": 1000000.0, "initial_balance": 1000000.0, "close_profit": 0.0}
    test_orders = [{"order_id": "ORDER_001", "volume": 2, "traded_volume": 2, "status": "全部成交"}]
    test_positions = [{"symbol": "SHFE.cu2401", "volume": 2}]
    test_trades = [
        {"order_id": "ORDER_001", "symbol": "SHFE.cu2401", "volume": 2, "amount": 136000.0, "commission": 12.5}
    ]

    # 测试数据一致性验证
    result = sync_service.validate_data_consistency(
        test_account, test_orders, test_positions, test_trades
    )

    print("数据一致性验证结果:")
    print(f"是否一致: {result['is_consistent']}")
    print(f"不一致项: {len(result['inconsistencies'])}")
    print(f"警告: {len(result['warnings'])}")

    # 测试同步报告
    report = sync_service.generate_sync_report()
    print("同步报告:", report)
