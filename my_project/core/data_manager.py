"""
改进的数据管理器
统一管理四个数据表的访问，增强数据一致性验证
"""
from typing import Dict, Any, List, Optional
from datetime import datetime
from core.thread_safe_manager import thread_safe_manager
from core.data_sync_service import DataSyncService
from core.data_adapter import DataAdapter

import os

class DataManager:
    """数据管理器（已增强数据一致性验证）"""

    def __init__(self, event_engine):
        self.event_engine = event_engine
        self.tables: Dict[str, Any] = {}
        self.sync_service = DataSyncService()  # 集成数据同步服务
        self.adapter = DataAdapter()  # 缺少的adapter属性

        self._setup_table_validation()

    def _setup_table_validation(self):
        """设置数据表验证规则"""
        self.validation_rules = {
            "account": self._validate_account_table,
            "order": self._validate_order_table,
            "position": self._validate_position_table,
            "trade": self._validate_trade_table
        }

    def add_table(self, table_name: str, table_instance):
        """添加数据表（线程安全）"""
        with thread_safe_manager.locked_resource("table_addition"):
            self.tables[table_name] = table_instance
            print(f"[{datetime.now()}] [DataManager] 添加数据表: {table_name}")

    def get_table(self, table_name: str) -> Optional[Any]:
        """获取数据表（线程安全）"""
        with thread_safe_manager.locked_resource("table_query"):
            return self.tables.get(table_name)

    def get_all_tables(self) -> Dict[str, Any]:
        """获取所有数据表（线程安全）"""
        with thread_safe_manager.locked_resource("table_query"):
            return self.tables.copy()

    def remove_table(self, table_name: str):
        """移除数据表（线程安全）"""
        with thread_safe_manager.locked_resource("table_removal"):
            if table_name in self.tables:
                del self.tables[table_name]
                print(f"[{datetime.now()}] [DataManager] 移除数据表: {table_name}")

    def validate_table_data(self, table_name: str) -> Dict[str, Any]:
        """验证数据表完整性（线程安全）"""
        with thread_safe_manager.locked_resource("table_validation"):
            table = self.tables.get(table_name)
            if not table:
                return {"valid": False, "error": f"数据表不存在: {table_name}"}

            validator = self.validation_rules.get(table_name)
            if not validator:
                return {"valid": False, "error": f"无验证规则: {table_name}"}

            try:
                return validator(table)
            except Exception as e:
                return {"valid": False, "error": f"验证异常: {str(e)}"}

    def validate_all_tables(self) -> Dict[str, Any]:
        """验证所有数据表（线程安全）"""
        with thread_safe_manager.locked_resource("all_tables_validation"):
            results = {}
            all_valid = True

            for table_name in self.tables:
                result = self.validate_table_data(table_name)
                results[table_name] = result
                if not result.get("valid", False):
                    all_valid = False

            return {
                "valid": all_valid,
                "timestamp": datetime.now(),
                "results": results,
                "summary": {
                    "total_tables": len(self.tables),
                    "valid_tables": sum(1 for r in results.values() if r.get("valid", False)),
                    "invalid_tables": sum(1 for r in results.values() if not r.get("valid", False))
                }
            }

    def sync_tables(self) -> bool:
        """同步所有数据表（线程安全）"""
        with thread_safe_manager.locked_resource("tables_sync"):
            try:
                # 获取四个核心数据表
                account_table = self.tables.get("account")
                order_table = self.tables.get("order")
                position_table = self.tables.get("position")
                trade_table = self.tables.get("trade")

                if not all([account_table, order_table, position_table, trade_table]):
                    print(f"[{datetime.now()}] [DataManager] 数据表不完整，无法同步")
                    return False

                # 执行数据同步
                sync_success = self.sync_service.sync_data_tables(
                    account_table, order_table, position_table, trade_table
                )

                if sync_success:
                    print(f"[{datetime.now()}] [DataManager] 数据表同步成功")
                else:
                    print(f"[{datetime.now()}] [DataManager] 数据表同步发现不一致")

                return sync_success

            except Exception as e:
                print(f"[{datetime.now()}] [DataManager] 数据表同步失败: {e}")
                return False

    def backup_tables(self, backup_path: str = None) -> bool:
        """备份数据表（线程安全）"""
        with thread_safe_manager.locked_resource("tables_backup"):
            try:
                import json
                import os
                from datetime import datetime

                if not backup_path:
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    backup_path = f"data_backup_{timestamp}.json"

                backup_data = {
                    "backup_time": datetime.now().isoformat(),
                    "tables": {}
                }

                # 备份每个表的数据
                for table_name, table_instance in self.tables.items():
                    if hasattr(table_instance, 'data'):
                        backup_data["tables"][table_name] = table_instance.data
                    elif hasattr(table_instance, 'get_all_data'):
                        backup_data["tables"][table_name] = table_instance.get_all_data()

                # 写入备份文件
                with open(backup_path, 'w', encoding='utf-8') as f:
                    json.dump(backup_data, f, indent=2, ensure_ascii=False, default=str)

                print(f"[{datetime.now()}] [DataManager] 数据表已备份到: {backup_path}")
                return True

            except Exception as e:
                print(f"[{datetime.now()}] [DataManager] 数据表备份失败: {e}")
                return False

    def restore_tables(self, backup_path: str) -> bool:
        """恢复数据表（线程安全）"""
        with thread_safe_manager.locked_resource("tables_restore"):
            try:
                import json

                if not os.path.exists(backup_path):
                    print(f"[{datetime.now()}] [DataManager] 备份文件不存在: {backup_path}")
                    return False

                with open(backup_path, 'r', encoding='utf-8') as f:
                    backup_data = json.load(f)

                # 恢复每个表的数据
                for table_name, table_data in backup_data.get("tables", {}).items():
                    table_instance = self.tables.get(table_name)
                    if table_instance and hasattr(table_instance, 'restore_data'):
                        table_instance.restore_data(table_data)

                print(f"[{datetime.now()}] [DataManager] 数据表已从备份恢复: {backup_path}")
                return True

            except Exception as e:
                print(f"[{datetime.now()}] [DataManager] 数据表恢复失败: {e}")
                return False

    def get_table_statistics(self) -> Dict[str, Any]:
        """获取数据表统计信息（线程安全）"""
        with thread_safe_manager.locked_resource("table_statistics"):
            stats = {
                "timestamp": datetime.now(),
                "total_tables": len(self.tables),
                "table_details": {},
                "sync_status": self.sync_service.get_sync_status()
            }

            for table_name, table_instance in self.tables.items():
                table_stats = self._get_table_stats(table_instance, table_name)
                stats["table_details"][table_name] = table_stats

            return stats

    def _get_table_stats(self, table_instance, table_name: str) -> Dict[str, Any]:
        """获取单个数据表的统计信息"""
        stats = {"name": table_name, "record_count": 0}

        try:
            if hasattr(table_instance, 'data') and isinstance(table_instance.data, list):
                stats["record_count"] = len(table_instance.data)
            elif hasattr(table_instance, 'get_record_count'):
                stats["record_count"] = table_instance.get_record_count()

            # 添加表特定统计
            if table_name == "account":
                if hasattr(table_instance, 'get_account'):
                    account_data = table_instance.get_account()
                    stats["balance"] = account_data.get("balance", 0)
            elif table_name == "order":
                if hasattr(table_instance, 'get_active_orders'):
                    active_orders = table_instance.get_active_orders()
                    stats["active_orders"] = len(active_orders)
            elif table_name == "position":
                if hasattr(table_instance, 'get_all_positions'):
                    positions = table_instance.get_all_positions()
                    stats["positions"] = len(positions)
            elif table_name == "trade":
                if hasattr(table_instance, 'get_all_trades'):
                    trades = table_instance.get_all_trades()
                    stats["trades"] = len(trades)

        except Exception as e:
            stats["error"] = str(e)

        return stats

    def _validate_account_table(self, account_table) -> Dict[str, Any]:
        """验证账户表数据完整性"""
        try:
            account_data = account_table.get_account()
            required_fields = ["balance", "available", "commission"]

            for field in required_fields:
                if field not in account_data:
                    return {"valid": False, "error": f"缺少必需字段: {field}"}

            # 检查资金逻辑
            balance = account_data.get("balance", 0)
            available = account_data.get("available", 0)
            margin = account_data.get("margin", 0)

            if available > balance:
                return {"valid": False, "error": "可用资金不能大于总资金"}

            if margin > balance:
                return {"valid": False, "error": "保证金不能大于总资金"}

            return {"valid": True, "record_count": 1}

        except Exception as e:
            return {"valid": False, "error": str(e)}

    def _validate_order_table(self, order_table) -> Dict[str, Any]:
        """验证订单表数据完整性"""
        try:
            orders = order_table.get_all_orders()

            for order in orders:
                required_fields = ["order_id", "symbol", "volume", "status"]
                for field in required_fields:
                    if field not in order:
                        return {"valid": False, "error": f"订单缺少字段: {field}"}

                # 检查成交量逻辑
                volume = order.get("volume", 0)
                traded_volume = order.get("traded_volume", 0)

                if traded_volume > volume:
                    return {"valid": False, "error": "已成交量不能大于订单量"}

            return {"valid": True, "record_count": len(orders)}

        except Exception as e:
            return {"valid": False, "error": str(e)}

    def _validate_position_table(self, position_table) -> Dict[str, Any]:
        """验证持仓表数据完整性"""
        try:
            positions = position_table.get_all_positions()

            for position in positions:
                if "symbol" not in position:
                    return {"valid": False, "error": "持仓缺少品种符号"}

            return {"valid": True, "record_count": len(positions)}

        except Exception as e:
            return {"valid": False, "error": str(e)}

    def _validate_trade_table(self, trade_table) -> Dict[str, Any]:
        """验证成交表数据完整性"""
        try:
            trades = trade_table.get_all_trades()

            for trade in trades:
                required_fields = ["trade_id", "symbol", "price", "volume"]
                for field in required_fields:
                    if field not in trade:
                        return {"valid": False, "error": f"成交缺少字段: {field}"}

            return {"valid": True, "record_count": len(trades)}

        except Exception as e:
            return {"valid": False, "error": str(e)}

    def clear_all_tables(self):
        """清空所有数据表（线程安全）"""
        with thread_safe_manager.locked_resource("tables_clear"):
            for table_name, table_instance in self.tables.items():
                if hasattr(table_instance, 'clear'):
                    table_instance.clear()
                    print(f"[{datetime.now()}] [DataManager] 清空数据表: {table_name}")


# 测试代码
if __name__ == "__main__":
    from core.event_engine import EventEngine

    # 创建数据管理器实例
    event_engine = EventEngine()
    data_manager = DataManager(event_engine)

    # 测试数据表管理
    class MockTable:
        def __init__(self, name):
            self.name = name
            self.data = []

        def get_all_data(self):
            return self.data.copy()

        def clear(self):
            self.data.clear()

    # 添加模拟表
    data_manager.add_table("account", MockTable("account"))
    data_manager.add_table("order", MockTable("order"))

    # 测试表验证
    validation_result = data_manager.validate_all_tables()
    print("数据表验证结果:", validation_result)

    # 测试数据同步
    sync_result = data_manager.sync_tables()
    print("数据同步结果:", sync_result)

    # 测试统计信息
    stats = data_manager.get_table_statistics()
    print("数据表统计:", stats)

    print("数据管理器测试完成")
