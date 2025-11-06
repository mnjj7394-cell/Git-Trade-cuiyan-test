"""
重构的数据管理器
统一管理所有数据表，提供标准的接口规范
"""
from typing import Dict, Any, List, Optional
from datetime import datetime
from core.thread_safe_manager import thread_safe_manager
from core.data_sync_service import DataSyncService
from core.data_adapter import DataAdapter
from core.event_engine import EventEngine
from core.data_table_base import IDataTable
import logging


class DataManager:
    """数据管理器（重构版本）"""

    def __init__(self, event_engine: EventEngine, config: Dict[str, Any] = None):
        self.event_engine = event_engine
        self.config = config or {}
        self.tables: Dict[str, IDataTable] = {}

        # 服务依赖
        self.sync_service = DataSyncService(self.config.get('sync', {}))
        self.adapter = DataAdapter(self.config.get('adapter', {}))

        # 日志设置
        self.logger = logging.getLogger("DataManager")
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
            self.logger.setLevel(logging.INFO)

        # 统一初始化流程
        self._initialize_tables()

    def _initialize_tables(self):
        """统一表初始化流程"""
        self.logger.info("开始初始化核心数据表...")

        # 标准表配置
        table_configs = {
            "account": {
                "table_name": "account",
                "type": "account",
                "persistent": True,
                "validation_rules": {"required_fields": ["account_id", "balance"]}
            },
            "order": {
                "table_name": "order",
                "type": "order",
                "persistent": True,
                "validation_rules": {"required_fields": ["order_id", "symbol", "direction"]}
            },
            "position": {
                "table_name": "position",
                "type": "position",
                "persistent": True,
                "validation_rules": {"required_fields": ["strategy", "symbol", "direction"]}
            },
            "trade": {
                "table_name": "trade",
                "type": "trade",
                "persistent": True,
                "validation_rules": {"required_fields": ["trade_id", "symbol", "volume"]}
            }
        }

        # 合并用户自定义配置
        user_table_configs = self.config.get('tables', {})
        for table_name, user_config in user_table_configs.items():
            if table_name in table_configs:
                table_configs[table_name].update(user_config)

        # 创建并初始化表
        success_count = 0
        for table_name, config in table_configs.items():
            table = self._create_table(table_name, config)
            if table and self._initialize_table(table, table_name):
                self.tables[table_name] = table
                success_count += 1
                self.logger.info(f"数据表 {table_name} 初始化成功")
            else:
                self.logger.error(f"数据表 {table_name} 初始化失败")

        self.logger.info(f"数据表初始化完成: {success_count}/{len(table_configs)} 成功")

    def _create_table(self, table_name: str, config: Dict[str, Any]) -> Optional[IDataTable]:
        """统一表创建工厂方法"""
        try:
            if table_name == "account":
                from tables.account_table import AccountTable
                return AccountTable(config)
            elif table_name == "order":
                from tables.order_table import OrderTable
                return OrderTable(config)
            elif table_name == "position":
                from tables.position_table import PositionTable
                return PositionTable(config)
            elif table_name == "trade":
                from tables.trade_table import TradeTable
                return TradeTable(config)
            else:
                self.logger.error(f"未知的表类型: {table_name}")
                return None

        except ImportError as e:
            self.logger.error(f"导入表类失败 {table_name}: {e}")
            return None
        except Exception as e:
            self.logger.error(f"创建表 {table_name} 失败: {e}")
            return None

    def _initialize_table(self, table: IDataTable, table_name: str) -> bool:
        """统一表初始化"""
        try:
            return table.initialize(
                adapter=self.adapter,
                sync_service=self.sync_service,
                event_engine=self.event_engine
            )
        except Exception as e:
            self.logger.error(f"初始化表 {table_name} 异常: {e}")
            return False

    def get_table(self, table_name: str) -> Optional[IDataTable]:
        """获取数据表"""
        with thread_safe_manager.locked_resource("table_query"):
            return self.tables.get(table_name)

    def get_all_tables(self) -> Dict[str, IDataTable]:
        """获取所有数据表"""
        with thread_safe_manager.locked_resource("table_query"):
            return self.tables.copy()

    def validate_table_data(self, table_name: str, data: Dict[str, Any]) -> bool:
        """验证表数据"""
        table = self.get_table(table_name)
        if not table:
            self.logger.error(f"表不存在: {table_name}")
            return False
        return table.validate_data(data)

    def save_table_data(self, table_name: str, data: Dict[str, Any]) -> bool:
        """保存表数据"""
        table = self.get_table(table_name)
        if not table:
            self.logger.error(f"表不存在: {table_name}")
            return False
        return table.save_data(data)

    def query_table_data(self, table_name: str, conditions: Dict[str, Any] = None) -> List[Dict[str, Any]]:
        """查询表数据"""
        table = self.get_table(table_name)
        if not table:
            self.logger.error(f"表不存在: {table_name}")
            return []
        return table.query_data(conditions)

    def sync_all_tables(self) -> bool:
        """同步所有数据表"""
        with thread_safe_manager.locked_resource("tables_sync"):
            try:
                success = self.sync_service.sync_all_tables(self.tables)
                if success:
                    self.logger.info("所有数据表同步成功")
                else:
                    self.logger.warning("数据表同步发现不一致")
                return success
            except Exception as e:
                self.logger.error(f"数据表同步失败: {e}")
                return False

    def get_system_status(self) -> Dict[str, Any]:
        """获取系统状态（修复状态报告）"""
        with thread_safe_manager.locked_resource("system_status"):
            # 计算表初始化状态
            tables_initialized = all(
                table.is_initialized() if hasattr(table, 'is_initialized') else True
                for table in self.tables.values()
            )

            return {
                "timestamp": datetime.now(),
                "tables_initialized": tables_initialized,  # 确保这个字段存在且正确
                "total_tables": len(self.tables),
                "initialized_tables": sum(1 for table in self.tables.values()
                                          if hasattr(table, 'is_initialized') and table.is_initialized()),
                "table_details": {name: table.get_table_info() for name, table in self.tables.items()}
            }
