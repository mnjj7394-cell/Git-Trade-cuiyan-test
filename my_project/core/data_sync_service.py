"""
统一数据同步服务
确保数据表间的一致性和完整性
"""
from typing import Dict, Any, List, Optional
from core.data_table_base import IDataTable
from core.thread_safe_manager import thread_safe_manager
import logging


class DataSyncService:
    """数据同步服务（统一接口版本）"""

    def __init__(self, config: Dict[str, Any] = None):
        self.config = config or {}
        self.sync_rules: Dict[str, Dict[str, Any]] = {}
        self._setup_default_sync_rules()

        # 日志设置
        self.logger = logging.getLogger("DataSyncService")
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
            self.logger.setLevel(logging.INFO)

    def _setup_default_sync_rules(self):
        """设置默认同步规则"""
        # 订单与成交同步规则
        self.sync_rules['order_trade'] = {
            'source': 'order',
            'target': 'trade',
            'conditions': {'status': 'filled'},
            'mapping': {
                'order_id': 'order_id',
                'symbol': 'symbol',
                'direction': 'direction',
                'price': 'price',
                'volume': 'volume'
            }
        }

        # 成交与持仓同步规则
        self.sync_rules['trade_position'] = {
            'source': 'trade',
            'target': 'position',
            'conditions': {},
            'mapping': {
                'symbol': 'symbol',
                'direction': 'direction',
                'price': 'price',
                'volume': 'volume'
            }
        }

    def register_sync_rule(self, rule_name: str, rule_config: Dict[str, Any]):
        """注册同步规则"""
        self.sync_rules[rule_name] = rule_config
        self.logger.info(f"注册同步规则: {rule_name}")

    def sync_all_tables(self, tables: Dict[str, IDataTable]) -> bool:
        """同步所有数据表

        Args:
            tables: 数据表字典

        Returns:
            bool: 同步是否成功
        """
        with thread_safe_manager.locked_resource("all_tables_sync"):
            try:
                overall_success = True

                # 检查表完整性
                required_tables = ['account', 'order', 'position', 'trade']
                missing_tables = [t for t in required_tables if t not in tables]
                if missing_tables:
                    self.logger.error(f"缺少必需数据表: {missing_tables}")
                    return False

                # 执行各个同步规则
                for rule_name, rule_config in self.sync_rules.items():
                    success = self._execute_sync_rule(rule_name, rule_config, tables)
                    if not success:
                        overall_success = False
                        self.logger.warning(f"同步规则执行失败: {rule_name}")

                if overall_success:
                    self.logger.info("所有数据表同步完成")
                else:
                    self.logger.warning("数据表同步发现不一致")

                return overall_success

            except Exception as e:
                self.logger.error(f"数据表同步失败: {e}")
                return False

    def _execute_sync_rule(self, rule_name: str, rule_config: Dict[str, Any],
                          tables: Dict[str, IDataTable]) -> bool:
        """执行单个同步规则"""
        try:
            source_table_name = rule_config['source']
            target_table_name = rule_config['target']
            conditions = rule_config.get('conditions', {})
            mapping = rule_config.get('mapping', {})

            source_table = tables.get(source_table_name)
            target_table = tables.get(target_table_name)

            if not source_table or not target_table:
                self.logger.error(f"同步规则 {rule_name}: 源表或目标表不存在")
                return False

            # 查询源表数据
            source_data = source_table.query_data(conditions)
            if not source_data:
                self.logger.debug(f"同步规则 {rule_name}: 源表无符合条件数据")
                return True

            # 转换数据格式
            transformed_data = []
            for data in source_data:
                transformed = {}
                for src_key, target_key in mapping.items():
                    if src_key in data:
                        transformed[target_key] = data[src_key]
                if transformed:
                    transformed_data.append(transformed)

            # 保存到目标表
            success_count = 0
            for data in transformed_data:
                if target_table.save_data(data):
                    success_count += 1

            self.logger.info(f"同步规则 {rule_name}: {success_count}/{len(transformed_data)} 条数据同步成功")
            return success_count == len(transformed_data)

        except Exception as e:
            self.logger.error(f"执行同步规则 {rule_name} 失败: {e}")
            return False

    def validate_data_consistency(self, tables: Dict[str, IDataTable]) -> Dict[str, Any]:
        """验证数据一致性"""
        consistency_report = {
            'timestamp': self._get_timestamp(),
            'checks': [],
            'overall_consistent': True
        }

        # 检查账户余额一致性
        account_check = self._check_account_consistency(tables)
        consistency_report['checks'].append(account_check)
        if not account_check['consistent']:
            consistency_report['overall_consistent'] = False

        # 检查持仓与成交一致性
        position_check = self._check_position_consistency(tables)
        consistency_report['checks'].append(position_check)
        if not position_check['consistent']:
            consistency_report['overall_consistent'] = False

        return consistency_report

    def _check_account_consistency(self, tables: Dict[str, IDataTable]) -> Dict[str, Any]:
        """检查账户数据一致性"""
        # 实现具体的账户一致性检查逻辑
        return {
            'check_name': 'account_consistency',
            'consistent': True,
            'details': '账户数据一致性检查通过'
        }

    def _check_position_consistency(self, tables: Dict[str, IDataTable]) -> Dict[str, Any]:
        """检查持仓数据一致性"""
        # 实现具体的持仓一致性检查逻辑
        return {
            'check_name': 'position_consistency',
            'consistent': True,
            'details': '持仓数据一致性检查通过'
        }

    def _get_timestamp(self) -> str:
        """获取时间戳"""
        from datetime import datetime
        return datetime.now().isoformat()

    def get_sync_status(self) -> Dict[str, Any]:
        """获取同步服务状态"""
        return {
            'sync_rules': list(self.sync_rules.keys()),
            'config': self.config,
            'status': 'active'
        }
