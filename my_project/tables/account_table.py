"""
账户表示例实现
展示如何按照统一接口规范实现具体的数据表
修复版本：确保数据包含必需字段，并修复数据同步时序问题
"""
from typing import Dict, Any, List, Optional
from datetime import datetime
from core.data_table_base import IDataTable
from core.data_adapter import DataAdapter
from core.data_sync_service import DataSyncService
from core.event_engine import EventEngine
import logging


class AccountTable(IDataTable):
    """账户表（修复版本：解决数据同步时序问题）"""

    def initialize(self, adapter: DataAdapter = None, sync_service: DataSyncService = None,
                  event_engine: EventEngine = None, **kwargs) -> bool:
        """统一初始化接口"""
        try:
            self.adapter = adapter
            self.sync_service = sync_service
            self.event_engine = event_engine

            # 初始化数据存储
            self.accounts: Dict[str, Dict[str, Any]] = {}
            self._transaction_history: List[Dict[str, Any]] = []

            # 创建默认账户
            self._create_default_account()

            self._initialized = True
            self.logger.info(f"账户表初始化完成: {self.table_name}")
            return True

        except Exception as e:
            self.logger.error(f"账户表初始化失败: {e}")
            return False

    def _create_default_account(self):
        """创建默认账户"""
        default_account = {
            'account_id': 'default',
            'balance': 1000000.0,
            'available': 1000000.0,
            'commission': 0.0,
            'margin': 0.0,
            'frozen': 0.0,
            'update_time': datetime.now().isoformat(),
            'currency': 'CNY'
        }
        self.accounts['default'] = default_account

    def get_table_info(self) -> Dict[str, Any]:
        """获取表信息"""
        return {
            "table_name": self.table_name,
            "initialized": self._initialized,
            "account_count": len(self.accounts),
            "config": self.table_config
        }

    def validate_data(self, data: Dict[str, Any]) -> bool:
        """数据验证"""
        try:
            required_fields = self.table_config.get('validation_rules', {}).get('required_fields', [])
            for field in required_fields:
                if field not in data:
                    self.logger.error(f"缺少必需字段: {field}")
                    return False

            # 验证数值类型
            if 'balance' in data and not isinstance(data['balance'], (int, float)):
                self.logger.error("balance必须是数值类型")
                return False

            return True

        except Exception as e:
            self.logger.error(f"数据验证失败: {e}")
            return False

    def save_data(self, data: Dict[str, Any]) -> bool:
        """保存数据（修复：添加数据同步调用，解决时序问题）"""
        try:
            # 修改处1：检查并生成缺失的account_id字段
            if 'account_id' not in data or not data['account_id']:
                data['account_id'] = f"account_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                self.logger.warning(f"自动生成account_id: {data['account_id']}")

            if not self.validate_data(data):
                return False

            # 使用适配器转换数据
            if self.adapter:
                data = self.adapter.adapt_data(self.table_name, data)

            account_id = data.get('account_id')
            if not account_id:
                self.logger.error("缺少account_id")
                return False

            # 保存或更新账户
            data['update_time'] = datetime.now().isoformat()
            self.accounts[account_id] = data

            # 修改处2：添加数据同步调用，确保外部数据表更新
            if self.sync_service:
                # 同步数据到外部存储
                sync_success = self.sync_service.sync_data(self.table_name, data)
                if not sync_success:
                    self.logger.warning(f"数据同步失败: {account_id}")
                    # 可以添加重试逻辑或回滚机制

            # 记录交易历史
            self._record_transaction(data)

            self.logger.debug(f"账户数据保存成功: {account_id}")
            return True

        except Exception as e:
            self.logger.error(f"保存账户数据失败: {e}")
            return False

    def _record_transaction(self, account_data: Dict[str, Any]):
        """记录交易历史"""
        transaction = {
            'timestamp': datetime.now().isoformat(),
            'account_id': account_data.get('account_id'),
            'balance': account_data.get('balance', 0),
            'available': account_data.get('available', 0),
            'type': 'update'
        }
        self._transaction_history.append(transaction)

    def query_data(self, conditions: Dict[str, Any] = None) -> List[Dict[str, Any]]:
        """查询数据"""
        try:
            conditions = conditions or {}
            results = []

            for account_id, account_data in self.accounts.items():
                if self._match_conditions(account_data, conditions):
                    results.append(account_data.copy())

            self.logger.debug(f"查询到 {len(results)} 条账户数据")
            return results

        except Exception as e:
            self.logger.error(f"查询账户数据失败: {e}")
            return []

    def _match_conditions(self, data: Dict[str, Any], conditions: Dict[str, Any]) -> bool:
        """匹配查询条件"""
        for key, value in conditions.items():
            if key not in data or data[key] != value:
                return False
        return True

    def get_account(self, account_id: str = 'default') -> Optional[Dict[str, Any]]:
        """获取特定账户信息"""
        return self.accounts.get(account_id)

    def update_balance(self, account_id: str, amount: float, description: str = "") -> bool:
        """更新账户余额（修复：确保数据同步）"""
        try:
            if account_id not in self.accounts:
                self.logger.error(f"账户不存在: {account_id}")
                return False

            account = self.accounts[account_id]
            new_balance = account.get('balance', 0) + amount

            if new_balance < 0:
                self.logger.warning(f"账户余额不足: {account_id}")
                return False

            update_data = {
                'account_id': account_id,
                'balance': new_balance,
                'available': new_balance - account.get('frozen', 0),
                'update_time': datetime.now().isoformat()
            }

            # 修改处3：使用save_data方法确保数据同步
            return self.save_data(update_data)

        except Exception as e:
            self.logger.error(f"更新账户余额失败: {e}")
            return False

    def get_transaction_history(self, account_id: str = None) -> List[Dict[str, Any]]:
        """获取交易历史"""
        if account_id:
            return [t for t in self._transaction_history if t.get('account_id') == account_id]
        return self._transaction_history.copy()

    def sync_with_external(self) -> bool:
        """与外部数据源同步（新增：解决数据一致性检查失败问题）"""
        try:
            if not self.sync_service:
                self.logger.warning("同步服务未初始化")
                return False

            # 获取所有账户数据
            all_accounts = list(self.accounts.values())

            # 批量同步数据
            sync_results = []
            for account_data in all_accounts:
                result = self.sync_service.sync_data(self.table_name, account_data)
                sync_results.append(result)

            # 检查同步结果
            success_count = sum(1 for result in sync_results if result)
            if success_count == len(all_accounts):
                self.logger.info(f"数据同步完成: {success_count}/{len(all_accounts)} 成功")
                return True
            else:
                self.logger.warning(f"数据同步部分失败: {success_count}/{len(all_accounts)} 成功")
                return False

        except Exception as e:
            self.logger.error(f"数据同步异常: {e}")
            return False
