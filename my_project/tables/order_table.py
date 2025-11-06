"""
订单表统一接口实现
遵循IDataTable接口规范
"""
from typing import Dict, Any, List, Optional
from datetime import datetime
from core.data_table_base import IDataTable
from core.data_adapter import DataAdapter
from core.data_sync_service import DataSyncService
from core.event_engine import EventEngine
import logging


class OrderTable(IDataTable):
    """订单表（统一接口实现）"""

    def initialize(self, adapter: DataAdapter = None, sync_service: DataSyncService = None,
                  event_engine: EventEngine = None, **kwargs) -> bool:
        """统一初始化接口"""
        try:
            self.adapter = adapter
            self.sync_service = sync_service
            self.event_engine = event_engine

            # 初始化数据存储
            self.orders: Dict[str, Dict[str, Any]] = {}
            self._order_history: List[Dict[str, Any]] = []
            self._order_counter = 0

            self._initialized = True
            self.logger.info(f"订单表初始化完成: {self.table_name}")
            return True

        except Exception as e:
            self.logger.error(f"订单表初始化失败: {e}")
            return False

    def get_table_info(self) -> Dict[str, Any]:
        """获取表信息"""
        return {
            "table_name": self.table_name,
            "initialized": self._initialized,
            "order_count": len(self.orders),
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
            if 'price' in data and not isinstance(data['price'], (int, float)):
                self.logger.error("price必须是数值类型")
                return False

            if 'volume' in data and not isinstance(data['volume'], (int, float)):
                self.logger.error("volume必须是数值类型")
                return False

            # 验证交易方向
            valid_directions = ['BUY', 'SELL', 'SHORT', 'COVER']
            if 'direction' in data and data['direction'] not in valid_directions:
                self.logger.error(f"无效的交易方向: {data['direction']}")
                return False

            return True

        except Exception as e:
            self.logger.error(f"数据验证失败: {e}")
            return False

    def save_data(self, data: Dict[str, Any]) -> bool:
        """保存数据"""
        try:
            if not self.validate_data(data):
                return False

            # 使用适配器转换数据
            if self.adapter:
                data = self.adapter.adapt_data(self.table_name, data)

            # 生成订单ID（如果未提供）
            if 'order_id' not in data or not data['order_id']:
                self._order_counter += 1
                data['order_id'] = f"ORDER_{self._order_counter:08d}"

            order_id = data['order_id']

            # 设置时间戳
            if 'order_time' not in data:
                data['order_time'] = datetime.now().isoformat()

            data['update_time'] = datetime.now().isoformat()

            # 保存订单
            self.orders[order_id] = data

            # 记录订单历史
            self._record_order_history(data)

            self.logger.debug(f"订单数据保存成功: {order_id}")
            return True

        except Exception as e:
            self.logger.error(f"保存订单数据失败: {e}")
            return False

    def _record_order_history(self, order_data: Dict[str, Any]):
        """记录订单历史"""
        history_record = {
            'timestamp': datetime.now().isoformat(),
            'order_id': order_data.get('order_id'),
            'symbol': order_data.get('symbol'),
            'direction': order_data.get('direction'),
            'price': order_data.get('price'),
            'volume': order_data.get('volume'),
            'status': order_data.get('status', 'pending'),
            'action': 'created' if order_data.get('order_id') not in self.orders else 'updated'
        }
        self._order_history.append(history_record)

    def query_data(self, conditions: Dict[str, Any] = None) -> List[Dict[str, Any]]:
        """查询数据"""
        try:
            conditions = conditions or {}
            results = []

            for order_id, order_data in self.orders.items():
                if self._match_conditions(order_data, conditions):
                    results.append(order_data.copy())

            self.logger.debug(f"查询到 {len(results)} 条订单数据")
            return results

        except Exception as e:
            self.logger.error(f"查询订单数据失败: {e}")
            return []

    def _match_conditions(self, data: Dict[str, Any], conditions: Dict[str, Any]) -> bool:
        """匹配查询条件"""
        for key, value in conditions.items():
            if key not in data:
                return False
            if isinstance(value, (list, tuple)):
                if data[key] not in value:
                    return False
            else:
                if data[key] != value:
                    return False
        return True

    def create_order(self, symbol: str, direction: str, price: float, volume: int,
                    strategy: str = "", **kwargs) -> Optional[Dict[str, Any]]:
        """创建新订单"""
        order_data = {
            'symbol': symbol,
            'direction': direction,
            'price': price,
            'volume': volume,
            'strategy': strategy,
            'status': 'pending',
            'order_time': datetime.now().isoformat(),
            **kwargs
        }

        if self.save_data(order_data):
            return self.orders.get(order_data.get('order_id'))
        return None

    def update_order_status(self, order_id: str, status: str, **kwargs) -> bool:
        """更新订单状态"""
        if order_id not in self.orders:
            self.logger.error(f"订单不存在: {order_id}")
            return False

        order_data = self.orders[order_id].copy()
        order_data['status'] = status
        order_data.update(kwargs)
        order_data['update_time'] = datetime.now().isoformat()

        return self.save_data(order_data)

    def get_orders_by_strategy(self, strategy: str) -> List[Dict[str, Any]]:
        """根据策略获取订单"""
        return self.query_data({'strategy': strategy})

    def get_orders_by_symbol(self, symbol: str) -> List[Dict[str, Any]]:
        """根据标的获取订单"""
        return self.query_data({'symbol': symbol})

    def get_order_history(self, order_id: str = None) -> List[Dict[str, Any]]:
        """获取订单历史"""
        if order_id:
            return [h for h in self._order_history if h.get('order_id') == order_id]
        return self._order_history.copy()

    def cancel_order(self, order_id: str, reason: str = "") -> bool:
        """取消订单"""
        return self.update_order_status(order_id, 'cancelled', cancel_reason=reason)

    def fill_order(self, order_id: str, fill_price: float, fill_volume: int) -> bool:
        """成交订单"""
        return self.update_order_status(order_id, 'filled',
                                      fill_price=fill_price,
                                      fill_volume=fill_volume,
                                      fill_time=datetime.now().isoformat())
