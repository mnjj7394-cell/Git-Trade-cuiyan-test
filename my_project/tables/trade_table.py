"""
成交表统一接口实现
遵循IDataTable接口规范
修复版本：修正数据验证顺序问题
"""
from typing import Dict, Any, List, Optional
from datetime import datetime
from core.data_table_base import IDataTable
from core.data_adapter import DataAdapter
from core.data_sync_service import DataSyncService
from core.event_engine import EventEngine
import logging


class TradeTable(IDataTable):
    """成交表（数据验证顺序修复版本）"""

    def initialize(self, adapter: DataAdapter = None, sync_service: DataSyncService = None,
                  event_engine: EventEngine = None, **kwargs) -> bool:
        """统一初始化接口"""
        try:
            self.adapter = adapter
            self.sync_service = sync_service
            self.event_engine = event_engine

            # 初始化数据存储
            self.trades: Dict[str, Dict[str, Any]] = {}
            self._trade_counter = 0

            self._initialized = True
            self.logger.info(f"成交表初始化完成: {self.table_name}")
            return True

        except Exception as e:
            self.logger.error(f"成交表初始化失败: {e}")
            return False

    def get_table_info(self) -> Dict[str, Any]:
        """获取表信息"""
        return {
            "table_name": self.table_name,
            "initialized": self._initialized,
            "trade_count": len(self.trades),
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

            if 'commission' in data and not isinstance(data['commission'], (int, float)):
                self.logger.error("commission必须是数值类型")
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
        """保存数据（修复验证顺序）"""
        try:
            # 修改处1：先使用适配器转换数据
            if self.adapter:
                data = self.adapter.adapt_data(self.table_name, data)

            # 修改处2：先生成成交ID（如果未提供）
            if 'trade_id' not in data or not data['trade_id']:
                self._trade_counter += 1
                data['trade_id'] = f"TRADE_{self._trade_counter:08d}"

            trade_id = data['trade_id']

            # 修改处3：设置时间戳
            if 'trade_time' not in data:
                data['trade_time'] = datetime.now().isoformat()

            data['update_time'] = datetime.now().isoformat()

            # 修改处4：在生成所有必需字段后进行验证
            if not self.validate_data(data):
                return False

            # 保存成交
            self.trades[trade_id] = data

            self.logger.debug(f"成交数据保存成功: {trade_id}")
            return True

        except Exception as e:
            self.logger.error(f"保存成交数据失败: {e}")
            return False

    def add_trade(self, **kwargs) -> bool:
        """
        添加成交记录
        BacktestEngine调用时传递关键字参数
        """
        try:
            self.logger.info("通过add_trade方法添加成交记录")
            return self.save_data(kwargs)
        except Exception as e:
            self.logger.error(f"add_trade方法执行失败: {e}")
            return False

    def query_data(self, conditions: Dict[str, Any] = None) -> List[Dict[str, Any]]:
        """查询数据"""
        try:
            conditions = conditions or {}
            results = []

            for trade_id, trade_data in self.trades.items():
                if self._match_conditions(trade_data, conditions):
                    results.append(trade_data.copy())

            self.logger.debug(f"查询到 {len(results)} 条成交数据")
            return results

        except Exception as e:
            self.logger.error(f"查询成交数据失败: {e}")
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

    def record_trade(self, order_id: str, symbol: str, direction: str, price: float,
                    volume: int, commission: float = 0.0, **kwargs) -> Optional[Dict[str, Any]]:
        """记录成交"""
        trade_data = {
            'order_id': order_id,
            'symbol': symbol,
            'direction': direction,
            'price': price,
            'volume': volume,
            'commission': commission,
            'trade_time': datetime.now().isoformat(),
            **kwargs
        }

        if self.save_data(trade_data):
            return self.trades.get(trade_data.get('trade_id'))
        return None

    def get_trades_by_order(self, order_id: str) -> List[Dict[str, Any]]:
        """根据订单获取成交"""
        return self.query_data({'order_id': order_id})

    def get_trades_by_symbol(self, symbol: str) -> List[Dict[str, Any]]:
        """根据标的获取成交"""
        return self.query_data({'symbol': symbol})

    def get_trades_by_time_range(self, start_time: str, end_time: str) -> List[Dict[str, Any]]:
        """根据时间范围获取成交"""
        all_trades = list(self.trades.values())
        filtered_trades = []

        for trade in all_trades:
            trade_time = trade.get('trade_time', '')
            if start_time <= trade_time <= end_time:
                filtered_trades.append(trade)

        return filtered_trades

    def calculate_trading_stats(self, symbol: str = None, strategy: str = None) -> Dict[str, Any]:
        """计算交易统计"""
        trades = self.query_data({})
        if symbol:
            trades = [t for t in trades if t.get('symbol') == symbol]
        if strategy:
            trades = [t for t in trades if t.get('strategy') == strategy]

        if not trades:
            return {
                'total_trades': 0,
                'total_volume': 0,
                'total_commission': 0,
                'avg_trade_size': 0
            }

        total_volume = sum(t.get('volume', 0) for t in trades)
        total_commission = sum(t.get('commission', 0) for t in trades)

        return {
            'total_trades': len(trades),
            'total_volume': total_volume,
            'total_commission': total_commission,
            'avg_trade_size': total_volume / len(trades) if trades else 0,
            'symbol': symbol,
            'strategy': strategy
        }

    def get_recent_trades(self, limit: int = 100) -> List[Dict[str, Any]]:
        """获取最近成交"""
        all_trades = list(self.trades.values())
        # 按时间排序，最新的在前
        sorted_trades = sorted(all_trades,
                             key=lambda x: x.get('trade_time', ''),
                             reverse=True)
        return sorted_trades[:limit]

    def get_trade_summary_by_direction(self, symbol: str = None) -> Dict[str, Any]:
        """按方向统计成交"""
        buys = self.query_data({'direction': 'BUY'})
        sells = self.query_data({'direction': 'SELL'})
        shorts = self.query_data({'direction': 'SHORT'})
        covers = self.query_data({'direction': 'COVER'})

        if symbol:
            buys = [t for t in buys if t.get('symbol') == symbol]
            sells = [t for t in sells if t.get('symbol') == symbol]
            shorts = [t for t in shorts if t.get('symbol') == symbol]
            covers = [t for t in covers if t.get('symbol') == symbol]

        def calculate_stats(trade_list):
            volume = sum(t.get('volume', 0) for t in trade_list)
            count = len(trade_list)
            avg_volume = volume / count if count > 0 else 0
            return {'count': count, 'volume': volume, 'avg_volume': avg_volume}

        return {
            'BUY': calculate_stats(buys),
            'SELL': calculate_stats(sells),
            'SHORT': calculate_stats(shorts),
            'COVER': calculate_stats(covers)
        }
