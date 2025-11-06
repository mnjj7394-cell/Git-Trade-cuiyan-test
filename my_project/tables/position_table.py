"""
持仓表统一接口实现
遵循IDataTable接口规范
修复版本：增强参数验证和错误处理
"""
from typing import Dict, Any, List, Optional
from datetime import datetime
from core.data_table_base import IDataTable
from core.data_adapter import DataAdapter
from core.data_sync_service import DataSyncService
from core.event_engine import EventEngine
import logging


class PositionTable(IDataTable):
    """持仓表（修复参数验证问题）"""

    def initialize(self, adapter: DataAdapter = None, sync_service: DataSyncService = None,
                  event_engine: EventEngine = None, **kwargs) -> bool:
        """统一初始化接口"""
        try:
            self.adapter = adapter
            self.sync_service = sync_service
            self.event_engine = event_engine

            # 初始化数据存储
            self.positions: Dict[str, Dict[str, Any]] = {}
            self._position_history: List[Dict[str, Any]] = []

            self._initialized = True
            self.logger.info(f"持仓表初始化完成: {self.table_name}")
            return True

        except Exception as e:
            self.logger.error(f"持仓表初始化失败: {e}")
            return False

    def _get_position_key(self, symbol: str, strategy: str) -> str:
        """生成持仓唯一键"""
        return f"{symbol}_{strategy}"

    def get_table_info(self) -> Dict[str, Any]:
        """获取表信息"""
        return {
            "table_name": self.table_name,
            "initialized": self._initialized,
            "position_count": len(self.positions),
            "config": self.table_config
        }

    def validate_data(self, data: Dict[str, Any]) -> bool:
        """数据验证（增强验证逻辑）"""
        try:
            required_fields = self.table_config.get('validation_rules', {}).get('required_fields', [])
            for field in required_fields:
                if field not in data:
                    self.logger.error(f"缺少必需字段: {field}")
                    return False

            # 验证数值类型
            if 'volume' in data and not isinstance(data['volume'], (int, float)):
                self.logger.error("volume必须是数值类型")
                return False

            if 'price' in data and not isinstance(data['price'], (int, float)):
                self.logger.error("price必须是数值类型")
                return False

            # 验证交易方向（增强验证）
            valid_directions = ['BUY', 'SELL', 'SHORT', 'COVER', '']
            if 'direction' in data:
                if not isinstance(data['direction'], str):
                    self.logger.error(f"direction必须是字符串类型，实际类型: {type(data['direction'])}")
                    return False
                if data['direction'] not in valid_directions:
                    self.logger.error(f"无效的交易方向: {data['direction']}，有效值: {valid_directions}")
                    return False

            # 验证策略名称和标的符号
            if 'strategy' in data and not isinstance(data['strategy'], str):
                self.logger.error("strategy必须是字符串类型")
                return False

            if 'symbol' in data and not isinstance(data['symbol'], str):
                self.logger.error("symbol必须是字符串类型")
                return False

            return True

        except Exception as e:
            self.logger.error(f"数据验证失败: {e}")
            return False

    def save_data(self, data: Dict[str, Any]) -> bool:
        """保存数据（增强错误处理）"""
        try:
            if not self.validate_data(data):
                self.logger.error("数据验证失败，无法保存")
                return False

            # 使用适配器转换数据
            if self.adapter:
                data = self.adapter.adapt_data(self.table_name, data)

            strategy = data.get('strategy', 'default')
            symbol = data.get('symbol')

            if not symbol:
                self.logger.error("缺少symbol字段，无法保存持仓")
                return False

            position_key = self._get_position_key(symbol, strategy)

            # 设置时间戳
            data['update_time'] = datetime.now().isoformat()

            # 如果持仓量为0，删除该持仓记录（修改处1）
            volume = data.get('volume', 0)
            if volume == 0:
                if position_key in self.positions:
                    del self.positions[position_key]
                    self.logger.debug(f"删除零持仓记录: {position_key}")
            else:
                # 保存或更新持仓
                self.positions[position_key] = data

            # 记录持仓历史
            self._record_position_history(data)

            self.logger.debug(f"持仓数据保存成功: {position_key}")
            return True

        except Exception as e:
            self.logger.error(f"保存持仓数据失败: {e}")
            return False

    def _record_position_history(self, position_data: Dict[str, Any]):
        """记录持仓历史"""
        history_record = {
            'timestamp': datetime.now().isoformat(),
            'strategy': position_data.get('strategy'),
            'symbol': position_data.get('symbol'),
            'direction': position_data.get('direction'),
            'volume': position_data.get('volume'),
            'price': position_data.get('price'),
            'float_pnl': position_data.get('float_pnl', 0.0),
            'pnl': position_data.get('pnl', 0.0)
        }
        self._position_history.append(history_record)

    def query_data(self, conditions: Dict[str, Any] = None) -> List[Dict[str, Any]]:
        """查询数据"""
        try:
            conditions = conditions or {}
            results = []

            for position_key, position_data in self.positions.items():
                if self._match_conditions(position_data, conditions):
                    results.append(position_data.copy())

            self.logger.debug(f"查询到 {len(results)} 条持仓数据")
            return results

        except Exception as e:
            self.logger.error(f"查询持仓数据失败: {e}")
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

    def update_position(self, strategy: str, symbol: str, direction: str,
                       price: float, volume: int, trade_id: str = None) -> bool:
        """更新持仓信息（修复：增强参数验证和错误处理）"""
        # 修改处2：添加参数类型验证
        try:
            if not isinstance(strategy, str):
                self.logger.error(f"strategy参数类型错误: 期望str, 实际{type(strategy)}")
                return False
            if not isinstance(symbol, str):
                self.logger.error(f"symbol参数类型错误: 期望str, 实际{type(symbol)}")
                return False
            if not isinstance(direction, str):
                self.logger.error(f"direction参数类型错误: 期望str, 实际{type(direction)}")
                return False
            if not isinstance(price, (int, float)):
                self.logger.error(f"price参数类型错误: 期望数值, 实际{type(price)}")
                return False
            if not isinstance(volume, (int, float)):
                self.logger.error(f"volume参数类型错误: 期望数值, 实际{type(volume)}")
                return False

            # 验证方向值
            valid_directions = ['BUY', 'SELL', 'SHORT', 'COVER']
            if direction not in valid_directions:
                self.logger.error(f"无效的direction: {direction}，有效值: {valid_directions}")
                return False

            position_key = self._get_position_key(symbol, strategy)
            current_position = self.positions.get(position_key, {})

            # 计算新持仓
            new_volume = 0
            new_direction = direction

            if current_position:
                current_volume = current_position.get('volume', 0)
                current_direction = current_position.get('direction', '')

                if direction in ['BUY', 'SHORT']:
                    if current_direction == direction:
                        new_volume = current_volume + volume
                    elif current_direction == '' or current_volume == 0:
                        new_volume = volume
                    else:
                        # 反向开仓，平掉原有持仓
                        if volume > current_volume:
                            new_volume = volume - current_volume
                            new_direction = direction
                        else:
                            new_volume = 0
                            new_direction = ''
                elif direction in ['SELL', 'COVER']:
                    if current_direction == self._get_opposite_direction(direction):
                        new_volume = max(0, current_volume - volume)
                        new_direction = current_direction if new_volume > 0 else ''
                    else:
                        self.logger.warning(f"平仓方向不匹配: 当前{current_direction}, 平仓{direction}")
                        new_volume = current_volume
                        new_direction = current_direction
            else:
                if direction in ['BUY', 'SHORT']:
                    new_volume = volume
                    new_direction = direction
                else:
                    self.logger.warning(f"尝试平仓但无持仓: {strategy} {symbol} {direction}")
                    new_volume = 0
                    new_direction = ''

            # 构建持仓数据
            position_data = {
                'strategy': strategy,
                'symbol': symbol,
                'direction': new_direction,
                'volume': new_volume,
                'price': price if new_volume > 0 else current_position.get('price', price),
                'update_time': datetime.now().isoformat(),
                'trade_id': trade_id,
                'float_pnl': current_position.get('float_pnl', 0.0) if current_position else 0.0,
                'pnl': current_position.get('pnl', 0.0) if current_position else 0.0
            }

            # 修改处3：检查保存结果并记录日志
            success = self.save_data(position_data)
            if not success:
                self.logger.error(f"持仓保存失败: {strategy} {symbol} {direction}")
                return False

            return True

        except Exception as e:
            self.logger.error(f"更新持仓异常: {e}")
            return False

    def _get_opposite_direction(self, direction: str) -> str:
        """获取相反的交易方向"""
        opposite_map = {
            'BUY': 'SELL',
            'SELL': 'BUY',
            'SHORT': 'COVER',
            'COVER': 'SHORT'
        }
        return opposite_map.get(direction, direction)

    def get_position(self, strategy: str, symbol: str) -> Optional[Dict[str, Any]]:
        """获取特定持仓"""
        position_key = self._get_position_key(symbol, strategy)
        return self.positions.get(position_key)

    def get_all_positions_by_strategy(self, strategy: str) -> List[Dict[str, Any]]:
        """获取策略的所有持仓"""
        return self.query_data({'strategy': strategy})

    def get_all_positions_by_symbol(self, symbol: str) -> List[Dict[str, Any]]:
        """获取标的所有持仓"""
        return self.query_data({'symbol': symbol})

    def calculate_float_pnl(self, symbol: str, current_price: float) -> Dict[str, float]:
        """计算浮动盈亏"""
        positions = self.get_all_positions_by_symbol(symbol)
        total_pnl = 0.0

        for position in positions:
            volume = position.get('volume', 0)
            avg_price = position.get('price', 0.0)
            direction = position.get('direction', '')

            if direction == 'BUY':
                pnl = (current_price - avg_price) * volume
            elif direction == 'SHORT':
                pnl = (avg_price - current_price) * volume
            else:
                pnl = 0.0

            total_pnl += pnl

        return {'symbol': symbol, 'float_pnl': total_pnl, 'current_price': current_price}

    def get_position_history(self, strategy: str = None, symbol: str = None) -> List[Dict[str, Any]]:
        """获取持仓历史"""
        filtered_history = self._position_history.copy()

        if strategy:
            filtered_history = [h for h in filtered_history if h.get('strategy') == strategy]
        if symbol:
            filtered_history = [h for h in filtered_history if h.get('symbol') == symbol]

        return filtered_history
