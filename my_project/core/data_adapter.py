"""
统一数据适配器接口
负责数据格式转换和标准化
"""
from typing import Dict, Any, List, Optional, Callable
import logging


class DataAdapter:
    """数据适配器（修复版本）"""

    def __init__(self, config: Dict[str, Any] = None):
        self.config = config or {}
        self.adapters: Dict[str, Callable] = {}
        self._setup_standard_adapters()

        # 日志设置
        self.logger = logging.getLogger("DataAdapter")
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
            self.logger.setLevel(logging.INFO)

    def _setup_standard_adapters(self):
        """设置标准数据适配器"""
        # 账户数据适配器
        self.adapters['account'] = self._adapt_account_data
        # 订单数据适配器
        self.adapters['order'] = self._adapt_order_data
        # 持仓数据适配器
        self.adapters['position'] = self._adapt_position_data
        # 成交数据适配器
        self.adapters['trade'] = self._adapt_trade_data

    def register_adapter(self, table_name: str, adapter_func: Callable):
        """注册自定义数据适配器"""
        self.adapters[table_name] = adapter_func
        self.logger.info(f"注册数据适配器: {table_name}")

    def adapt_data(self, table_name: str, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """统一数据转换接口

        Args:
            table_name: 表名
            raw_data: 原始数据

        Returns:
            Dict[str, Any]: 转换后的标准数据
        """
        try:
            adapter = self.adapters.get(table_name)
            if not adapter:
                self.logger.warning(f"未找到表 {table_name} 的适配器，使用原始数据")
                return raw_data

            adapted_data = adapter(raw_data)
            self.logger.debug(f"数据适配完成: {table_name}")
            return adapted_data

        except Exception as e:
            self.logger.error(f"数据适配失败 {table_name}: {e}")
            return raw_data

    def extract_core_data(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        提取核心数据（新增方法 - 修复兼容性问题）
        用于BacktestEngine的数据处理流程
        """
        try:
            self.logger.debug("开始提取核心数据")

            # 基础数据验证和清理
            if not raw_data or not isinstance(raw_data, dict):
                self.logger.warning("原始数据为空或非字典类型")
                return {}

            # 提取核心字段，过滤掉None值和空字符串
            core_data = {}
            for key, value in raw_data.items():
                if value is not None and value != "":
                    if isinstance(value, (int, float, str, bool)):
                        core_data[key] = value
                    elif isinstance(value, dict):
                        # 递归处理嵌套字典
                        core_data[key] = self.extract_core_data(value)
                    elif isinstance(value, list):
                        # 处理列表类型，只保留基本类型元素
                        filtered_list = []
                        for item in value:
                            if isinstance(item, (int, float, str, bool)):
                                filtered_list.append(item)
                            elif isinstance(item, dict):
                                filtered_list.append(self.extract_core_data(item))
                        core_data[key] = filtered_list

            self.logger.debug(f"核心数据提取完成，字段数: {len(core_data)}")
            return core_data

        except Exception as e:
            self.logger.error(f"提取核心数据失败: {e}")
            return raw_data if isinstance(raw_data, dict) else {}

    def batch_extract_core_data(self, raw_data_list: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        批量提取核心数据（新增方法 - 修复兼容性问题）
        """
        try:
            extracted_data = []
            for raw_data in raw_data_list:
                core_data = self.extract_core_data(raw_data)
                extracted_data.append(core_data)

            self.logger.debug(f"批量提取完成，处理了 {len(extracted_data)} 条数据")
            return extracted_data

        except Exception as e:
            self.logger.error(f"批量提取核心数据失败: {e}")
            return raw_data_list

    def _adapt_account_data(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """适配账户数据"""
        standard_data = {
            'account_id': raw_data.get('account_id', ''),
            'balance': float(raw_data.get('balance', 0.0)),
            'available': float(raw_data.get('available', 0.0)),
            'commission': float(raw_data.get('commission', 0.0)),
            'margin': float(raw_data.get('margin', 0.0)),
            'frozen': float(raw_data.get('frozen', 0.0)),
            'update_time': raw_data.get('update_time', '')
        }
        return {k: v for k, v in standard_data.items() if v is not None}

    def _adapt_order_data(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """适配订单数据"""
        standard_data = {
            'order_id': raw_data.get('order_id', ''),
            'symbol': raw_data.get('symbol', ''),
            'direction': raw_data.get('direction', ''),
            'price': float(raw_data.get('price', 0.0)),
            'volume': int(raw_data.get('volume', 0)),
            'status': raw_data.get('status', ''),
            'order_time': raw_data.get('order_time', ''),
            'strategy': raw_data.get('strategy', '')
        }
        return {k: v for k, v in standard_data.items() if v is not None}

    def _adapt_position_data(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """适配持仓数据"""
        standard_data = {
            'strategy': raw_data.get('strategy', ''),
            'symbol': raw_data.get('symbol', ''),
            'direction': raw_data.get('direction', ''),
            'volume': int(raw_data.get('volume', 0)),
            'price': float(raw_data.get('price', 0.0)),
            'float_pnl': float(raw_data.get('float_pnl', 0.0)),
            'pnl': float(raw_data.get('pnl', 0.0)),
            'update_time': raw_data.get('update_time', ''),
            'trade_id': raw_data.get('trade_id', '')
        }
        return {k: v for k, v in standard_data.items() if v is not None}

    def _adapt_trade_data(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """适配成交数据"""
        standard_data = {
            'trade_id': raw_data.get('trade_id', ''),
            'order_id': raw_data.get('order_id', ''),
            'symbol': raw_data.get('symbol', ''),
            'direction': raw_data.get('direction', ''),
            'price': float(raw_data.get('price', 0.0)),
            'volume': int(raw_data.get('volume', 0)),
            'trade_time': raw_data.get('trade_time', ''),
            'commission': float(raw_data.get('commission', 0.0))
        }
        return {k: v for k, v in standard_data.items() if v is not None}

    def batch_adapt_data(self, table_name: str, raw_data_list: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """批量数据适配"""
        adapted_list = []
        for raw_data in raw_data_list:
            adapted_data = self.adapt_data(table_name, raw_data)
            adapted_list.append(adapted_data)
        return adapted_list

    def get_adapter_info(self) -> Dict[str, Any]:
        """获取适配器信息"""
        return {
            "registered_adapters": list(self.adapters.keys()),
            "config": self.config
        }
