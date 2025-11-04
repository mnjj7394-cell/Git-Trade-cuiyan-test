"""
数据适配器模块
负责将不同来源的数据格式转换为统一格式，供策略使用
"""
from typing import Dict, Any, List
import pandas as pd


class DataAdapter:
    """数据格式适配器，处理TqsdkGateway到策略的数据转换"""

    @staticmethod
    def convert_tqsdk_to_strategy_format(event_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        将TqsdkGateway的事件数据转换为策略期望的格式

        Args:
            event_data: TqsdkGateway返回的原始事件数据

        Returns:
            转换后的策略数据格式
        """
        if not event_data or 'data' not in event_data:
            return {}

        data_type = event_data.get('type', '')
        table = event_data.get('table', '')
        raw_data = event_data.get('data', {})

        # 根据事件类型确定数据类型
        if data_type == 'data_update' and 'datetime' in raw_data:
            # 判断是Tick数据还是Bar数据
            if 'open' in raw_data and 'high' in raw_data and 'low' in raw_data and 'close' in raw_data:
                data_type_conv = 'bar'
            else:
                data_type_conv = 'tick'
        else:
            data_type_conv = 'unknown'

        # 构建统一格式
        converted_data = {
            'data_type': data_type_conv,
            'symbol': raw_data.get('symbol', ''),
            'datetime': raw_data.get('datetime', 0),
            'data': raw_data
        }

        # 添加具体字段
        if data_type_conv == 'bar':
            converted_data.update({
                'open': raw_data.get('open', 0.0),
                'high': raw_data.get('high', 0.0),
                'low': raw_data.get('low', 0.0),
                'close': raw_data.get('close', 0.0),
                'volume': raw_data.get('volume', 0),
                'open_interest': raw_data.get('open_interest', 0)
            })
        elif data_type_conv == 'tick':
            converted_data.update({
                'last_price': raw_data.get('last_price', 0.0),
                'volume': raw_data.get('volume', 0),
                'amount': raw_data.get('amount', 0.0)
            })

        return converted_data

    @staticmethod
    def validate_data_format(data: Dict[str, Any]) -> bool:
        """
        验证数据格式是否符合规范

        Args:
            data: 要验证的数据

        Returns:
            bool: 数据格式是否有效
        """
        required_fields = ['data_type', 'symbol', 'datetime']
        return all(field in data for field in required_fields)

    @staticmethod
    def extract_core_data(data: Dict[str, Any]) -> Dict[str, Any]:
        """
        从嵌套数据中提取核心数据（解决数据层级问题）

        Args:
            data: 可能嵌套的数据

        Returns:
            提取后的核心数据
        """
        # 如果数据已经扁平，直接返回
        if 'data' not in data or not isinstance(data.get('data'), dict):
            return data

        # 从data字段中提取核心数据
        core_data = data.get('data', {})
        core_data['data_type'] = data.get('data_type', 'unknown')
        core_data['symbol'] = data.get('symbol', '')
        core_data['datetime'] = data.get('datetime', 0)

        return core_data


# 测试代码
if __name__ == "__main__":
    # 测试数据转换
    adapter = DataAdapter()

    # 模拟TqsdkGateway返回的Bar数据
    sample_bar_event = {
        'type': 'data_update',
        'table': 'market_data',
        'data': {
            'symbol': 'SHFE.cu2401',
            'datetime': 1704067200000000000,  # 纳秒时间戳
            'open': 68020.0,
            'high': 68090.0,
            'low': 67970.0,
            'close': 68000.0,
            'volume': 1000,
            'open_interest': 50000
        }
    }

    # 测试转换
    converted = adapter.convert_tqsdk_to_strategy_format(sample_bar_event)
    print("转换后的Bar数据:", converted)
    print("数据验证结果:", adapter.validate_data_format(converted))

    # 测试核心数据提取
    core_data = adapter.extract_core_data(converted)
    print("提取的核心数据:", core_data)
