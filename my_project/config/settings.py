"""
改进版配置文件 - 优化配置管理和性能参数
"""

import os
from datetime import datetime
from typing import Dict, Any, List


class Settings:
    """
    改进版配置类 - 支持性能调优和灵活配置
    """

    def __init__(self):
        # === API连接配置 ===
        self.tqsdk_username = os.getenv("TQSDK_USERNAME", "test")
        self.tqsdk_password = os.getenv("TQSDK_PASSWORD", "test")
        self.tqsdk_server = os.getenv("TQSDK_SERVER", "wss://md1.shinnytech.com/t/md/front/mobile")

        # === 性能优化配置 ===
        self.connection_pool_size = 3           # 连接池大小
        self.connection_timeout = 30.0          # 连接超时(秒)
        self.data_request_timeout = 60.0        # 数据请求超时(秒)
        self.cache_ttl = 300                    # 缓存存活时间(秒)
        self.cache_max_size = 1000              # 缓存最大条目数

        # === 回测时间配置 ===
        self.backtest_start_date = "2024-01-01"
        self.backtest_end_date = "2024-01-05"
        self.backtest_time_range = {
            "start_time": "09:00:00",
            "end_time": "15:00:00",
        }

        # === 交易品种配置 ===
        self.symbols = [
            "SHFE.cu2401",  # 沪铜2401
            "SHFE.au2406",  # 沪金2406
            "DCE.m2405",    # 豆粕2405
            "CZCE.CF401",   # 棉花401
        ]

        # === 数据频率配置 ===
        self.data_frequencies = {
            "tick": 0,
            "1min": 60,
            "5min": 300,
            "15min": 900,
            "1hour": 3600,
            "1day": 86400,
        }
        self.default_frequency = "1hour"

        # === 账户和风控配置 ===
        self.initial_balance = 1000000.0
        self.transaction_ratio = 0.0003
        self.slippage_ratio = 0.0001
        self.margin_ratio = 0.1
        self.max_position_ratio = 0.3
        self.max_drawdown_limit = 0.1

        # === 网关性能配置 ===
        self.gateway_config = {
            "enable_cache": True,           # 启用缓存
            "enable_connection_pool": True, # 启用连接池
            "enable_smart_routing": True,   # 启用智能路由
            "max_retry_attempts": 3,        # 最大重试次数
            "retry_delay": 1.0,             # 重试延迟(秒)
        }

        # === 日志配置 ===
        self.log_config = {
            "level": "INFO",
            "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            "file": "quant_system.log",
            "max_bytes": 10485760,  # 10MB
            "backup_count": 5
        }

        # === 服务器性能指标初始值 ===
        self.server_metrics = {
            "wss://md1.shinnytech.com/t/md/front/mobile": {
                "response_time": 1.0,
                "success_rate": 0.95,
                "request_count": 0
            },
            "wss://md2.shinnytech.com/t/md/front/mobile": {
                "response_time": 1.0,
                "success_rate": 0.95,
                "request_count": 0
            },
            "wss://md3.shinnytech.com/t/md/front/mobile": {
                "response_time": 1.0,
                "success_rate": 0.95,
                "request_count": 0
            }
        }

    def get_optimized_settings(self, mode: str = "balanced") -> Dict[str, Any]:
        """
        根据运行模式获取优化配置
        :param mode: 运行模式 - "speed"(速度优先), "reliable"(可靠优先), "balanced"(平衡)
        """
        base_config = self._get_base_config()

        if mode == "speed":
            return {**base_config, **{
                "connection_pool_size": 5,
                "connection_timeout": 15.0,
                "cache_ttl": 600,
                "gateway_config.enable_cache": True,
                "gateway_config.max_retry_attempts": 2
            }}
        elif mode == "reliable":
            return {**base_config, **{
                "connection_pool_size": 2,
                "connection_timeout": 60.0,
                "cache_ttl": 1800,
                "gateway_config.max_retry_attempts": 5,
                "gateway_config.retry_delay": 2.0
            }}
        else:  # balanced
            return base_config

    def _get_base_config(self) -> Dict[str, Any]:
        """获取基础配置"""
        return {
            "tqsdk_username": self.tqsdk_username,
            "tqsdk_password": self.tqsdk_password,
            "backtest_start_date": self.backtest_start_date,
            "backtest_end_date": self.backtest_end_date,
            "symbols": self.symbols,
            "default_frequency": self.default_frequency,
            "initial_balance": self.initial_balance,
            "connection_pool_size": self.connection_pool_size,
            "connection_timeout": self.connection_timeout,
            "gateway_config": self.gateway_config
        }

    def validate_settings(self) -> bool:
        """验证配置有效性"""
        try:
            # 验证日期
            start_dt = datetime.strptime(self.backtest_start_date, "%Y-%m-%d")
            end_dt = datetime.strptime(self.backtest_end_date, "%Y-%m-%d")

            if start_dt >= end_dt:
                raise ValueError("回测开始日期必须早于结束日期")

            # 验证数值参数
            if self.initial_balance <= 0:
                raise ValueError("初始资金必须大于0")

            if not self.symbols:
                raise ValueError("至少需要配置一个交易品种")

            # 验证网关配置
            if self.gateway_config["max_retry_attempts"] < 0:
                raise ValueError("重试次数不能为负数")

            return True

        except ValueError as e:
            print(f"配置验证失败: {e}")
            return False
        except Exception as e:
            print(f"配置验证异常: {e}")
            return False

    def get_symbol_config(self, symbol: str) -> Dict[str, Any]:
        """获取特定品种的配置"""
        symbol_configs = {
            "SHFE.cu2401": {
                "name": "沪铜2401",
                "multiplier": 5,
                "price_tick": 10,
                "margin_ratio": 0.08,
                "trading_hours": ["09:00-10:15", "10:30-11:30", "13:30-15:00"]
            },
            "SHFE.au2406": {
                "name": "沪金2406",
                "multiplier": 1000,
                "price_tick": 0.02,
                "margin_ratio": 0.06,
                "trading_hours": ["09:00-10:15", "10:30-11:30", "13:30-15:00"]
            },
            "DCE.m2405": {
                "name": "豆粕2405",
                "multiplier": 10,
                "price_tick": 1,
                "margin_ratio": 0.07,
                "trading_hours": ["09:00-10:15", "10:30-11:30", "13:30-15:00"]
            },
            "CZCE.CF401": {
                "name": "棉花401",
                "multiplier": 5,
                "price_tick": 5,
                "margin_ratio": 0.05,
                "trading_hours": ["09:00-10:15", "10:30-11:30", "13:30-15:00"]
            }
        }
        return symbol_configs.get(symbol, {})

    def update_from_dict(self, config_dict: Dict[str, Any]):
        """从字典更新配置"""
        for key, value in config_dict.items():
            if hasattr(self, key):
                setattr(self, key, value)

    def to_dict(self) -> Dict[str, Any]:
        """将配置转换为字典"""
        return {key: value for key, value in self.__dict__.items()
                if not key.startswith('_')}

    def print_performance_settings(self):
        """打印性能相关配置"""
        print("=== 性能优化配置 ===")
        print(f"连接池大小: {self.connection_pool_size}")
        print(f"连接超时: {self.connection_timeout}秒")
        print(f"缓存TTL: {self.cache_ttl}秒")
        print(f"缓存最大大小: {self.cache_max_size}")
        print(f"智能路由: {'启用' if self.gateway_config['enable_smart_routing'] else '禁用'}")
        print(f"最大重试次数: {self.gateway_config['max_retry_attempts']}")


# 测试代码
if __name__ == "__main__":
    # 创建配置实例
    config = Settings()

    # 验证配置
    if config.validate_settings():
        print("✓ 配置验证通过")

        # 打印配置摘要
        print("\n=== 配置摘要 ===")
        print(f"回测期间: {config.backtest_start_date} 至 {config.backtest_end_date}")
        print(f"交易品种: {', '.join(config.symbols)}")
        print(f"初始资金: {config.initial_balance:,.2f}")

        # 测试性能配置
        speed_config = config.get_optimized_settings("speed")
        print(f"\n速度优先模式 - 连接超时: {speed_config['connection_timeout']}秒")

        # 测试品种配置
        cu_config = config.get_symbol_config("SHFE.cu2401")
        print(f"\n沪铜配置: {cu_config}")

    else:
        print("✗ 配置验证失败")
