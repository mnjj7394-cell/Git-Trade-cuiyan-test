"""
系统配置文件
第四阶段：完整配置参数和性能调优
"""
import os
from typing import Dict, Any, List, Optional
from datetime import timedelta


class Settings:
    """系统配置类（已增强财务计算、订单管理和性能调优）"""

    def __init__(self, config_file: str = None):
        self.config_file = config_file
        self._load_default_settings()

        if config_file and os.path.exists(config_file):
            self._load_from_file(config_file)
        else:
            self._load_environment_variables()

    def _load_default_settings(self):
        """加载默认配置"""
        # 天勤API配置
        self.tqsdk_config = {
            "username": os.getenv("TQSDK_USERNAME", "nuyoah"),
            "password": os.getenv("TQSDK_PASSWORD", "Cy704462@@"),
            "server": os.getenv("TQSDK_SERVER", "wss://free-openmd.shinnytech.com/t/md/front/mobile"),
            "timeout": int(os.getenv("TQSDK_TIMEOUT", "30")),
            "reconnect_interval": int(os.getenv("TQSDK_RECONNECT_INTERVAL", "5")),
            "max_reconnect_attempts": int(os.getenv("TQSDK_MAX_RECONNECT", "10"))
        }

        # 财务计算配置
        self.accounting_config = {
            "initial_balance": float(os.getenv("INITIAL_BALANCE", "1000000.0")),
            "commission_rate": float(os.getenv("COMMISSION_RATE", "0.0001")),
            "margin_rate": float(os.getenv("MARGIN_RATE", "0.1")),
            "slippage_rate": float(os.getenv("SLIPPAGE_RATE", "0.0001")),
            "risk_free_rate": float(os.getenv("RISK_FREE_RATE", "0.03")),
            "tax_rate": float(os.getenv("TAX_RATE", "0.001")),
            "min_commission": float(os.getenv("MIN_COMMISSION", "5.0"))
        }

        # 订单管理配置
        self.order_config = {
            "max_open_orders": int(os.getenv("MAX_OPEN_ORDERS", "100")),
            "order_timeout": int(os.getenv("ORDER_TIMEOUT", "300")),
            "auto_cancel_pending": os.getenv("AUTO_CANCEL_PENDING", "True").lower() == "true",
            "partial_fill_allowed": os.getenv("PARTIAL_FILL_ALLOWED", "True").lower() == "true",
            "max_order_volume": int(os.getenv("MAX_ORDER_VOLUME", "100")),
            "price_tick": float(os.getenv("PRICE_TICK", "1.0"))
        }

        # 风险控制配置
        self.risk_config = {
            "max_position_ratio": float(os.getenv("MAX_POSITION_RATIO", "0.3")),
            "max_drawdown_limit": float(os.getenv("MAX_DRAWDOWN_LIMIT", "0.1")),
            "stop_loss_rate": float(os.getenv("STOP_LOSS_RATE", "0.05")),
            "take_profit_rate": float(os.getenv("TAKE_PROFIT_RATE", "0.1")),
            "daily_loss_limit": float(os.getenv("DAILY_LOSS_LIMIT", "0.05")),
            "var_limit": float(os.getenv("VAR_LIMIT", "0.02")),
            "margin_alert_level": float(os.getenv("MARGIN_ALERT_LEVEL", "0.8"))
        }

        # 线程池和性能配置
        self.performance_config = {
            "max_workers": int(os.getenv("MAX_WORKERS", "10")),
            "thread_pool_size": int(os.getenv("THREAD_POOL_SIZE", "20")),
            "event_queue_size": int(os.getenv("EVENT_QUEUE_SIZE", "1000")),
            "data_buffer_size": int(os.getenv("DATA_BUFFER_SIZE", "10000")),
            "log_buffer_size": int(os.getenv("LOG_BUFFER_SIZE", "1000")),
            "cache_ttl": int(os.getenv("CACHE_TTL", "300")),
            "gc_interval": int(os.getenv("GC_INTERVAL", "60")),
            "profiling_enabled": os.getenv("PROFILING_ENABLED", "False").lower() == "true"
        }

        # 回测配置
        self.backtest_config = {
            "initial_capital": float(os.getenv("INITIAL_CAPITAL", "1000000.0")),
            "commission_rate": float(os.getenv("BACKTEST_COMMISSION_RATE", "0.0001")),
            "slippage": float(os.getenv("BACKTEST_SLIPPAGE", "0.0001")),
            "frequency": os.getenv("BACKTEST_FREQUENCY", "1min"),
            "start_date": os.getenv("BACKTEST_START_DATE", "2024-01-01"),
            "end_date": os.getenv("BACKTEST_END_DATE", "2024-01-05"),
            "benchmark": os.getenv("BACKTEST_BENCHMARK", "SHFE.cu2401"),
            "warmup_period": int(os.getenv("BACKTEST_WARMUP", "50")),
            "output_dir": os.getenv("BACKTEST_OUTPUT_DIR", "./backtest_results")
        }

        # 监控和日志配置
        self.monitoring_config = {
            "log_level": os.getenv("LOG_LEVEL", "INFO"),
            "log_file": os.getenv("LOG_FILE", "quant_system.log"),
            "metrics_port": int(os.getenv("METRICS_PORT", "9090")),
            "health_check_interval": int(os.getenv("HEALTH_CHECK_INTERVAL", "30")),
            "performance_metrics_interval": int(os.getenv("METRICS_INTERVAL", "60")),
            "alert_webhook": os.getenv("ALERT_WEBHOOK", ""),
            "data_retention_days": int(os.getenv("DATA_RETENTION_DAYS", "30")),
            "backup_interval": int(os.getenv("BACKUP_INTERVAL", "3600")),
            "enable_telemetry": os.getenv("ENABLE_TELEMETRY", "True").lower() == "true"
        }

        # 数据源配置
        self.data_source_config = {
            "primary_source": os.getenv("PRIMARY_DATA_SOURCE", "tqsdk"),
            "fallback_source": os.getenv("FALLBACK_DATA_SOURCE", "file"),
            "local_data_path": os.getenv("LOCAL_DATA_PATH", "./data"),
            "real_time_enabled": os.getenv("REAL_TIME_ENABLED", "True").lower() == "true",
            "historical_data_days": int(os.getenv("HISTORICAL_DATA_DAYS", "365")),
            "data_quality_check": os.getenv("DATA_QUALITY_CHECK", "True").lower() == "true",
            "auto_update_symbols": os.getenv("AUTO_UPDATE_SYMBOLS", "True").lower() == "true"
        }

        # 策略配置
        self.strategy_config = {
            "default_strategy": os.getenv("DEFAULT_STRATEGY", "double_ma"),
            "max_concurrent_strategies": int(os.getenv("MAX_CONCURRENT_STRATEGIES", "5")),
            "strategy_restart_interval": int(os.getenv("STRATEGY_RESTART_INTERVAL", "86400")),
            "performance_review_interval": int(os.getenv("PERFORMANCE_REVIEW_INTERVAL", "3600")),
            "auto_stop_loss": os.getenv("AUTO_STOP_LOSS", "True").lower() == "true",
            "risk_adjustment": os.getenv("RISK_ADJUSTMENT", "True").lower() == "true"
        }

    def _load_from_file(self, config_file: str):
        """从配置文件加载设置"""
        try:
            import json
            with open(config_file, 'r', encoding='utf-8') as f:
                config_data = json.load(f)

            # 合并配置
            self._update_nested_dict(self.tqsdk_config, config_data.get('tqsdk', {}))
            self._update_nested_dict(self.accounting_config, config_data.get('accounting', {}))
            self._update_nested_dict(self.order_config, config_data.get('order', {}))
            self._update_nested_dict(self.risk_config, config_data.get('risk', {}))
            self._update_nested_dict(self.performance_config, config_data.get('performance', {}))
            self._update_nested_dict(self.backtest_config, config_data.get('backtest', {}))
            self._update_nested_dict(self.monitoring_config, config_data.get('monitoring', {}))
            self._update_nested_dict(self.data_source_config, config_data.get('data_source', {}))
            self._update_nested_dict(self.strategy_config, config_data.get('strategy', {}))

        except Exception as e:
            print(f"配置文件加载失败: {e}，使用默认配置")

    def _load_environment_variables(self):
        """从环境变量加载配置"""
        # 天勤API配置
        self._update_from_env(self.tqsdk_config, "TQSDK_")

        # 财务计算配置
        self._update_from_env(self.accounting_config, "ACCOUNTING_")

        # 订单管理配置
        self._update_from_env(self.order_config, "ORDER_")

        # 风险控制配置
        self._update_from_env(self.risk_config, "RISK_")

        # 性能配置
        self._update_from_env(self.performance_config, "PERF_")

        # 回测配置
        self._update_from_env(self.backtest_config, "BACKTEST_")

        # 监控配置
        self._update_from_env(self.monitoring_config, "MONITORING_")

        # 数据源配置
        self._update_from_env(self.data_source_config, "DATA_")

        # 策略配置
        self._update_from_env(self.strategy_config, "STRATEGY_")

    def _update_from_env(self, config_dict: Dict[str, Any], prefix: str):
        """从环境变量更新配置字典"""
        for key in list(config_dict.keys()):
            env_key = prefix + key.upper()
            if env_key in os.environ:
                value = os.environ[env_key]
                # 类型转换
                if isinstance(config_dict[key], bool):
                    config_dict[key] = value.lower() == "true"
                elif isinstance(config_dict[key], int):
                    config_dict[key] = int(value)
                elif isinstance(config_dict[key], float):
                    config_dict[key] = float(value)
                else:
                    config_dict[key] = value

    def _update_nested_dict(self, target: Dict[str, Any], source: Dict[str, Any]):
        """递归更新嵌套字典"""
        for key, value in source.items():
            if key in target and isinstance(target[key], dict) and isinstance(value, dict):
                self._update_nested_dict(target[key], value)
            else:
                target[key] = value

    def get_tqsdk_config(self) -> Dict[str, Any]:
        """获取天勤API配置"""
        return self.tqsdk_config.copy()

    def get_accounting_config(self) -> Dict[str, Any]:
        """获取财务计算配置"""
        return self.accounting_config.copy()

    def get_order_config(self) -> Dict[str, Any]:
        """获取订单管理配置"""
        return self.order_config.copy()

    def get_risk_config(self) -> Dict[str, Any]:
        """获取风险控制配置"""
        return self.risk_config.copy()

    def get_performance_config(self) -> Dict[str, Any]:
        """获取性能配置"""
        return self.performance_config.copy()

    def get_backtest_config(self) -> Dict[str, Any]:
        """获取回测配置"""
        return self.backtest_config.copy()

    def get_monitoring_config(self) -> Dict[str, Any]:
        """获取监控配置"""
        return self.monitoring_config.copy()

    def get_data_source_config(self) -> Dict[str, Any]:
        """获取数据源配置"""
        return self.data_source_config.copy()

    def get_strategy_config(self) -> Dict[str, Any]:
        """获取策略配置"""
        return self.strategy_config.copy()

    def get_all_config(self) -> Dict[str, Any]:
        """获取所有配置"""
        return {
            "tqsdk": self.get_tqsdk_config(),
            "accounting": self.get_accounting_config(),
            "order": self.get_order_config(),
            "risk": self.get_risk_config(),
            "performance": self.get_performance_config(),
            "backtest": self.get_backtest_config(),
            "monitoring": self.get_monitoring_config(),
            "data_source": self.get_data_source_config(),
            "strategy": self.get_strategy_config(),
            "version": "1.0.0",
            "config_file": self.config_file,
            "timestamp": self._get_timestamp()
        }

    def validate_config(self) -> Dict[str, Any]:
        """验证配置有效性"""
        errors = []
        warnings = []

        # 验证天勤配置
        if not self.tqsdk_config.get("username") or not self.tqsdk_config.get("password"):
            errors.append("天勤API用户名和密码必须配置")

        # 验证财务配置
        if self.accounting_config.get("initial_balance") <= 0:
            errors.append("初始资金必须大于0")

        if self.accounting_config.get("commission_rate") < 0:
            errors.append("手续费率不能为负数")

        # 验证风险配置
        if self.risk_config.get("max_position_ratio") <= 0 or self.risk_config.get("max_position_ratio") > 1:
            errors.append("最大持仓比例必须在0-1之间")

        # 验证性能配置
        if self.performance_config.get("max_workers") <= 0:
            errors.append("最大工作线程数必须大于0")

        # 验证监控配置
        if self.monitoring_config.get("metrics_port") < 1024 or self.monitoring_config.get("metrics_port") > 65535:
            warnings.append("监控端口应在1024-65535之间")

        return {
            "is_valid": len(errors) == 0,
            "errors": errors,
            "warnings": warnings,
            "timestamp": self._get_timestamp()
        }

    def save_config(self, file_path: str = None):
        """保存配置到文件"""
        if not file_path:
            file_path = self.config_file or "quant_config.json"

        try:
            import json
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(self.get_all_config(), f, indent=2, ensure_ascii=False, default=str)
            print(f"配置已保存到: {file_path}")
        except Exception as e:
            print(f"配置保存失败: {e}")

    def _get_timestamp(self) -> str:
        """获取时间戳"""
        from datetime import datetime
        return datetime.now().isoformat()

    def __str__(self) -> str:
        """配置信息字符串表示"""
        config = self.get_all_config()
        # 隐藏敏感信息
        if 'password' in config.get('tqsdk', {}):
            config['tqsdk']['password'] = '***'

        import json
        return json.dumps(config, indent=2, ensure_ascii=False, default=str)


# 测试代码
if __name__ == "__main__":
    # 创建配置实例
    settings = Settings()

    # 验证配置
    validation = settings.validate_config()
    print("配置验证结果:", validation)

    # 显示配置摘要
    print("系统配置摘要:")
    print(f"天勤API: {settings.tqsdk_config['username']}")
    print(f"初始资金: {settings.accounting_config['initial_balance']:,.2f}")
    print(f"最大持仓比例: {settings.risk_config['max_position_ratio']:.1%}")
    print(f"线程数: {settings.performance_config['max_workers']}")
    print(f"日志级别: {settings.monitoring_config['log_level']}")

    # 测试配置保存
    settings.save_config("test_config.json")

    print("配置系统测试完成")
