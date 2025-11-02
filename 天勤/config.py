#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
配置文件 - 天勤SDK自定义数据表项目
功能：集中管理所有项目配置参数，直接在代码中定义，不使用环境变量
创建日期：2024年
"""

import os
import sys
from datetime import datetime
from typing import Dict, List, Any, Optional


class Config:
    """
    项目配置类
    集中管理所有配置参数，直接在代码中定义，不使用环境变量
    """

    # =========================================================================
    # 1. 天勤账户配置（直接在代码中定义）
    # =========================================================================

    # 天勤账号认证信息（直接在此处填写）
    TQ_USERNAME: str = "nuyoah"  # 请替换为您的实际天勤账号
    TQ_PASSWORD: str = "Cy704462@@"  # 请替换为您的实际天勤密码

    # 天勤服务器地址（通常使用默认值）
    TQ_API_URL: str = "ws://127.0.0.1:7777"

    # 账户类型：模拟盘/实盘
    TQ_ACCOUNT_TYPE: str = "SIM"  # SIM-模拟盘， REAL-实盘

    # =========================================================================
    # 2. 交易合约配置
    # =========================================================================

    # 默认测试合约（上海期货交易所-铜2410合约）
    DEFAULT_SYMBOL: str = "SHFE.cu2410"

    # 备用测试合约列表
    BACKUP_SYMBOLS: List[str] = [
        "SHFE.au2412",  # 沪金2412
        "DCE.m2409",  # 豆粕2409
        "CFFEX.IF2406",  # 股指期货IF2406
    ]

    # 合约乘数映射（用于计算合约价值）
    SYMBOL_MULTIPLIERS: Dict[str, int] = {
        "SHFE.cu2410": 5,  # 铜：5吨/手
        "SHFE.au2412": 1000,  # 黄金：1000克/手
        "DCE.m2409": 10,  # 豆粕：10吨/手
        "CFFEX.IF2406": 300,  # 股指：300元/点
    }

    # =========================================================================
    # 3. 数据表配置
    # =========================================================================

    # 自定义数据表网关名称
    GATEWAY_NAME: str = "TQSIM"

    # 数据更新频率（秒）
    DATA_UPDATE_INTERVAL: float = 0.1  # 100毫秒

    # 是否启用详细日志
    ENABLE_DETAILED_LOGGING: bool = True

    # 数据表字段验证精度（浮点数比较容差）
    FLOAT_PRECISION: float = 1e-6

    # =========================================================================
    # 4. 风险控制参数
    # =========================================================================

    # 实时止损参数
    RISK_MANAGEMENT: Dict[str, Any] = {
        # 总资金止损比例（亏损达到总资金的2%时触发止损）
        "STOP_LOSS_RATIO": -0.02,

        # 单笔交易最大亏损比例
        "SINGLE_TRADE_MAX_LOSS_RATIO": 0.01,

        # 单日最大亏损比例
        "DAILY_MAX_LOSS_RATIO": 0.05,

        # 最大持仓比例
        "MAX_POSITION_RATIO": 0.8,

        # 启用实时风控监控
        "ENABLE_REALTIME_MONITORING": True,
    }

    # =========================================================================
    # 5. 测试配置
    # =========================================================================

    # 集成测试运行时长（秒）
    TEST_DURATION: int = 30

    # 测试模式
    TEST_MODE: str = "SIMULATION"  # SIMULATION-模拟测试，REAL-实盘测试

    # 测试合约符号
    TEST_SYMBOL: str = DEFAULT_SYMBOL

    # 是否在测试中启用自动下单（谨慎使用！）
    ENABLE_AUTO_TRADING_IN_TEST: bool = False

    # =========================================================================
    # 6. 日志配置
    # =========================================================================

    # 日志级别
    LOG_LEVEL: str = "INFO"  # DEBUG, INFO, WARNING, ERROR, CRITICAL

    # 日志文件路径
    LOG_DIR: str = "logs"
    LOG_FILENAME: str = f"tqsdk_tables_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

    # 是否在控制台显示日志
    LOG_TO_CONSOLE: bool = True

    # 是否记录到文件
    LOG_TO_FILE: bool = True

    # =========================================================================
    # 7. 路径配置
    # =========================================================================

    # 项目根目录
    PROJECT_ROOT: str = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    # 数据存储目录
    DATA_DIR: str = os.path.join(PROJECT_ROOT, "data")

    # 自定义数据表模块路径
    MODULES_DIR: str = os.path.join(PROJECT_ROOT, "my_tqsdk_tables")

    # =========================================================================
    # 8. 性能配置
    # =========================================================================

    # 最大数据缓存大小
    MAX_CACHE_SIZE: int = 10000

    # 数据清理间隔（秒）
    CLEANUP_INTERVAL: int = 300  # 5分钟

    # 心跳检测间隔（秒）
    HEARTBEAT_INTERVAL: int = 30

    # =========================================================================
    # 9. 环境配置（直接在代码中定义）
    # =========================================================================

    # 运行环境（直接在代码中指定）
    TQ_ENV: str = "sim"  # sim-模拟环境, dev-开发环境, prod-生产环境

    # =========================================================================
    # 类方法
    # =========================================================================

    @classmethod
    def validate_config(cls) -> bool:
        """
        验证配置参数的合法性

        Returns:
            bool: 配置是否有效
        """
        try:
            # 验证天勤账户配置
            if cls.TQ_USERNAME in ["您的天勤账号", ""] or cls.TQ_PASSWORD in ["您的密码", ""]:
                print("错误: 请设置正确的天勤账号和密码")
                print("提示: 请修改 config.py 文件中的 TQ_USERNAME 和 TQ_PASSWORD 为您的实际账号密码")
                return False

            # 验证合约配置
            if cls.DEFAULT_SYMBOL not in cls.SYMBOL_MULTIPLIERS:
                print(f"警告: 默认合约 {cls.DEFAULT_SYMBOL} 未配置合约乘数")

            # 验证风险参数
            if cls.RISK_MANAGEMENT["STOP_LOSS_RATIO"] >= 0:
                print("错误: 止损比例应为负值")
                return False

            # 验证环境配置
            if cls.TQ_ENV not in ["sim", "dev", "prod"]:
                print("错误: TQ_ENV 必须是 'sim', 'dev' 或 'prod'")
                return False

            # 创建必要的目录
            os.makedirs(cls.LOG_DIR, exist_ok=True)
            os.makedirs(cls.DATA_DIR, exist_ok=True)

            print("✓ 配置验证通过")
            return True

        except Exception as e:
            print(f"配置验证失败: {e}")
            return False

    @classmethod
    def get_log_filepath(cls) -> str:
        """获取完整的日志文件路径"""
        return os.path.join(cls.LOG_DIR, cls.LOG_FILENAME)

    @classmethod
    def get_symbol_multiplier(cls, symbol: str) -> int:
        """获取指定合约的乘数"""
        return cls.SYMBOL_MULTIPLIERS.get(symbol, 1)

    @classmethod
    def get_test_config_summary(cls) -> Dict[str, Any]:
        """获取测试配置摘要，用于日志记录"""
        return {
            "username": cls.TQ_USERNAME,
            "account_type": cls.TQ_ACCOUNT_TYPE,
            "test_symbol": cls.TEST_SYMBOL,
            "test_duration": cls.TEST_DURATION,
            "stop_loss_ratio": cls.RISK_MANAGEMENT["STOP_LOSS_RATIO"],
            "gateway_name": cls.GATEWAY_NAME,
            "log_level": cls.LOG_LEVEL,
            "environment": cls.TQ_ENV,
        }


# =============================================================================
# 环境特定配置覆盖
# =============================================================================

class DevelopmentConfig(Config):
    """开发环境配置"""
    LOG_LEVEL = "DEBUG"
    ENABLE_DETAILED_LOGGING = True
    TEST_DURATION = 60  # 开发环境测试时间更长
    TQ_ENV = "dev"  # 开发环境


class ProductionConfig(Config):
    """生产环境配置"""
    LOG_LEVEL = "WARNING"
    ENABLE_DETAILED_LOGGING = False
    ENABLE_AUTO_TRADING_IN_TEST = False
    TQ_ACCOUNT_TYPE = "REAL"  # 生产环境使用实盘
    TQ_ENV = "prod"  # 生产环境


class SimulationConfig(Config):
    """模拟环境配置（默认）"""
    TQ_ENV = "sim"  # 模拟环境


# =============================================================================
# 配置选择逻辑
# =============================================================================

def get_config(env: str = None) -> Config:
    """
    根据环境获取配置

    Args:
        env: 环境类型 (dev, prod, sim), 如果为None则使用默认配置

    Returns:
        Config: 配置类实例
    """
    # 如果未指定环境，使用默认配置中的TQ_ENV
    if env is None:
        env = Config.TQ_ENV

    config_map = {
        "dev": DevelopmentConfig,
        "development": DevelopmentConfig,
        "prod": ProductionConfig,
        "production": ProductionConfig,
        "sim": SimulationConfig,
        "simulation": SimulationConfig,
    }

    config_class = config_map.get(env, SimulationConfig)
    print(f"使用配置环境: {env} -> {config_class.__name__}")

    return config_class


# 当前激活的配置
CurrentConfig = get_config()


# =============================================================================
# 安全提醒
# =============================================================================

def security_warning():
    """安全提醒函数"""
    print("!" * 60)
    print("安全提醒: 您正在使用硬编码的账号密码")
    print("!" * 60)
    print("建议:")
    print("1. 不要在版本控制系统中提交包含真实密码的代码")
    print("2. 考虑使用环境变量或配置文件来管理敏感信息")
    print("3. 定期更换密码")
    print("!" * 60)


# =============================================================================
# 主程序验证
# =============================================================================
if __name__ == "__main__":
    """配置模块自验证"""
    print("=" * 60)
    print("天勤SDK自定义数据表项目 - 配置验证")
    print("=" * 60)

    # 显示安全提醒
    security_warning()

    # 验证配置
    if CurrentConfig.validate_config():
        print("✓ 配置验证成功")

        # 打印配置摘要
        summary = CurrentConfig.get_test_config_summary()
        print("\n配置摘要:")
        for key, value in summary.items():
            if "password" not in key.lower():
                # 对密码进行部分隐藏显示
                if "username" in key.lower():
                    display_value = f"{value[:3]}****{value[-2:]}" if len(value) > 5 else "****"
                else:
                    display_value = value
                print(f"  {key}: {display_value}")
    else:
        print("✗ 配置验证失败，请检查配置")
        sys.exit(1)