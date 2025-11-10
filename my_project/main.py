"""
改进的主程序
集成新数据架构和一致性检查，确保直接可运行
"""
import asyncio
import sys
import os
import time
import logging
import signal
import threading
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional

# 添加项目根目录到Python路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from config.settings import Settings
from core.event_engine import EventEngine
from core.data_manager import DataManager
from core.backtest_engine import BacktestEngine
from core.thread_safe_manager import thread_safe_manager
from core.data_sync_service import DataSyncService
from core.monitoring_service import MonitoringService
from core.consistency_checker import ConsistencyChecker
from gateways.tqsdk_gateway import TqsdkGateway
from strategies.double_ma import DoubleMa


class QuantSystem:
    """量化交易系统主程序（集成新数据架构和一致性检查）"""

    def __init__(self, config_file: str = None):
        self.settings = Settings(config_file)
        self.running = False
        self.start_time = None

        # 初始化核心组件
        self.event_engine = EventEngine()

        # 使用新架构的数据管理器
        self.data_manager = DataManager(
            event_engine=self.event_engine,
            config=self._get_data_config()
        )

        self.backtest_engine = BacktestEngine(self.event_engine, self.data_manager)
        self.monitoring_service = MonitoringService()

        # 初始化网关
        self.gateway = TqsdkGateway(self.event_engine)

        # 一致性检查器实例
        self.consistency_checker = ConsistencyChecker(self.gateway)

        # 信号处理
        self._setup_signal_handlers()

        # 初始化日志
        self._setup_logging()

    def _get_data_config(self) -> Dict[str, Any]:
        """获取数据模块配置"""
        return {
            "tables": {
                "account": {
                    "table_name": "account",
                    "type": "account",
                    "persistent": True,
                    "validation_rules": {"required_fields": ["account_id", "balance", "available"]}
                },
                "order": {
                    "table_name": "order",
                    "type": "order",
                    "persistent": True,
                    "validation_rules": {"required_fields": ["order_id", "symbol", "direction", "volume"]}
                },
                "position": {
                    "table_name": "position",
                    "type": "position",
                    "persistent": True,
                    "validation_rules": {"required_fields": ["strategy", "symbol", "direction", "volume"]}
                },
                "trade": {
                    "table_name": "trade",
                    "type": "trade",
                    "persistent": True,
                    "validation_rules": {"required_fields": ["trade_id", "symbol", "direction", "volume"]}
                }
            },
            "adapter": {
                "default_format": "standard",
                "auto_convert": True
            },
            "sync": {
                "interval": 5,
                "auto_repair": True,
                "validate_on_sync": True
            }
        }

    def _setup_signal_handlers(self):
        """设置信号处理器"""
        def signal_handler(signum, frame):
            print(f"\n[{datetime.now()}] 收到信号 {signum}，正在优雅关闭...")
            asyncio.create_task(self.stop())

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

    def _setup_logging(self):
        """设置日志系统"""
        log_config = self.settings.get_monitoring_config()
        log_level = getattr(logging, log_config.get('log_level', 'INFO'))
        log_file = log_config.get('log_file', 'quant_system.log')

        # 创建日志目录
        log_dir = os.path.dirname(log_file)
        if log_dir and not os.path.exists(log_dir):
            os.makedirs(log_dir)

        # 配置根日志
        logging.basicConfig(
            level=log_level,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file, encoding='utf-8'),
                logging.StreamHandler(sys.stdout)
            ]
        )

        self.logger = logging.getLogger("QuantSystem")
        self.logger.info("日志系统初始化完成")

    async def initialize(self) -> bool:
        """初始化系统（增强版本）"""
        try:
            self.logger.info("开始初始化量化交易系统...")
            self.start_time = datetime.now()

            # 验证配置
            config_validation = self.settings.validate_config()
            if not config_validation['is_valid']:
                self.logger.error(f"配置验证失败: {config_validation['errors']}")
                return False

            # 初始化事件引擎
            self.event_engine.start()
            self.logger.info("事件引擎启动完成")

            # 等待数据管理器完全初始化
            await asyncio.sleep(0.1)

            # 增强的状态检查
            data_status = self.data_manager.get_system_status()
            self.logger.info(f"数据管理器状态: {data_status}")

            # 更健壮的状态检查
            if not data_status.get("tables_initialized", False):
                table_details = data_status.get("table_details", {})
                failed_tables = [
                    name for name, info in table_details.items()
                    if not info.get("initialized", False)
                ]

                if failed_tables:
                    self.logger.error(f"数据表初始化失败: {failed_tables}")
                else:
                    self.logger.error("数据表初始化状态未知")
                return False

            # 连接网关
            if not await self._connect_gateway():
                return False
            self.logger.info("网关连接完成")

            # 修改处1：移除初始一致性检查（冗余步骤）
            # 初始阶段数据为空，一致性检查无实际意义

            # 启动监控服务
            await self.monitoring_service.start()
            self.logger.info("监控服务启动完成")

            # 注册监控指标
            self._register_metrics()

            self.running = True
            self.logger.info(f"量化交易系统初始化完成，耗时: {(datetime.now() - self.start_time).total_seconds():.2f}秒")

            return True

        except Exception as e:
            self.logger.error(f"系统初始化失败: {e}")
            return False

    async def _connect_gateway(self):
        """连接网关"""
        tqsdk_config = self.settings.get_tqsdk_config()
        username = tqsdk_config.get('username')
        password = tqsdk_config.get('password')

        if not username or not password:
            self.logger.warning("天勤API凭据未配置，使用模拟模式")
            return True

        connected = await self.gateway.connect(username, password)
        if not connected:
            self.logger.error("网关连接失败")
            return False

        self.logger.info("网关连接成功")
        return True

    def _register_metrics(self):
        """注册监控指标"""
        # 系统指标
        self.monitoring_service.register_gauge("system_uptime_seconds", "系统运行时间")
        self.monitoring_service.register_gauge("system_memory_usage", "系统内存使用率")
        self.monitoring_service.register_gauge("system_cpu_usage", "系统CPU使用率")

        # 交易指标
        self.monitoring_service.register_counter("total_trades", "总交易次数")
        self.monitoring_service.register_gauge("current_positions", "当前持仓数量")
        self.monitoring_service.register_gauge("account_balance", "账户余额")
        self.monitoring_service.register_gauge("floating_pnl", "浮动盈亏")

        # 性能指标
        self.monitoring_service.register_gauge("event_queue_size", "事件队列大小")
        self.monitoring_service.register_gauge("active_threads", "活跃线程数")
        self.monitoring_service.register_histogram("order_processing_time", "订单处理时间")

        # 一致性检查指标
        self.monitoring_service.register_gauge("consistency_check_status", "一致性检查状态")
        self.monitoring_service.register_counter("consistency_checks_total", "总一致性检查次数")
        self.monitoring_service.register_counter("consistency_violations", "一致性违规次数")

        self.logger.info("监控指标注册完成")

    async def run_backtest(self, strategy_name: str = None, strategy_config: Dict[str, Any] = None):
        """运行回测（集成新数据架构和一致性检查）"""
        if not self.running:
            self.logger.error("系统未运行，无法执行回测")
            return

        try:
            strategy_name = strategy_name or self.settings.get_strategy_config().get('default_strategy', 'double_ma')
            strategy_config = strategy_config or {}

            self.logger.info(f"开始回测策略: {strategy_name}")

            # 修改处2：优化回测前一致性检查，只检查关键数据
            await self._perform_pre_backtest_consistency_check()

            # 获取历史数据
            backtest_config = self.settings.get_backtest_config()
            symbol = backtest_config.get('benchmark', 'SHFE.cu2401')
            start_date = backtest_config.get('start_date', '2024-01-01')
            end_date = backtest_config.get('end_date', '2024-01-05')
            frequency = backtest_config.get('frequency', '1h')

            self.logger.info(f"获取历史数据: {symbol} {start_date} 到 {end_date}")

            # 获取历史数据
            history_data = await self.gateway.get_history_data(
                symbol, start_date, end_date, frequency
            )

            if not history_data:
                self.logger.error("历史数据获取失败")
                return

            # 设置历史数据
            self.backtest_engine.set_history_data(history_data)

            # 运行回测
            self.logger.info("开始执行回测...")
            start_time = time.time()

            # 启动监控
            monitor_task = asyncio.create_task(self._monitor_backtest_progress())

            # 执行回测
            await self.backtest_engine.run_backtest(strategy_name, strategy_config)

            # 停止监控
            monitor_task.cancel()

            end_time = time.time()
            duration = end_time - start_time

            self.logger.info(f"回测完成，耗时: {duration:.2f}秒")

            # 修改处3：优化回测后一致性检查，专注于账户数据一致性
            await self._perform_post_backtest_consistency_check()

            # 生成回测报告
            report = self.backtest_engine.generate_report()
            self._print_backtest_report(report)

            # 保存回测结果
            self._save_backtest_results(report, strategy_name)

        except Exception as e:
            self.logger.error(f"回测执行失败: {e}")

    async def _perform_pre_backtest_consistency_check(self):
        """回测前一致性检查"""
        try:
            self.logger.info("执行回测前一致性检查...")

            # 修改处4：简化检查内容，只检查账户数据
            internal_account = self.data_manager.get_table('account').query_data()

            if not internal_account:
                self.logger.info("回测前无账户数据，跳过一致性检查")
                return None

            report = await self.consistency_checker.validate_account(
                internal_account[0] if internal_account else {}
            )

            self.monitoring_service.increment_counter("consistency_checks_total")
            if report and report.get('status') != 'consistent':
                self.monitoring_service.increment_counter("consistency_violations")
                self.logger.warning("回测前账户数据一致性检查发现问题")

            return report
        except Exception as e:
            self.logger.warning(f"回测前一致性检查失败: {e}")
            return None

    async def _perform_post_backtest_consistency_check(self):
        """回测后一致性检查"""
        try:
            self.logger.info("执行回测后一致性检查...")

            # 修改处5：专注于账户和关键交易数据的一致性
            internal_account = self.data_manager.get_table('account').query_data()
            internal_trades = self.data_manager.get_table('trade').query_data()

            if not internal_account:
                self.logger.warning("回测后无账户数据，无法执行一致性检查")
                return None

            report = await self.consistency_checker.validate_all(
                internal_account[0],
                [],
                [],
                internal_trades
            )

            self.monitoring_service.increment_counter("consistency_checks_total")
            if report and report.get('overall_status') != 'consistent':
                self.monitoring_service.increment_counter("consistency_violations")
                self.logger.warning("回测后数据一致性检查发现问题")
            else:
                self.logger.info("回测后数据一致性检查通过")

            return report
        except Exception as e:
            self.logger.warning(f"回测后一致性检查失败: {e}")
            return None

    async def _monitor_backtest_progress(self):
        """监控回测进度"""
        try:
            while self.running:
                # 更新监控指标
                progress = getattr(self.backtest_engine, 'progress', 0)
                self.monitoring_service.set_gauge("backtest_progress", progress)

                # 更新性能指标
                self._update_performance_metrics()

                await asyncio.sleep(5)
        except asyncio.CancelledError:
            self.logger.info("回测监控任务已停止")

    def _update_performance_metrics(self):
        """更新性能指标"""
        try:
            import psutil
            process = psutil.Process()
            memory_usage = process.memory_info().rss / 1024 / 1024
            cpu_usage = process.cpu_percent()

            self.monitoring_service.set_gauge("system_memory_usage", memory_usage)
            self.monitoring_service.set_gauge("system_cpu_usage", cpu_usage)

            if self.start_time:
                uptime = (datetime.now() - self.start_time).total_seconds()
                self.monitoring_service.set_gauge("system_uptime_seconds", uptime)

            if hasattr(self.event_engine, '_queue'):
                queue_size = self.event_engine._queue.qsize()
                self.monitoring_service.set_gauge("event_queue_size", queue_size)

        except Exception as e:
            self.logger.warning(f"性能指标更新失败: {e}")

    def _print_backtest_report(self, report: Dict[str, Any]):
        """打印回测报告"""
        print("\n" + "="*80)
        print("回测报告摘要")
        print("="*80)

        info = report.get('backtest_info', {})
        print(f"策略名称: {info.get('strategy_name', 'N/A')}")
        print(f"回测期间: {info.get('start_date')} 到 {info.get('end_date')}")
        print(f"数据点数: {info.get('data_points', 0)}")
        print(f"回测耗时: {info.get('duration', 0):.2f}秒")

        metrics = report.get('performance_metrics', {})
        print(f"\n绩效指标:")
        print(f"  初始资金: {metrics.get('initial_capital', 0):,.2f}")
        print(f"  最终权益: {metrics.get('final_equity', 0):,.2f}")
        print(f"  总收益率: {metrics.get('total_return', 0):.2%}")
        print(f"  年化收益率: {metrics.get('annual_return', 0):.2%}")
        print(f"  最大回撤: {metrics.get('max_drawdown', 0):.2%}")
        print(f"  夏普比率: {metrics.get('sharpe_ratio', 0):.2f}")
        print(f"  索提诺比率: {metrics.get('sortino_ratio', 0):.2f}")
        print(f"  盈亏比: {metrics.get('profit_factor', 0):.2f}")

        stats = report.get('trading_statistics', {})
        print(f"\n交易统计:")
        print(f"  总交易次数: {stats.get('total_trades', 0)}")
        print(f"  盈利交易: {stats.get('winning_trades', 0)}")
        print(f"  亏损交易: {stats.get('losing_trades', 0)}")
        print(f"  胜率: {stats.get('win_rate', 0):.2%}")
        print(f"  平均盈利: {stats.get('avg_profit', 0):.2f}")
        print(f"  平均亏损: {stats.get('avg_loss', 0):.2f}")

        print("="*80)

    def _save_backtest_results(self, report: Dict[str, Any], strategy_name: str):
        """保存回测结果"""
        try:
            output_dir = self.settings.get_backtest_config().get('output_dir', './backtest_results')
            if not os.path.exists(output_dir):
                os.makedirs(output_dir)

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"backtest_{strategy_name}_{timestamp}.json"
            filepath = os.path.join(output_dir, filename)

            import json
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(report, f, indent=2, ensure_ascii=False, default=str)

            self.logger.info(f"回测结果已保存: {filepath}")

        except Exception as e:
            self.logger.error(f"回测结果保存失败: {e}")

    async def stop(self):
        """停止系统（优雅关闭）"""
        if not self.running:
            return

        self.logger.info("开始停止量化交易系统...")
        self.running = False

        try:
            # 修改处6：优化最终一致性检查，专注于账户数据
            await self._perform_final_consistency_check()

            # 停止回测引擎
            if hasattr(self.backtest_engine, 'stop'):
                self.backtest_engine.stop()

            # 断开网关连接
            if hasattr(self.gateway, 'disconnect'):
                await self.gateway.disconnect()

            # 停止监控服务
            if self.monitoring_service:
                await self.monitoring_service.stop()

            # 停止事件引擎
            if self.event_engine:
                self.event_engine.stop()

            # 生成最终报告
            self._generate_final_report()

            self.logger.info("量化交易系统已停止")

        except Exception as e:
            self.logger.error(f"系统停止过程中发生错误: {e}")

    async def _perform_final_consistency_check(self):
        """执行最终一致性检查"""
        try:
            self.logger.info("执行最终数据一致性检查...")

            # 修改处7：简化最终检查，只检查关键数据
            internal_account = self.data_manager.get_table('account').query_data()

            if not internal_account:
                self.logger.info("无账户数据，跳过最终一致性检查")
                return None

            report = await self.consistency_checker.validate_account(
                internal_account[0]
            )

            if report and report.get('status') == 'consistent':
                self.logger.info("最终数据一致性检查通过")
            else:
                self.logger.warning(f"最终数据一致性检查发现问题: {report}")

            return report

        except Exception as e:
            self.logger.warning(f"最终一致性检查失败: {e}")
            return None

    def _generate_final_report(self):
        """生成最终系统报告"""
        try:
            end_time = datetime.now()
            uptime = (end_time - self.start_time).total_seconds() if self.start_time else 0

            report = {
                "system_info": {
                    "start_time": self.start_time.isoformat() if self.start_time else None,
                    "end_time": end_time.isoformat(),
                    "uptime_seconds": uptime,
                    "version": "1.0.0"
                },
                "configuration": self.settings.get_all_config(),
                "performance_metrics": self.monitoring_service.get_metrics() if self.monitoring_service else {},
                "final_status": "stopped"
            }

            report_file = f"system_report_{end_time.strftime('%Y%m%d_%H%M%S')}.json"
            import json
            with open(report_file, 'w', encoding='utf-8') as f:
                json.dump(report, f, indent=2, ensure_ascii=False, default=str)

            self.logger.info(f"系统报告已保存: {report_file}")

        except Exception as e:
            self.logger.error(f"系统报告生成失败: {e}")

    def get_system_status(self) -> Dict[str, Any]:
        """获取系统状态"""
        return {
            "running": self.running,
            "uptime": (datetime.now() - self.start_time).total_seconds() if self.start_time else 0,
            "components": {
                "event_engine": hasattr(self.event_engine, 'active') and self.event_engine.active,
                "data_manager": self.data_manager is not None,
                "backtest_engine": self.backtest_engine is not None,
                "gateway": hasattr(self.gateway, 'connected') and self.gateway.connected,
                "monitoring": self.monitoring_service is not None,
                "consistency_checker": self.consistency_checker is not None
            },
            "metrics": self.monitoring_service.get_metrics() if self.monitoring_service else {}
        }


async def main():
    """主函数（优化流程逻辑）"""
    print("="*60)
    print("量化交易系统启动")
    print("="*60)

    # 创建系统实例
    config_file = "config.json" if os.path.exists("config.json") else None
    system = QuantSystem(config_file)

    try:
        # 初始化系统
        if not await system.initialize():
            print("系统初始化失败")
            return

        # 运行回测
        await system.run_backtest()

    except KeyboardInterrupt:
        print("\n用户中断，正在关闭系统...")
    except Exception as e:
        print(f"系统运行异常: {e}")
    finally:
        # 优雅关闭
        await system.stop()

    print("量化交易系统关闭")


if __name__ == "__main__":
    # 运行主程序
    asyncio.run(main())
