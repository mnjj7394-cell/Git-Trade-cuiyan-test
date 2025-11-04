"""
监控服务
第四阶段：系统监控、性能指标和健康检查
"""
import asyncio
import time
import psutil
import threading
from typing import Dict, Any, List, Optional, Callable
from datetime import datetime, timedelta
from collections import defaultdict, deque
from dataclasses import dataclass
from enum import Enum
from core.thread_safe_manager import thread_safe_manager


class MetricType(Enum):
    """指标类型枚举"""
    COUNTER = "counter"  # 计数器，只增不减
    GAUGE = "gauge"  # 仪表盘，可增可减
    HISTOGRAM = "histogram"  # 直方图，统计分布
    SUMMARY = "summary"  # 摘要，分位数统计


@dataclass
class Metric:
    """监控指标"""
    name: str
    type: MetricType
    description: str
    value: float = 0.0
    labels: Dict[str, str] = None
    timestamp: datetime = None
    history: deque = None
    max_history: int = 1000


class AlertLevel(Enum):
    """告警级别"""
    INFO = "info"  # 信息
    WARNING = "warning"  # 警告
    ERROR = "error"  # 错误
    CRITICAL = "critical"  # 严重


@dataclass
class Alert:
    """告警信息"""
    level: AlertLevel
    message: str
    component: str
    timestamp: datetime
    details: Dict[str, Any] = None
    resolved: bool = False


class MonitoringService:
    """监控服务（系统监控、性能指标和健康检查）"""

    def __init__(self):
        self.metrics: Dict[str, Metric] = {}
        self.alerts: List[Alert] = []
        self.health_checks: Dict[str, Callable] = {}
        self.running = False
        self.monitor_thread = None
        self.lock = threading.RLock()
        self.start_time = datetime.now()

        # 性能数据缓冲区
        self.performance_data = {
            "cpu_usage": deque(maxlen=300),  # 5分钟数据（1秒间隔）
            "memory_usage": deque(maxlen=300),
            "disk_io": deque(maxlen=300),
            "network_io": deque(maxlen=300)
        }

        # 初始化默认指标
        self._setup_default_metrics()

    def _setup_default_metrics(self):
        """设置默认监控指标"""
        # 系统指标
        self.register_gauge("system_uptime_seconds", "系统运行时间")
        self.register_gauge("system_cpu_usage_percent", "系统CPU使用率")
        self.register_gauge("system_memory_usage_mb", "系统内存使用量(MB)")
        self.register_gauge("system_disk_usage_percent", "系统磁盘使用率")

        # 应用指标
        self.register_counter("total_requests", "总请求数")
        self.register_counter("total_errors", "总错误数")
        self.register_gauge("active_connections", "活跃连接数")
        self.register_histogram("request_duration_seconds", "请求处理时间")

        # 业务指标
        self.register_counter("total_trades", "总交易次数")
        self.register_gauge("current_positions", "当前持仓数")
        self.register_gauge("account_balance", "账户余额")

    def register_metric(self, name: str, metric_type: MetricType,
                        description: str = "", labels: Dict[str, str] = None,
                        max_history: int = 1000) -> bool:
        """注册监控指标（线程安全）"""
        with thread_safe_manager.locked_resource("metric_registration"):
            if name in self.metrics:
                return False

            metric = Metric(
                name=name,
                type=metric_type,
                description=description,
                labels=labels or {},
                timestamp=datetime.now(),
                history=deque(maxlen=max_history),
                max_history=max_history
            )

            self.metrics[name] = metric
            return True

    def register_counter(self, name: str, description: str = "",
                         labels: Dict[str, str] = None) -> bool:
        """注册计数器指标"""
        return self.register_metric(name, MetricType.COUNTER, description, labels)

    def register_gauge(self, name: str, description: str = "",
                       labels: Dict[str, str] = None) -> bool:
        """注册仪表盘指标"""
        return self.register_metric(name, MetricType.GAUGE, description, labels)

    def register_histogram(self, name: str, description: str = "",
                           labels: Dict[str, str] = None) -> bool:
        """注册直方图指标"""
        return self.register_metric(name, MetricType.HISTOGRAM, description, labels)

    def register_summary(self, name: str, description: str = "",
                         labels: Dict[str, str] = None) -> bool:
        """注册摘要指标"""
        return self.register_metric(name, MetricType.SUMMARY, description, labels)

    def increment_counter(self, name: str, value: float = 1.0,
                          labels: Dict[str, str] = None) -> bool:
        """增加计数器值（线程安全）"""
        with thread_safe_manager.locked_resource("counter_increment"):
            if name not in self.metrics or self.metrics[name].type != MetricType.COUNTER:
                return False

            metric = self.metrics[name]
            metric.value += value
            metric.timestamp = datetime.now()
            self._record_history(metric, metric.value)
            return True

    def set_gauge(self, name: str, value: float, labels: Dict[str, str] = None) -> bool:
        """设置仪表盘值（线程安全）"""
        with thread_safe_manager.locked_resource("gauge_set"):
            if name not in self.metrics or self.metrics[name].type != MetricType.GAUGE:
                return False

            metric = self.metrics[name]
            metric.value = value
            metric.timestamp = datetime.now()
            self._record_history(metric, value)
            return True

    def observe_histogram(self, name: str, value: float, labels: Dict[str, str] = None) -> bool:
        """观察直方图值（线程安全）"""
        with thread_safe_manager.locked_resource("histogram_observe"):
            if name not in self.metrics or self.metrics[name].type != MetricType.HISTOGRAM:
                return False

            metric = self.metrics[name]
            metric.value = value  # 最新值
            metric.timestamp = datetime.now()
            self._record_history(metric, value)
            return True

    def _record_history(self, metric: Metric, value: float):
        """记录指标历史数据"""
        history_point = {
            "timestamp": datetime.now(),
            "value": value,
            "labels": metric.labels.copy() if metric.labels else {}
        }
        metric.history.append(history_point)

    def add_health_check(self, name: str, check_function: Callable) -> bool:
        """添加健康检查（线程安全）"""
        with thread_safe_manager.locked_resource("health_check_addition"):
            if name in self.health_checks:
                return False

            self.health_checks[name] = check_function
            return True

    async def perform_health_checks(self) -> Dict[str, Dict[str, Any]]:
        """执行健康检查（线程安全）"""
        with thread_safe_manager.locked_resource("health_check_execution"):
            results = {}

            for name, check_func in self.health_checks.items():
                try:
                    # 支持异步和同步健康检查函数
                    if asyncio.iscoroutinefunction(check_func):
                        result = await check_func()
                    else:
                        result = check_func()

                    results[name] = {
                        "status": "healthy" if result else "unhealthy",
                        "timestamp": datetime.now(),
                        "details": result if isinstance(result, dict) else {"result": result}
                    }

                    # 如果不健康，生成告警
                    if not result:
                        self.raise_alert(
                            AlertLevel.WARNING,
                            f"健康检查失败: {name}",
                            "health_check",
                            {"check_name": name, "result": result}
                        )

                except Exception as e:
                    results[name] = {
                        "status": "error",
                        "timestamp": datetime.now(),
                        "error": str(e)
                    }

                    self.raise_alert(
                        AlertLevel.ERROR,
                        f"健康检查异常: {name} - {e}",
                        "health_check",
                        {"check_name": name, "error": str(e)}
                    )

            return results

    def raise_alert(self, level: AlertLevel, message: str, component: str,
                    details: Dict[str, Any] = None) -> str:
        """触发告警（线程安全）"""
        with thread_safe_manager.locked_resource("alert_raising"):
            alert_id = f"ALERT_{int(datetime.now().timestamp() * 1000)}"

            alert = Alert(
                level=level,
                message=message,
                component=component,
                timestamp=datetime.now(),
                details=details or {},
                resolved=False
            )

            self.alerts.append(alert)

            # 记录告警指标
            self.increment_counter("total_alerts", 1.0, {"level": level.value})

            # 根据级别记录不同计数器
            if level == AlertLevel.CRITICAL:
                self.increment_counter("critical_alerts", 1.0)
            elif level == AlertLevel.ERROR:
                self.increment_counter("error_alerts", 1.0)
            elif level == AlertLevel.WARNING:
                self.increment_counter("warning_alerts", 1.0)
            else:
                self.increment_counter("info_alerts", 1.0)

            return alert_id

    def resolve_alert(self, alert_id: str = None, component: str = None) -> bool:
        """解决告警（线程安全）"""
        with thread_safe_manager.locked_resource("alert_resolution"):
            if alert_id:
                # 通过ID解决特定告警
                for alert in self.alerts:
                    if not alert.resolved and alert.timestamp.strftime("ALERT_%Y%m%d_%H%M%S_%f") == alert_id:
                        alert.resolved = True
                        alert.resolved_time = datetime.now()
                        return True
            elif component:
                # 解决特定组件的所有告警
                resolved_count = 0
                for alert in self.alerts:
                    if not alert.resolved and alert.component == component:
                        alert.resolved = True
                        alert.resolved_time = datetime.now()
                        resolved_count += 1
                return resolved_count > 0

            return False

    def get_metrics(self, name: str = None) -> Dict[str, Any]:
        """获取指标数据（线程安全）"""
        with thread_safe_manager.locked_resource("metrics_retrieval"):
            if name:
                metric = self.metrics.get(name)
                if not metric:
                    return {}

                return {
                    "name": metric.name,
                    "type": metric.type.value,
                    "value": metric.value,
                    "description": metric.description,
                    "timestamp": metric.timestamp,
                    "history_size": len(metric.history),
                    "labels": metric.labels
                }
            else:
                return {name: self.get_metrics(name) for name in self.metrics}

    def get_metric_history(self, name: str, hours: int = 24) -> List[Dict[str, Any]]:
        """获取指标历史数据（线程安全）"""
        with thread_safe_manager.locked_resource("metric_history_retrieval"):
            if name not in self.metrics:
                return []

            metric = self.metrics[name]
            cutoff_time = datetime.now() - timedelta(hours=hours)

            return [
                point for point in metric.history
                if point["timestamp"] >= cutoff_time
            ]

    def get_alerts(self, resolved: bool = None, level: AlertLevel = None,
                   component: str = None) -> List[Alert]:
        """获取告警列表（线程安全）"""
        with thread_safe_manager.locked_resource("alerts_retrieval"):
            filtered_alerts = self.alerts.copy()

            if resolved is not None:
                filtered_alerts = [a for a in filtered_alerts if a.resolved == resolved]

            if level is not None:
                filtered_alerts = [a for a in filtered_alerts if a.level == level]

            if component is not None:
                filtered_alerts = [a for a in filtered_alerts if a.component == component]

            return filtered_alerts

    def get_system_stats(self) -> Dict[str, Any]:
        """获取系统统计信息（线程安全）"""
        with thread_safe_manager.locked_resource("system_stats_retrieval"):
            try:
                # CPU使用率
                cpu_percent = psutil.cpu_percent(interval=1)

                # 内存使用
                memory = psutil.virtual_memory()
                memory_used_mb = memory.used / 1024 / 1024
                memory_percent = memory.percent

                # 磁盘使用
                disk = psutil.disk_usage('/')
                disk_percent = disk.percent

                # 网络IO
                net_io = psutil.net_io_counters()
                network_stats = {
                    "bytes_sent": net_io.bytes_sent,
                    "bytes_recv": net_io.bytes_recv,
                    "packets_sent": net_io.packets_sent,
                    "packets_recv": net_io.packets_recv
                }

                # 系统运行时间
                uptime_seconds = (datetime.now() - self.start_time).total_seconds()

                return {
                    "timestamp": datetime.now(),
                    "cpu": {
                        "percent": cpu_percent,
                        "cores": psutil.cpu_count()
                    },
                    "memory": {
                        "used_mb": memory_used_mb,
                        "percent": memory_percent,
                        "total_mb": memory.total / 1024 / 1024
                    },
                    "disk": {
                        "used_percent": disk_percent,
                        "free_gb": disk.free / 1024 / 1024 / 1024
                    },
                    "network": network_stats,
                    "system_uptime_seconds": uptime_seconds,
                    "process_uptime_seconds": uptime_seconds
                }

            except Exception as e:
                self.raise_alert(
                    AlertLevel.ERROR,
                    f"系统统计获取失败: {e}",
                    "monitoring"
                )
                return {}

    async def start(self):
        """启动监控服务"""
        if self.running:
            return

        self.running = True
        self.monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self.monitor_thread.start()

        # 添加系统健康检查
        self.add_health_check("system_resources", self._check_system_resources)
        self.add_health_check("monitoring_service", self._check_monitoring_service)

    async def stop(self):
        """停止监控服务"""
        self.running = False
        if self.monitor_thread and self.monitor_thread.is_alive():
            self.monitor_thread.join(timeout=5.0)

    def _monitor_loop(self):
        """监控循环"""
        import asyncio

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        try:
            while self.running:
                # 更新系统指标
                self._update_system_metrics()

                # 执行健康检查
                loop.run_until_complete(self.perform_health_checks())

                # 清理旧数据
                self._cleanup_old_data()

                time.sleep(5)  # 5秒间隔
        except Exception as e:
            print(f"监控循环异常: {e}")
        finally:
            loop.close()

    def _update_system_metrics(self):
        """更新系统指标"""
        try:
            stats = self.get_system_stats()
            if not stats:
                return

            # 更新系统指标
            self.set_gauge("system_uptime_seconds", stats["system_uptime_seconds"])
            self.set_gauge("system_cpu_usage_percent", stats["cpu"]["percent"])
            self.set_gauge("system_memory_usage_mb", stats["memory"]["used_mb"])
            self.set_gauge("system_disk_usage_percent", stats["disk"]["used_percent"])

            # 记录性能数据
            self.performance_data["cpu_usage"].append(stats["cpu"]["percent"])
            self.performance_data["memory_usage"].append(stats["memory"]["used_mb"])

        except Exception as e:
            self.raise_alert(
                AlertLevel.ERROR,
                f"系统指标更新失败: {e}",
                "monitoring"
            )

    def _check_system_resources(self) -> bool:
        """检查系统资源健康状态"""
        try:
            stats = self.get_system_stats()
            if not stats:
                return False

            # CPU使用率检查
            if stats["cpu"]["percent"] > 90:
                self.raise_alert(
                    AlertLevel.WARNING,
                    f"CPU使用率过高: {stats['cpu']['percent']}%",
                    "system_resources",
                    {"cpu_percent": stats["cpu"]["percent"]}
                )
                return False

            # 内存使用率检查
            if stats["memory"]["percent"] > 90:
                self.raise_alert(
                    AlertLevel.WARNING,
                    f"内存使用率过高: {stats['memory']['percent']}%",
                    "system_resources",
                    {"memory_percent": stats["memory"]["percent"]}
                )
                return False

            # 磁盘使用率检查
            if stats["disk"]["used_percent"] > 95:
                self.raise_alert(
                    AlertLevel.CRITICAL,
                    f"磁盘空间不足: {stats['disk']['used_percent']}%",
                    "system_resources",
                    {"disk_percent": stats["disk"]["used_percent"]}
                )
                return False

            return True

        except Exception as e:
            self.raise_alert(
                AlertLevel.ERROR,
                f"系统资源检查失败: {e}",
                "health_check"
            )
            return False

    def _check_monitoring_service(self) -> bool:
        """检查监控服务自身健康状态"""
        try:
            # 检查指标数量
            if len(self.metrics) == 0:
                self.raise_alert(
                    AlertLevel.WARNING,
                    "监控服务指标数量为0",
                    "monitoring_service"
                )
                return False

            # 检查最近数据更新时间
            recent_metric = max(
                (metric.timestamp for metric in self.metrics.values()
                 if metric.timestamp is not None),
                default=None
            )

            if recent_metric and (datetime.now() - recent_metric).total_seconds() > 60:
                self.raise_alert(
                    AlertLevel.WARNING,
                    "监控指标数据更新延迟",
                    "monitoring_service",
                    {"delay_seconds": (datetime.now() - recent_metric).total_seconds()}
                )
                return False

            return True

        except Exception as e:
            # 监控服务自身异常，记录但不触发告警循环
            print(f"监控服务自检异常: {e}")
            return False

    def _cleanup_old_data(self):
        """清理旧数据"""
        try:
            # 清理过期告警（保留7天）
            cutoff_time = datetime.now() - timedelta(days=7)
            self.alerts = [
                alert for alert in self.alerts
                if alert.timestamp > cutoff_time or not alert.resolved
            ]

            # 清理性能数据（保留1小时）
            for data_queue in self.performance_data.values():
                if len(data_queue) > 3600:  # 1小时数据
                    while len(data_queue) > 300:  # 保留5分钟
                        data_queue.popleft()

        except Exception as e:
            self.raise_alert(
                AlertLevel.WARNING,
                f"数据清理失败: {e}",
                "monitoring"
            )

    def get_performance_report(self) -> Dict[str, Any]:
        """获取性能报告"""
        with thread_safe_manager.locked_resource("performance_report"):
            try:
                stats = self.get_system_stats()
                health_results = asyncio.run(self.perform_health_checks())

                report = {
                    "timestamp": datetime.now(),
                    "system_stats": stats,
                    "health_checks": health_results,
                    "metrics_summary": {
                        "total_metrics": len(self.metrics),
                        "metric_types": {
                            "counters": len([m for m in self.metrics.values()
                                             if m.type == MetricType.COUNTER]),
                            "gauges": len([m for m in self.metrics.values()
                                           if m.type == MetricType.GAUGE]),
                            "histograms": len([m for m in self.metrics.values()
                                               if m.type == MetricType.HISTOGRAM]),
                            "summaries": len([m for m in self.metrics.values()
                                              if m.type == MetricType.SUMMARY])
                        }
                    },
                    "alerts_summary": {
                        "total_alerts": len(self.alerts),
                        "active_alerts": len([a for a in self.alerts if not a.resolved]),
                        "by_level": {
                            level.value: len([a for a in self.alerts
                                              if a.level == level and not a.resolved])
                            for level in AlertLevel
                        }
                    },
                    "performance_data": {
                        name: list(data) for name, data in self.performance_data.items()
                    }
                }

                return report

            except Exception as e:
                self.raise_alert(
                    AlertLevel.ERROR,
                    f"性能报告生成失败: {e}",
                    "monitoring"
                )
                return {}


# 测试代码
if __name__ == "__main__":
    import asyncio


    async def test_monitoring_service():
        # 创建监控服务实例
        monitor = MonitoringService()

        # 启动监控服务
        await monitor.start()

        # 测试指标操作
        monitor.register_counter("test_counter", "测试计数器")
        monitor.register_gauge("test_gauge", "测试仪表盘")

        monitor.increment_counter("test_counter", 5.0)
        monitor.set_gauge("test_gauge", 42.0)

        # 测试健康检查
        def dummy_health_check():
            return {"status": "healthy", "message": "测试通过"}

        monitor.add_health_check("dummy_check", dummy_health_check)

        # 执行健康检查
        health_results = await monitor.perform_health_checks()
        print("健康检查结果:", health_results)

        # 测试告警
        monitor.raise_alert(AlertLevel.WARNING, "测试告警", "test_component")

        # 获取指标数据
        metrics = monitor.get_metrics()
        print("指标数据:", list(metrics.keys()))

        # 获取系统统计
        stats = monitor.get_system_stats()
        print("系统统计:", stats.keys())

        # 等待一段时间收集数据
        await asyncio.sleep(2)

        # 获取性能报告
        report = monitor.get_performance_report()
        print("性能报告生成完成")

        # 停止监控服务
        await monitor.stop()

        print("监控服务测试完成")


    # 运行测试
    asyncio.run(test_monitoring_service())
