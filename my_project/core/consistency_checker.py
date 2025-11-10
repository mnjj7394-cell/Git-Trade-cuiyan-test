"""
一致性检查器
用于验证内部数据与天勤平台数据的一致性
修复版本：添加超时控制和错误处理，解决网络阻塞问题
"""
import asyncio
import logging
from typing import Dict, Any, List, Optional, Union
from datetime import datetime
from dataclasses import dataclass
from enum import Enum


class ConsistencyStatus(Enum):
    """一致性状态枚举"""
    CONSISTENT = "consistent"  # 完全一致
    INCONSISTENT = "inconsistent"  # 不一致
    PARTIAL_CONSISTENT = "partial_consistent"  # 部分一致
    CHECK_FAILED = "check_failed"  # 检查失败


@dataclass
class ConsistencyResult:
    """一致性检查结果"""
    status: ConsistencyStatus
    message: str
    differences: List[Dict[str, Any]]
    internal_count: int
    external_count: int
    matched_count: int


class ConsistencyChecker:
    """一致性检查器：验证内部数据与天勤平台数据的一致性（修复超时问题）"""

    def __init__(self, gateway):
        """
        初始化一致性检查器

        Args:
            gateway: TqsdkGateway实例，用于获取天勤平台数据
        """
        self.gateway = gateway
        self.logger = self._setup_logger()
        # 修改处1：添加超时配置
        self.timeout_config = {
            'api_call_timeout': 10.0,  # API调用超时时间（秒）
            'retry_attempts': 3,  # 重试次数
            'retry_delay': 1.0  # 重试延迟（秒）
        }

    def _setup_logger(self):
        """设置日志记录器"""
        logger = logging.getLogger("ConsistencyChecker")
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            logger.setLevel(logging.INFO)
        return logger

    async def validate_all(self,
                          internal_account: Dict[str, Any],
                          internal_orders: List[Dict[str, Any]],
                          internal_positions: List[Dict[str, Any]],
                          internal_trades: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        验证所有数据的一致性（修复：添加整体超时控制）
        """
        self.logger.info("开始全面一致性检查...")
        start_time = datetime.now()

        report = {
            'timestamp': datetime.now().isoformat(),
            'overall_status': ConsistencyStatus.CONSISTENT.value,
            'checks': {},
            'summary': {
                'total_checks': 0,
                'passed_checks': 0,
                'failed_checks': 0
            }
        }

        try:
            # 修改处2：为整体检查添加超时控制
            overall_timeout = self.timeout_config['api_call_timeout'] * 4  # 四个检查的总超时
            await asyncio.wait_for(
                self._perform_all_checks(
                    report, internal_account, internal_orders, internal_positions, internal_trades
                ),
                timeout=overall_timeout
            )

            elapsed_time = (datetime.now() - start_time).total_seconds()
            report['check_duration_seconds'] = elapsed_time

            self.logger.info(f"一致性检查完成，耗时: {elapsed_time:.2f}秒")
            self.logger.info(f"整体状态: {report['overall_status']}")
            self.logger.info(f"通过检查: {report['summary']['passed_checks']}/{report['summary']['total_checks']}")

            return report

        except asyncio.TimeoutError:
            self.logger.error("全面一致性检查超时，网络连接可能存在问题")
            report['overall_status'] = ConsistencyStatus.CHECK_FAILED.value
            report['error'] = "检查超时，网络连接异常"
            return report
        except Exception as e:
            self.logger.error(f"全面一致性检查失败: {e}")
            report['overall_status'] = ConsistencyStatus.CHECK_FAILED.value
            report['error'] = str(e)
            return report

    async def _perform_all_checks(self, report: Dict[str, Any],
                                 internal_account: Dict[str, Any],
                                 internal_orders: List[Dict[str, Any]],
                                 internal_positions: List[Dict[str, Any]],
                                 internal_trades: List[Dict[str, Any]]):
        """执行所有检查（内部方法，用于超时控制）"""
        # 1. 验证账户一致性
        account_result = await self.validate_account(internal_account)
        report['checks']['account'] = self._result_to_dict(account_result)
        self._update_summary(report, account_result)

        # 2. 验证订单一致性
        orders_result = await self.validate_orders(internal_orders)
        report['checks']['orders'] = self._result_to_dict(orders_result)
        self._update_summary(report, orders_result)

        # 3. 验证持仓一致性
        positions_result = await self.validate_positions(internal_positions)
        report['checks']['positions'] = self._result_to_dict(positions_result)
        self._update_summary(report, positions_result)

        # 4. 验证成交一致性
        trades_result = await self.validate_trades(internal_trades)
        report['checks']['trades'] = self._result_to_dict(trades_result)
        self._update_summary(report, trades_result)

        # 确定整体状态
        if report['summary']['failed_checks'] > 0:
            report['overall_status'] = ConsistencyStatus.INCONSISTENT.value
        elif report['summary']['passed_checks'] == report['summary']['total_checks']:
            report['overall_status'] = ConsistencyStatus.CONSISTENT.value
        else:
            report['overall_status'] = ConsistencyStatus.PARTIAL_CONSISTENT.value

    async def validate_account(self, internal_account: Dict[str, Any]) -> ConsistencyResult:
        """验证账户数据一致性（修复：添加超时和重试机制）"""
        self.logger.info("开始验证账户数据一致性...")

        try:
            # 修改处3：为网关调用添加超时和重试机制
            external_account = await self._call_with_retry(
                self.gateway.get_account_info,
                "获取天勤平台账户数据"
            )

            if not external_account:
                return ConsistencyResult(
                    status=ConsistencyStatus.CHECK_FAILED,
                    message="无法获取天勤平台账户数据",
                    differences=[],
                    internal_count=1 if internal_account else 0,
                    external_count=0,
                    matched_count=0
                )

            differences = []
            matched_count = 0
            total_fields = 0

            # 比较关键账户字段
            key_fields = ['balance', 'available', 'margin', 'frozen', 'commission']
            for field in key_fields:
                total_fields += 1
                internal_value = internal_account.get(field, 0)
                external_value = external_account.get(field, 0)

                # 允许小的浮点误差
                if abs(float(internal_value) - float(external_value)) > 0.01:
                    differences.append({
                        'field': field,
                        'internal': internal_value,
                        'external': external_value,
                        'difference': float(internal_value) - float(external_value)
                    })
                else:
                    matched_count += 1

            # 确定状态
            if not differences:
                status = ConsistencyStatus.CONSISTENT
                message = "账户数据完全一致"
            elif matched_count / total_fields >= 0.8:
                status = ConsistencyStatus.PARTIAL_CONSISTENT
                message = f"账户数据部分一致 ({matched_count}/{total_fields} 字段匹配)"
            else:
                status = ConsistencyStatus.INCONSISTENT
                message = f"账户数据不一致 ({matched_count}/{total_fields} 字段匹配)"

            self.logger.info(f"账户验证结果: {message}")
            return ConsistencyResult(
                status=status,
                message=message,
                differences=differences,
                internal_count=1,
                external_count=1,
                matched_count=matched_count
            )

        except asyncio.TimeoutError:
            self.logger.error("账户验证超时，网络连接可能异常")
            return ConsistencyResult(
                status=ConsistencyStatus.CHECK_FAILED,
                message="账户验证超时，网络连接异常",
                differences=[],
                internal_count=1 if internal_account else 0,
                external_count=0,
                matched_count=0
            )
        except Exception as e:
            self.logger.error(f"账户验证失败: {e}")
            return ConsistencyResult(
                status=ConsistencyStatus.CHECK_FAILED,
                message=f"账户验证失败: {e}",
                differences=[],
                internal_count=1 if internal_account else 0,
                external_count=0,
                matched_count=0
            )

    async def validate_orders(self, internal_orders: List[Dict[str, Any]]) -> ConsistencyResult:
        """验证订单数据一致性（修复：添加超时控制）"""
        self.logger.info(f"开始验证订单数据一致性，内部订单数: {len(internal_orders)}")

        try:
            # 修改处4：为订单数据获取添加超时
            external_orders = await self._call_with_retry(
                self.gateway.get_orders,
                "获取天勤平台订单数据"
            )
            if external_orders is None:
                external_orders = []

            differences = []
            matched_orders = 0

            # 创建订单ID映射以便快速查找
            internal_order_map = {order.get('order_id'): order for order in internal_orders}
            external_order_map = {order.get('order_id'): order for order in external_orders}

            all_order_ids = set(internal_order_map.keys()) | set(external_order_map.keys())

            for order_id in all_order_ids:
                internal_order = internal_order_map.get(order_id)
                external_order = external_order_map.get(order_id)

                if internal_order and external_order:
                    # 比较订单字段
                    order_differences = self._compare_orders(internal_order, external_order)
                    if not order_differences:
                        matched_orders += 1
                    else:
                        differences.extend(order_differences)
                elif internal_order and not external_order:
                    differences.append({
                        'order_id': order_id,
                        'issue': '订单存在于内部但不存在于天勤平台',
                        'internal_order': internal_order,
                        'external_order': None
                    })
                elif not internal_order and external_order:
                    differences.append({
                        'order_id': order_id,
                        'issue': '订单存在于天勤平台但不存在于内部',
                        'internal_order': None,
                        'external_order': external_order
                    })

            # 确定状态
            total_orders = len(all_order_ids)
            if total_orders == 0:
                status = ConsistencyStatus.CONSISTENT
                message = "无订单数据，一致性通过"
            elif not differences:
                status = ConsistencyStatus.CONSISTENT
                message = f"订单数据完全一致 ({matched_orders}/{total_orders} 订单匹配)"
            elif matched_orders / total_orders >= 0.8:
                status = ConsistencyStatus.PARTIAL_CONSISTENT
                message = f"订单数据部分一致 ({matched_orders}/{total_orders} 订单匹配)"
            else:
                status = ConsistencyStatus.INCONSISTENT
                message = f"订单数据不一致 ({matched_orders}/{total_orders} 订单匹配)"

            self.logger.info(f"订单验证结果: {message}")
            return ConsistencyResult(
                status=status,
                message=message,
                differences=differences,
                internal_count=len(internal_orders),
                external_count=len(external_orders),
                matched_count=matched_orders
            )

        except asyncio.TimeoutError:
            self.logger.error("订单验证超时，网络连接可能异常")
            return ConsistencyResult(
                status=ConsistencyStatus.CHECK_FAILED,
                message="订单验证超时，网络连接异常",
                differences=[],
                internal_count=len(internal_orders),
                external_count=0,
                matched_count=0
            )
        except Exception as e:
            self.logger.error(f"订单验证失败: {e}")
            return ConsistencyResult(
                status=ConsistencyStatus.CHECK_FAILED,
                message=f"订单验证失败: {e}",
                differences=[],
                internal_count=len(internal_orders),
                external_count=0,
                matched_count=0
            )

    def _compare_orders(self, internal_order: Dict[str, Any], external_order: Dict[str, Any]) -> List[Dict[str, Any]]:
        """比较两个订单的差异"""
        differences = []
        key_fields = ['symbol', 'direction', 'volume', 'price', 'status']

        for field in key_fields:
            internal_value = internal_order.get(field)
            external_value = external_order.get(field)

            if internal_value != external_value:
                differences.append({
                    'order_id': internal_order.get('order_id'),
                    'field': field,
                    'internal': internal_value,
                    'external': external_value
                })

        return differences

    async def validate_positions(self, internal_positions: List[Dict[str, Any]]) -> ConsistencyResult:
        """验证持仓数据一致性（修复：添加超时控制）"""
        self.logger.info(f"开始验证持仓数据一致性，内部持仓数: {len(internal_positions)}")

        try:
            # 修改处5：为持仓数据获取添加超时
            external_positions = await self._call_with_retry(
                self.gateway.get_positions,
                "获取天勤平台持仓数据"
            )
            if external_positions is None:
                external_positions = []

            differences = []
            matched_positions = 0

            # 按symbol和direction分组持仓
            internal_pos_map = {}
            for pos in internal_positions:
                key = (pos.get('symbol'), pos.get('direction'))
                internal_pos_map[key] = pos

            external_pos_map = {}
            for pos in external_positions:
                key = (pos.get('symbol'), pos.get('direction'))
                external_pos_map[key] = pos

            all_position_keys = set(internal_pos_map.keys()) | set(external_pos_map.keys())

            for key in all_position_keys:
                symbol, direction = key
                internal_pos = internal_pos_map.get(key)
                external_pos = external_pos_map.get(key)

                if internal_pos and external_pos:
                    # 比较持仓字段
                    pos_differences = self._compare_positions(internal_pos, external_pos)
                    if not pos_differences:
                        matched_positions += 1
                    else:
                        differences.extend(pos_differences)
                elif internal_pos and not external_pos:
                    differences.append({
                        'symbol': symbol,
                        'direction': direction,
                        'issue': '持仓存在于内部但不存在于天勤平台',
                        'internal_position': internal_pos,
                        'external_position': None
                    })
                elif not internal_pos and external_pos:
                    differences.append({
                        'symbol': symbol,
                        'direction': direction,
                        'issue': '持仓存在于天勤平台但不存在于内部',
                        'internal_position': None,
                        'external_position': external_pos
                    })

            # 确定状态
            total_positions = len(all_position_keys)
            if total_positions == 0:
                status = ConsistencyStatus.CONSISTENT
                message = "无持仓数据，一致性通过"
            elif not differences:
                status = ConsistencyStatus.CONSISTENT
                message = f"持仓数据完全一致 ({matched_positions}/{total_positions} 持仓匹配)"
            elif matched_positions / total_positions >= 0.8:
                status = ConsistencyStatus.PARTIAL_CONSISTENT
                message = f"持仓数据部分一致 ({matched_positions}/{total_positions} 持仓匹配)"
            else:
                status = ConsistencyStatus.INCONSISTENT
                message = f"持仓数据不一致 ({matched_positions}/{total_positions} 持仓匹配)"

            self.logger.info(f"持仓验证结果: {message}")
            return ConsistencyResult(
                status=status,
                message=message,
                differences=differences,
                internal_count=len(internal_positions),
                external_count=len(external_positions),
                matched_count=matched_positions
            )

        except asyncio.TimeoutError:
            self.logger.error("持仓验证超时，网络连接可能异常")
            return ConsistencyResult(
                status=ConsistencyStatus.CHECK_FAILED,
                message="持仓验证超时，网络连接异常",
                differences=[],
                internal_count=len(internal_positions),
                external_count=0,
                matched_count=0
            )
        except Exception as e:
            self.logger.error(f"持仓验证失败: {e}")
            return ConsistencyResult(
                status=ConsistencyStatus.CHECK_FAILED,
                message=f"持仓验证失败: {e}",
                differences=[],
                internal_count=len(internal_positions),
                external_count=0,
                matched_count=0
            )

    def _compare_positions(self, internal_position: Dict[str, Any], external_position: Dict[str, Any]) -> List[Dict[str, Any]]:
        """比较两个持仓的差异"""
        differences = []
        key_fields = ['volume', 'available_volume', 'frozen_volume', 'open_price', 'position_price']

        for field in key_fields:
            internal_value = internal_position.get(field, 0)
            external_value = external_position.get(field, 0)

            # 对于数值字段，允许小的误差
            if field in ['volume', 'available_volume', 'frozen_volume']:
                if abs(int(internal_value) - int(external_value)) > 0:
                    differences.append({
                        'symbol': internal_position.get('symbol'),
                        'direction': internal_position.get('direction'),
                        'field': field,
                        'internal': internal_value,
                        'external': external_value,
                        'difference': int(internal_value) - int(external_value)
                    })
            elif field in ['open_price', 'position_price']:
                if abs(float(internal_value) - float(external_value)) > 0.01:
                    differences.append({
                        'symbol': internal_position.get('symbol'),
                        'direction': internal_position.get('direction'),
                        'field': field,
                        'internal': internal_value,
                        'external': external_value,
                        'difference': float(internal_value) - float(external_value)
                    })

        return differences

    async def validate_trades(self, internal_trades: List[Dict[str, Any]]) -> ConsistencyResult:
        """验证成交数据一致性（修复：添加超时控制）"""
        self.logger.info(f"开始验证成交数据一致性，内部成交数: {len(internal_trades)}")

        try:
            # 修改处6：为成交数据获取添加超时
            external_trades = await self._call_with_retry(
                self.gateway.get_trades,
                "获取天勤平台成交数据"
            )
            if external_trades is None:
                external_trades = []

            differences = []
            matched_trades = 0

            # 创建成交ID映射
            internal_trade_map = {trade.get('trade_id'): trade for trade in internal_trades}
            external_trade_map = {trade.get('trade_id'): trade for trade in external_trades}

            all_trade_ids = set(internal_trade_map.keys()) | set(external_trade_map.keys())

            for trade_id in all_trade_ids:
                internal_trade = internal_trade_map.get(trade_id)
                external_trade = external_trade_map.get(trade_id)

                if internal_trade and external_trade:
                    # 比较成交字段
                    trade_differences = self._compare_trades(internal_trade, external_trade)
                    if not trade_differences:
                        matched_trades += 1
                    else:
                        differences.extend(trade_differences)
                elif internal_trade and not external_trade:
                    differences.append({
                        'trade_id': trade_id,
                        'issue': '成交存在于内部但不存在于天勤平台',
                        'internal_trade': internal_trade,
                        'external_trade': None
                    })
                elif not internal_trade and external_trade:
                    differences.append({
                        'trade_id': trade_id,
                        'issue': '成交存在于天勤平台但不存在于内部',
                        'internal_trade': None,
                        'external_trade': external_trade
                    })

            # 确定状态
            total_trades = len(all_trade_ids)
            if total_trades == 0:
                status = ConsistencyStatus.CONSISTENT
                message = "无成交数据，一致性通过"
            elif not differences:
                status = ConsistencyStatus.CONSISTENT
                message = f"成交数据完全一致 ({matched_trades}/{total_trades} 成交匹配)"
            elif matched_trades / total_trades >= 0.8:
                status = ConsistencyStatus.PARTIAL_CONSISTENT
                message = f"成交数据部分一致 ({matched_trades}/{total_trades} 成交匹配)"
            else:
                status = ConsistencyStatus.INCONSISTENT
                message = f"成交数据不一致 ({matched_trades}/{total_trades} 成交匹配)"

            self.logger.info(f"成交验证结果: {message}")
            return ConsistencyResult(
                status=status,
                message=message,
                differences=differences,
                internal_count=len(internal_trades),
                external_count=len(external_trades),
                matched_count=matched_trades
            )

        except asyncio.TimeoutError:
            self.logger.error("成交验证超时，网络连接可能异常")
            return ConsistencyResult(
                status=ConsistencyStatus.CHECK_FAILED,
                message="成交验证超时，网络连接异常",
                differences=[],
                internal_count=len(internal_trades),
                external_count=0,
                matched_count=0
            )
        except Exception as e:
            self.logger.error(f"成交验证失败: {e}")
            return ConsistencyResult(
                status=ConsistencyStatus.CHECK_FAILED,
                message=f"成交验证失败: {e}",
                differences=[],
                internal_count=len(internal_trades),
                external_count=0,
                matched_count=0
            )

    def _compare_trades(self, internal_trade: Dict[str, Any], external_trade: Dict[str, Any]) -> List[Dict[str, Any]]:
        """比较两个成交的差异"""
        differences = []
        key_fields = ['symbol', 'direction', 'volume', 'price', 'trade_time']

        for field in key_fields:
            internal_value = internal_trade.get(field)
            external_value = external_trade.get(field)

            if field in ['volume']:
                if abs(int(internal_value) - int(external_value)) > 0:
                    differences.append({
                        'trade_id': internal_trade.get('trade_id'),
                        'field': field,
                        'internal': internal_value,
                        'external': external_value,
                        'difference': int(internal_value) - int(external_value)
                    })
            elif field in ['price']:
                if abs(float(internal_value) - float(external_value)) > 0.01:
                    differences.append({
                        'trade_id': internal_trade.get('trade_id'),
                        'field': field,
                        'internal': internal_value,
                        'external': external_value,
                        'difference': float(internal_value) - float(external_value)
                    })
            elif internal_value != external_value:
                differences.append({
                    'trade_id': internal_trade.get('trade_id'),
                    'field': field,
                    'internal': internal_value,
                    'external': external_value
                })

        return differences

    async def _call_with_retry(self, func, operation_name: str):
        """带重试和超时的函数调用（新增方法）"""
        # 修改处7：实现带重试和超时的调用机制
        for attempt in range(self.timeout_config['retry_attempts']):
            try:
                result = await asyncio.wait_for(
                    func(),
                    timeout=self.timeout_config['api_call_timeout']
                )
                return result
            except asyncio.TimeoutError:
                if attempt < self.timeout_config['retry_attempts'] - 1:
                    self.logger.warning(f"{operation_name}超时，第{attempt + 1}次重试...")
                    await asyncio.sleep(self.timeout_config['retry_delay'])
                else:
                    self.logger.error(f"{operation_name}多次重试后仍超时")
                    raise
            except Exception as e:
                if attempt < self.timeout_config['retry_attempts'] - 1:
                    self.logger.warning(f"{operation_name}失败，第{attempt + 1}次重试: {e}")
                    await asyncio.sleep(self.timeout_config['retry_delay'])
                else:
                    self.logger.error(f"{operation_name}多次重试后仍失败: {e}")
                    raise

    def _result_to_dict(self, result: ConsistencyResult) -> Dict[str, Any]:
        """将ConsistencyResult转换为字典"""
        return {
            'status': result.status.value,
            'message': result.message,
            'differences': result.differences,
            'internal_count': result.internal_count,
            'external_count': result.external_count,
            'matched_count': result.matched_count
        }

    def _update_summary(self, report: Dict[str, Any], result: ConsistencyResult):
        """更新汇总信息"""
        report['summary']['total_checks'] += 1
        if result.status == ConsistencyStatus.CONSISTENT:
            report['summary']['passed_checks'] += 1
        elif result.status == ConsistencyStatus.INCONSISTENT:
            report['summary']['failed_checks'] += 1


# 测试代码
async def test_consistency_checker():
    """测试一致性检查器"""
    print("开始测试一致性检查器...")

    # 模拟网关类（实际使用时传入真实的TqsdkGateway实例）
    class MockGateway:
        async def get_account_info(self):
            return {
                'balance': 1000000.0,
                'available': 999000.0,
                'margin': 1000.0,
                'frozen': 0.0,
                'commission': 0.0
            }

        async def get_orders(self):
            return [
                {
                    'order_id': 'order_001',
                    'symbol': 'SHFE.cu2401',
                    'direction': 'SELL',
                    'volume': 1,
                    'price': 67900.0,
                    'status': 'filled'
                }
            ]

        async def get_positions(self):
            return [
                {
                    'symbol': 'SHFE.cu2401',
                    'direction': 'SELL',
                    'volume': 1,
                    'available_volume': 1,
                    'frozen_volume': 0,
                    'open_price': 67900.0,
                    'position_price': 67900.0
                }
            ]

        async def get_trades(self):
            return [
                {
                    'trade_id': 'trade_001',
                    'order_id': 'order_001',
                    'symbol': 'SHFE.cu2401',
                    'direction': 'SELL',
                    'volume': 1,
                    'price': 67900.0,
                    'trade_time': 1700000000
                }
            ]

    # 创建检查器和模拟数据
    gateway = MockGateway()
    checker = ConsistencyChecker(gateway)

    # 模拟内部数据（与外部数据一致）
    internal_account = {
        'balance': 1000000.0,
        'available': 999000.0,
        'margin': 1000.0,
        'frozen': 0.0,
        'commission': 0.0
    }

    internal_orders = [
        {
            'order_id': 'order_001',
            'symbol': 'SHFE.cu2401',
            'direction': 'SELL',
            'volume': 1,
            'price': 67900.0,
            'status': 'filled'
        }
    ]

    internal_positions = [
        {
            'symbol': 'SHFE.cu2401',
            'direction': 'SELL',
            'volume': 1,
            'available_volume': 1,
            'frozen_volume': 0,
            'open_price': 67900.0,
            'position_price': 67900.0
        }
    ]

    internal_trades = [
        {
            'trade_id': 'trade_001',
            'order_id': 'order_001',
            'symbol': 'SHFE.cu2401',
            'direction': 'SELL',
            'volume': 1,
            'price': 67900.0,
            'trade_time': 1700000000
        }
    ]

    # 运行一致性检查
    report = await checker.validate_all(
        internal_account, internal_orders, internal_positions, internal_trades
    )

    print("一致性检查报告:")
    print(f"整体状态: {report['overall_status']}")
    print(f"检查耗时: {report.get('check_duration_seconds', 0):.2f}秒")
    print(f"通过检查: {report['summary']['passed_checks']}/{report['summary']['total_checks']}")

    for check_name, check_result in report['checks'].items():
        print(f"{check_name}: {check_result['status']} - {check_result['message']}")

    return report


if __name__ == "__main__":
    # 运行测试
    asyncio.run(test_consistency_checker())
