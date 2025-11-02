#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
集成测试主脚本 - 天勤SDK自定义数据表项目
功能：验证自定义数据表（MyAccount, MyPosition, MyOrder, MyTrade）与天勤原始数据的一致性
创建日期：2024年
"""

import asyncio
import unittest
import time
import logging
from datetime import datetime
from typing import Dict, Any
import inspect  # 新增导入

from config import CurrentConfig
from my_data_manager import MyDataManager
from trading_strategies import create_strategy, STRATEGY_CONFIGS


class TQSdkIntegrationTest(unittest.TestCase):
    """
    天勤SDK集成测试主类
    用于验证自定义数据表与天勤原始数据的一致性
    """

    # 测试运行时长（秒）
    TEST_DURATION = CurrentConfig.TEST_DURATION

    @classmethod
    def setUpClass(cls):
        """测试类级别的设置，在所有测试方法前执行一次"""
        cls.logger = logging.getLogger("TQSdkIntegrationTest")
        cls.logger.setLevel(getattr(logging, CurrentConfig.LOG_LEVEL))

        # 确保日志处理器已设置
        if not cls.logger.handlers:
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler = logging.StreamHandler()
            handler.setFormatter(formatter)
            cls.logger.addHandler(handler)

        cls.logger.info("=" * 60)
        cls.logger.info("开始天勤SDK集成测试")
        cls.logger.info("=" * 60)

        # 验证配置
        if not CurrentConfig.validate_config():
            raise unittest.SkipTest("配置验证失败，跳过所有测试")

    # 在 setUpClass 方法后添加以下代码
    def log_progress(self, step_name):
        """记录测试进度"""
        current_time = time.time()
        if not hasattr(self, 'test_start_time'):
            self.test_start_time = current_time

        elapsed = current_time - self.test_start_time
        print(f"[进度监控] {step_name} - 已运行: {elapsed:.1f}秒")
    def setUp(self):
        """每个测试方法前的设置"""
        self.start_time = time.time()
        self.data_manager = None
        self.test_passed = False

    def tearDown(self):
        """每个测试方法后的清理"""
        if self.data_manager:
            # 检查当前事件循环状态，避免在已有事件循环中创建新循环
            try:
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    # 如果事件循环已在运行，创建新线程执行异步清理
                    import threading
                    def run_async():
                        new_loop = asyncio.new_event_loop()
                        asyncio.set_event_loop(new_loop)
                        new_loop.run_until_complete(self._async_teardown())
                        new_loop.close()

                    thread = threading.Thread(target=run_async)
                    thread.start()
                    thread.join()
                else:
                    asyncio.run(self._async_teardown())
            except:
                asyncio.run(self._async_teardown())

        duration = time.time() - self.start_time
        status = "通过" if self.test_passed else "失败"
        self.logger.info(f"测试 {self._testMethodName} {status}，耗时: {duration:.2f}秒")

    async def _async_teardown(self):
        """异步清理资源"""
        if hasattr(self.data_manager, 'is_connected') and self.data_manager.is_connected:
            await self.data_manager.disconnect()

    async def _initialize_data_manager(self):
        """初始化数据管理器并连接天勤API"""
        self.data_manager = MyDataManager()
        success = await self.data_manager.connect()

        if not success:
            self.skipTest("无法连接天勤API，跳过测试")

        # 等待初始数据同步
        await asyncio.sleep(2)
        return True


class TestDataConsistency(TQSdkIntegrationTest):
    """
    数据一致性测试类
    验证自定义数据表与天勤原始数据的一致性
    """

    def test_account_data_consistency(self):
        """测试账户数据一致性"""
        asyncio.run(self._test_account_data_consistency_async())

    async def _test_account_data_consistency_async(self):
        """异步的账户数据一致性测试实现"""
        await self._initialize_data_manager()


        # 获取天勤原始账户和自定义账户
        tq_account = self.data_manager.tq_account
        my_account = self.data_manager.get_account()

        # 验证账户数据一致性
        self.assertIsNotNone(tq_account, "天勤账户对象不应为None")
        self.assertIsNotNone(my_account, "自定义账户对象不应为None")

        # 比较关键账户字段
        tolerance = CurrentConfig.FLOAT_PRECISION

        self.assertAlmostEqual(
            tq_account.balance, my_account.balance,
            places=6, msg="账户余额不一致"
        )

        self.assertAlmostEqual(
            tq_account.available, my_account.available,
            places=6, msg="可用资金不一致"
        )

        # 验证其他重要字段
        if hasattr(tq_account, 'frozen_margin') and hasattr(my_account, 'frozen'):
            self.assertAlmostEqual(
                tq_account.frozen_margin, my_account.frozen,
                places=6, msg="冻结资金不一致"
            )

        self.logger.info("账户数据一致性验证通过")
        self.test_passed = True

    def test_position_data_consistency(self):
        """测试持仓数据一致性"""
        asyncio.run(self._test_position_data_consistency_async())

    async def _test_position_data_consistency_async(self):
        """异步的持仓数据一致性测试实现"""
        await self._initialize_data_manager()


        test_symbol = CurrentConfig.TEST_SYMBOL

        # 获取天勤原始持仓
        tq_position = self.data_manager.tq_positions.get(test_symbol)
        self.assertIsNotNone(tq_position, f"天勤持仓对象不应为None（合约: {test_symbol}）")

        # 获取自定义持仓（多头和空头）
        my_long_position = self.data_manager.get_position(test_symbol, "LONG")
        my_short_position = self.data_manager.get_position(test_symbol, "SHORT")

        self.assertIsNotNone(my_long_position, "自定义多头持仓对象不应为None")
        self.assertIsNotNone(my_short_position, "自定义空头持仓对象不应为None")

        # 验证多头持仓数据
        tq_long_volume = tq_position.pos_long_his + tq_position.pos_long_today
        self.assertEqual(
            tq_long_volume, my_long_position.volume,
            "多头持仓手数不一致"
        )

        self.assertEqual(
            tq_position.pos_long_his, my_long_position.yd_volume,
            "多头昨仓手数不一致"
        )

        # 验证空头持仓数据
        tq_short_volume = tq_position.pos_short_his + tq_position.pos_short_today
        self.assertEqual(
            tq_short_volume, my_short_position.volume,
            "空头持仓手数不一致"
        )

        self.assertEqual(
            tq_position.pos_short_his, my_short_position.yd_volume,
            "空头昨仓手数不一致"
        )

        # 验证持仓价格数据（如果存在持仓）
        if tq_long_volume > 0:
            self.assertAlmostEqual(
                tq_position.position_price_long, my_long_position.price,
                places=6, msg="多头持仓均价不一致"
            )

        if tq_short_volume > 0:
            self.assertAlmostEqual(
                tq_position.position_price_short, my_short_position.price,
                places=6, msg="空头持仓均价不一致"
            )

        self.logger.info("持仓数据一致性验证通过")
        self.test_passed = True

    def test_order_data_consistency(self):
        """测试订单数据一致性（抽样验证）"""
        asyncio.run(self._test_order_data_consistency_async())

    async def _test_order_data_consistency_async(self):
        """异步的订单数据一致性测试实现"""
        await self._initialize_data_manager()


        # 获取订单数据
        tq_orders = self.data_manager._get_tq_orders_dict()
        my_orders = self.data_manager.get_all_orders()

        # 如果存在订单，进行抽样验证
        if tq_orders and my_orders:
            # 抽样验证前3个订单
            sample_orders = list(my_orders)[:3]

            for my_order in sample_orders:
                order_id = my_order.orderid
                if order_id in tq_orders:
                    tq_order = tq_orders[order_id]

                    # 验证订单基本信息
                    self.assertEqual(
                        tq_order.instrument_id, my_order.symbol,
                        f"订单{order_id}的合约代码不一致"
                    )

                    self.assertEqual(
                        tq_order.volume_orign, my_order.volume,
                        f"订单{order_id}的委托数量不一致"
                    )

                    # 验证方向映射
                    expected_direction = "LONG" if tq_order.direction == "BUY" else "SHORT"
                    self.assertEqual(
                        expected_direction, my_order.direction,
                        f"订单{order_id}的方向映射不一致"
                    )

            self.logger.info(f"订单数据一致性验证通过（抽样{len(sample_orders)}个订单）")
        else:
            self.logger.info("⚠ 无订单数据可验证，跳过订单一致性测试")

        self.test_passed = True

    def test_trade_data_consistency(self):
        """测试成交数据一致性（抽样验证）"""
        asyncio.run(self._test_trade_data_consistency_async())

    async def _test_trade_data_consistency_async(self):
        """异步的成交数据一致性测试实现"""
        await self._initialize_data_manager()


        # 获取成交数据
        tq_trades = self.data_manager._get_tq_trades_dict()
        my_trades = self.data_manager.get_all_trades()

        # 如果存在成交，进行抽样验证
        if tq_trades and my_trades:
            # 抽样验证前3个成交
            sample_trades = list(my_trades)[:3]

            for my_trade in sample_trades:
                trade_id = my_trade.tradeid
                if trade_id in tq_trades:
                    tq_trade = tq_trades[trade_id]

                    # 验证成交基本信息
                    self.assertEqual(
                        tq_trade.instrument_id, my_trade.symbol,
                        f"成交{trade_id}的合约代码不一致"
                    )

                    self.assertEqual(
                        tq_trade.volume, my_trade.volume,
                        f"成交{trade_id}的成交数量不一致"
                    )

                    self.assertAlmostEqual(
                        tq_trade.price, my_trade.price,
                        places=6, msg=f"成交{trade_id}的成交价格不一致"
                    )

            self.logger.info(f"成交数据一致性验证通过（抽样{len(sample_trades)}个成交）")
        else:
            self.logger.info("⚠ 无成交数据可验证，跳过成交一致性测试")

        self.test_passed = True

    async def test_with_trading_strategy(self):
        """使用交易策略测试数据一致性"""
        self.log_progress("开始策略测试")
        await self._initialize_data_manager()

        # 创建测试策略
        test_strategy = create_strategy("simple", STRATEGY_CONFIGS["simple"])

        # 添加策略到数据管理器
        self.data_manager.add_trading_strategy(test_strategy)

        # 启动策略
        self.log_progress("启动交易策略")
        await self.data_manager.start_trading()

        # 运行策略一段时间
        self.log_progress("等待策略生成交易数据")
        test_duration = 30  # 测试运行30秒
        start_time = time.time()

        while time.time() - start_time < test_duration:
            await asyncio.sleep(5)
            elapsed = time.time() - start_time
            self.log_progress(f"策略运行中: {elapsed:.1f}秒")

        # 停止策略
        await self.data_manager.stop_trading()

        # 验证数据一致性
        self.log_progress("验证策略生成的数据一致性")

        # 验证账户数据
        self._test_account_data_consistency()

        # 验证持仓数据
        self._test_position_data_consistency()

        # 验证订单数据（如果有）
        orders = self.data_manager.get_all_orders()
        if orders:
            self._test_order_data_consistency()
            self.logger.info(f"✓ 验证了{len(orders)}个订单的数据一致性")
        else:
            self.logger.info("⚠ 无订单数据可验证")

        # 验证成交数据（如果有）
        trades = self.data_manager.get_all_trades()
        if trades:
            self._test_trade_data_consistency()
            self.logger.info(f"✓ 验证了{len(trades)}个成交的数据一致性")
        else:
            self.logger.info("⚠ 无成交数据可验证")

        self.test_passed = True
        self.log_progress("策略测试完成")


# 3. 在清理方法中添加策略停止
async def _async_teardown(self):
    """异步清理资源"""
    # 停止所有策略
    if hasattr(self.data_manager, 'is_trading') and self.data_manager.is_trading:
        await self.data_manager.stop_trading()

    # 断开API连接
    if hasattr(self.data_manager, 'is_connected') and self.data_manager.is_connected:
        await self.data_manager.disconnect()

class TestRealTimeUpdates(TQSdkIntegrationTest):
    """
    实时更新测试类
    验证数据在实时更新过程中的一致性
    """

    def test_real_time_data_sync(self):
        """测试实时数据同步的一致性"""
        asyncio.run(self._test_real_time_data_sync_async())

    async def _test_real_time_data_sync_async(self):
        """异步的实时数据同步测试实现"""
        self.log_progress("开始初始化数据管理器")
        await self._initialize_data_manager()

        self.log_progress("启动数据同步")

        update_count = 0
        max_updates = 10  # 最大更新次数
        start_time = time.time()

        self.logger.info("开始实时数据同步测试...")

        # 监控多次数据更新
        while update_count < max_updates and time.time() - start_time < self.TEST_DURATION:
            self.log_progress(f"等待第{update_count + 1}次数据更新")

            try:
                # 等待数据更新
                await asyncio.wait_for(
                    self.data_manager._wait_for_update(),
                    timeout=5.0
                )

                # 验证数据一致性
                consistency_results = self.data_manager.validate_data_consistency()

                # 检查所有数据表的一致性
                for data_type, is_consistent in consistency_results.items():
                    self.assertTrue(
                        is_consistent,
                        f"实时更新中{data_type}数据不一致（第{update_count + 1}次更新）"
                    )

                update_count += 1
                self.log_progress(f"收到第{update_count}次数据更新")

            except asyncio.TimeoutError:
                self.log_progress("数据更新等待超时，继续循环")
                continue
            except Exception as e:
                self.fail(f"实时数据同步过程中发生错误: {e}")

        self.assertGreaterEqual(update_count, 3, "至少应完成3次数据更新")
        self.logger.info(f"实时数据同步测试通过（共处理{update_count}次更新）")
        self.test_passed = True


class TestStopLossFunctionality(TQSdkIntegrationTest):
    """
    止损功能测试类
    验证基于自定义数据表的实时止损功能
    """

    def test_stop_loss_condition_detection(self):
        """测试止损条件检测功能"""
        asyncio.run(self._test_stop_loss_condition_detection_async())

    async def _test_stop_loss_condition_detection_async(self):
        """异步的止损条件检测测试实现"""
        await self._initialize_data_manager()


        # 获取账户数据
        my_account = self.data_manager.get_account()
        self.assertIsNotNone(my_account, "自定义账户对象不应为None")

        # 获取止损比例配置
        stop_loss_ratio = CurrentConfig.RISK_MANAGEMENT["STOP_LOSS_RATIO"]

        # 模拟止损条件检测
        initial_balance = my_account.balance
        float_profit = my_account.pnl  # 假设pnl字段表示浮动盈亏

        # 计算当前收益率
        if initial_balance > 0:
            current_return = float_profit / initial_balance

            # 测试止损条件
            should_stop_loss = current_return <= stop_loss_ratio

            self.logger.info(f"账户初始权益: {initial_balance:.2f}")
            self.logger.info(f"当前浮动盈亏: {float_profit:.2f}")
            self.logger.info(f"当前收益率: {current_return:.4%}")
            self.logger.info(f"止损阈值: {stop_loss_ratio:.4%}")
            self.logger.info(f"是否触发止损: {should_stop_loss}")

            # 验证止损逻辑是否正确
            if should_stop_loss:
                self.logger.warning("⚠ 触发止损条件，应执行平仓操作")
                # 这里可以添加实际的止损操作逻辑
            else:
                self.logger.info("未触发止损条件，持仓安全")

        else:
            self.logger.warning("账户权益为0或负值，无法计算收益率")

        self.test_passed = True

    def test_risk_management_integration(self):
        """测试风控系统集成"""
        asyncio.run(self._test_risk_management_integration_async())

    async def _test_risk_management_integration_async(self):
        """异步的风控系统集成测试实现"""
        await self._initialize_data_manager()


        # 获取风控配置
        risk_config = CurrentConfig.RISK_MANAGEMENT

        # 验证风控参数加载
        self.assertIsInstance(risk_config, dict, "风控配置应为字典类型")
        self.assertIn("STOP_LOSS_RATIO", risk_config, "风控配置应包含止损比例")
        self.assertIn("MAX_POSITION_RATIO", risk_config, "风控配置应包含最大持仓比例")

        # 验证止损比例合理性
        stop_loss_ratio = risk_config["STOP_LOSS_RATIO"]
        self.assertLess(stop_loss_ratio, 0, "止损比例应为负值")
        self.assertGreater(stop_loss_ratio, -0.5, "止损比例不应过于激进")

        # 验证最大持仓比例合理性
        max_position_ratio = risk_config["MAX_POSITION_RATIO"]
        self.assertGreater(max_position_ratio, 0, "最大持仓比例应为正值")
        self.assertLessEqual(max_position_ratio, 1, "最大持仓比例不应超过100%")

        self.logger.info("风控系统集成测试通过")
        self.test_passed = True


# 异步测试运行器
class AsyncTestRunner:
    """异步测试运行器，用于执行异步测试方法"""

    @classmethod
    def run_tests(cls):
        """运行所有测试"""
        # 创建测试加载器
        loader = unittest.TestLoader()

        # 加载测试用例
        test_suite = loader.loadTestsFromModule(__import__(__name__))

        # 创建测试运行器
        runner = unittest.TextTestRunner(
            verbosity=2,  # 详细输出
            failfast=False  # 不遇到失败就停止
        )

        # 运行测试
        print("=" * 60)
        print("开始执行天勤SDK集成测试套件")
        print("=" * 60)

        result = runner.run(test_suite)

        # 打印测试总结
        print("\n" + "=" * 60)
        print("测试执行摘要")
        print("=" * 60)
        print(f"运行测试数: {result.testsRun}")
        print(f"通过数: {result.testsRun - len(result.failures) - len(result.errors)}")
        print(f"失败数: {len(result.failures)}")
        print(f"错误数: {len(result.errors)}")
        print(f"跳过数: {len(result.skipped)}")

        # 打印失败和错误的详细信息
        if result.failures:
            print("\n失败的测试:")
            for test, traceback in result.failures:
                print(f"  {test}: {traceback.splitlines()[-1]}")

        if result.errors:
            print("\n错误的测试:")
            for test, traceback in result.errors:
                print(f"  {test}: {traceback.splitlines()[-1]}")

        return result.wasSuccessful()


async def main():
    """主异步函数"""
    print("天勤SDK自定义数据表集成测试")
    print(f"测试环境: {CurrentConfig.TQ_ENV}")
    print(f"测试合约: {CurrentConfig.TEST_SYMBOL}")
    print(f"测试时长: {CurrentConfig.TEST_DURATION}秒")
    print(f"开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # 运行测试
    success = AsyncTestRunner.run_tests()

    print(f"\n测试完成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    if success:
        print("所有测试通过！自定义数据表与天勤SDK兼容性良好。")
    else:
        print("部分测试失败，请检查实现。")

    return success


if __name__ == "__main__":
    # 运行异步主函数
    try:
        success = asyncio.run(main())
        exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n测试被用户中断")
        exit(1)
    except Exception as e:
        print(f"测试执行过程中发生错误: {e}")
        exit(1)