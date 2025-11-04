"""
改进的主程序
支持通过参数选择策略，集成数据适配和异步同步桥接
"""
import argparse
import time
from datetime import datetime
from core.event_engine import EventEngine
from core.data_manager import DataManager
from core.backtest_engine import BacktestEngine
from gateways.tqsdk_gateway import TqsdkGateway
from config.settings import Settings


def main():
    """主函数"""
    # 解析命令行参数
    parser = argparse.ArgumentParser(description="量化交易回测系统")
    parser.add_argument("--strategy", "-s", type=str, default="double_ma",
                       help="策略名称（如：double_ma, breakout, mean_reversion）")
    parser.add_argument("--symbol", type=str, default="SHFE.cu2401",
                       help="交易品种")
    parser.add_argument("--start", type=str, default="2024-01-01",
                       help="开始日期")
    parser.add_argument("--end", type=str, default="2024-01-05",
                       help="结束日期")

    args = parser.parse_args()

    # 运行回测（同步版本）
    run_backtest(args)


def run_backtest(args):
    """运行回测（同步版本）"""
    print("=" * 50)
    print("量化交易回测系统启动")
    print(f"策略: {args.strategy}")
    print(f"品种: {args.symbol}")
    print(f"期间: {args.start} 至 {args.end}")
    print("=" * 50)

    try:
        # 初始化核心组件
        event_engine = EventEngine()
        data_manager = DataManager(event_engine)
        backtest_engine = BacktestEngine(event_engine, data_manager)

        # 初始化网关
        gateway = TqsdkGateway(event_engine)

        # 从配置获取凭据并连接天勤API（同步调用）
        config = Settings()
        print("正在连接天勤API...")
        if not gateway.connect_sync(config.tqsdk_username, config.tqsdk_password):
            print("天勤API连接失败，请检查网络和凭据")
            return

        print("天勤API连接成功")

        # 获取历史数据（同步调用）
        print("正在获取历史数据...")
        history_data = gateway.get_history_data_sync(
            symbol=args.symbol,
            start_dt=args.start,
            end_dt=args.end,
            frequency=3600  # 1小时K线
        )

        if not history_data:
            print("历史数据获取失败")
            return

        # 设置历史数据
        backtest_engine.set_history_data(history_data)

        # 配置策略参数
        strategy_config = _get_strategy_config(args.strategy, args.symbol)

        # 运行回测
        print("开始回测...")
        backtest_engine.run_backtest(
            strategy_name=args.strategy,
            strategy_config=strategy_config
        )

        # 生成回测报告
        generate_report(backtest_engine, args)

    except Exception as e:
        print(f"回测过程发生异常: {e}")
    finally:
        # 清理资源（同步调用）
        if 'gateway' in locals():
            gateway.disconnect_sync()
        print("回测系统关闭")


def _get_strategy_config(strategy_name, symbol):
    """根据策略名称获取对应的配置参数"""
    base_config = {
        "symbol": symbol,
        "volume": 1  # 默认交易手数
    }

    if strategy_name == "double_ma":
        # 双均线策略配置
        base_config.update({
            "short_period": 30,  # 短周期
            "long_period": 60,   # 长周期
        })
    elif strategy_name == "breakout":
        # 突破策略配置
        base_config.update({
            "entry_period": 20,   # 入场周期
            "exit_period": 10,    # 出场周期
            "threshold": 0.02,    # 突破阈值
        })
    elif strategy_name == "mean_reversion":
        # 均值回归策略配置
        base_config.update({
            "period": 14,        # 计算周期
            "deviation": 2.0,    # 标准差倍数
        })
    else:
        # 默认配置
        base_config.update({
            "short_period": 30,
            "long_period": 60,
        })

    return base_config


def generate_report(engine, args):
    """生成回测报告"""
    print("\n" + "=" * 50)
    print("回测报告")
    print("=" * 50)
    print(f"策略名称: {args.strategy}")
    print(f"交易品种: {args.symbol}")
    print(f"回测期间: {args.start} 至 {args.end}")

    # 获取账户信息
    account = engine.get_account()
    if account:
        print(f"初始资金: {account.get('balance', 0):,.2f}")
        print(f"最终权益: {account.get('available', 0):,.2f}")
        print(f"手续费: {account.get('commission', 0):.2f}")
        print(f"平仓盈亏: {account.get('close_profit', 0):.2f}")

    print("=" * 50)


if __name__ == "__main__":
    main()
