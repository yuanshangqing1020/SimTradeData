"""
测试批量财务数据导入优化

验证批量模式相比逐个查询的性能提升
"""

import time
from datetime import date

from simtradedata.config import Config
from simtradedata.data_sources import DataSourceManager
from simtradedata.database import DatabaseManager
from simtradedata.sync import SyncManager


def test_batch_financial_import():
    """测试批量财务数据导入功能"""
    print("\n" + "=" * 80)
    print("测试1: 批量财务数据导入功能")
    print("=" * 80)

    config = Config()
    db_manager = DatabaseManager(config=config)
    data_source_manager = DataSourceManager(config=config, db_manager=db_manager)

    # 测试批量导入
    report_date = "2023-12-31"
    report_type = "Q4"

    print(f"\n开始批量导入财务数据: {report_date} {report_type}")
    start_time = time.time()

    try:
        result = data_source_manager.batch_import_financial_data(
            report_date, report_type
        )

        elapsed = time.time() - start_time

        if result.get("success"):
            count = result.get("count", 0)
            source = result.get("source", "unknown")

            print(f"\n✅ 批量导入成功:")
            print(f"   - 数据源: {source}")
            print(f"   - 获取股票数: {count}")
            print(f"   - 耗时: {elapsed:.2f}秒")
            print(f"   - 平均速度: {count/elapsed:.2f} 只/秒")

            # 检查数据样本
            if result.get("data") and len(result["data"]) > 0:
                sample = result["data"][0]
                print(f"\n数据样本 (第1条):")
                print(f"   - 股票代码: {sample.get('symbol')}")
                print(
                    f"   - 字段数量: {len(sample.get('data', {})) if sample.get('data') else 0}"
                )

                # 显示部分字段
                if sample.get("data"):
                    data = sample["data"]
                    print(f"   - 样本字段: {list(data.keys())[:10]}...")
            elif count == 0:
                print(f"\n⚠️ 警告: 批量导入未获取到数据")
                print(f"   这可能是因为:")
                print(f"   - mootdx无法连接或访问数据文件")
                print(f"   - 报告期数据不存在")
                return False

            return True
        else:
            print(f"\n❌ 批量导入失败: {result.get('error')}")
            return False

    except Exception as e:
        print(f"\n❌ 测试失败: {e}")
        import traceback

        traceback.print_exc()
        return False


def test_batch_mode_in_sync():
    """测试同步管理器中的批量模式"""
    print("\n" + "=" * 80)
    print("测试2: 同步管理器批量模式")
    print("=" * 80)

    from simtradedata.preprocessor import DataProcessingEngine

    config = Config()
    db_manager = DatabaseManager(config=config)
    data_source_manager = DataSourceManager(config=config, db_manager=db_manager)
    processing_engine = DataProcessingEngine(
        db_manager=db_manager,
        data_source_manager=data_source_manager,
        config=config,
    )

    sync_manager = SyncManager(
        db_manager=db_manager,
        data_source_manager=data_source_manager,
        processing_engine=processing_engine,
        config=config,
    )

    # 使用少量股票测试（避免等待过久）
    test_symbols = [
        "000001.SZ",
        "000002.SZ",
        "000004.SZ",
        "000005.SZ",
        "000006.SZ",
        "000007.SZ",
        "000008.SZ",
        "000009.SZ",
        "000010.SZ",
        "000011.SZ",
        # 添加更多股票到50+以触发批量模式
    ]

    # 扩展到51只股票以触发批量模式（阈值=50）
    for i in range(12, 62):
        test_symbols.append(f"0000{i:02d}.SZ")

    target_date = date(2024, 12, 31)

    print(f"\n开始扩展数据同步测试:")
    print(f"   - 股票数量: {len(test_symbols)}")
    print(f"   - 目标日期: {target_date}")
    print(f"   - 批量阈值: 50")
    print(f"   - 预期: 启用批量模式")

    start_time = time.time()

    try:
        result = sync_manager._sync_extended_data(test_symbols, target_date)

        elapsed = time.time() - start_time

        print(f"\n✅ 同步完成:")
        print(f"   - 批量模式: {'是' if result.get('batch_mode') else '否'}")
        print(f"   - 处理股票数: {result.get('processed_symbols', 0)}")
        print(f"   - 财务数据: {result.get('financials_count', 0)} 条")
        print(f"   - 估值数据: {result.get('valuations_count', 0)} 条")
        print(f"   - 失败数: {result.get('failed_symbols', 0)}")
        print(f"   - 总耗时: {elapsed:.2f}秒")

        if result.get("processed_symbols", 0) > 0:
            print(f"   - 平均速度: {result['processed_symbols']/elapsed:.2f} 只/秒")

        # 验证批量模式是否启用
        if len(test_symbols) >= 50:
            if result.get("batch_mode"):
                print(f"\n✅ 批量模式验证通过")
                return True
            else:
                print(f"\n⚠️ 警告: 股票数>50但批量模式未启用")
                return False
        else:
            return True

    except Exception as e:
        print(f"\n❌ 测试失败: {e}")
        import traceback

        traceback.print_exc()
        return False


def test_performance_comparison():
    """性能对比: 批量模式 vs 逐个查询（估算）"""
    print("\n" + "=" * 80)
    print("测试3: 性能对比估算")
    print("=" * 80)

    # 基于速度测试报告的数据
    baostock_per_stock = 5.78  # 秒/股
    mootdx_per_stock = 201.55  # 秒/股（逐个查询）
    mootdx_batch = 201.55  # 秒（批量导入所有股票）

    stock_counts = [100, 500, 1000, 5000]

    print("\n预期性能对比:")
    print(
        f"{'股票数':>8s} | {'BaoStock逐个':>15s} | {'Mootdx逐个':>15s} | {'Mootdx批量':>15s} | {'vs BaoStock':>12s} | {'vs Mootdx逐个':>15s}"
    )
    print("-" * 100)

    for count in stock_counts:
        baostock_time = count * baostock_per_stock
        mootdx_single_time = count * mootdx_per_stock
        mootdx_batch_time = mootdx_batch

        speedup_vs_baostock = baostock_time / mootdx_batch_time
        speedup_vs_mootdx_single = mootdx_single_time / mootdx_batch_time

        def format_time(seconds):
            if seconds < 60:
                return f"{seconds:.1f}秒"
            elif seconds < 3600:
                return f"{seconds/60:.1f}分钟"
            else:
                return f"{seconds/3600:.1f}小时"

        print(
            f"{count:>8d} | {format_time(baostock_time):>15s} | {format_time(mootdx_single_time):>15s} | "
            f"{format_time(mootdx_batch_time):>15s} | {speedup_vs_baostock:>10.1f}x | {speedup_vs_mootdx_single:>13.0f}x"
        )

    print("\n结论:")
    print("  - 批量模式对比BaoStock逐个查询: 在5000股场景下快 ~85倍 (从8小时降到3分钟)")
    print(
        "  - 批量模式对比Mootdx逐个查询: 在5000股场景下快 ~5000倍 (从278小时降到3分钟)"
    )
    print("  - 批量模式使得大规模同步从不可行变为可行")


if __name__ == "__main__":
    print("\n" + "=" * 80)
    print("批量财务数据导入优化测试")
    print("=" * 80)

    # 测试1: 批量导入API
    success1 = test_batch_financial_import()

    # 测试2: 同步管理器批量模式
    success2 = test_batch_mode_in_sync()

    # 测试3: 性能对比
    test_performance_comparison()

    # 总结
    print("\n" + "=" * 80)
    print("测试总结")
    print("=" * 80)
    print(f"批量导入API: {'✅ 通过' if success1 else '❌ 失败'}")
    print(f"同步批量模式: {'✅ 通过' if success2 else '❌ 失败'}")

    if success1 and success2:
        print("\n🎉 所有测试通过！批量优化功能正常工作。")
    else:
        print("\n⚠️ 部分测试失败，请检查日志")
