"""
批处理调度器

负责定时任务调度、任务状态监控和错误处理。
"""

import logging
import threading
import time
from datetime import date, datetime, timedelta
from typing import Any, Dict, List

try:
    import schedule
except ImportError:
    schedule = None

from ..config import Config
from ..core import BaseManager
from ..data_sources import DataSourceManager
from ..database import DatabaseManager
from .engine import DataProcessingEngine

logger = logging.getLogger(__name__)


class BatchScheduler(BaseManager):
    """批处理调度器"""

    def __init__(
        self,
        db_manager: DatabaseManager,
        data_source_manager: DataSourceManager,
        config: Config = None,
        **kwargs,
    ):
        """
        初始化批处理调度器

        Args:
            db_manager: 数据库管理器
            data_source_manager: 数据源管理器
            config: 配置对象
        """
        # 设置依赖
        self.db_manager = db_manager
        self.data_source_manager = data_source_manager
        if not self.db_manager:
            raise ValueError("数据库管理器不能为空")
        if not self.data_source_manager:
            raise ValueError("数据源管理器不能为空")

        # 调用BaseManager初始化
        super().__init__(
            config=config,
            db_manager=db_manager,
            data_source_manager=data_source_manager,
            **kwargs,
        )

    def _init_specific_config(self):
        """初始化批处理调度器特定配置"""
        # 调度配置
        self.sync_schedule = self._get_config("sync_schedule", "09:00")
        self.enable_weekend_sync = self._get_config("enable_weekend_sync", False)
        self.max_retry_count = self._get_config("max_retry_count", 3)
        self.retry_delay_minutes = self._get_config("retry_delay_minutes", 30)

    def _init_components(self):
        """初始化批处理调度器组件"""
        # 初始化数据处理引擎
        self.processing_engine = DataProcessingEngine(
            self.db_manager, self.data_source_manager, self.config
        )

        # 延迟初始化同步管理器以避免循环导入
        self._sync_manager = None

        # 调度配置 - 已在_init_specific_config中设置
        self.enable_scheduler = self._get_config("enabled", True)
        self.daily_sync_time = self._get_config("daily_sync_time", "02:00")
        self.max_workers = self._get_config("max_workers", 3)
        self.retry_times = self._get_config("retry_times", 3)
        self.retry_delay = self._get_config("retry_delay", 300)  # 5分钟

        # 运行状态
        self.is_running = False
        self.current_tasks = {}
        self.task_history = []
        self.scheduler_thread = None

        self.logger.info("批处理调度器初始化完成")

    def _get_required_attributes(self) -> List[str]:
        """必需属性列表"""
        return ["db_manager", "data_source_manager", "processing_engine"]

    def start_scheduler(self):
        """启动调度器"""
        if not self.enable_scheduler:
            logger.info("调度器已禁用")
            return

        if self.is_running:
            logger.warning("调度器已在运行")
            return

        if schedule is None:
            logger.error("schedule模块未安装，无法启动调度器")
            return

        try:
            # 设置定时任务
            schedule.clear()
            schedule.every().day.at(self.daily_sync_time).do(self._run_daily_sync)

            # 启动调度线程
            self.is_running = True
            self.scheduler_thread = threading.Thread(
                target=self._scheduler_loop, daemon=True
            )
            self.scheduler_thread.start()

            logger.info(f"批处理调度器已启动，每日同步时间: {self.daily_sync_time}")

        except Exception as e:
            logger.error(f"启动调度器失败: {e}")
            self.is_running = False

    def stop_scheduler(self):
        """停止调度器"""
        if not self.is_running:
            return

        try:
            self.is_running = False
            if schedule:
                schedule.clear()

            if self.scheduler_thread and self.scheduler_thread.is_alive():
                self.scheduler_thread.join(timeout=5)

            logger.info("批处理调度器已停止")

        except Exception as e:
            logger.error(f"停止调度器失败: {e}")

    def _scheduler_loop(self):
        """调度器主循环"""
        while self.is_running:
            try:
                if schedule:
                    schedule.run_pending()
                time.sleep(60)  # 每分钟检查一次
            except Exception as e:
                logger.error(f"调度器循环异常: {e}")
                time.sleep(60)

    def _run_daily_sync(self):
        """运行每日同步任务"""
        task_id = f"daily_sync_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        try:
            logger.info(f"开始每日同步任务: {task_id}")

            # 记录任务开始
            task_info = {
                "task_id": task_id,
                "task_type": "daily_sync",
                "start_time": datetime.now(),
                "status": "running",
                "target_date": datetime.now().date(),
            }
            self.current_tasks[task_id] = task_info

            # 执行同步
            result = self.run_daily_sync()

            # 更新任务状态
            task_info.update(
                {
                    "status": "completed",
                    "end_time": datetime.now(),
                    "result": result,
                }
            )

            # 移动到历史记录
            self.task_history.append(task_info)
            del self.current_tasks[task_id]

            logger.info(f"每日同步任务完成: {task_id}")

        except Exception as e:
            logger.error(f"每日同步任务失败: {task_id}, 错误: {e}")

            # 更新任务状态
            if task_id in self.current_tasks:
                self.current_tasks[task_id].update(
                    {
                        "status": "failed",
                        "end_time": datetime.now(),
                        "error": str(e),
                    }
                )

                # 移动到历史记录
                self.task_history.append(self.current_tasks[task_id])
                del self.current_tasks[task_id]

    def run_daily_sync(self, target_date: date = None) -> Dict[str, Any]:
        """
        运行每日数据同步 (委托给SyncManager)

        Args:
            target_date: 目标日期，默认为今天

        Returns:
            Dict[str, Any]: 同步结果
        """
        if target_date is None:
            target_date = datetime.now().date()

        logger.info(f"调度器启动每日数据同步: {target_date}")

        try:
            # 委托给专业的同步管理器
            result = self.sync_manager.run_full_sync(
                target_date=target_date,
                symbols=None,  # 使用默认的所有活跃股票
                frequencies=["1d"],  # 默认日线数据
            )

            logger.info(f"每日数据同步完成: {target_date}")
            return result

        except Exception as e:
            logger.error(f"每日数据同步失败: {target_date}, 错误: {e}")
            raise

    def run_historical_sync(
        self, start_date: date, end_date: date = None, symbols: List[str] = None
    ) -> Dict[str, Any]:
        """
        运行历史数据同步 (委托给SyncManager)

        Args:
            start_date: 开始日期
            end_date: 结束日期，默认为今天
            symbols: 股票代码列表，默认为所有股票

        Returns:
            Dict[str, Any]: 同步结果
        """
        if end_date is None:
            end_date = datetime.now().date()

        logger.info(f"调度器启动历史数据同步: {start_date} 到 {end_date}")

        try:
            # 对于历史数据同步，我们需要逐日处理
            if symbols is None:
                # 使用最后一个日期作为目标，让SyncManager处理范围
                result = self.sync_manager.run_full_sync(
                    target_date=end_date, symbols=symbols, frequencies=["1d"]
                )
            else:
                # 如果指定了股票列表，使用增量同步器处理范围
                results = []
                for symbol in symbols:
                    symbol_result = (
                        self.sync_manager.incremental_sync.sync_symbol_range(
                            symbol, start_date, end_date, "1d"
                        )
                    )
                    results.append(symbol_result)

                # 汇总结果
                result = {
                    "start_date": str(start_date),
                    "end_date": str(end_date),
                    "symbol_results": results,
                    "total_symbols": len(symbols),
                    "success_count": sum(r["success_count"] for r in results),
                    "error_count": sum(r["error_count"] for r in results),
                }

            logger.info(f"历史数据同步完成: {start_date} 到 {end_date}")
            return result

        except Exception as e:
            logger.error(f"历史数据同步失败: {e}")
            raise

    def run_parallel_sync(
        self, target_date: date, symbols: List[str]
    ) -> Dict[str, Any]:
        """
        并行数据同步 (委托给SyncManager)

        Args:
            target_date: 目标日期
            symbols: 股票代码列表

        Returns:
            Dict[str, Any]: 同步结果
        """
        logger.info(f"调度器启动并行数据同步: {target_date}, 股票数量: {len(symbols)}")

        try:
            # 委托给专业的同步管理器，它内部已经实现了并行处理
            result = self.sync_manager.run_full_sync(
                target_date=target_date, symbols=symbols, frequencies=["1d"]
            )

            logger.info(f"并行数据同步完成: {target_date}")
            return result

        except Exception as e:
            logger.error(f"并行数据同步失败: {e}")
            raise

    # _process_symbol_batch 方法已移除 - 批处理逻辑现在由 SyncManager 处理

    def _update_stock_list(self):
        """更新股票列表"""
        try:
            logger.info("更新股票列表...")
            stock_list = self.data_source_manager.get_stock_info()

            if (isinstance(stock_list, list) and stock_list) or (
                hasattr(stock_list, "shape") and not stock_list.empty
            ):
                # 委托给数据处理引擎更新股票信息
                self.processing_engine._update_stock_info(stock_list)
                logger.info(f"股票列表更新完成，数量: {len(stock_list)}")
            else:
                logger.warning("未获取到股票列表")

        except Exception as e:
            logger.error(f"更新股票列表失败: {e}")

    def _update_trading_calendar(self, target_date: date):
        """更新交易日历"""
        try:
            # 更新前后一个月的交易日历
            start_date = target_date - timedelta(days=30)
            end_date = target_date + timedelta(days=30)

            calendar_data = self.data_source_manager.get_trade_calendar(
                start_date, end_date
            )

            if calendar_data:
                sql = """
                INSERT OR REPLACE INTO trading_calendar
                (date, market, is_trading)
                VALUES (?, ?, ?)
                """

                data_to_insert = []
                for day_info in calendar_data:
                    # 插入所有日期（包括交易日和非交易日）
                    data_to_insert.append(
                        (
                            day_info.get("trade_date", ""),
                            "CN",  # 中国A股市场（包括SZ和SS）
                            day_info.get("is_trading", 0) == 1,  # 布尔值
                        )
                    )

                self.db_manager.executemany(sql, data_to_insert)
                logger.info(f"交易日历更新完成，交易日数量: {len(data_to_insert)}")

        except Exception as e:
            logger.error(f"更新交易日历失败: {e}")

    def _is_trading_day(self, target_date: date) -> bool:
        """检查是否为交易日"""
        try:
            sql = """
            SELECT 1 FROM trading_calendar
            WHERE date = ? AND market = 'CN' AND is_trading = 1
            """
            result = self.db_manager.fetchone(sql, (str(target_date),))

            # 存在于表中 = 交易日，不存在 = 非交易日
            return result is not None

        except Exception as e:
            logger.error(f"检查交易日失败: {e}")
            # 不再使用简化fallback，必须有正确的交易日历数据
            raise RuntimeError(f"无法获取交易日历数据，请确保交易日历已正确初始化: {e}")

    def _update_sync_status(self, target_date: date, result: Dict[str, Any]):
        """更新同步状态"""
        try:
            # 使用正确的字段名
            sql = """
            INSERT OR REPLACE INTO sync_status
            (symbol, market, frequency, last_sync_date, last_data_date, sync_status,
             error_message, sync_count, last_update)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """

            status = "completed" if result["error_count"] == 0 else "failed"
            error_msg = f"成功={result['success_count']}, 错误={result['error_count']}"

            # 为调度器创建一个汇总记录
            self.db_manager.execute(
                sql,
                (
                    "SCHEDULER",  # symbol
                    "ALL",  # market
                    "1d",  # frequency
                    str(target_date),  # last_sync_date
                    str(target_date),  # last_data_date
                    status,  # sync_status
                    error_msg,  # error_message
                    1,  # sync_count
                    datetime.now().isoformat(),  # last_update
                ),
            )

        except Exception as e:
            logger.error(f"更新同步状态失败: {e}")

    def get_scheduler_status(self) -> Dict[str, Any]:
        """获取调度器状态"""
        return {
            "is_running": self.is_running,
            "enable_scheduler": self.enable_scheduler,
            "daily_sync_time": self.daily_sync_time,
            "max_workers": self.max_workers,
            "current_tasks": len(self.current_tasks),
            "task_history_count": len(self.task_history),
            "last_sync": self.task_history[-1] if self.task_history else None,
        }

    def get_task_history(self, limit: int = 10) -> List[Dict[str, Any]]:
        """获取任务历史"""
        return self.task_history[-limit:] if self.task_history else []

    @property
    def sync_manager(self):
        """延迟创建同步管理器"""
        if self._sync_manager is None:
            from ..sync import SyncManager

            self._sync_manager = SyncManager(
                self.db_manager,
                self.data_source_manager,
                self.processing_engine,
                self.config,
            )
        return self._sync_manager
