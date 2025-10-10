"""
进度条管理器

为全量同步的各个阶段提供清晰的进度显示。
"""

import logging
import sys
from contextlib import contextmanager
from typing import Any, Dict, Iterator, Optional

from tqdm import tqdm

logger = logging.getLogger(__name__)


class SyncProgressBar:
    """同步进度条管理器"""

    def __init__(self, disable_logs: bool = True):
        """
        初始化进度条管理器

        Args:
            disable_logs: 是否禁用详细日志输出
        """
        self.disable_logs = disable_logs
        self.current_phase = None
        self.phase_progress_bars = {}
        self.start_time = None

        # 如果禁用日志，设置日志级别为WARNING
        if disable_logs:
            # 设置特定模块的日志级别
            modules_to_quiet = [
                "simtradedata.preprocessor.engine",
                "simtradedata.sync.incremental",
                "simtradedata.data_sources.manager",
                "simtradedata.data_sources.baostock_adapter",
                "simtradedata.data_sources.mootdx_adapter",
                "simtradedata.core.logging_mixin",
                "simtradedata.config.manager",
                "simtradedata.database.manager",
                "simtradedata.data_sources.base",
                "simtradedata.sync.validator",
                "urllib3.connectionpool",
            ]

            for module_name in modules_to_quiet:
                module_logger = logging.getLogger(module_name)
                module_logger.setLevel(logging.WARNING)

    @contextmanager
    def phase_progress(
        self,
        phase_name: str,
        total: int,
        desc: Optional[str] = None,
        unit: str = "item",
        phase_info: str = "",
    ) -> Iterator[Optional["SimpleProgress"]]:
        """
        创建阶段进度条

        Args:
            phase_name: 阶段名称
            total: 总数量
            desc: 描述
            unit: 单位
            phase_info: 阶段信息（如 "阶段1/4"）

        Yields:
            SimpleProgress进度条对象
        """
        if desc is None:
            desc = phase_name

        self.current_phase = phase_name

        # 创建简单的进度显示器（传入阶段信息）
        progress = SimpleProgress(total, desc, phase_info)
        # 立即设置进度管理器引用
        progress.progress_manager = self
        self.phase_progress_bars[phase_name] = progress

        interrupted = False
        try:
            yield progress
        except KeyboardInterrupt:
            # 检测到中断，快速清理并重新抛出
            interrupted = True
            print(
                f"\r{' ' * 100}\r", end="", flush=True, file=sys.stderr
            )  # 快速清除进度行
            raise  # 立即重新抛出，不做任何延迟操作
        finally:
            # 如果被中断，跳过所有清理操作
            if not interrupted:
                # 关闭进度条
                try:
                    progress.close()
                except:
                    pass  # 忽略close时的任何错误
            # 清理引用（快速操作）
            if phase_name in self.phase_progress_bars:
                del self.phase_progress_bars[phase_name]

    def update_phase_description(self, desc: str):
        """更新当前阶段的描述"""
        if self.current_phase and self.current_phase in self.phase_progress_bars:
            pbar = self.phase_progress_bars[self.current_phase]
            if hasattr(pbar, "set_description"):
                pbar.set_description(f"🔄 {desc}")

    def log_phase_start(self, phase_name: str, desc: Optional[str] = None):
        """记录阶段开始"""
        if not self.disable_logs:
            logger.info(f"🚀 {phase_name}: {desc or '开始'}")

    def log_phase_complete(
        self, phase_name: str, stats: Optional[Dict[str, Any]] = None
    ):
        """记录阶段完成"""
        if stats:
            stats_str = ", ".join([f"{k}={v}" for k, v in stats.items()])
            message = f"✅ {phase_name}完成: {stats_str}"
        else:
            message = f"✅ {phase_name}完成"

        # 使用tqdm.write避免干扰进度条
        from tqdm import tqdm

        tqdm.write(message, file=sys.stderr)

        if not self.disable_logs:
            logger.info(message)

    def log_error(self, message: str):
        """记录错误（总是显示）"""
        logger.error(f"❌ {message}")

    def log_warning(self, message: str):
        """记录警告（总是显示）"""
        logger.warning(f"⚠️  {message}")


class SimpleProgress:
    """进度显示器（基于tqdm）"""

    def __init__(self, total: int, desc: str = "Processing", phase_info: str = ""):
        self.total = total
        self.desc = desc
        self.phase_info = phase_info
        self.progress_manager = None

        # 构建完整描述（包含阶段信息）
        full_desc = f"{phase_info} {desc}" if phase_info else desc

        # 创建tqdm进度条
        self.pbar = tqdm(
            total=total,
            desc=full_desc,
            ncols=None,  # 自动检测终端宽度
            file=sys.stderr,
            # 禁用平滑更新，减少刷新次数
            smoothing=0.1,
            # 使用ASCII字符以避免编码问题
            ascii=False,
            # 显示速率和预估时间
            unit="it",
            unit_scale=False,
            # 确保进度条在同一行更新
            dynamic_ncols=True,
            # 关闭miniters以确保每次更新都刷新
            miniters=1,
            # 设置最小刷新间隔（避免过度刷新）
            mininterval=0.5,
        )

    def update(self, n: int = 1):
        """更新进度"""
        self.pbar.update(n)

    def set_description(self, desc: str):
        """设置描述"""
        self.desc = desc
        full_desc = f"{self.phase_info} {desc}" if self.phase_info else desc
        self.pbar.set_description(full_desc)

    def close(self):
        """关闭进度条"""
        self.pbar.close()


# 全局进度条管理器实例
sync_progress = SyncProgressBar()


@contextmanager
def create_phase_progress(
    phase_name: str,
    total: int,
    desc: Optional[str] = None,
    unit: str = "item",
    phase_info: str = "",
):
    """创建阶段进度条的便捷函数"""
    with sync_progress.phase_progress(
        phase_name, total, desc, unit, phase_info
    ) as pbar:
        yield pbar


def log_phase_start(phase_name: str, desc: Optional[str] = None):
    """记录阶段开始"""
    sync_progress.log_phase_start(phase_name, desc)


def log_phase_complete(phase_name: str, stats: Optional[Dict[str, Any]] = None):
    """记录阶段完成"""
    sync_progress.log_phase_complete(phase_name, stats)


def update_phase_description(desc: str):
    """更新当前阶段描述"""
    sync_progress.update_phase_description(desc)


def log_error(message: str):
    """记录错误"""
    sync_progress.log_error(message)


def log_warning(message: str):
    """记录警告"""
    sync_progress.log_warning(message)
