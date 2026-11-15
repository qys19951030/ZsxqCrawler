#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""可中断、可续跑的话题批次采集进度管理。

每个群组维护一条水位（import_progress 表），记录最近一个完整入库批次
边界话题的 create_time 与 topic_id；进程重启后从水位之后继续扫描，
被平台反爬拦下的批次不推进水位。
"""

from typing import Any, Callable, Dict, List, Optional

from backend.zsxq_database import ZSXQDatabase


class BatchBlockedError(Exception):
    """平台反爬/网络原因导致整批话题未能获取，按失败批次处理。"""


class ImportProgressStore:
    """import_progress 表的读写封装：群组水位与批次成败计数。"""

    TABLE_NAME = "import_progress"

    def __init__(self, db: ZSXQDatabase):
        pass

    def ensure_table(self) -> None:
        """确保进度表存在（幂等）。"""
        pass

    def get_watermark(self, group_id: int) -> Optional[Dict[str, Any]]:
        """读取群组水位，无记录返回 None。"""
        pass

    def upsert_watermark(
        self,
        group_id: int,
        create_time: str,
        topic_id: int,
        last_batch_at: Optional[str] = None,
    ) -> None:
        """写入/更新群组水位。"""
        pass

    def record_batch_result(self, group_id: int, success: bool) -> Dict[str, int]:
        """累加成功/失败批次数，返回更新后的计数。"""
        pass

    def get_progress(self, group_id: int) -> Dict[str, Any]:
        """返回单个群组的进度视图（水位 + 批次计数）。"""
        pass

    def list_progress(self) -> List[Dict[str, Any]]:
        """返回库中所有群组的进度视图。"""
        pass


class BatchCrawlRunner:
    """按批次驱动采集：整批整体生效，失败整批回滚、水位不动。"""

    def __init__(
        self,
        db: ZSXQDatabase,
        group_id: int,
        fetch_batch: Callable[[Optional[str]], Optional[List[Dict[str, Any]]]],
        log: Optional[Callable[[str], None]] = None,
    ):
        pass

    def run_batch(self) -> Dict[str, Any]:
        """执行一个批次：抓取 -> 整批事务导入 -> 成功推进水位。"""
        pass

    def run(self, max_batches: Optional[int] = None) -> Dict[str, Any]:
        """连续执行批次，直到没有更多数据、被拦或达到上限。"""
        pass
