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
        self.db = db
        self.cursor = db.cursor
        self.ensure_table()

    def ensure_table(self) -> None:
        """确保进度表存在（幂等）。"""
        self.cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS {self.TABLE_NAME} (
                group_id INTEGER PRIMARY KEY,
                watermark_time TEXT,
                watermark_topic_id INTEGER,
                success_batches INTEGER NOT NULL DEFAULT 0,
                failed_batches INTEGER NOT NULL DEFAULT 0,
                last_batch_at TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        self.db.conn.commit()

    def get_watermark(self, group_id: int) -> Optional[Dict[str, Any]]:
        """读取群组水位，无记录返回 None。"""
        self.cursor.execute(
            f'''SELECT watermark_time, watermark_topic_id
                FROM {self.TABLE_NAME} WHERE group_id = ?''',
            (group_id,),
        )
        row = self.cursor.fetchone()
        if not row or row[0] is None:
            return None
        return {"create_time": row[0], "topic_id": row[1]}

    def upsert_watermark(
        self,
        group_id: int,
        create_time: str,
        topic_id: int,
        last_batch_at: Optional[str] = None,
    ) -> None:
        """写入/更新群组水位。"""
        self.cursor.execute(f'''
            INSERT INTO {self.TABLE_NAME}
                (group_id, watermark_time, watermark_topic_id,
                 success_batches, failed_batches, last_batch_at, updated_at)
            VALUES (?, ?, ?, 0, 0, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(group_id) DO UPDATE SET
                watermark_time = excluded.watermark_time,
                watermark_topic_id = excluded.watermark_topic_id,
                last_batch_at = COALESCE(excluded.last_batch_at,
                                         {self.TABLE_NAME}.last_batch_at),
                updated_at = CURRENT_TIMESTAMP
        ''', (group_id, create_time, topic_id, last_batch_at))

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
