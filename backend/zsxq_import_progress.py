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
        column = "success_batches" if success else "failed_batches"
        # 先确保群组行存在（水位字段留空），再做条件累加
        self.cursor.execute(f'''
            INSERT INTO {self.TABLE_NAME}
                (group_id, watermark_time, watermark_topic_id,
                 success_batches, failed_batches)
            VALUES (?, NULL, NULL, 0, 0)
            ON CONFLICT(group_id) DO NOTHING
        ''', (group_id,))
        self.cursor.execute(f'''
            UPDATE {self.TABLE_NAME}
            SET {column} = {column} + 1,
                last_batch_at = CURRENT_TIMESTAMP,
                updated_at = CURRENT_TIMESTAMP
            WHERE group_id = ?
        ''', (group_id,))
        self.cursor.execute(f'''
            SELECT success_batches, failed_batches
            FROM {self.TABLE_NAME} WHERE group_id = ?
        ''', (group_id,))
        row = self.cursor.fetchone()
        return {"success_batches": row[0], "failed_batches": row[1]}

    def _row_to_progress(self, row: tuple) -> Dict[str, Any]:
        return {
            "group_id": row[0],
            "watermark_time": row[1],
            "watermark_topic_id": row[2],
            "success_batches": row[3],
            "failed_batches": row[4],
            "last_batch_at": row[5],
            "updated_at": row[6],
        }

    def get_progress(self, group_id: int) -> Dict[str, Any]:
        """返回单个群组的进度视图（水位 + 批次计数）。"""
        self.cursor.execute(f'''
            SELECT group_id, watermark_time, watermark_topic_id,
                   success_batches, failed_batches, last_batch_at, updated_at
            FROM {self.TABLE_NAME} WHERE group_id = ?
        ''', (group_id,))
        row = self.cursor.fetchone()
        if not row:
            return {
                "group_id": group_id,
                "watermark_time": None,
                "watermark_topic_id": None,
                "success_batches": 0,
                "failed_batches": 0,
                "last_batch_at": None,
                "updated_at": None,
            }
        return self._row_to_progress(row)

    def list_progress(self) -> List[Dict[str, Any]]:
        """返回库中所有群组的进度视图。"""
        self.cursor.execute(f'''
            SELECT group_id, watermark_time, watermark_topic_id,
                   success_batches, failed_batches, last_batch_at, updated_at
            FROM {self.TABLE_NAME}
            ORDER BY group_id
        ''')
        return [self._row_to_progress(row) for row in self.cursor.fetchall()]


class BatchCrawlRunner:
    """按批次驱动采集：整批整体生效，失败整批回滚、水位不动。"""

    def __init__(
        self,
        db: ZSXQDatabase,
        group_id: int,
        fetch_batch: Callable[[Optional[Dict[str, Any]]], Optional[List[Dict[str, Any]]]],
        log: Optional[Callable[[str], None]] = None,
    ):
        """
        :param fetch_batch: 回调，入参为当前水位（None 表示从头扫），
            返回话题列表；返回 None 或抛出 BatchBlockedError 表示被平台拦截。
        """
        self.db = db
        self.group_id = int(group_id)
        self.fetch_batch = fetch_batch
        self._log = log or (lambda message: None)
        self.store = ImportProgressStore(db)

    def _record_failure(self, error: Exception) -> Dict[str, Any]:
        """登记一个失败批次（独立事务），水位保持不动。"""
        counts = self.store.record_batch_result(self.group_id, success=False)
        self.db.conn.commit()
        return {
            "status": "blocked" if isinstance(error, BatchBlockedError) else "failed",
            "topics": 0,
            "new_topics": 0,
            "updated_topics": 0,
            "watermark": self.store.get_watermark(self.group_id),
            "success_batches": counts["success_batches"],
            "failed_batches": counts["failed_batches"],
            "error": str(error),
        }

    def run_batch(self) -> Dict[str, Any]:
        """执行一个批次：抓取 -> 整批事务导入 -> 成功推进水位。

        整批话题在同一个 SAVEPOINT 内导入，任一条失败或抓取被拦截，
        整批回滚、一条不留，水位停在上一批不动。
        """
        watermark = self.store.get_watermark(self.group_id)

        # 1. 抓取批次（被反爬拦下：返回 None 或抛 BatchBlockedError）
        try:
            topics = self.fetch_batch(watermark)
        except BatchBlockedError as exc:
            self._log(f"🚫 群组 {self.group_id} 批次被平台拦截，本批不计入库: {exc}")
            return self._record_failure(exc)
        if topics is None:
            return self._record_failure(
                BatchBlockedError("抓取返回空响应，批次按拦截失败处理")
            )
        if not topics:
            self._log(f"📭 群组 {self.group_id} 已无更多话题")
            return {
                "status": "exhausted",
                "topics": 0,
                "new_topics": 0,
                "updated_topics": 0,
                "watermark": watermark,
                "success_batches": self.store.get_progress(self.group_id)["success_batches"],
                "failed_batches": self.store.get_progress(self.group_id)["failed_batches"],
                "error": None,
            }

        # 2. 写库前统计新增/更新（import_topic_data 本身幂等，此处仅用于汇报）
        existing_ids = set()
        for topic in topics:
            tid = topic.get("topic_id")
            if tid and self.db.topic_exists(tid):
                existing_ids.add(tid)

        # 3. 整批事务：任何一条话题抛异常，保存点回滚整批
        try:
            with self.db.transaction("batch_import"):
                for topic in topics:
                    # 原子方法失败会抛异常并先回滚该话题的保存点，
                    # 异常继续向上传播触发整批回滚
                    if not self.db.import_topic_data_atomic(topic):
                        raise RuntimeError(
                            f"话题 {topic.get('topic_id')} 缺少 topic_id，导入失败"
                        )

                boundary = topics[-1]
                boundary_time = boundary.get("create_time")
                boundary_id = boundary.get("topic_id")
                if not boundary_time or not boundary_id:
                    raise RuntimeError(
                        "批次边界话题缺少 create_time/topic_id，无法推进水位"
                    )
                self.store.upsert_watermark(
                    self.group_id, boundary_time, boundary_id
                )
                counts = self.store.record_batch_result(
                    self.group_id, success=True
                )
            self.db.conn.commit()
        except Exception as exc:
            # SAVEPOINT 已回滚整批；失败计数单独落盘，水位不变
            self._log(f"❌ 群组 {self.group_id} 批次导入失败，整批撤销: {exc}")
            return self._record_failure(exc)

        self._log(
            f"✅ 群组 {self.group_id} 批次入库 {len(topics)} 条，"
            f"水位推进至 {boundary_id}"
        )
        return {
            "status": "success",
            "topics": len(topics),
            "new_topics": len([t for t in topics if t.get("topic_id") not in existing_ids]),
            "updated_topics": len([t for t in topics if t.get("topic_id") in existing_ids]),
            "watermark": self.store.get_watermark(self.group_id),
            "success_batches": counts["success_batches"],
            "failed_batches": counts["failed_batches"],
            "error": None,
        }

    def run(self, max_batches: Optional[int] = None) -> Dict[str, Any]:
        """连续执行批次，直到没有更多数据、被拦或达到上限。

        被拦/失败的批次会立即停下，已成功的批次与水位不受影响；
        进程重启后重新构造 runner 调用本方法即可从水位之后续跑。
        """
        totals = {
            "batches": 0,
            "success_batches": 0,
            "failed_batches": 0,
            "topics": 0,
            "new_topics": 0,
            "updated_topics": 0,
            "stop_reason": None,
            "last_error": None,
        }
        while max_batches is None or totals["batches"] < max_batches:
            result = self.run_batch()
            totals["batches"] += 1
            status = result["status"]

            if status == "success":
                totals["success_batches"] += 1
                totals["topics"] += result["topics"]
                totals["new_topics"] += result["new_topics"]
                totals["updated_topics"] += result["updated_topics"]
                continue

            if status == "exhausted":
                totals["stop_reason"] = "exhausted"
                break

            # blocked / failed：水位未动，下一轮仍从水位之后取
            totals["failed_batches"] += 1
            totals["stop_reason"] = status
            totals["last_error"] = result.get("error")
            break

        if totals["stop_reason"] is None:
            totals["stop_reason"] = "max_batches_reached"

        totals["watermark"] = self.store.get_watermark(self.group_id)
        return totals
