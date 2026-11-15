#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""批次采集的原子性、续跑、幂等与反爬失败处理验收测试。

全程使用临时目录中的 SQLite，不发起任何网络请求；抓取行为由内存中的
假回调驱动。
"""

import os
import tempfile
import unittest
from unittest import mock

from backend.zsxq_database import ZSXQDatabase
from backend.zsxq_import_progress import (
    BatchBlockedError,
    BatchCrawlRunner,
    ImportProgressStore,
)

# 验收覆盖的全部话题相关表（含进度表）
ALL_TABLES = [
    "groups", "users", "topics", "talks", "articles", "images",
    "likes", "like_emojis", "user_liked_emojis", "comments",
    "questions", "answers", "topic_tags", "tags", "topic_files",
    "import_progress",
]


def make_topic(topic_id, create_time, group_id=100):
    """构造一条横跨所有主要表的完整话题数据。"""
    return {
        "topic_id": topic_id,
        "group": {"group_id": group_id, "name": "测试群", "type": "group"},
        "type": "talk",
        "title": f"话题{topic_id}",
        "create_time": create_time,
        "likes_count": 1,
        "comments_count": 1,
        "talk": {
            "owner": {"user_id": 1000 + topic_id, "name": f"楼主{topic_id}"},
            "text": (
                "正文 <e type=\"hashtag\" hid=\"h1\" title=\"%E6%A0%87%E7%AD%BE\" />"
            ),
            "images": [
                {
                    "image_id": 5000 + topic_id,
                    "type": "jpeg",
                    "thumbnail": {"url": "t", "width": 1, "height": 2},
                    "large": {"url": "l"},
                    "original": {"url": "o", "size": 9},
                }
            ],
            "files": [
                {
                    "file_id": 7000 + topic_id,
                    "name": "f.zip",
                    "hash": "h",
                    "size": 123,
                }
            ],
        },
        "latest_likes": [
            {
                "owner": {"user_id": 2000 + topic_id, "name": "点赞者"},
                "create_time": create_time,
            }
        ],
        "likes_detail": {
            "emojis": [{"emoji_key": "👍", "likes_count": 2}]
        },
        "user_specific": {"liked_emojis": ["🌹"]},
        "show_comments": [
            {
                "comment_id": 6000 + topic_id,
                "owner": {"user_id": 3000 + topic_id, "name": "评论者"},
                "text": "评论",
                "create_time": create_time,
                "images": [
                    {"image_id": 5500 + topic_id, "type": "png",
                     "thumbnail": {"url": "ct"}}
                ],
            }
        ],
        "question": {
            "owner": {"user_id": 4000 + topic_id, "name": "提问者"},
            "questionee": {"user_id": 4100 + topic_id, "name": "答主"},
            "text": "问题",
        },
        "answer": {
            "owner": {"user_id": 4100 + topic_id, "name": "答主"},
            "text": "回答",
        },
    }


class BatchImportAcceptanceTest(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self._tmp.name, "accept.db")
        self.db = ZSXQDatabase(self.db_path)
        self.store = ImportProgressStore(self.db)

    def tearDown(self):
        self.db.close()
        self._tmp.cleanup()

    def _counts(self):
        return {
            table: self.db.cursor.execute(
                f"SELECT COUNT(*) FROM {table}"
            ).fetchone()[0]
            for table in ALL_TABLES
        }

    def _topic_rows(self, topic_id):
        """该话题在所有子表中的残留行数。"""
        result = {}
        for table in [
            "topics", "talks", "articles", "images", "likes",
            "like_emojis", "user_liked_emojis", "comments",
            "questions", "answers", "topic_files", "topic_tags",
        ]:
            result[table] = self.db.cursor.execute(
                f"SELECT COUNT(*) FROM {table} WHERE topic_id = ?",
                (topic_id,),
            ).fetchone()[0]
        return result

    # 情形 1：话题写到一半抛异常，库里查不到该话题任何残留
    def test_topic_write_failure_leaves_no_residue(self):
        good = make_topic(1, "2024-03-01T10:00:00.000+0800")
        self.assertTrue(self.db.import_topic_data(good))
        self.db.conn.commit()
        before = self._counts()

        broken = make_topic(2, "2024-03-02T10:00:00.000+0800")
        with mock.patch.object(
            self.db, "_import_files", side_effect=RuntimeError("磁盘炸了")
        ):
            ok = self.db.import_topic_data(broken)
        self.assertFalse(ok)  # 旧入口的 bool 语义保持不变
        self.db.conn.commit()

        residue = self._topic_rows(2)
        self.assertEqual(
            set(residue.values()), {0},
            msg=f"失败话题存在残留: {residue}",
        )
        # 已成功入库的话题与全库行数不受影响
        self.assertEqual(self._counts(), before)

    # 情形 2：一批里有一条坏数据时整批不进、水位不动
    def test_one_bad_topic_rolls_back_whole_batch_and_keeps_watermark(self):
        first_batch = [
            make_topic(1, "2024-03-03T10:00:00.000+0800"),
            make_topic(2, "2024-03-02T10:00:00.000+0800"),
        ]
        runner = BatchCrawlRunner(self.db, 100, lambda wm: first_batch)
        r1 = runner.run_batch()
        self.assertEqual(r1["status"], "success")
        self.assertEqual(r1["watermark"]["topic_id"], 2)
        counts_after_first = self._counts()

        # 第二批：让其中一条话题在写库中途抛异常
        second_batch = [
            make_topic(3, "2024-03-01T10:00:00.000+0800"),
            make_topic(4, "2024-02-29T10:00:00.000+0800"),
        ]
        original_import = self.db.import_topic_data_atomic

        def flaky_import(topic_data):
            if topic_data["topic_id"] == 4:
                raise RuntimeError("模拟坏数据")
            return original_import(topic_data)

        runner.fetch_batch = lambda wm: second_batch
        with mock.patch.object(self.db, "import_topic_data_atomic",
                               side_effect=flaky_import):
            r2 = runner.run_batch()

        self.assertEqual(r2["status"], "failed")
        # 水位停在上一批
        self.assertEqual(
            self.store.get_watermark(100)["topic_id"], 2
        )
        # 整批一条都没留下
        self.assertEqual(self._counts(), counts_after_first)
        self.assertEqual(
            self.db.cursor.execute(
                "SELECT COUNT(*) FROM topics WHERE topic_id IN (3, 4)"
            ).fetchone()[0],
            0,
        )
        progress = self.store.get_progress(100)
        self.assertEqual(progress["success_batches"], 1)
        self.assertEqual(progress["failed_batches"], 1)

    # 情形 3：重启后从水位续跑，且不重复扫描入库
    def test_restart_resumes_from_watermark_without_duplicates(self):
        path = self.db_path
        pages = [
            [make_topic(10, "2024-03-05T10:00:00.000+0800"),
             make_topic(9, "2024-03-04T10:00:00.000+0800")],
            [make_topic(8, "2024-03-03T10:00:00.000+0800"),
             make_topic(7, "2024-03-02T10:00:00.000+0800")],
        ]

        def first_run():
            db = ZSXQDatabase(path)
            calls = []
            runner = BatchCrawlRunner(
                db, 100,
                lambda wm: (calls.append(wm), pages[len(calls) - 1])[1],
            )
            summary = runner.run(max_batches=1)  # 跑一批后"进程被杀"
            db.close()
            return summary

        first_run()
        # 模拟进程重启：全新数据库连接
        self.db.close()
        self.db = ZSXQDatabase(path)
        self.store = ImportProgressStore(self.db)
        watermark = self.store.get_watermark(100)
        self.assertEqual(watermark["topic_id"], 9)

        received_watermarks = []

        def fetch_after_restart(wm):
            received_watermarks.append(wm)
            if len(received_watermarks) == 1:
                # 续跑首页与上批边界重叠（topic 9 重复扫到）
                return [
                    make_topic(9, "2024-03-04T10:00:00.000+0800"),
                    make_topic(8, "2024-03-03T10:00:00.000+0800"),
                    make_topic(7, "2024-03-02T10:00:00.000+0800"),
                ]
            return []

        runner = BatchCrawlRunner(self.db, 100, fetch_after_restart)
        summary = runner.run()
        self.assertEqual(summary["stop_reason"], "exhausted")
        # 续跑第一次抓取确实带着上次水位，而不是从头扫
        self.assertEqual(received_watermarks[0]["topic_id"], 9)
        # 4 条话题，无重复
        self.assertEqual(
            self.db.cursor.execute("SELECT COUNT(*) FROM topics").fetchone()[0],
            4,
        )
        self.assertEqual(
            self.db.cursor.execute(
                "SELECT COUNT(DISTINCT topic_id) FROM topics"
            ).fetchone()[0],
            4,
        )

    # 情形 4：同一条话题灌两遍，行数与统计数字不变
    def test_duplicate_topic_import_keeps_rows_and_stats_identical(self):
        topic = make_topic(5, "2024-03-10T10:00:00.000+0800")
        self.assertTrue(self.db.import_topic_data(topic))
        self.db.conn.commit()
        first_counts = self._counts()
        # 统计只比较库表行数，进度表属于批次记账，单独验证
        first_stats = {
            k: v for k, v in self.db.get_database_stats().items()
            if k != "import_progress"
        }

        # 再灌一遍同样的数据
        self.assertTrue(self.db.import_topic_data(topic))
        self.db.conn.commit()
        second_counts = self._counts()
        second_stats = {
            k: v for k, v in self.db.get_database_stats().items()
            if k != "import_progress"
        }

        self.assertEqual(first_counts, second_counts)
        self.assertEqual(first_stats, second_stats)

        # 批次层重复灌同一批，业务表行数同样不变（进度表只记批次账）
        runner = BatchCrawlRunner(self.db, 100, lambda wm: [topic])
        runner.run_batch()
        runner.run_batch()
        business_counts = {
            table: count for table, count in self._counts().items()
            if table != "import_progress"
        }
        first_business_counts = {
            table: count for table, count in first_counts.items()
            if table != "import_progress"
        }
        self.assertEqual(business_counts, first_business_counts)
        # 两批都成功记账，但话题相关表行数没有增加
        self.assertEqual(
            self.store.get_progress(100)["success_batches"], 2
        )

    # 情形 5：被平台拦下的批次不算入库、水位不前进
    def test_blocked_batch_not_imported_and_watermark_unchanged(self):
        good_batch = [
            make_topic(1, "2024-03-03T10:00:00.000+0800"),
            make_topic(2, "2024-03-02T10:00:00.000+0800"),
        ]
        runner = BatchCrawlRunner(self.db, 100, lambda wm: good_batch)
        runner.run_batch()
        watermark_before = self.store.get_watermark(100)
        counts_before = self._counts()

        # 反爬方式一：回调返回 None
        runner.fetch_batch = lambda wm: None
        blocked = runner.run_batch()
        self.assertEqual(blocked["status"], "blocked")
        self.assertEqual(
            self.store.get_watermark(100), watermark_before
        )
        self.assertEqual(self._counts(), counts_before)

        # 反爬方式二：回调抛 BatchBlockedError
        def raise_blocked(wm):
            raise BatchBlockedError("403 反爬")

        runner.fetch_batch = raise_blocked
        blocked2 = runner.run_batch()
        self.assertEqual(blocked2["status"], "blocked")
        self.assertEqual(
            self.store.get_watermark(100), watermark_before
        )
        self.assertEqual(self._counts(), counts_before)

        # 下一批恢复正常，仍然从原水位往后取，成功后水位才推进
        next_batch = [
            make_topic(3, "2024-03-01T10:00:00.000+0800"),
        ]
        seen_watermarks = []
        runner.fetch_batch = lambda wm: (
            seen_watermarks.append(wm), next_batch
        )[1]
        ok = runner.run_batch()
        self.assertEqual(ok["status"], "success")
        self.assertEqual(seen_watermarks[0], watermark_before)
        self.assertEqual(
            self.store.get_watermark(100)["topic_id"], 3
        )
        progress = self.store.get_progress(100)
        self.assertEqual(progress["failed_batches"], 2)
        self.assertEqual(progress["success_batches"], 2)


if __name__ == "__main__":
    unittest.main()
