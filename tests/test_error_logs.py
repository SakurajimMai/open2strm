import unittest
import uuid
from pathlib import Path

from task_service import TaskStore


class ErrorLogTest(unittest.TestCase):
    def test_error_log_store_records_entries(self):
        temp_root = Path.cwd() / ".test_tmp"
        temp_root.mkdir(exist_ok=True)
        temp_dir = temp_root / f"error_logs_{uuid.uuid4().hex}"
        temp_dir.mkdir()
        store = TaskStore(temp_dir / "tasks.sqlite3")

        store.append_error_log(
            source="task",
            message="下载失败",
            task_id=3,
            details="traceback line 1",
        )

        rows = store.list_error_logs()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["source"], "task")
        self.assertEqual(rows[0]["message"], "下载失败")
        self.assertEqual(rows[0]["task_id"], 3)
        self.assertEqual(rows[0]["details"], "traceback line 1")

    def test_task_error_log_handler_writes_only_errors(self):
        temp_root = Path.cwd() / ".test_tmp"
        temp_root.mkdir(exist_ok=True)
        temp_dir = temp_root / f"error_handler_{uuid.uuid4().hex}"
        temp_dir.mkdir()
        store = TaskStore(temp_dir / "tasks.sqlite3")
        handler = store.build_error_log_handler(task_id=8)

        logger_name = "demo"
        import logging

        logger = logging.getLogger(logger_name)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logger.info("info ignored")
        logger.error("error captured")
        logger.removeHandler(handler)

        rows = store.list_error_logs()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["message"], "error captured")

    def test_error_log_filters_by_task_source_and_keyword(self):
        temp_root = Path.cwd() / ".test_tmp"
        temp_root.mkdir(exist_ok=True)
        temp_dir = temp_root / f"error_filters_{uuid.uuid4().hex}"
        temp_dir.mkdir()
        store = TaskStore(temp_dir / "tasks.sqlite3")

        store.append_error_log(source="task_manager", task_id=1, message="下载失败", details="timeout")
        store.append_error_log(source="openlist", task_id=2, message="认证失败", details="token invalid")

        rows = store.list_error_logs(task_id=1, source="task_manager", keyword="timeout")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["message"], "下载失败")

        rows = store.list_error_logs(keyword="token")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["source"], "openlist")


if __name__ == "__main__":
    unittest.main()
