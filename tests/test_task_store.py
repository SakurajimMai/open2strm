import json
import unittest
import uuid
from datetime import datetime, UTC
from pathlib import Path

from strm import DownloadCancelled
from task_service import CronScheduler, TaskManager, TaskStore, default_config


class TaskStoreTest(unittest.TestCase):
    def test_create_update_list_and_rerun_task(self):
        temp_root = Path.cwd() / ".test_tmp"
        temp_root.mkdir(exist_ok=True)
        temp_dir = temp_root / f"task_store_{uuid.uuid4().hex}"
        temp_dir.mkdir()
        db_path = temp_dir / "tasks.sqlite3"
        store = TaskStore(db_path)
        profile_id = store.create_config_profile("默认配置", default_config())

        task_id = store.create_task(profile_id)
        store.mark_running(task_id)
        store.mark_succeeded(
            task_id,
            {
                "strm_created": 3,
                "files_downloaded": 2,
                "directories_scanned": 1,
                "errors": 0,
            },
        )

        original = store.get_task(task_id)
        self.assertEqual(original["status"], "succeeded")
        self.assertEqual(json.loads(original["stats_json"])["strm_created"], 3)

        rerun_id = store.create_rerun(task_id)
        rerun = store.get_task(rerun_id)
        self.assertEqual(rerun["profile_id"], profile_id)
        self.assertEqual(rerun["parent_task_id"], task_id)
        self.assertEqual(rerun["status"], "queued")

        tasks = store.list_tasks()
        self.assertEqual([task["id"] for task in tasks], [rerun_id, task_id])

    def test_restore_pending_tasks_requeues_incomplete_work(self):
        temp_root = Path.cwd() / ".test_tmp"
        temp_root.mkdir(exist_ok=True)
        temp_dir = temp_root / f"task_restore_{uuid.uuid4().hex}"
        temp_dir.mkdir()
        store = TaskStore(temp_dir / "tasks.sqlite3")
        profile_id = store.create_config_profile("默认配置", default_config())
        queued_id = store.create_task(profile_id)
        running_id = store.create_task(profile_id)
        store.mark_running(running_id)

        manager = TaskManager(store)
        restored = manager.restore_pending_tasks()

        self.assertEqual(restored, [queued_id, running_id])
        self.assertEqual(store.get_task(running_id)["status"], "queued")
        self.assertIn("服务重启", store.get_task(running_id)["error"])

    def test_cancel_queued_task_marks_cancelled(self):
        temp_root = Path.cwd() / ".test_tmp"
        temp_root.mkdir(exist_ok=True)
        temp_dir = temp_root / f"task_cancel_{uuid.uuid4().hex}"
        temp_dir.mkdir()
        store = TaskStore(temp_dir / "tasks.sqlite3")
        profile_id = store.create_config_profile("默认配置", default_config())
        task_id = store.create_task(profile_id)

        self.assertTrue(store.request_cancel(task_id))
        self.assertEqual(store.get_task(task_id)["status"], "cancelled")

    def test_running_task_cancel_exception_marks_cancelled(self):
        temp_root = Path.cwd() / ".test_tmp"
        temp_root.mkdir(exist_ok=True)
        temp_dir = temp_root / f"task_cancel_running_{uuid.uuid4().hex}"
        temp_dir.mkdir()
        store = TaskStore(temp_dir / "tasks.sqlite3")
        profile_id = store.create_config_profile("默认配置", default_config())

        def runner(config, handler, progress_callback=None, cancel_checker=None):
            raise DownloadCancelled("用户取消")

        manager = TaskManager(store, runner=runner)
        task_id = store.create_task(profile_id)
        manager._run_task(task_id)

        self.assertEqual(store.get_task(task_id)["status"], "cancelled")

    def test_success_and_cancel_keep_progress_state(self):
        temp_root = Path.cwd() / ".test_tmp"
        temp_root.mkdir(exist_ok=True)
        temp_dir = temp_root / f"task_progress_{uuid.uuid4().hex}"
        temp_dir.mkdir()
        store = TaskStore(temp_dir / "tasks.sqlite3")
        profile_id = store.create_config_profile("默认配置", default_config())
        task_id = store.create_task(profile_id)

        store.mark_succeeded(task_id, {"strm_created": 1}, progress={"phase": "完成", "current_path": "/a"})
        task = store.get_task(task_id)
        self.assertEqual(task["status"], "succeeded")
        self.assertIn("完成", task["progress_json"])

        cancelled_id = store.create_task(profile_id)
        store.mark_cancelled(cancelled_id)
        cancelled = store.get_task(cancelled_id)
        self.assertEqual(cancelled["status"], "cancelled")
        self.assertIn("已取消", cancelled["progress_json"])

    def test_schedule_rules_support_multiple_cron_per_profile(self):
        temp_root = Path.cwd() / ".test_tmp"
        temp_root.mkdir(exist_ok=True)
        temp_dir = temp_root / f"schedule_rules_{uuid.uuid4().hex}"
        temp_dir.mkdir()
        store = TaskStore(temp_dir / "tasks.sqlite3")
        profile_id = store.create_config_profile("默认配置", default_config())

        first_id = store.create_schedule_rule(profile_id, "每天凌晨", "0 3 * * *")
        second_id = store.create_schedule_rule(profile_id, "每六小时", "0 */6 * * *")

        rules = store.list_schedule_rules(profile_id=profile_id)
        self.assertEqual([rule["id"] for rule in rules], [first_id, second_id])
        self.assertEqual(rules[0]["profile_name"], "默认配置")
        self.assertTrue(rules[0]["enabled"])
        self.assertIsNotNone(rules[0]["next_run_at"])

        store.update_schedule_rule(first_id, "每天四点", "0 4 * * *", enabled=False)
        updated = store.get_schedule_rule(first_id)
        self.assertEqual(updated["name"], "每天四点")
        self.assertFalse(updated["enabled"])

        store.delete_schedule_rule(second_id)
        self.assertEqual([rule["id"] for rule in store.list_schedule_rules(profile_id=profile_id)], [first_id])

    def test_cron_scheduler_enqueues_due_enabled_rules(self):
        temp_root = Path.cwd() / ".test_tmp"
        temp_root.mkdir(exist_ok=True)
        temp_dir = temp_root / f"cron_scheduler_{uuid.uuid4().hex}"
        temp_dir.mkdir()
        store = TaskStore(temp_dir / "tasks.sqlite3")
        profile_id = store.create_config_profile("默认配置", default_config())
        due_id = store.create_schedule_rule(profile_id, "到点规则", "* * * * *")
        disabled_id = store.create_schedule_rule(profile_id, "禁用规则", "* * * * *", enabled=False)
        past = "2026-01-01T00:00:00Z"
        store._execute_update("UPDATE schedule_rules SET next_run_at = ? WHERE id IN (?, ?)", (past, due_id, disabled_id))

        manager = TaskManager(store)
        scheduler = CronScheduler(store, manager)
        triggered = scheduler.run_due_once(now=datetime(2026, 1, 1, 0, 1, tzinfo=UTC))

        self.assertEqual(triggered, [due_id])
        tasks = store.list_tasks()
        self.assertEqual(len(tasks), 1)
        self.assertEqual(tasks[0]["profile_id"], profile_id)
        self.assertEqual(store.get_schedule_rule(due_id)["last_task_id"], tasks[0]["id"])
        self.assertIsNone(store.get_schedule_rule(disabled_id)["last_task_id"])


if __name__ == "__main__":
    unittest.main()
