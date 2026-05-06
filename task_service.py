"""后台任务存储与执行队列。"""

from __future__ import annotations

import asyncio
import inspect
import json
import logging
import sqlite3
import secrets
import threading
import traceback
from datetime import datetime, UTC
from pathlib import Path
from queue import Empty, Queue
from copy import deepcopy
from typing import Any, Callable, Dict, List, Optional

from croniter import croniter
from werkzeug.security import generate_password_hash

from strm import DownloadCancelled, run_download_config


ProgressCallback = Callable[[Dict[str, Any]], None]
CancelChecker = Callable[[], bool]
TaskRunner = Callable[
    [Dict[str, Any], Optional[logging.Handler], Optional[ProgressCallback], Optional[CancelChecker]],
    Dict[str, Any],
]

DEFAULT_AUTH_USERNAME = "admin"
DEFAULT_AUTH_PASSWORD = "admin"


class TaskErrorLogHandler(logging.Handler):
    """把 ERROR 级别日志写入数据库，便于前端排错。"""

    def __init__(self, store: "TaskStore", task_id: Optional[int] = None):
        super().__init__(level=logging.ERROR)
        self.store = store
        self.task_id = task_id

    def emit(self, record: logging.LogRecord) -> None:
        if record.levelno < logging.ERROR:
            return
        try:
            self.store.append_error_log(
                source=record.name or "root",
                message=record.getMessage(),
                task_id=self.task_id,
                details=self.format(record),
                level=record.levelname,
            )
        except Exception:
            self.handleError(record)


def default_config() -> Dict[str, Any]:
    """返回数据库配置档案的默认值。"""
    return {
        "openlist": {
            "base_url": "",
            "username": "",
            "password": "",
            "token": "",
            "source_paths": {"/": "./downloads"},
        },
        "local": {"base_path": "./downloads"},
        "strm": {
            "url_format": "full",
            "custom_prefix": "",
            "url_encode": False,
        },
        "max_depth": 10,
        "concurrent_downloads": 5,
        "download_metadata": True,
        "sync": {
            "mode": "incremental",
            "cleanup_invalid": True,
            "confirm_mode": False,
        },
        "advanced": {
            "timeout": 30,
            "retry_count": 3,
            "retry_delay": 5,
            "verify_ssl": True,
            "proxy": "",
        },
        "filter": {
            "video_extensions": [],
            "exclude_patterns": [],
            "min_file_size": 0,
            "max_file_size": 0,
        },
    }


def utc_now() -> str:
    """返回便于排序和展示的 UTC 时间字符串。"""
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def default_task_runner(
    config: Dict[str, Any],
    log_handler: Optional[logging.Handler],
    progress_callback: Optional[ProgressCallback] = None,
    cancel_checker: Optional[CancelChecker] = None,
) -> Dict[str, Any]:
    """在独立事件循环中执行一次 STRM 同步。"""
    root_logger = logging.getLogger()
    if log_handler:
        root_logger.addHandler(log_handler)
    try:
        return asyncio.run(
            run_download_config(
                deepcopy(config),
                progress_callback=progress_callback,
                cancel_checker=cancel_checker,
            )
        )
    finally:
        if log_handler:
            root_logger.removeHandler(log_handler)


class TaskStore:
    """使用 SQLite 保存任务状态、统计结果和错误信息。"""

    def __init__(self, db_path: Path | str):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self) -> None:
        with self._lock:
            conn = self._connect()
            try:
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS tasks (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        config_path TEXT,
                        profile_id INTEGER,
                        profile_name TEXT,
                        config_json TEXT,
                        status TEXT NOT NULL,
                        created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL,
                        started_at TEXT,
                        finished_at TEXT,
                        parent_task_id INTEGER,
                        stats_json TEXT,
                        progress_json TEXT,
                        cancel_requested INTEGER NOT NULL DEFAULT 0,
                        error TEXT
                    )
                    """
                )
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS config_profiles (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        name TEXT NOT NULL,
                        config_json TEXT NOT NULL,
                        created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL
                    )
                    """
                )
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS error_logs (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        created_at TEXT NOT NULL,
                        source TEXT NOT NULL,
                        task_id INTEGER,
                        level TEXT NOT NULL,
                        message TEXT NOT NULL,
                        details TEXT
                    )
                    """
                )
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS app_settings (
                        key TEXT PRIMARY KEY,
                        value_json TEXT NOT NULL,
                        updated_at TEXT NOT NULL
                    )
                    """
                )
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS schedule_rules (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        profile_id INTEGER NOT NULL,
                        name TEXT NOT NULL,
                        cron_expr TEXT NOT NULL,
                        enabled INTEGER NOT NULL DEFAULT 1,
                        created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL,
                        last_run_at TEXT,
                        next_run_at TEXT,
                        last_task_id INTEGER,
                        last_error TEXT
                    )
                    """
                )
                self._ensure_task_columns(conn)
                conn.commit()
            finally:
                conn.close()

    def _ensure_task_columns(self, conn: sqlite3.Connection) -> None:
        columns = {row["name"] for row in conn.execute("PRAGMA table_info(tasks)").fetchall()}
        migrations = {
            "profile_id": "ALTER TABLE tasks ADD COLUMN profile_id INTEGER",
            "profile_name": "ALTER TABLE tasks ADD COLUMN profile_name TEXT",
            "config_json": "ALTER TABLE tasks ADD COLUMN config_json TEXT",
            "progress_json": "ALTER TABLE tasks ADD COLUMN progress_json TEXT",
            "cancel_requested": "ALTER TABLE tasks ADD COLUMN cancel_requested INTEGER NOT NULL DEFAULT 0",
        }
        for column, sql in migrations.items():
            if column not in columns:
                conn.execute(sql)

    def get_setting(self, key: str, default: Any = None) -> Any:
        with self._lock:
            conn = self._connect()
            try:
                row = conn.execute("SELECT value_json FROM app_settings WHERE key = ?", (key,)).fetchone()
            finally:
                conn.close()
        if not row:
            return default
        try:
            return json.loads(row["value_json"])
        except json.JSONDecodeError:
            return default

    def set_setting(self, key: str, value: Any) -> None:
        now = utc_now()
        payload = json.dumps(value, ensure_ascii=False)
        with self._lock:
            conn = self._connect()
            try:
                conn.execute(
                    """
                    INSERT INTO app_settings (key, value_json, updated_at)
                    VALUES (?, ?, ?)
                    ON CONFLICT(key) DO UPDATE SET
                        value_json = excluded.value_json,
                        updated_at = excluded.updated_at
                    """,
                    (key, payload, now),
                )
                conn.commit()
            finally:
                conn.close()

    def get_auth_settings(self) -> Dict[str, Any]:
        settings = self.get_setting("auth_settings", {})
        if not isinstance(settings, dict):
            settings = {}
        return {
            "enabled": bool(settings.get("enabled", False)),
            "username": str(settings.get("username", "admin")).strip() or "admin",
            "password_hash": str(settings.get("password_hash", "")),
        }

    def save_auth_settings(self, settings: Dict[str, Any]) -> None:
        self.set_setting(
            "auth_settings",
            {
                "enabled": bool(settings.get("enabled", False)),
                "username": str(settings.get("username", DEFAULT_AUTH_USERNAME)).strip() or DEFAULT_AUTH_USERNAME,
                "password_hash": str(settings.get("password_hash", "")),
            },
        )

    def ensure_default_auth_settings(self) -> Dict[str, Any]:
        settings = self.get_setting("auth_settings", None)
        if not isinstance(settings, dict) or not settings.get("password_hash"):
            default_settings = {
                "enabled": True,
                "username": DEFAULT_AUTH_USERNAME,
                "password_hash": generate_password_hash(DEFAULT_AUTH_PASSWORD),
            }
            self.save_auth_settings(default_settings)
            return default_settings
        return self.get_auth_settings()

    def ensure_session_secret(self) -> str:
        secret = self.get_setting("session_secret_key", "")
        if isinstance(secret, str) and secret:
            return secret
        secret = secrets.token_hex(32)
        self.set_setting("session_secret_key", secret)
        return secret

    def create_config_profile(self, name: str, config: Dict[str, Any]) -> int:
        now = utc_now()
        with self._lock:
            conn = self._connect()
            try:
                cursor = conn.execute(
                    """
                    INSERT INTO config_profiles (name, config_json, created_at, updated_at)
                    VALUES (?, ?, ?, ?)
                    """,
                    (name, json.dumps(config, ensure_ascii=False), now, now),
                )
                conn.commit()
                return int(cursor.lastrowid)
            finally:
                conn.close()

    def update_config_profile(self, profile_id: int, name: str, config: Dict[str, Any]) -> None:
        self._execute_update(
            """
            UPDATE config_profiles
            SET name = ?, config_json = ?, updated_at = ?
            WHERE id = ?
            """,
            (name, json.dumps(config, ensure_ascii=False), utc_now(), profile_id),
        )

    def ensure_default_config_profile(self) -> int:
        profiles = self.list_config_profiles()
        if profiles:
            return int(profiles[0]["id"])
        return self.create_config_profile("默认配置", default_config())

    def get_config_profile(self, profile_id: int) -> Optional[Dict[str, Any]]:
        with self._lock:
            conn = self._connect()
            try:
                row = conn.execute("SELECT * FROM config_profiles WHERE id = ?", (profile_id,)).fetchone()
            finally:
                conn.close()
        if not row:
            return None
        profile = dict(row)
        profile["config"] = json.loads(profile.pop("config_json"))
        return profile

    def list_config_profiles(self) -> List[Dict[str, Any]]:
        with self._lock:
            conn = self._connect()
            try:
                rows = conn.execute("SELECT * FROM config_profiles ORDER BY id ASC").fetchall()
            finally:
                conn.close()
        profiles = []
        for row in rows:
            profile = dict(row)
            profile["config"] = json.loads(profile.pop("config_json"))
            profiles.append(profile)
        return profiles

    def append_error_log(
        self,
        source: str,
        message: str,
        task_id: Optional[int] = None,
        details: Optional[str] = None,
        level: str = "ERROR",
    ) -> int:
        now = utc_now()
        with self._lock:
            conn = self._connect()
            try:
                cursor = conn.execute(
                    """
                    INSERT INTO error_logs (
                        created_at, source, task_id, level, message, details
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (now, source, task_id, level, message, details),
                )
                conn.commit()
                return int(cursor.lastrowid)
            finally:
                conn.close()

    def build_error_log_handler(self, task_id: Optional[int] = None) -> TaskErrorLogHandler:
        handler = TaskErrorLogHandler(self, task_id)
        handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
        return handler

    def create_schedule_rule(self, profile_id: int, name: str, cron_expr: str, enabled: bool = True) -> int:
        profile = self.get_config_profile(profile_id)
        if not profile:
            raise ValueError(f"配置档案不存在: {profile_id}")
        cron_expr = normalize_cron_expr(cron_expr)
        now = utc_now()
        next_run_at = next_cron_run(cron_expr, datetime.now(UTC)) if enabled else None
        with self._lock:
            conn = self._connect()
            try:
                cursor = conn.execute(
                    """
                    INSERT INTO schedule_rules (
                        profile_id, name, cron_expr, enabled, created_at, updated_at, next_run_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (profile_id, name.strip() or cron_expr, cron_expr, 1 if enabled else 0, now, now, next_run_at),
                )
                conn.commit()
                return int(cursor.lastrowid)
            finally:
                conn.close()

    def update_schedule_rule(self, rule_id: int, name: str, cron_expr: str, enabled: bool = True) -> None:
        rule = self.get_schedule_rule(rule_id)
        if not rule:
            raise ValueError(f"定时规则不存在: {rule_id}")
        cron_expr = normalize_cron_expr(cron_expr)
        now = utc_now()
        base = parse_utc(rule.get("last_run_at")) or datetime.now(UTC)
        next_run_at = next_cron_run(cron_expr, base) if enabled else None
        self._execute_update(
            """
            UPDATE schedule_rules
            SET name = ?, cron_expr = ?, enabled = ?, updated_at = ?, next_run_at = ?, last_error = NULL
            WHERE id = ?
            """,
            (name.strip() or cron_expr, cron_expr, 1 if enabled else 0, now, next_run_at, rule_id),
        )

    def delete_schedule_rule(self, rule_id: int) -> None:
        self._execute_update("DELETE FROM schedule_rules WHERE id = ?", (rule_id,))

    def get_schedule_rule(self, rule_id: int) -> Optional[Dict[str, Any]]:
        rows = self._select_schedule_rules("WHERE s.id = ?", (rule_id,))
        return rows[0] if rows else None

    def list_schedule_rules(self, profile_id: Optional[int] = None) -> List[Dict[str, Any]]:
        if profile_id is None:
            return self._select_schedule_rules("", ())
        return self._select_schedule_rules("WHERE s.profile_id = ?", (profile_id,))

    def list_due_schedule_rules(self, now: Optional[datetime] = None) -> List[Dict[str, Any]]:
        now_text = format_utc(now or datetime.now(UTC))
        return self._select_schedule_rules(
            "WHERE s.enabled = 1 AND s.next_run_at IS NOT NULL AND s.next_run_at <= ?",
            (now_text,),
        )

    def mark_schedule_triggered(self, rule_id: int, task_id: int, now: Optional[datetime] = None) -> None:
        rule = self.get_schedule_rule(rule_id)
        if not rule:
            return
        current = now or datetime.now(UTC)
        try:
            next_run_at = next_cron_run(rule["cron_expr"], current)
            error = None
        except ValueError as exc:
            next_run_at = None
            error = str(exc)
        self._execute_update(
            """
            UPDATE schedule_rules
            SET last_run_at = ?, next_run_at = ?, last_task_id = ?, last_error = ?, updated_at = ?
            WHERE id = ?
            """,
            (format_utc(current), next_run_at, task_id, error, utc_now(), rule_id),
        )

    def mark_schedule_error(self, rule_id: int, error: str, now: Optional[datetime] = None) -> None:
        rule = self.get_schedule_rule(rule_id)
        if not rule:
            return
        current = now or datetime.now(UTC)
        try:
            next_run_at = next_cron_run(rule["cron_expr"], current)
        except ValueError:
            next_run_at = None
        self._execute_update(
            """
            UPDATE schedule_rules
            SET last_error = ?, next_run_at = ?, updated_at = ?
            WHERE id = ?
            """,
            (error, next_run_at, utc_now(), rule_id),
        )

    def _select_schedule_rules(self, where_sql: str, params: tuple[Any, ...]) -> List[Dict[str, Any]]:
        sql = f"""
            SELECT s.*, p.name AS profile_name
            FROM schedule_rules s
            LEFT JOIN config_profiles p ON p.id = s.profile_id
            {where_sql}
            ORDER BY s.id ASC
        """
        with self._lock:
            conn = self._connect()
            try:
                rows = conn.execute(sql, params).fetchall()
            finally:
                conn.close()
        rules = []
        for row in rows:
            rule = dict(row)
            rule["enabled"] = bool(rule["enabled"])
            rules.append(rule)
        return rules

    def list_error_logs(
        self,
        limit: int = 100,
        task_id: Optional[int] = None,
        source: Optional[str] = None,
        keyword: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        sql = """
            SELECT e.*, t.profile_name
            FROM error_logs e
            LEFT JOIN tasks t ON t.id = e.task_id
        """
        params: List[Any] = []
        conditions: List[str] = []
        if task_id is not None:
            conditions.append("e.task_id = ?")
            params.append(task_id)
        if source:
            conditions.append("e.source = ?")
            params.append(source)
        if keyword:
            conditions.append("(e.message LIKE ? OR COALESCE(e.details, '') LIKE ?)")
            pattern = f"%{keyword}%"
            params.extend([pattern, pattern])
        if conditions:
            sql += " WHERE " + " AND ".join(conditions)
        sql += " ORDER BY e.id DESC LIMIT ?"
        params.append(limit)

        with self._lock:
            conn = self._connect()
            try:
                rows = conn.execute(sql, params).fetchall()
            finally:
                conn.close()
        return [dict(row) for row in rows]

    def create_task(self, profile_id: int, parent_task_id: Optional[int] = None, config_snapshot: Optional[Dict[str, Any]] = None) -> int:
        profile = self.get_config_profile(profile_id)
        if not profile:
            raise ValueError(f"配置档案不存在: {profile_id}")
        config = deepcopy(config_snapshot if config_snapshot is not None else profile["config"])
        profile_name = profile["name"]
        now = utc_now()
        with self._lock:
            conn = self._connect()
            try:
                cursor = conn.execute(
                    """
                    INSERT INTO tasks (
                        config_path, profile_id, profile_name, config_json, status,
                        created_at, updated_at, parent_task_id, stats_json, progress_json, cancel_requested
                    ) VALUES (?, ?, ?, ?, 'queued', ?, ?, ?, ?, ?, 0)
                    """,
                    (
                        "",
                        profile_id,
                        profile_name,
                        json.dumps(config, ensure_ascii=False),
                        now,
                        now,
                        parent_task_id,
                        "{}",
                        "{}",
                    ),
                )
                conn.commit()
                return int(cursor.lastrowid)
            finally:
                conn.close()

    def create_rerun(self, task_id: int) -> int:
        task = self.get_task(task_id)
        if not task:
            raise ValueError(f"任务不存在: {task_id}")
        if not task.get("profile_id") or not task.get("config_json"):
            raise ValueError(f"任务缺少数据库配置快照: {task_id}")
        return self.create_task(
            int(task["profile_id"]),
            parent_task_id=task_id,
            config_snapshot=json.loads(task["config_json"]),
        )

    def mark_running(self, task_id: int) -> None:
        now = utc_now()
        self._execute_update(
            """
            UPDATE tasks
            SET status = 'running', started_at = ?, updated_at = ?, error = NULL, cancel_requested = 0
            WHERE id = ?
            """,
            (now, now, task_id),
        )

    def mark_queued(self, task_id: int, error: Optional[str] = None) -> None:
        now = utc_now()
        self._execute_update(
            """
            UPDATE tasks
            SET status = 'queued', started_at = NULL, finished_at = NULL, updated_at = ?, error = ?, cancel_requested = 0
            WHERE id = ?
            """,
            (now, error, task_id),
        )

    def mark_succeeded(self, task_id: int, stats: Dict[str, Any], progress: Optional[Dict[str, Any]] = None) -> None:
        now = utc_now()
        self._execute_update(
            """
            UPDATE tasks
            SET status = 'succeeded', finished_at = ?, updated_at = ?, stats_json = ?, progress_json = ?, error = NULL
            WHERE id = ?
            """,
            (
                now,
                now,
                json.dumps(stats, ensure_ascii=False),
                json.dumps(progress or {"phase": "完成", **stats}, ensure_ascii=False),
                task_id,
            ),
        )

    def mark_failed(self, task_id: int, error: str, stats: Optional[Dict[str, Any]] = None) -> None:
        now = utc_now()
        self._execute_update(
            """
            UPDATE tasks
            SET status = 'failed', finished_at = ?, updated_at = ?, stats_json = ?, error = ?
            WHERE id = ?
            """,
            (now, now, json.dumps(stats or {}, ensure_ascii=False), error, task_id),
        )

    def mark_cancelled(
        self,
        task_id: int,
        stats: Optional[Dict[str, Any]] = None,
        progress: Optional[Dict[str, Any]] = None,
    ) -> None:
        now = utc_now()
        self._execute_update(
            """
            UPDATE tasks
            SET status = 'cancelled', finished_at = ?, updated_at = ?, stats_json = ?, error = NULL
            WHERE id = ?
            """,
            (now, now, json.dumps(stats or {}, ensure_ascii=False), task_id),
        )
        if progress is None:
            self.update_progress(task_id, {"phase": "已取消", **(stats or {})})
        else:
            self.update_progress(task_id, progress)

    def request_cancel(self, task_id: int) -> bool:
        task = self.get_task(task_id)
        if not task or task.get("status") not in {"queued", "running"}:
            return False
        now = utc_now()
        self._execute_update(
            """
            UPDATE tasks
            SET cancel_requested = 1, updated_at = ?
            WHERE id = ?
            """,
            (now, task_id),
        )
        if task.get("status") == "queued":
            self.mark_cancelled(task_id, progress={"phase": "已取消"})
        return True

    def is_cancel_requested(self, task_id: int) -> bool:
        task = self.get_task(task_id)
        return bool(task and task.get("cancel_requested"))

    def update_progress(self, task_id: int, progress: Dict[str, Any]) -> None:
        now = utc_now()
        self._execute_update(
            """
            UPDATE tasks
            SET progress_json = ?, updated_at = ?
            WHERE id = ?
            """,
            (json.dumps(progress, ensure_ascii=False), now, task_id),
        )

    def get_task(self, task_id: int) -> Optional[Dict[str, Any]]:
        with self._lock:
            conn = self._connect()
            try:
                row = conn.execute("SELECT * FROM tasks WHERE id = ?", (task_id,)).fetchone()
            finally:
                conn.close()
        return dict(row) if row else None

    def list_tasks(self, limit: int = 50) -> List[Dict[str, Any]]:
        with self._lock:
            conn = self._connect()
            try:
                rows = conn.execute(
                    "SELECT * FROM tasks ORDER BY id DESC LIMIT ?",
                    (limit,),
                ).fetchall()
            finally:
                conn.close()
        return [dict(row) for row in rows]

    def list_incomplete_tasks(self) -> List[Dict[str, Any]]:
        with self._lock:
            conn = self._connect()
            try:
                rows = conn.execute(
                    "SELECT * FROM tasks WHERE status IN ('queued', 'running') ORDER BY id ASC"
                ).fetchall()
            finally:
                conn.close()
        return [dict(row) for row in rows]

    def latest_running_task(self) -> Optional[Dict[str, Any]]:
        with self._lock:
            conn = self._connect()
            try:
                row = conn.execute(
                    "SELECT * FROM tasks WHERE status = 'running' ORDER BY id DESC LIMIT 1"
                ).fetchone()
            finally:
                conn.close()
        return dict(row) if row else None

    def _execute_update(self, sql: str, params: tuple[Any, ...]) -> None:
        with self._lock:
            conn = self._connect()
            try:
                conn.execute(sql, params)
                conn.commit()
            finally:
                conn.close()


class TaskManager:
    """串行执行下载任务，避免多个任务同时写同一输出目录。"""

    def __init__(
        self,
        store: TaskStore,
        runner: TaskRunner = default_task_runner,
    ):
        self.store = store
        self.runner = runner
        self._queue: Queue[int] = Queue()
        self._started = False
        self._thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()

    def start(self) -> None:
        with self._lock:
            if self._started:
                return
            self._started = True
            self.restore_pending_tasks()
            self._thread = threading.Thread(target=self._worker_loop, name="strm-task-worker", daemon=True)
            self._thread.start()

    def restore_pending_tasks(self) -> List[int]:
        restored: List[int] = []
        for task in self.store.list_incomplete_tasks():
            task_id = int(task["id"])
            if task.get("status") == "running":
                self.store.mark_queued(task_id, error="服务重启后恢复到队列，等待重新执行")
            self._queue.put(task_id)
            restored.append(task_id)
        return restored

    def enqueue(self, profile_id: int) -> int:
        task_id = self.store.create_task(profile_id)
        self._queue.put(task_id)
        return task_id

    def rerun(self, task_id: int) -> int:
        rerun_id = self.store.create_rerun(task_id)
        self._queue.put(rerun_id)
        return rerun_id

    def _worker_loop(self) -> None:
        while True:
            try:
                task_id = self._queue.get(timeout=1)
            except Empty:
                continue
            try:
                self._run_task(task_id)
            finally:
                self._queue.task_done()

    def _run_task(self, task_id: int) -> None:
        task = self.store.get_task(task_id)
        if not task:
            return
        if task.get("cancel_requested"):
            self.store.mark_cancelled(task_id)
            return

        self.store.mark_running(task_id)
        handler = self.store.build_error_log_handler(task_id)

        try:
            if not task.get("config_json"):
                raise ValueError(f"任务缺少数据库配置快照: {task_id}")
            stats = self._call_runner(
                json.loads(task["config_json"]),
                handler,
                lambda progress: self.store.update_progress(task_id, progress),
                lambda: self.store.is_cancel_requested(task_id),
            )
            if self.store.is_cancel_requested(task_id):
                self.store.mark_cancelled(task_id, stats, progress={"phase": "已取消", **stats})
                return
            self.store.mark_succeeded(task_id, stats, progress={"phase": "完成", **stats})
        except DownloadCancelled:
            self.store.mark_cancelled(task_id, progress={"phase": "已取消"})
        except Exception as exc:
            error = f"{type(exc).__name__}: {exc}\n{traceback.format_exc()}"
            self.store.append_error_log(
                source="task_manager",
                task_id=task_id,
                message=str(exc),
                details=error,
                level="ERROR",
            )
            self.store.mark_failed(task_id, error)
        finally:
            handler.close()

    def _call_runner(
        self,
        config: Dict[str, Any],
        handler: logging.Handler,
        progress_callback: ProgressCallback,
        cancel_checker: CancelChecker,
    ) -> Dict[str, Any]:
        parameters = inspect.signature(self.runner).parameters
        accepts_varargs = any(param.kind == inspect.Parameter.VAR_POSITIONAL for param in parameters.values())
        positional_count = len(
            [
                param
                for param in parameters.values()
                if param.kind in {inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD}
            ]
        )
        if accepts_varargs or positional_count >= 4:
            return self.runner(config, handler, progress_callback, cancel_checker)
        return self.runner(config, handler)


class CronScheduler:
    """按数据库中的 cron 规则定时创建后台任务。"""

    def __init__(self, store: TaskStore, manager: TaskManager, poll_interval: int = 30):
        self.store = store
        self.manager = manager
        self.poll_interval = poll_interval
        self._started = False
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()

    def start(self) -> None:
        with self._lock:
            if self._started:
                return
            self._started = True
            self._thread = threading.Thread(target=self._loop, name="strm-cron-scheduler", daemon=True)
            self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()

    def run_due_once(self, now: Optional[datetime] = None) -> List[int]:
        current = now or datetime.now(UTC)
        triggered: List[int] = []
        for rule in self.store.list_due_schedule_rules(current):
            try:
                task_id = self.manager.enqueue(int(rule["profile_id"]))
                self.store.mark_schedule_triggered(int(rule["id"]), task_id, current)
                triggered.append(int(rule["id"]))
            except Exception as exc:
                self.store.mark_schedule_error(int(rule["id"]), f"{type(exc).__name__}: {exc}", current)
        return triggered

    def _loop(self) -> None:
        while not self._stop_event.is_set():
            self.run_due_once()
            self._stop_event.wait(self.poll_interval)


def format_utc(value: datetime) -> str:
    return value.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def parse_utc(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)
    except ValueError:
        return None


def normalize_cron_expr(cron_expr: str) -> str:
    cron_expr = " ".join(str(cron_expr).strip().split())
    if not croniter.is_valid(cron_expr):
        raise ValueError(f"无效 cron 表达式: {cron_expr}")
    return cron_expr


def next_cron_run(cron_expr: str, base: Optional[datetime] = None) -> str:
    base_time = base or datetime.now(UTC)
    if base_time.tzinfo is None:
        base_time = base_time.replace(tzinfo=UTC)
    iterator = croniter(normalize_cron_expr(cron_expr), base_time.astimezone(UTC))
    return format_utc(iterator.get_next(datetime))
