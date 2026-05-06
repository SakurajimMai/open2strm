"""OpenList STRM Flask 后台服务。"""

from __future__ import annotations

import json
import os
from datetime import timedelta
from pathlib import Path

from flask import Flask, abort, jsonify, redirect, render_template, request, session, url_for
from werkzeug.security import check_password_hash, generate_password_hash

from config_forms import ConfigValidationError, config_from_form, form_values_from_config
from task_service import CronScheduler, TaskManager, TaskRunner, TaskStore


BASE_DIR = Path(__file__).resolve().parent
DEFAULT_CONFIG = os.environ.get("STRM_CONFIG", "config.yaml")
DATA_DIR = Path(os.environ.get("STRM_DATA_DIR", BASE_DIR / "data"))
DB_PATH = DATA_DIR / "tasks.sqlite3"
def create_app(
    data_dir: Path | str | None = None,
    default_config: str | None = None,
    task_runner: TaskRunner | None = None,
    start_worker: bool = True,
) -> Flask:
    """创建 Flask 应用，便于本地验证和生产部署复用。"""
    app = Flask(__name__)
    runtime_dir = Path(data_dir) if data_dir is not None else DATA_DIR
    config_path = default_config or DEFAULT_CONFIG
    runtime_dir.mkdir(parents=True, exist_ok=True)

    store = TaskStore(runtime_dir / "tasks.sqlite3")
    app.secret_key = os.environ.get("STRM_SECRET_KEY") or store.ensure_session_secret()
    app.permanent_session_lifetime = timedelta(days=30)
    store.ensure_default_auth_settings()
    default_profile_id = store.ensure_default_config_profile()
    if task_runner:
        manager = TaskManager(store, runner=task_runner)
    else:
        manager = TaskManager(store)
    if start_worker:
        manager.start()
    scheduler = CronScheduler(store, manager)
    if start_worker:
        scheduler.start()

    app.config["TASK_STORE"] = store
    app.config["TASK_MANAGER"] = manager
    app.config["CRON_SCHEDULER"] = scheduler
    app.config["DEFAULT_CONFIG"] = config_path
    app.config["DEFAULT_PROFILE_ID"] = default_profile_id

    @app.before_request
    def require_login():
        if request.path in {"/health", "/healthz"}:
            abort(404)
        if request.endpoint in {"login", "login_submit", "static", "favicon", "metrics"}:
            return None
        auth = store.get_auth_settings()
        if not auth["enabled"]:
            return None
        if session.get("authenticated"):
            return None
        if request.accept_mimetypes.best == "application/json" or request.is_json:
            return jsonify({"error": "unauthorized"}), 401
        return redirect(url_for("login", next=request.full_path if request.query_string else request.path))

    @app.get("/login")
    def login():
        if session.get("authenticated"):
            return redirect(url_for("index"))
        return render_template("login.html", error=None)

    @app.post("/login")
    def login_submit():
        auth = store.get_auth_settings()
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "")
        if (
            auth["enabled"]
            and username == auth["username"]
            and auth["password_hash"]
            and check_password_hash(auth["password_hash"], password)
        ):
            session.permanent = True
            session["authenticated"] = True
            return redirect(request.form.get("next") or url_for("index"))
        return render_template("login.html", error="用户名或密码错误"), 401

    @app.post("/logout")
    def logout():
        session.clear()
        return redirect(url_for("login"))

    @app.get("/admin")
    def admin_settings():
        auth = store.get_auth_settings()
        return render_template("admin_settings.html", auth=auth, error=None, success=None)

    @app.post("/admin")
    def update_admin_settings():
        current = store.get_auth_settings()
        username = request.form.get("username", current["username"]).strip() or current["username"]
        password = request.form.get("password", "")
        confirm = request.form.get("confirm_password", "")
        enabled = request.form.get("enabled") == "on"

        errors = []
        if password or confirm:
            if password != confirm:
                errors.append("两次输入的密码不一致")
            elif len(password) < 4:
                errors.append("密码至少 4 位")

        if errors:
            return render_template("admin_settings.html", auth=current, error=None, errors=errors, success=None), 400

        payload = {
            "enabled": enabled,
            "username": username,
            "password_hash": current["password_hash"],
        }
        if password:
            payload["password_hash"] = generate_password_hash(password)
        store.save_auth_settings(payload)
        session.permanent = True
        session["authenticated"] = True
        return render_template("admin_settings.html", auth=store.get_auth_settings(), error=None, success="管理员设置已更新")

    @app.get("/")
    def index():
        tasks = [_decorate_task(task) for task in store.list_tasks(limit=50)]
        profiles = store.list_config_profiles()
        return render_template(
            "index.html",
            tasks=tasks,
            profiles=profiles,
            default_profile_id=app.config["DEFAULT_PROFILE_ID"],
        )

    @app.post("/tasks")
    def create_task():
        if request.is_json:
            profile_id = (request.get_json(silent=True) or {}).get("profile_id")
        else:
            profile_id = request.form.get("profile_id")
        if not profile_id:
            profile_id = app.config["DEFAULT_PROFILE_ID"]
        try:
            task_id = manager.enqueue(int(profile_id))
        except ValueError as exc:
            if request.accept_mimetypes.best == "application/json" or request.is_json:
                return jsonify({"error": str(exc)}), 400
            return (str(exc), 400)
        if request.accept_mimetypes.best == "application/json" or request.is_json:
            return jsonify({"id": task_id, "status": "queued"}), 202
        return redirect(url_for("task_detail", task_id=task_id))

    @app.get("/config")
    def config_index():
        profiles = store.list_config_profiles()
        return render_template("config_index.html", profiles=profiles)

    @app.get("/config/<int:profile_id>")
    def edit_config(profile_id: int):
        profile = store.get_config_profile(profile_id)
        if not profile:
            return ("配置档案不存在", 404)
        return render_template(
            "config_form.html",
            profile=profile,
            values=form_values_from_config(profile["config"]),
            schedule_rules=store.list_schedule_rules(profile_id=profile_id),
        )

    @app.post("/config/<int:profile_id>")
    def update_config(profile_id: int):
        profile = store.get_config_profile(profile_id)
        if not profile:
            return ("配置档案不存在", 404)
        name = request.form.get("name", profile["name"]).strip() or profile["name"]
        try:
            config = config_from_form(request.form)
        except ConfigValidationError as exc:
            return render_template(
                "config_form.html",
                profile=profile,
                values=form_values_from_config(config_from_form(request.form, validate=False)),
                schedule_rules=store.list_schedule_rules(profile_id=profile_id),
                errors=exc.errors,
            ), 400
        store.update_config_profile(profile_id, name, config)
        return redirect(url_for("edit_config", profile_id=profile_id))

    @app.post("/config/<int:profile_id>/schedules")
    def create_schedule(profile_id: int):
        profile = store.get_config_profile(profile_id)
        if not profile:
            return ("配置档案不存在", 404)
        name = request.form.get("schedule_name", "").strip() or "定时同步"
        cron_expr = request.form.get("cron_expr", "").strip()
        enabled = request.form.get("enabled") == "on"
        try:
            store.create_schedule_rule(profile_id, name, cron_expr, enabled=enabled)
        except ValueError as exc:
            return _render_config_with_schedule_error(store, profile_id, str(exc)), 400
        return redirect(url_for("edit_config", profile_id=profile_id))

    @app.post("/schedules/<int:rule_id>")
    def update_schedule(rule_id: int):
        rule = store.get_schedule_rule(rule_id)
        if not rule:
            return ("定时规则不存在", 404)
        name = request.form.get("schedule_name", rule["name"]).strip() or rule["name"]
        cron_expr = request.form.get("cron_expr", rule["cron_expr"]).strip()
        enabled = request.form.get("enabled") == "on"
        try:
            store.update_schedule_rule(rule_id, name, cron_expr, enabled=enabled)
        except ValueError as exc:
            return _render_config_with_schedule_error(store, int(rule["profile_id"]), str(exc)), 400
        return redirect(url_for("edit_config", profile_id=rule["profile_id"]))

    @app.post("/schedules/<int:rule_id>/delete")
    def delete_schedule(rule_id: int):
        rule = store.get_schedule_rule(rule_id)
        if not rule:
            return ("定时规则不存在", 404)
        profile_id = int(rule["profile_id"])
        store.delete_schedule_rule(rule_id)
        return redirect(url_for("edit_config", profile_id=profile_id))

    @app.post("/config")
    def create_config():
        name = request.form.get("name", "新配置").strip() or "新配置"
        try:
            config = config_from_form(request.form)
        except ConfigValidationError as exc:
            profiles = store.list_config_profiles()
            return render_template("config_index.html", profiles=profiles, errors=exc.errors), 400
        profile_id = store.create_config_profile(name, config)
        return redirect(url_for("edit_config", profile_id=profile_id))

    @app.get("/tasks")
    def list_tasks():
        return jsonify([_decorate_task(task) for task in store.list_tasks(limit=100)])

    @app.get("/tasks/<int:task_id>")
    def task_detail(task_id: int):
        task = store.get_task(task_id)
        if not task:
            return ("任务不存在", 404)
        task = _decorate_task(task)
        error_logs = store.list_error_logs(limit=100, task_id=task_id)
        if request.accept_mimetypes.best == "application/json":
            payload = dict(task)
            payload["error_logs"] = error_logs
            return jsonify(payload)
        return render_template("task_detail.html", task=task, error_logs=error_logs)

    @app.get("/logs")
    def error_logs():
        filters = _error_log_filters()
        logs = store.list_error_logs(**filters)
        return render_template("error_logs.html", logs=logs, filters=filters)

    @app.get("/logs.json")
    def error_logs_json():
        return jsonify(store.list_error_logs(**_error_log_filters()))

    @app.post("/tasks/<int:task_id>/rerun")
    def rerun_task(task_id: int):
        try:
            new_task_id = manager.rerun(task_id)
        except ValueError:
            return ("任务不存在", 404)
        if request.accept_mimetypes.best == "application/json" or request.is_json:
            return jsonify({"id": new_task_id, "status": "queued", "parent_task_id": task_id}), 202
        return redirect(url_for("task_detail", task_id=new_task_id))

    @app.post("/tasks/<int:task_id>/cancel")
    def cancel_task(task_id: int):
        cancelled = store.request_cancel(task_id)
        if request.accept_mimetypes.best == "application/json" or request.is_json:
            return jsonify({"id": task_id, "cancel_requested": cancelled}), 202 if cancelled else 409
        return redirect(url_for("task_detail", task_id=task_id))

    @app.get("/favicon.ico")
    def favicon():
        return ("", 204)

    @app.get("/metrics")
    def metrics():
        tasks = store.list_tasks(limit=1000)
        counts: dict[str, int] = {}
        for task in tasks:
            counts[task["status"]] = counts.get(task["status"], 0) + 1
        running = store.latest_running_task()
        schedule_rules = store.list_schedule_rules()
        return jsonify(
            {
                "status": "ok",
                "tasks": counts,
                "running_task_id": running["id"] if running else None,
                "error_logs": len(store.list_error_logs(limit=1000)),
                "schedule_rules": {
                    "total": len(schedule_rules),
                    "enabled": len([rule for rule in schedule_rules if rule["enabled"]]),
                },
            }
        )

    return app


def _decorate_task(task: dict) -> dict:
    """补充展示友好的统计字段。"""
    task = dict(task)
    raw_stats = task.get("stats_json") or "{}"
    try:
        task["stats"] = json.loads(raw_stats)
    except json.JSONDecodeError:
        task["stats"] = {}
    raw_config = task.get("config_json") or "{}"
    try:
        task["config"] = json.loads(raw_config)
    except json.JSONDecodeError:
        task["config"] = {}
    raw_progress = task.get("progress_json") or "{}"
    try:
        task["progress"] = json.loads(raw_progress)
    except json.JSONDecodeError:
        task["progress"] = {}
    return task


def _render_config_with_schedule_error(store: TaskStore, profile_id: int, error: str):
    profile = store.get_config_profile(profile_id)
    if not profile:
        return ("配置档案不存在", 404)
    return render_template(
        "config_form.html",
        profile=profile,
        values=form_values_from_config(profile["config"]),
        schedule_rules=store.list_schedule_rules(profile_id=profile_id),
        schedule_errors=[error],
    )


def _error_log_filters() -> dict:
    limit_raw = request.args.get("limit", "100")
    try:
        limit = int(limit_raw)
    except ValueError:
        limit = 100
    limit = max(1, min(limit, 500))
    task_id_raw = request.args.get("task_id", "").strip()
    task_id = int(task_id_raw) if task_id_raw.isdigit() else None
    source = request.args.get("source", "").strip() or None
    keyword = request.args.get("keyword", "").strip() or None
    return {"limit": limit, "task_id": task_id, "source": source, "keyword": keyword}


def enable_auth(store: TaskStore, username: str, password: str) -> None:
    """给初始化脚本复用的鉴权设置入口。"""
    store.save_auth_settings(
        {
            "enabled": True,
            "username": username,
            "password_hash": generate_password_hash(password),
        }
    )


app = create_app(start_worker=os.environ.get("STRM_START_WORKER", "1") != "0")


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "5000")), debug=False)
