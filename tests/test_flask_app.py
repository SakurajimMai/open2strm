import os
import unittest
import uuid
from pathlib import Path

os.environ["STRM_START_WORKER"] = "0"

from app import create_app
from task_service import DEFAULT_AUTH_PASSWORD, DEFAULT_AUTH_USERNAME


class FlaskAppTest(unittest.TestCase):
    def _make_app(self, prefix: str):
        temp_root = Path.cwd() / ".test_tmp"
        temp_root.mkdir(exist_ok=True)
        temp_dir = temp_root / f"{prefix}_{uuid.uuid4().hex}"
        temp_dir.mkdir()
        app = create_app(data_dir=temp_dir, default_config="config.example.yaml", start_worker=False)
        app.config.update(TESTING=True)
        return app, app.test_client()

    def _login(self, client):
        response = client.post("/login", data={"username": DEFAULT_AUTH_USERNAME, "password": DEFAULT_AUTH_PASSWORD})
        self.assertEqual(response.status_code, 302)

    def test_health_routes_removed_and_create_task_api(self):
        app, client = self._make_app("flask_app")

        health = client.get("/health")
        self.assertEqual(health.status_code, 404)
        healthz = client.get("/healthz")
        self.assertEqual(healthz.status_code, 404)

        self._login(client)
        response = client.post("/tasks", json={}, headers={"Accept": "application/json"})
        self.assertEqual(response.status_code, 202)
        payload = response.get_json()
        self.assertEqual(payload["status"], "queued")
        self.assertIsInstance(payload["id"], int)

    def test_login_sets_persistent_session_cookie(self):
        _, client = self._make_app("flask_cookie")

        response = client.post("/login", data={"username": DEFAULT_AUTH_USERNAME, "password": DEFAULT_AUTH_PASSWORD})

        self.assertEqual(response.status_code, 302)
        cookie = response.headers.get("Set-Cookie", "")
        self.assertIn("session=", cookie)
        self.assertTrue("Expires=" in cookie or "Max-Age=" in cookie)

    def test_config_page_updates_profile_and_task_uses_profile(self):
        app, client = self._make_app("flask_config")

        self._login(client)
        profile_id = app.config["DEFAULT_PROFILE_ID"]
        response = client.post(
            f"/config/{profile_id}",
            data={
                "name": "Web配置",
                "openlist_base_url": "https://openlist.example.com",
                "openlist_token": "token-from-web",
                "source_paths": "/movies => ./downloads/movies",
                "local_base_path": "./downloads",
                "strm_url_format": "custom",
                "strm_custom_prefix": "http://127.0.0.1:5244",
                "sync_mode": "incremental",
                "max_depth": "8",
                "concurrent_downloads": "4",
                "download_metadata": "on",
                "cleanup_invalid": "on",
                "advanced_timeout": "30",
                "advanced_retry_count": "3",
                "advanced_retry_delay": "5",
                "advanced_verify_ssl": "on",
                "filter_min_file_size": "0",
                "filter_max_file_size": "0",
            },
        )
        self.assertEqual(response.status_code, 302)

        response = client.post(
            "/tasks",
            json={"profile_id": profile_id},
            headers={"Accept": "application/json"},
        )
        task_id = response.get_json()["id"]
        task = app.config["TASK_STORE"].get_task(task_id)

        self.assertEqual(task["profile_name"], "Web配置")
        self.assertIn("token-from-web", task["config_json"])
        self.assertIsInstance(app.config["TASK_STORE"].list_error_logs(), list)

    def test_config_page_manages_multiple_schedule_rules(self):
        app, client = self._make_app("flask_schedules")
        self._login(client)
        profile_id = app.config["DEFAULT_PROFILE_ID"]

        first = client.post(
            f"/config/{profile_id}/schedules",
            data={"schedule_name": "每天凌晨", "cron_expr": "0 3 * * *", "enabled": "on"},
        )
        second = client.post(
            f"/config/{profile_id}/schedules",
            data={"schedule_name": "每六小时", "cron_expr": "0 */6 * * *", "enabled": "on"},
        )

        self.assertEqual(first.status_code, 302)
        self.assertEqual(second.status_code, 302)
        rules = app.config["TASK_STORE"].list_schedule_rules(profile_id=profile_id)
        self.assertEqual(len(rules), 2)
        self.assertEqual([rule["name"] for rule in rules], ["每天凌晨", "每六小时"])

        response = client.get(f"/config/{profile_id}")
        html = response.get_data(as_text=True)
        self.assertIn("每天凌晨", html)
        self.assertIn("每六小时", html)

        rule_id = rules[0]["id"]
        response = client.post(
            f"/schedules/{rule_id}",
            data={"schedule_name": "每天四点", "cron_expr": "0 4 * * *"},
        )
        self.assertEqual(response.status_code, 302)
        updated = app.config["TASK_STORE"].get_schedule_rule(rule_id)
        self.assertEqual(updated["name"], "每天四点")
        self.assertFalse(updated["enabled"])

        response = client.post(f"/schedules/{rules[1]['id']}/delete")
        self.assertEqual(response.status_code, 302)
        self.assertEqual(len(app.config["TASK_STORE"].list_schedule_rules(profile_id=profile_id)), 1)

    def test_task_detail_exposes_progress_for_frontend_polling(self):
        app, client = self._make_app("flask_progress")
        self._login(client)

        response = client.post("/tasks", json={}, headers={"Accept": "application/json"})
        task_id = response.get_json()["id"]
        app.config["TASK_STORE"].update_progress(
            task_id,
            {
                "phase": "扫描远程目录",
                "current_path": "/movies",
                "processed_paths": 12,
                "progress_percent": 34,
            },
        )

        response = client.get(f"/tasks/{task_id}", headers={"Accept": "application/json"})
        payload = response.get_json()
        self.assertEqual(payload["progress"]["phase"], "扫描远程目录")
        self.assertEqual(payload["progress"]["current_path"], "/movies")
        self.assertEqual(payload["progress"]["progress_percent"], 34)

        response = client.get(f"/tasks/{task_id}")
        html = response.get_data(as_text=True)
        self.assertIn("data-task-progress", html)
        self.assertIn("data-progress-phase", html)
        self.assertIn("data-progress-current", html)

    def test_admin_settings_page_is_available(self):
        _, client = self._make_app("flask_admin")
        self._login(client)

        response = client.get("/admin")
        self.assertEqual(response.status_code, 200)
        self.assertIn("管理员设置", response.get_data(as_text=True))

    def test_cancel_and_metrics_endpoints(self):
        app, client = self._make_app("flask_cancel")
        self._login(client)

        response = client.post("/tasks", json={}, headers={"Accept": "application/json"})
        task_id = response.get_json()["id"]

        response = client.post(f"/tasks/{task_id}/cancel", headers={"Accept": "application/json"})
        self.assertEqual(response.status_code, 202)
        self.assertTrue(response.get_json()["cancel_requested"])
        self.assertEqual(app.config["TASK_STORE"].get_task(task_id)["status"], "cancelled")

        response = client.get("/metrics")
        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["status"], "ok")
        self.assertEqual(payload["tasks"]["cancelled"], 1)
        self.assertEqual(payload["schedule_rules"]["total"], 0)


if __name__ == "__main__":
    unittest.main()
