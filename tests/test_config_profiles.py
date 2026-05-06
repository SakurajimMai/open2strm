import json
import unittest
import uuid
from pathlib import Path

from config_forms import ConfigValidationError, config_from_form
from task_service import TaskStore, default_config


class ConfigProfileTest(unittest.TestCase):
    def test_profile_save_and_task_snapshot(self):
        temp_root = Path.cwd() / ".test_tmp"
        temp_root.mkdir(exist_ok=True)
        temp_dir = temp_root / f"config_profiles_{uuid.uuid4().hex}"
        temp_dir.mkdir()
        store = TaskStore(temp_dir / "tasks.sqlite3")

        profile = default_config()
        profile["openlist"]["base_url"] = "https://openlist.example.com"
        profile["openlist"]["token"] = "secret-token"
        profile["openlist"]["source_paths"] = {"/movies": "./downloads/movies"}

        profile_id = store.create_config_profile("主配置", profile)
        saved = store.get_config_profile(profile_id)
        self.assertEqual(saved["name"], "主配置")
        self.assertEqual(saved["config"]["openlist"]["base_url"], "https://openlist.example.com")

        task_id = store.create_task(profile_id)
        task = store.get_task(task_id)
        self.assertEqual(task["profile_id"], profile_id)
        self.assertEqual(json.loads(task["config_json"])["openlist"]["token"], "secret-token")

        profile["openlist"]["token"] = "changed-token"
        store.update_config_profile(profile_id, "主配置", profile)

        rerun_id = store.create_rerun(task_id)
        rerun = store.get_task(rerun_id)
        self.assertEqual(json.loads(rerun["config_json"])["openlist"]["token"], "secret-token")

    def test_ensure_default_profile_creates_profile(self):
        temp_root = Path.cwd() / ".test_tmp"
        temp_root.mkdir(exist_ok=True)
        temp_dir = temp_root / f"default_profile_{uuid.uuid4().hex}"
        temp_dir.mkdir()
        store = TaskStore(temp_dir / "tasks.sqlite3")

        profile_id = store.ensure_default_config_profile()
        profiles = store.list_config_profiles()

        self.assertEqual(len(profiles), 1)
        self.assertEqual(profiles[0]["id"], profile_id)
        self.assertEqual(profiles[0]["name"], "默认配置")

    def test_config_form_rejects_invalid_values(self):
        with self.assertRaises(ConfigValidationError) as context:
            config_from_form(
                {
                    "openlist_base_url": "ftp://example.com",
                    "source_paths": "movies => ",
                    "strm_url_format": "custom",
                    "strm_custom_prefix": "not-url",
                    "sync_mode": "bad",
                    "max_depth": "-1",
                    "concurrent_downloads": "0",
                    "advanced_timeout": "0",
                    "advanced_retry_count": "-1",
                    "advanced_retry_delay": "-1",
                    "filter_min_file_size": "100",
                    "filter_max_file_size": "10",
                    "filter_exclude_patterns": "[",
                }
            )

        joined = "\n".join(context.exception.errors)
        self.assertIn("OpenList 地址", joined)
        self.assertIn("远程路径必须以 / 开头", joined)
        self.assertIn("排除正则无效", joined)


if __name__ == "__main__":
    unittest.main()
