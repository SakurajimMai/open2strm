"""配置档案表单转换工具。"""

from __future__ import annotations

from copy import deepcopy
import re
from typing import Any, Dict
from urllib.parse import urlparse

from task_service import default_config


class ConfigValidationError(ValueError):
    """配置表单校验失败。"""

    def __init__(self, errors: list[str]):
        super().__init__("; ".join(errors))
        self.errors = errors


def parse_bool(value: Any) -> bool:
    return str(value).lower() in {"1", "true", "yes", "on"}


def parse_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def parse_source_paths(text: str) -> Dict[str, str]:
    """解析源目录映射，每行格式为 远程路径 => 本地路径。"""
    source_paths: Dict[str, str] = {}
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if "=>" in line:
            remote, local = line.split("=>", 1)
        else:
            remote, local = line, "./downloads"
        remote = remote.strip()
        local = local.strip()
        if remote:
            source_paths[remote] = local or "./downloads"
    return source_paths or {"/": "./downloads"}


def source_paths_to_text(config: Dict[str, Any]) -> str:
    source_paths = config.get("openlist", {}).get("source_paths")
    if isinstance(source_paths, dict):
        return "\n".join(f"{remote} => {local}" for remote, local in source_paths.items())
    if isinstance(source_paths, list):
        return "\n".join(str(path) for path in source_paths)
    source_path = config.get("openlist", {}).get("source_path", "/")
    local_path = config.get("local", {}).get("base_path", "./downloads")
    return f"{source_path} => {local_path}"


def parse_list_lines(text: str) -> list[str]:
    return [line.strip() for line in text.splitlines() if line.strip()]


def config_from_form(form: Dict[str, Any], validate: bool = True) -> Dict[str, Any]:
    """从 Flask 表单构建下载配置。"""
    config = deepcopy(default_config())
    config["openlist"] = {
        "base_url": form.get("openlist_base_url", "").strip(),
        "username": form.get("openlist_username", "").strip(),
        "password": form.get("openlist_password", ""),
        "token": form.get("openlist_token", ""),
        "source_paths": parse_source_paths(form.get("source_paths", "")),
    }
    config["local"] = {"base_path": form.get("local_base_path", "./downloads").strip() or "./downloads"}
    config["strm"] = {
        "url_format": form.get("strm_url_format", "full"),
        "custom_prefix": form.get("strm_custom_prefix", "").strip(),
        "url_encode": parse_bool(form.get("strm_url_encode")),
    }
    config["max_depth"] = parse_int(form.get("max_depth"), 10)
    config["concurrent_downloads"] = parse_int(form.get("concurrent_downloads"), 5)
    config["download_metadata"] = parse_bool(form.get("download_metadata"))
    config["sync"] = {
        "mode": form.get("sync_mode", "incremental"),
        "cleanup_invalid": parse_bool(form.get("cleanup_invalid")),
        "confirm_mode": False,
    }
    config["advanced"] = {
        "timeout": parse_int(form.get("advanced_timeout"), 30),
        "retry_count": parse_int(form.get("advanced_retry_count"), 3),
        "retry_delay": parse_int(form.get("advanced_retry_delay"), 5),
        "verify_ssl": parse_bool(form.get("advanced_verify_ssl")),
        "proxy": form.get("advanced_proxy", "").strip(),
    }
    config["filter"] = {
        "video_extensions": parse_list_lines(form.get("filter_video_extensions", "")),
        "exclude_patterns": parse_list_lines(form.get("filter_exclude_patterns", "")),
        "min_file_size": parse_int(form.get("filter_min_file_size"), 0),
        "max_file_size": parse_int(form.get("filter_max_file_size"), 0),
    }
    if not config["advanced"]["proxy"]:
        config["advanced"].pop("proxy")
    if validate:
        validate_config(config)
    return config


def validate_config(config: Dict[str, Any]) -> None:
    """校验网页配置，尽量在任务创建前发现明显错误。"""
    errors: list[str] = []
    openlist = config.get("openlist", {})
    base_url = openlist.get("base_url", "").strip()
    if base_url:
        parsed = urlparse(base_url)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            errors.append("OpenList 地址必须是 http 或 https URL")

    source_paths = openlist.get("source_paths")
    if not isinstance(source_paths, dict) or not source_paths:
        errors.append("至少需要配置一个源目录映射")
    else:
        for remote, local in source_paths.items():
            if not str(remote).startswith("/"):
                errors.append(f"远程路径必须以 / 开头: {remote}")
            if not str(local).strip():
                errors.append(f"本地目录不能为空: {remote}")

    if config.get("max_depth", 0) < 0:
        errors.append("最大深度不能小于 0")
    if config.get("concurrent_downloads", 0) < 1:
        errors.append("并发数必须大于 0")

    sync_mode = config.get("sync", {}).get("mode")
    if sync_mode not in {"incremental", "update-only", "full"}:
        errors.append("同步模式必须是 incremental、update-only 或 full")

    strm_config = config.get("strm", {})
    url_format = strm_config.get("url_format")
    if url_format not in {"full", "relative", "custom"}:
        errors.append("URL 格式必须是 full、relative 或 custom")
    if url_format == "custom":
        custom_prefix = strm_config.get("custom_prefix", "").strip()
        parsed = urlparse(custom_prefix)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            errors.append("自定义 URL 前缀必须是 http 或 https URL")

    advanced = config.get("advanced", {})
    if advanced.get("timeout", 0) < 1:
        errors.append("请求超时必须大于 0")
    if advanced.get("retry_count", 0) < 0:
        errors.append("重试次数不能小于 0")
    if advanced.get("retry_delay", 0) < 0:
        errors.append("重试延迟不能小于 0")

    filter_config = config.get("filter", {})
    min_size = filter_config.get("min_file_size", 0)
    max_size = filter_config.get("max_file_size", 0)
    if min_size < 0 or max_size < 0:
        errors.append("文件大小过滤不能小于 0")
    if max_size and min_size > max_size:
        errors.append("最小文件大小不能大于最大文件大小")

    for pattern in filter_config.get("exclude_patterns", []):
        try:
            re.compile(pattern)
        except re.error as exc:
            errors.append(f"排除正则无效: {pattern} ({exc})")

    if errors:
        raise ConfigValidationError(errors)


def form_values_from_config(config: Dict[str, Any]) -> Dict[str, Any]:
    """把配置 dict 展平为模板字段。"""
    openlist = config.get("openlist", {})
    local = config.get("local", {})
    strm = config.get("strm", {})
    sync = config.get("sync", {})
    advanced = config.get("advanced", {})
    filter_config = config.get("filter", {})

    return {
        "openlist_base_url": openlist.get("base_url", ""),
        "openlist_username": openlist.get("username", ""),
        "openlist_password": openlist.get("password", ""),
        "openlist_token": openlist.get("token", ""),
        "source_paths": source_paths_to_text(config),
        "local_base_path": local.get("base_path", "./downloads"),
        "strm_url_format": strm.get("url_format", "full"),
        "strm_custom_prefix": strm.get("custom_prefix", ""),
        "strm_url_encode": strm.get("url_encode", False),
        "max_depth": config.get("max_depth", 10),
        "concurrent_downloads": config.get("concurrent_downloads", 5),
        "download_metadata": config.get("download_metadata", True),
        "sync_mode": sync.get("mode", "incremental"),
        "cleanup_invalid": sync.get("cleanup_invalid", True),
        "advanced_timeout": advanced.get("timeout", 30),
        "advanced_retry_count": advanced.get("retry_count", 3),
        "advanced_retry_delay": advanced.get("retry_delay", 5),
        "advanced_verify_ssl": advanced.get("verify_ssl", True),
        "advanced_proxy": advanced.get("proxy", ""),
        "filter_video_extensions": "\n".join(filter_config.get("video_extensions", [])),
        "filter_exclude_patterns": "\n".join(filter_config.get("exclude_patterns", [])),
        "filter_min_file_size": filter_config.get("min_file_size", 0),
        "filter_max_file_size": filter_config.get("max_file_size", 0),
    }
