# Repository Guidelines

## 项目结构与模块组织
本仓库是一个 Python STRM 同步工具，核心下载逻辑在 `strm.py`，Flask 后台服务入口在 `app.py`。`task_service.py` 负责 SQLite 配置档案、任务状态、后台队列、任务恢复、cron 调度、鉴权设置和错误日志，`config_forms.py` 负责网页表单到配置 dict 的转换与校验，`templates/` 与 `static/` 提供管理控制台。Flask 模式下所有配置和定时规则保存在 `data/tasks.sqlite3`，`config.yaml` 仅用于命令行兼容。

## 运行、测试与开发命令
先安装依赖：
```bash
pip install -r requirements.txt
```
本地运行主程序：
```bash
python strm.py
```
启动 Flask 后台服务：
```bash
python app.py
```
Docker 部署：
```bash
docker compose up -d --build
```
命令行指定配置文件：
```bash
python strm.py -c custom_config.yaml
```
运行测试：
```bash
python -m unittest discover -s tests -p "test_*.py" -v
```
修改下载流程或 cron 调度后建议再用小范围源目录做一次烟雾测试，检查任务详情、进度字段、`downloads/`、定时规则状态和数据库错误日志。

## 代码风格与命名规范
Python 代码使用 4 空格缩进，优先保持现有的异步 `async/await` 写法。函数与变量使用 `snake_case`，类使用 `PascalCase`，常量使用 `UPPER_SNAKE_CASE`。新增日志与注释应简短、明确，尽量使用中文说明业务意图。YAML 配置键保持小写、下划线风格，例如 `base_url`、`source_path`。

## 测试指南
测试使用 Python `unittest`，测试文件放在 `tests/`，命名采用 `test_*.py`。新增任务管理、cron 调度、配置解析、鉴权、日志筛选或下载行为时，优先补单元测试；涉及真实 OpenList 的流程保留为小范围烟雾测试。

## 提交与合并请求
当前工作区未包含 Git 历史，无法提取既有提交风格；建议使用简短的祈使句提交信息，例如 `fix retry backoff` 或 `add proxy support`。PR 需要说明变更目的、影响范围和验证方式；若涉及配置或输出格式变化，请附上示例配置片段或关键日志。不要提交 `config.yaml`、`downloads/` 或日志文件。

## 安全与配置提示
Flask 后台的账号、token、目录映射、cron 规则、登录设置和错误日志都写入 `data/tasks.sqlite3`，不要提交 `data/`。Docker 部署会挂载 `./data:/app/data` 和 `./downloads:/app/downloads`，这两个目录都属于运行态数据。`config.yaml` 仅用于命令行模式，可能包含账号、密码或 token，同样不要共享到公共仓库。后台任务创建时会保存配置快照，重跑历史任务和定时任务都应沿用任务创建时的快照；服务启动会恢复 `queued`/`running` 任务。
