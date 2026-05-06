# OpenList STRM 后台服务

这是一个用于从 OpenList/AList 类服务同步媒体目录、生成 STRM 文件并下载 NFO、图片、字幕等元数据的工具。Flask 后台服务使用数据库保存全部配置，用户可以直接在网页里维护配置和触发任务；命令行模式仍保留 YAML 兼容。

## 功能概览

- 异步扫描 OpenList 目录，按原目录结构生成本地媒体库。
- 视频文件生成 `.strm`，元数据文件可直接下载到本地。
- 支持多源目录、增量同步、失效 STRM 清理、排除规则和大小过滤。
- Flask 后台支持网页配置、提交任务、定时执行、查看状态、查看日志、查看统计结果和重复触发。
- 任务详情页会轮询 `progress_json`，实时显示阶段、当前路径、已处理路径和进度条。

## 安装

```bash
pip install -r requirements.txt
```

如果只使用 Flask 后台，可以不编辑 `config.yaml`。如需命令行模式，建议复制配置模板：

```bash
copy config.example.yaml config.yaml
```

Linux/macOS 使用：

```bash
cp config.example.yaml config.yaml
```

## Flask 后台配置

启动服务：

```bash
python app.py
```

默认监听 `http://127.0.0.1:5000`。打开浏览器后进入“配置”页面，编辑默认配置档案即可。

后台配置会写入 `data/tasks.sqlite3` 的 `config_profiles` 表。任务创建时会保存一份配置快照，因此历史任务重跑时不会被之后的配置修改影响。

配置页面支持：

- OpenList 地址、token、用户名、密码。
- 源目录到本地目录映射，例如 `/movies => ./downloads/movies`。
- STRM URL 格式、同步模式、元数据下载、清理失效 STRM。
- 并发数、超时、重试、代理、SSL 验证。
- 视频扩展名、排除正则和文件大小过滤。

### 定时运行与 Cron

每个配置档案都可以添加多条 cron 规则。进入“配置”页面，打开某个配置档案，在“定时规则”区域填写规则名称和 cron 表达式后保存即可。后台服务会启动内置调度线程，到点后自动创建一个普通后台任务，任务状态、错误日志、取消和重跑能力都沿用现有任务系统。

常用示例：

```text
0 3 * * *      每天 03:00 执行
*/30 * * * *   每 30 分钟执行
0 */6 * * *    每 6 小时执行
```

定时规则保存在 `data/tasks.sqlite3` 的 `schedule_rules` 表。一个配置档案可以同时启用多条规则；规则可在页面上单独启用、停用、修改或删除。`/metrics` 会返回定时规则总数和启用数量。

后台任务启动时会自动恢复数据库里处于 `queued` 或 `running` 的任务。服务异常重启后，遗留的 `running` 任务会回到队列等待重新执行。

### 后台登录鉴权

后台默认开启登录鉴权，初始账号是 `admin` / `admin`。登录后可以在“管理员”页面修改用户名和密码。

如果需要通过脚本重置账号，也可以直接调用：

```python
from app import enable_auth
from task_service import TaskStore

store = TaskStore("data/tasks.sqlite3")
enable_auth(store, "admin", "your-strong-password")
```

任务、配置和日志页面都会要求登录；`/metrics` 可用于查看运行指标。

## Docker Compose 部署

仓库已提供 `Dockerfile` 和 `docker-compose.yml`，适合直接部署 Flask 后台服务。默认镜像为：

```text
ghcr.io/sakurajimmai/open2strm:latest
```

首次启动：

```bash
docker compose pull
docker compose up -d
```

启动后访问：

```text
http://服务器IP:5000
```

默认账号为 `admin` / `admin`，首次登录后请进入“管理员”页面修改密码。

Compose 默认挂载两个目录：

```text
./data      -> /app/data       SQLite 数据库、登录设置、任务状态、cron 规则
./downloads -> /app/downloads  默认下载和 STRM 输出目录
```

常用运维命令：

```bash
docker compose logs -f
docker compose restart
docker compose down
docker compose pull
docker compose up -d
```

升级时保留 `data/` 和 `downloads/`，拉取最新镜像并启动即可：

```bash
docker compose down
docker compose pull
docker compose up -d
```

如果需要在本机基于当前源码构建镜像，可以继续使用：

```bash
docker compose up -d --build
```

备份时至少备份 `data/tasks.sqlite3`。该文件包含后台配置、任务记录、错误日志、定时规则和登录设置。生产部署建议同时备份 `downloads/`，并定期查看 `/metrics`。

如果宿主机目录不是当前项目目录，可以在 `docker-compose.yml` 中调整卷映射，例如：

```yaml
volumes:
  - /opt/strm/data:/app/data
  - /mnt/media/strm:/app/downloads
```

### GitHub Actions 镜像发布

推送到 `main` 分支或创建 `v*.*.*` 标签时，GitHub Actions 会自动构建并推送 Docker 镜像到 GHCR：

```text
ghcr.io/sakurajimmai/open2strm:latest
ghcr.io/sakurajimmai/open2strm:main
ghcr.io/sakurajimmai/open2strm:sha-<commit>
ghcr.io/sakurajimmai/open2strm:<tag>
```

工作流会在每次发布后清理 GHCR 包版本，只保留最新 5 个 Docker 版本。工作流文件位于 `.github/workflows/docker.yml`。

## 命令行 YAML 配置

命令行模式仍然读取 `config.yaml`：

```yaml
openlist:
  base_url: "https://your-openlist-server.com"
  token: ""
  source_paths:
    "/movies": "./downloads/movies"
    "/tv-shows": "./downloads/tv-shows"
  password: ""

local:
  base_path: "./downloads"

strm:
  url_format: "custom"
  custom_prefix: "http://192.168.1.100:5244"
  url_encode: false

sync:
  mode: "incremental"
  cleanup_invalid: true
  confirm_mode: false
```

`sync.mode` 可选值：

- `full`：清空本地输出后重新生成。
- `incremental`：添加新增文件，并清理远程已删除的 STRM。
- `update-only`：只添加新增文件，不删除本地文件。

Flask 后台不再依赖这个文件；它只用于 `python strm.py`。

可用环境变量：

```bash
set STRM_CONFIG=config.yaml
set STRM_DATA_DIR=data
set PORT=5000
python app.py
```

PowerShell：

```powershell
$env:STRM_CONFIG="config.yaml"
$env:STRM_DATA_DIR="data"
$env:PORT="5000"
python app.py
```

运行态数据保存到 `data/`：

- `data/tasks.sqlite3`：任务状态数据库。

错误日志写入 `tasks.sqlite3` 的 `error_logs` 表，前端“错误日志”页面只展示 ERROR 级别记录。

## API 使用

提交任务，默认使用数据库里的默认配置档案：

```bash
curl -X POST http://127.0.0.1:5000/tasks ^
  -H "Content-Type: application/json" ^
  -d "{}"
```

指定配置档案：

```bash
curl -X POST http://127.0.0.1:5000/tasks ^
  -H "Content-Type: application/json" ^
  -d "{\"profile_id\":1}"
```

查询任务列表：

```bash
curl http://127.0.0.1:5000/tasks
```

查询单个任务：

```bash
curl http://127.0.0.1:5000/tasks/1
```

筛选错误日志：

```bash
curl "http://127.0.0.1:5000/logs.json?task_id=1&source=task_manager&keyword=timeout&limit=50"
```

重复执行历史任务：

```bash
curl -X POST http://127.0.0.1:5000/tasks/1/rerun
```

取消排队中或运行中的任务：

```bash
curl -X POST http://127.0.0.1:5000/tasks/1/cancel
```

运行指标：

```bash
curl http://127.0.0.1:5000/metrics
```

## 命令行模式

仍然可以直接运行原始同步流程：

```bash
python strm.py
```

指定配置：

```bash
python strm.py -c config.yaml
```

命令行模式会保留 `sync.confirm_mode` 的交互确认行为，适合临时调试或兼容旧流程。

## 支持文件类型

视频文件会生成 STRM：

```text
.mp4 .mkv .avi .mov .wmv .flv .webm .m4v .mpg .mpeg .3gp .rmvb .ts .m2ts .vob .asf .rm .m3u8
```

元数据文件可下载：

```text
.nfo .jpg .jpeg .png .gif .bmp .webp .svg .ico .srt .ass .ssa .vtt .sub
```

## 开发与验证

正式仓库不保留测试文件。需要本地回归时，可以自行创建 `tests/` 并使用 Python `unittest`：

```bash
python -m unittest discover -s tests -p "test_*.py" -v
```

`tests/` 已加入 `.gitignore` 和 `.dockerignore`，不会进入 GitHub 仓库或 Docker 构建上下文。

语法检查：

```bash
python -m py_compile app.py task_service.py strm.py
```

## 注意事项

- Flask 后台配置保存在 SQLite 数据库里，`data/` 不要提交到公共仓库。
- `config.yaml` 仍可能包含账号、密码或 token，仅在命令行模式下使用。
- 后台 worker 当前是单线程串行执行，适合保护同一输出目录不被并发写入。
- 任务详情会展示当前阶段、当前路径和取消请求状态；取消运行中任务需要等下载流程下一次检查取消信号后生效。
- 大目录首次同步耗时较长，建议先用小目录验证配置。
- 生产部署时建议尽快把默认密码改掉，并把 `data/tasks.sqlite3` 纳入备份。
