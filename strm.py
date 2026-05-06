#!/usr/bin/env python3
"""
OpenList STRM下载工具
用于从OpenList服务器下载视频目录结构并生成STRM文件
支持下载NFO元数据和图片文件
"""

import asyncio
import aiohttp
import aiofiles
import yaml
import os
import sys
import logging
import hashlib
import urllib.parse
import re
import shutil
from pathlib import Path
from typing import Callable, Dict, List, Optional, Set, Tuple
from datetime import datetime
import argparse
from functools import wraps
import time


# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('strm_downloader.log', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)


class DownloadCancelled(Exception):
    """后台任务收到取消请求。"""


def retry_async(retries=3, delay=1.0, backoff=2.0):
    """异步重试装饰器"""
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            retry_count = 0
            current_delay = delay
            
            while retry_count < retries:
                try:
                    return await func(*args, **kwargs)
                except DownloadCancelled:
                    raise
                except Exception as e:
                    retry_count += 1
                    if retry_count >= retries:
                        logger.error(f"{func.__name__} 失败，已重试 {retries} 次: {e}")
                        raise
                    
                    logger.warning(f"{func.__name__} 失败，{current_delay}秒后重试 ({retry_count}/{retries}): {e}")
                    await asyncio.sleep(current_delay)
                    current_delay *= backoff
                    
        return wrapper
    return decorator


class OpenListClient:
    """OpenList API客户端"""
    
    def __init__(self, config: Dict):
        self.config = config  # 保存配置引用
        self.base_url = config['openlist']['base_url'].rstrip('/')
        self.username = config['openlist'].get('username', '')
        self.password = config['openlist'].get('password', '')
        # 支持直接使用token
        self.token = config['openlist'].get('token', '')
        self.session = None
        self.semaphore = asyncio.Semaphore(config.get('concurrent_downloads', 5))
        
        # 高级配置
        advanced = config.get('advanced', {})
        self.timeout = aiohttp.ClientTimeout(total=advanced.get('timeout', 30))
        self.verify_ssl = advanced.get('verify_ssl', True)
        self.proxy = advanced.get('proxy')
        self.retry_count = advanced.get('retry_count', 3)
        self.retry_delay = advanced.get('retry_delay', 5)
        
    async def __aenter__(self):
        """异步上下文管理器入口"""
        connector = aiohttp.TCPConnector(ssl=self.verify_ssl)
        self.session = aiohttp.ClientSession(
            connector=connector,
            timeout=self.timeout
        )
        await self.login()
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """异步上下文管理器出口"""
        try:
            if self.session:
                await self.session.close()
        except Exception as e:
            logger.warning(f"关闭session时出错: {e}")
        return False
        
    async def close(self):
        """手动关闭客户端"""
        try:
            if self.session:
                await self.session.close()
        except Exception as e:
            logger.warning(f"关闭session时出错: {e}")
            
    @retry_async(retries=3, delay=2.0)
    async def login(self):
        """登录获取token"""
        # 如果已经有token，直接使用
        if self.token:
            logger.info("使用配置的token")
            return
            
        # 如果没有用户名，尝试匿名访问
        if not self.username:
            logger.info("未配置用户名和token，尝试匿名访问")
            return
            
        login_url = f"{self.base_url}/api/auth/login"
        login_data = {
            "username": self.username,
            "password": self.password
        }
        
        try:
            logger.debug(f"尝试登录: {login_url}")
            async with self.session.post(
                login_url, 
                json=login_data,
                proxy=self.proxy
            ) as resp:
                logger.debug(f"登录响应状态: {resp.status}")
                
                if resp.status == 200:
                    try:
                        data = await resp.json()
                        logger.debug(f"登录响应内容: {data}")
                        
                        if data and isinstance(data, dict):
                            # 检查响应code字段
                            response_code = data.get('code', 0)
                            if response_code != 200:
                                message = data.get('message', '未知错误')
                                logger.error(f"登录失败: {message} (code: {response_code})")
                                raise Exception(f"登录失败: {message}")
                            
                            # 尝试多种token字段
                            token_fields = [
                                ('data', 'token'),  # data.token
                                ('token',),         # token
                                ('access_token',),  # access_token
                                ('data', 'access_token'),  # data.access_token
                            ]
                            
                            for fields in token_fields:
                                try:
                                    current = data
                                    for field in fields:
                                        current = current[field]
                                    if current:
                                        self.token = current
                                        logger.info("登录成功")
                                        return
                                except (KeyError, TypeError):
                                    continue
                            
                            # 如果找不到token，记录完整响应
                            logger.warning(f"登录响应中未找到token，完整响应: {data}")
                            raise Exception("登录响应中未找到token")
                        else:
                            raise Exception("无效的登录响应格式")
                    except ValueError as e:
                        text = await resp.text()
                        logger.error(f"解析登录响应失败: {e}, 响应内容: {text[:500]}")
                        raise Exception(f"解析登录响应失败: {e}")
                else:
                    text = await resp.text()
                    logger.warning(f"登录失败: {resp.status}, {text[:500]}")
                    raise Exception(f"登录失败: {resp.status}")
        except aiohttp.ClientError as e:
            logger.error(f"网络请求失败: {e}")
            raise
        except Exception as e:
            logger.error(f"登录出错: {e}")
            raise
            
    def _get_headers(self):
        """获取请求头"""
        headers = {}
        if self.token:
            headers['Authorization'] = self.token
        return headers
        
    async def list_dir(self, path: str, password: str = "") -> Dict:
        """列出目录内容"""
        list_url = f"{self.base_url}/api/fs/list"
        data = {
            "path": path,
            "password": password,
            "page": 1,
            "per_page": 0,
            "refresh": False
        }
        
        # 为目录列表使用更长的超时时间
        advanced = self.config.get('advanced', {})
        dir_timeout = advanced.get('dir_timeout', 120)  # 默认2分钟
        timeout = aiohttp.ClientTimeout(total=dir_timeout)
        
        async with self.semaphore:
            try:
                async with self.session.post(
                    list_url, 
                    json=data, 
                    headers=self._get_headers(),
                    proxy=self.proxy,
                    timeout=timeout
                ) as resp:
                    if resp.status == 200:
                        try:
                            result = await resp.json()
                            data = result.get('data', {})
                            # 添加调试信息
                            if not data:
                                logger.debug(f"目录响应为空 {path}: {result}")
                            return data
                        except ValueError as e:
                            logger.error(f"解析目录列表响应失败 {path}: {e}")
                            logger.debug(f"响应内容: {await resp.text()}")
                            return {}
                    else:
                        text = await resp.text()
                        logger.error(f"列出目录失败 {path}: HTTP {resp.status}")
                        logger.debug(f"错误响应: {text[:500]}")
                        return {}
            except Exception as e:
                import traceback
                logger.error(f"列出目录出错 {path}: {type(e).__name__}: {e}")
                logger.debug(f"完整错误堆栈: {traceback.format_exc()}")
                return {}
                
    async def get_file_info(self, path: str, password: str = "") -> Dict:
        """获取文件信息"""
        get_url = f"{self.base_url}/api/fs/get"
        data = {
            "path": path,
            "password": password
        }
        
        async with self.semaphore:
            try:
                async with self.session.post(
                    get_url,
                    json=data,
                    headers=self._get_headers(),
                    proxy=self.proxy
                ) as resp:
                    if resp.status == 200:
                        try:
                            result = await resp.json()
                            return result.get('data', {})
                        except ValueError as e:
                            logger.error(f"解析文件信息响应失败 {path}: {e}")
                            return {}
                    else:
                        text = await resp.text()
                        logger.error(f"获取文件信息失败 {path}: {resp.status}, {text[:200]}")
                        return {}
            except Exception as e:
                import traceback
                logger.error(f"获取文件信息出错 {path}: {type(e).__name__}: {e}")
                logger.debug(f"完整错误堆栈: {traceback.format_exc()}")
                return {}


class StrmDownloader:
    """STRM下载器"""
    
    # 默认视频文件扩展名
    DEFAULT_VIDEO_EXTENSIONS = {
        '.mp4', '.mkv', '.avi', '.mov', '.wmv', '.flv',
        '.webm', '.m4v', '.mpg', '.mpeg', '.3gp', '.rmvb',
        '.ts', '.m2ts', '.vob', '.asf', '.rm', '.m3u8'
    }
    
    # 图片文件扩展名
    IMAGE_EXTENSIONS = {
        '.jpg', '.jpeg', '.png', '.gif', '.bmp',
        '.webp', '.svg', '.ico', '.tiff', '.tif'
    }
    
    # NFO文件扩展名
    NFO_EXTENSIONS = {'.nfo'}
    
    # 字幕文件扩展名
    SUBTITLE_EXTENSIONS = {
        '.srt', '.ass', '.ssa', '.vtt', '.sub', '.sbv',
        '.lrc', '.txt', '.smi', '.rt', '.scc', '.ttml',
        '.dfxp', '.stl', '.xml', '.cap', '.itt', '.mpl'
    }
    
    def __init__(
        self,
        config: Dict,
        progress_callback: Optional[Callable[[Dict], None]] = None,
        cancel_checker: Optional[Callable[[], bool]] = None,
    ):
        self.config = config
        self.progress_callback = progress_callback
        self.cancel_checker = cancel_checker or (lambda: False)
        self.local_base_path = Path(config['local']['base_path'])
        self.local_base_path.mkdir(parents=True, exist_ok=True)
        self.max_depth = config.get('max_depth', 10)
        self.processed_paths: Set[str] = set()
        
        # 过滤器配置
        filter_config = config.get('filter', {})
        
        # 视频扩展名
        custom_video_ext = filter_config.get('video_extensions', [])
        if custom_video_ext:
            self.video_extensions = set(custom_video_ext)
        else:
            self.video_extensions = self.DEFAULT_VIDEO_EXTENSIONS
            
        # 排除模式
        self.exclude_patterns = []
        for pattern in filter_config.get('exclude_patterns', []):
            try:
                self.exclude_patterns.append(re.compile(pattern))
            except re.error as e:
                logger.warning(f"无效的正则表达式 '{pattern}': {e}")
                
        # 文件大小限制
        self.min_file_size = filter_config.get('min_file_size', 0)
        self.max_file_size = filter_config.get('max_file_size', 0)
        
        # 统计信息
        self.stats = {
            'strm_created': 0,
            'files_downloaded': 0,
            'directories_scanned': 0,
            'errors': 0,
            'start_time': time.time(),
            'current_path': '',
            'phase': '初始化',
        }

    def _check_cancelled(self) -> None:
        if self.cancel_checker():
            raise DownloadCancelled("任务已取消")

    def _emit_progress(self, phase: str, current_path: str = "") -> None:
        self.stats['phase'] = phase
        if current_path:
            self.stats['current_path'] = current_path
        self.stats['progress_percent'] = self._estimate_progress_percent(phase)
        if self.progress_callback:
            self.progress_callback(self.get_stats())

    def _estimate_progress_percent(self, phase: str) -> int:
        if phase in {'完成', '已取消'}:
            return 100
        if phase == '准备扫描':
            return 5
        if phase == '扫描本地文件':
            return 10
        if phase == '扫描远程目录':
            return min(85, 15 + int(self.stats['directories_scanned'] * 0.5))
        if phase == '创建 STRM':
            return min(95, 45 + int(self.stats['strm_created'] * 1.5))
        if phase == '下载元数据':
            return min(97, 70 + int(self.stats['files_downloaded'] * 1.0))
        if phase == '清理失效文件':
            return 92
        return self.stats.get('progress_percent', 0) or 0
        
    async def download(self, client: OpenListClient):
        """开始下载"""
        self._check_cancelled()
        self._emit_progress("准备扫描")
        # 获取源目录列表（支持多目录和目录映射）
        source_paths = self._get_source_paths()
        password = self.config['openlist'].get('password', '')
        
        # 检查运行模式
        sync_mode = await self._check_sync_mode()
        
        # 处理全量模式
        if sync_mode == 'full':
            logger.info("使用完全覆盖模式，清除所有本地文件...")
            for _, local_path in source_paths:
                self._check_cancelled()
                if local_path.exists():
                    shutil.rmtree(local_path)
                local_path.mkdir(parents=True, exist_ok=True)
        
        # 扫描现有本地文件（增量模式需要）
        local_files = {}
        if sync_mode in ['incremental', 'update-only']:
            self._emit_progress("扫描本地文件")
            local_files = await self._scan_local_files(source_paths)
            logger.info(f"发现 {len(local_files)} 个现有STRM文件")
        
        # 扫描远程文件
        remote_files = set()
        for source_path, local_path in source_paths:
            self._check_cancelled()
            logger.info(f"开始扫描: {source_path} -> {local_path}")
            await self._scan_directory(
                client, 
                source_path, 
                local_path,
                password,
                current_depth=0,
                remote_files=remote_files
            )
        
        # 清理失效文件
        if sync_mode == 'incremental' and self.config.get('sync', {}).get('cleanup_invalid', True):
            self._emit_progress("清理失效文件")
            await self._cleanup_invalid_files(local_files, remote_files)
        
        # 打印统计信息
        duration = time.time() - self.stats['start_time']
        logger.info("=" * 60)
        logger.info("扫描完成！")
        logger.info(f"总耗时: {duration:.2f} 秒")
        logger.info(f"扫描目录数: {self.stats['directories_scanned']}")
        logger.info(f"创建STRM文件数: {self.stats['strm_created']}")
        logger.info(f"下载文件数: {self.stats['files_downloaded']}")
        logger.info(f"清理文件数: {self.stats.get('files_cleaned', 0)}")
        logger.info(f"错误数: {self.stats['errors']}")
        logger.info("=" * 60)
        self._emit_progress("完成")
        return self.get_stats()

    def get_stats(self) -> Dict:
        """获取当前任务统计信息。"""
        stats = dict(self.stats)
        stats['duration'] = time.time() - self.stats['start_time']
        stats['processed_paths'] = len(self.processed_paths)
        return stats
        
    async def _scan_directory(
        self, 
        client: OpenListClient,
        remote_path: str,
        local_path: Path,
        password: str,
        current_depth: int,
        remote_files: Optional[Set[str]] = None
    ):
        """递归扫描目录"""
        try:
            self._check_cancelled()
            # 检查深度限制
            if current_depth > self.max_depth:
                logger.warning(f"达到最大深度限制: {remote_path}")
                return
                
            # 避免重复处理
            if remote_path in self.processed_paths:
                return
            self.processed_paths.add(remote_path)
            
            # 创建本地目录
            try:
                local_path.mkdir(parents=True, exist_ok=True)
            except Exception as e:
                logger.error(f"创建本地目录失败 {local_path}: {e}")
                self.stats['errors'] += 1
                return
            
            # 更新统计
            self.stats['directories_scanned'] += 1
            self._emit_progress("扫描远程目录", remote_path)
            logger.info(f"扫描目录 [{self.stats['directories_scanned']}]: {remote_path}")
            
            # 获取目录内容
            try:
                start_time = time.time()
                dir_data = await client.list_dir(remote_path, password)
                elapsed = time.time() - start_time
                
                # 显示网络耗时（如果启用）
                if self.config.get('advanced', {}).get('show_network_stats', False):
                    logger.debug(f"目录扫描耗时: {elapsed:.2f}s - {remote_path}")
            except Exception as e:
                logger.error(f"列出目录出错 {remote_path}: {type(e).__name__}: {e}")
                logger.debug(f"详细错误信息: {str(e)}")
                if hasattr(e, 'response'):
                    logger.debug(f"HTTP状态码: {getattr(e.response, 'status', 'N/A')}")
                self.stats['errors'] += 1
                
                # 检查是否跳过错误目录
                if self.config.get('skip_error_dirs', False):
                    logger.warning(f"跳过错误目录（skip_error_dirs=true）: {remote_path}")
                    return
                else:
                    # 为TimeoutError提供特殊处理建议
                    if isinstance(e, (asyncio.TimeoutError, TimeoutError)):
                        logger.info(f"提示：可以在配置中增加dir_timeout或启用skip_error_dirs跳过超时目录")
                    return
                
            if not dir_data:
                logger.warning(f"目录数据为空: {remote_path}")
                return
                
            # 检查响应结构
            if not isinstance(dir_data, dict):
                logger.error(f"目录响应格式错误 {remote_path}: {type(dir_data)}")
                return
                
            content = dir_data.get('content', [])
            
            # 确保content不为None并且是可迭代的
            if not content:
                logger.debug(f"目录内容为空: {remote_path}")
                return
            
            if not isinstance(content, (list, tuple)):
                logger.error(f"目录内容格式错误 {remote_path}: content类型为 {type(content)}")
                return
                
            tasks = []
            
            for item in content:
                self._check_cancelled()
                # 确保item不为None并且是字典
                if not item or not isinstance(item, dict):
                    logger.debug(f"跳过无效项目: {item}")
                    continue
                    
                item_name = item.get('name', '')
                if not item_name:
                    logger.debug(f"跳过无名称项目: {item}")
                    continue
                    
                item_path = f"{remote_path.rstrip('/')}/{item_name}"
                item_local_path = local_path / item_name
                
                if item.get('is_dir', False):
                    # 递归处理子目录
                    task = self._scan_directory(
                        client,
                        item_path,
                        item_local_path,
                        password,
                        current_depth + 1,
                        remote_files
                    )
                    tasks.append(task)
                else:
                    # 处理文件
                    # 检查是否被排除
                    if self._is_excluded(item_path):
                        logger.debug(f"文件被排除: {item_path}")
                        continue
                        
                    # 检查文件大小（如果有信息）
                    file_size = item.get('size', 0)
                    if not self._check_file_size(file_size, item_name):
                        continue
                        
                    file_ext = Path(item_name).suffix.lower()
                    
                    if file_ext in self.video_extensions:
                        # 记录远程文件
                        if remote_files is not None:
                            remote_files.add(item_path)
                        
                        # 生成STRM文件
                        task = self._create_strm_file(
                            client,
                            item_path,
                            item_local_path.with_suffix('.strm'),
                            password
                        )
                        tasks.append(task)
                        
                    elif file_ext in self.IMAGE_EXTENSIONS or file_ext in self.NFO_EXTENSIONS or file_ext in self.SUBTITLE_EXTENSIONS:
                        # 下载图片、NFO和字幕文件
                        if self.config.get('download_metadata', True):
                            task = self._download_file(
                                client,
                                item_path,
                                item_local_path,
                                password
                            )
                            tasks.append(task)
                            
            # 并发执行所有任务
            if tasks:
                # 显示任务队列信息
                logger.info(f"处理 {len(tasks)} 个任务...")
                
                # 如果配置了请求延迟，分批处理
                request_delay = self.config.get('request_delay', 0)
                if request_delay > 0 and len(tasks) > 1:
                    logger.debug(f"使用 {request_delay}s 请求延迟")
                    
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                # 检查任务执行结果
                success_count = 0
                for i, result in enumerate(results):
                    if isinstance(result, DownloadCancelled):
                        raise result
                    if isinstance(result, Exception):
                        logger.error(f"任务执行失败: {result}")
                        self.stats['errors'] += 1
                    else:
                        success_count += 1
                        
                if len(tasks) > 1:
                    logger.info(f"批次完成: {success_count}/{len(tasks)} 成功")
                    
                # 添加批次间延迟
                if request_delay > 0 and len(tasks) > 0:
                    await asyncio.sleep(request_delay)
                        
        except DownloadCancelled:
            raise
        except Exception as e:
            import traceback
            logger.error(f"扫描目录出错 {remote_path}: {type(e).__name__}: {e}")
            logger.debug(f"完整错误堆栈: {traceback.format_exc()}")
            self.stats['errors'] += 1
            
            # 检查是否跳过错误目录
            if self.config.get('skip_error_dirs', False):
                logger.warning(f"跳过错误目录（skip_error_dirs=true）: {remote_path}")
                return
            
    @retry_async(retries=3, delay=1.0)
    async def _create_strm_file(
        self,
        client: OpenListClient,
        remote_path: str,
        local_path: Path,
        password: str
    ):
        """创建STRM文件"""
        try:
            self._check_cancelled()
            self._emit_progress("创建 STRM", remote_path)
            # 文件名安全处理
            safe_local_path = self._sanitize_path(local_path)
            if safe_local_path != local_path:
                logger.debug(f"STRM文件名已清理: {local_path.name} -> {safe_local_path.name}")
                local_path = safe_local_path
                
            # 检查文件是否已存在
            if local_path.exists():
                logger.debug(f"STRM文件已存在，跳过: {local_path}")
                return
                
            # 获取STRM配置
            strm_config = self.config.get('strm', {})
            url_format = strm_config.get('url_format', 'full')
            custom_prefix = strm_config.get('custom_prefix', '').rstrip('/')
            url_encode = strm_config.get('url_encode', False)
            
            # 根据配置决定URL内容
            if url_format == 'relative':
                # 只使用相对路径
                strm_url = remote_path
            elif url_format == 'custom' and custom_prefix:
                # 使用自定义前缀 + /d + 相对路径
                strm_url = f"{custom_prefix}/d{remote_path}"
            else:
                # 默认：获取完整URL
                file_info = await client.get_file_info(remote_path, password)
                if not file_info:
                    logger.warning(f"无法获取文件信息用于STRM: {remote_path}")
                    return
                    
                strm_url = file_info.get('raw_url', '')
                if not strm_url:
                    logger.warning(f"无法获取直链: {remote_path}")
                    return
            
            # URL编码处理
            if url_encode and url_format != 'full':
                # 对路径部分进行编码（完整URL通常已经编码）
                strm_url = urllib.parse.quote(strm_url, safe='/:')
                
            # 确保父目录存在
            try:
                local_path.parent.mkdir(parents=True, exist_ok=True)
            except Exception as e:
                logger.error(f"创建STRM目录失败 {local_path.parent}: {e}")
                self.stats['errors'] += 1
                return
                
            # 写入STRM文件
            try:
                async with aiofiles.open(local_path, 'w', encoding='utf-8') as f:
                    await f.write(strm_url)
                    
                self.stats['strm_created'] += 1
                
                # 显示进度信息
                progress_interval = self.config.get('advanced', {}).get('progress_interval', 10)
                if self.stats['strm_created'] % progress_interval == 0:
                    logger.info(f"进度更新: 已创建 {self.stats['strm_created']} 个STRM文件")
                else:
                    logger.info(f"创建STRM [{self.stats['strm_created']}]: {local_path.name}")
            except DownloadCancelled:
                raise
            except Exception as e:
                logger.error(f"写入STRM文件失败 {local_path}: {e}")
                self.stats['errors'] += 1
            
        except DownloadCancelled:
            raise
        except Exception as e:
            self.stats['errors'] += 1
            logger.error(f"创建STRM文件失败 {local_path}: {type(e).__name__}: {e}")
            
    async def _download_file(
        self,
        client: OpenListClient,
        remote_path: str,
        local_path: Path,
        password: str
    ):
        """下载文件"""
        # 重试逻辑 - 针对下载优化
        max_retries = 3
        retry_delay = 1.0
        
        for attempt in range(max_retries):
            self._check_cancelled()
            try:
                await self._download_file_impl(client, remote_path, local_path, password)
                return  # 成功则直接返回
            except DownloadCancelled:
                raise
            except Exception as e:
                if attempt < max_retries - 1:
                    logger.warning(f"下载失败，{retry_delay}秒后重试 ({attempt+1}/{max_retries}) {remote_path}: {e}")
                    await asyncio.sleep(retry_delay)
                    retry_delay *= 2  # 指数退避
                else:
                    logger.error(f"下载重试失败 {remote_path}: {e}")
                    self.stats['errors'] += 1
                    return
                    
    async def _download_file_impl(
        self,
        client: OpenListClient,
        remote_path: str,
        local_path: Path,
        password: str
    ):
        """下载文件的实际实现"""
        try:
            self._check_cancelled()
            self._emit_progress("下载元数据", remote_path)
            # 检查文件是否已存在
            if local_path.exists():
                logger.debug(f"文件已存在，跳过: {local_path}")
                return
                
            # 文件名安全处理
            safe_local_path = self._sanitize_path(local_path)
            if safe_local_path != local_path:
                logger.debug(f"文件名已清理: {local_path.name} -> {safe_local_path.name}")
                local_path = safe_local_path
                
            # 再次检查清理后的文件是否存在
            if local_path.exists():
                logger.debug(f"清理后的文件已存在，跳过: {local_path}")
                return
                
            # 获取下载URL - 始终使用本地服务器进行下载
            # 首先尝试通过API获取raw_url
            file_info = await client.get_file_info(remote_path, password)
            if file_info and file_info.get('raw_url'):
                raw_url = file_info.get('raw_url')
                logger.debug(f"使用API获取的下载链接: {raw_url}")
            else:
                # 如果API获取失败，使用本地服务器构建下载URL
                encoded_path = urllib.parse.quote(remote_path, safe='/')
                raw_url = f"{client.base_url}/d{encoded_path}"
                logger.debug(f"API获取下载链接失败，使用本地服务器URL: {raw_url}")
                
            # 确保父目录存在
            try:
                local_path.parent.mkdir(parents=True, exist_ok=True)
            except Exception as e:
                logger.error(f"创建目录失败 {local_path.parent}: {e}")
                self.stats['errors'] += 1
                return
                
            # 下载文件（使用更长的超时时间）
            download_timeout = aiohttp.ClientTimeout(total=120)  # 2分钟超时
            
            # 构建请求头 - 添加浏览器标识以提高成功率
            headers = client._get_headers()
            headers.update({
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                'Accept': '*/*',
                'Accept-Encoding': 'gzip, deflate',
                'Connection': 'keep-alive'
            })
            
            async with client.session.get(
                raw_url,
                headers=headers,
                proxy=client.proxy,
                timeout=download_timeout
            ) as resp:
                if resp.status == 200:
                    try:
                        # 检查Content-Type
                        content_type = resp.headers.get('content-type', '').lower()
                        logger.debug(f"Content-Type: {content_type} for {remote_path}")
                        
                        # 读取内容
                        content = await resp.read()
                        
                        # 验证文件大小和内容
                        file_size = len(content)
                        
                        # 详细分析小文件内容
                        if file_size < 100:
                            logger.warning(f"文件太小: {remote_path} ({file_size} bytes)")
                            logger.warning(f"下载URL: {raw_url}")
                            logger.warning(f"Content-Type: {content_type}")
                            logger.warning(f"文件内容(文本): {content}")
                            
                            # 检查是否是JSON错误响应
                            if content_type == 'application/json' or content.startswith(b'{'):
                                try:
                                    import json
                                    error_data = json.loads(content.decode('utf-8'))
                                    error_code = error_data.get('code', 0)
                                    error_message = error_data.get('message', '')
                                    
                                    logger.error(f"服务器返回API错误 {remote_path}:")
                                    logger.error(f"  错误代码: {error_code}")
                                    logger.error(f"  错误信息: {error_message}")
                                    logger.error(f"  下载URL: {raw_url}")
                                    logger.error(f"  请求头: {headers}")
                                    
                                    # 如果是文件不存在，记录但不算作严重错误
                                    if 'object not found' in error_message or 'file not found' in error_message:
                                        logger.info(f"→ 服务器报告文件不存在，跳过")
                                        return  # 不算作错误，直接返回
                                    else:
                                        logger.error(f"→ 这可能是认证或权限问题！")
                                        # 尝试备用下载策略
                                        success = await self._try_alternative_download(client, remote_path, local_path, password)
                                        if success:
                                            logger.info(f"✅ 备用策略成功下载: {remote_path}")
                                            return
                                        
                                except Exception as e:
                                    logger.error(f"解析错误响应失败 {remote_path}: {e}")
                            
                            # 抛出异常以触发重试
                            raise Exception(f"下载到错误响应: {file_size} bytes")
                        
                        # 检查是否是HTML错误页面
                        if content.startswith(b'<!DOCTYPE') or content.startswith(b'<html'):
                            logger.warning(f"下载的是HTML页面而不是文件 {remote_path}")
                            logger.debug(f"HTML内容: {content[:200]}")
                            self.stats['errors'] += 1
                            return
                        
                        # 写入文件
                        async with aiofiles.open(local_path, 'wb') as f:
                            await f.write(content)
                        
                        # 验证写入的文件大小
                        actual_size = local_path.stat().st_size
                        if actual_size != file_size:
                            logger.warning(f"文件大小不匹配 {local_path}: 预期{file_size}, 实际{actual_size}")
                        
                        self.stats['files_downloaded'] += 1
                        
                        # 显示下载进度
                        progress_interval = self.config.get('advanced', {}).get('progress_interval', 10)
                        if self.stats['files_downloaded'] % progress_interval == 0:
                            logger.info(f"下载进度: 已完成 {self.stats['files_downloaded']} 个文件")
                        else:
                            logger.info(f"下载完成 [{self.stats['files_downloaded']}]: {local_path.name} ({file_size} bytes)")
                    except Exception as e:
                        logger.error(f"写入文件失败 {local_path}: {e}")
                        # 尝试删除损坏的文件
                        if local_path.exists():
                            try:
                                local_path.unlink()
                            except:
                                pass
                        self.stats['errors'] += 1
                elif resp.status == 404:
                    logger.warning(f"文件不存在: {remote_path}")
                    self.stats['errors'] += 1
                elif resp.status == 403:
                    logger.warning(f"无权限访问: {remote_path}")
                    self.stats['errors'] += 1
                else:
                    # 记录响应内容以便调试
                    error_content = await resp.text()
                    logger.error(f"下载失败 {remote_path}: HTTP {resp.status}")
                    logger.debug(f"错误响应内容: {error_content[:500]}")
                    self.stats['errors'] += 1
        
        except DownloadCancelled:
            raise
        except Exception as e:
            # 抛出异常由重试逻辑处理
            raise
            
    async def _try_alternative_download(
        self,
        client: OpenListClient,
        remote_path: str,
        local_path: Path,
        password: str
    ) -> bool:
        """尝试备用下载策略"""
        import urllib.parse
        
        # 备用策略列表（基于测试结果）
        strategies = [
            # 策略1: URL编码路径
            ('URL编码', f"{client.base_url}/d{urllib.parse.quote(remote_path, safe='/')}"),
            # 策略2: 双重编码（某些服务器需要）
            ('双重编码', f"{client.base_url}/d{urllib.parse.quote(urllib.parse.quote(remote_path, safe=''), safe='/')}"),
        ]
        
        for strategy_name, url in strategies:
            try:
                logger.info(f"尝试备用策略 {strategy_name}: {url}")
                
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                    'Accept': '*/*'
                }
                # 添加认证头
                headers.update(client._get_headers())
                
                timeout = aiohttp.ClientTimeout(total=60)
                async with client.session.get(url, headers=headers, timeout=timeout) as resp:
                    if resp.status == 200:
                        content = await resp.read()
                        file_size = len(content)
                        
                        # 检查是否是有效文件
                        if file_size > 100 and not content.startswith(b'{') and not content.startswith(b'<!DOCTYPE'):
                            # 保存文件
                            async with aiofiles.open(local_path, 'wb') as f:
                                await f.write(content)
                            
                            self.stats['files_downloaded'] += 1
                            logger.info(f"备用策略 {strategy_name} 成功: {file_size} bytes")
                            return True
                        else:
                            logger.debug(f"备用策略 {strategy_name} 获得无效响应: {file_size} bytes")
                            
            except Exception as e:
                logger.debug(f"备用策略 {strategy_name} 失败: {e}")
                
        return False
            
    def _sanitize_path(self, path: Path) -> Path:
        """清理文件路径，移除不安全的字符"""
        try:
            # Windows文件名不能包含的字符
            invalid_chars = '<>:"|?*'
            
            # 处理文件名
            name = path.name
            stem = path.stem
            suffix = path.suffix
            
            # 清理文件名
            for char in invalid_chars:
                stem = stem.replace(char, '_')
                
            # 处理过长的文件名（Windows限制约255字符）
            if len(stem) > 200:
                stem = stem[:200]
                
            # 移除文件名末尾的空格和点号
            stem = stem.rstrip(' .')
            
            # 构建新路径
            clean_name = f"{stem}{suffix}"
            clean_path = path.parent / clean_name
            
            return clean_path
            
        except Exception as e:
            logger.warning(f"路径清理失败 {path}: {e}")
            return path
            
    def _is_excluded(self, path: str) -> bool:
        """检查路径是否被排除"""
        for pattern in self.exclude_patterns:
            if pattern.search(path):
                return True
        return False
        
    def _check_file_size(self, size: int, filename: str) -> bool:
        """检查文件大小是否符合要求"""
        if size == 0:
            return True  # 如果没有大小信息，默认通过
            
        if self.min_file_size > 0 and size < self.min_file_size:
            logger.debug(f"文件太小，跳过 {filename}: {size} bytes")
            return False
            
        if self.max_file_size > 0 and size > self.max_file_size:
            logger.debug(f"文件太大，跳过 {filename}: {size} bytes")
            return False
            
        return True
        
    def _get_source_paths(self) -> List[Tuple[str, Path]]:
        """获取源目录列表，支持单个路径和多个路径，返回(源路径, 本地路径)元组列表"""
        openlist_config = self.config['openlist']
        
        # 优先使用source_paths
        if 'source_paths' in openlist_config and openlist_config['source_paths']:
            source_paths = openlist_config['source_paths']
            
            if isinstance(source_paths, dict):
                # 目录映射格式 {源路径: 本地路径}
                return [(src, Path(local)) for src, local in source_paths.items()]
            elif isinstance(source_paths, list):
                # 简单列表格式，所有目录存储到同一个本地目录
                return [(src, self.local_base_path) for src in source_paths]
            else:
                raise ValueError("source_paths格式错误，应为列表或字典")
        
        # 回退到source_path
        if 'source_path' in openlist_config:
            return [(openlist_config['source_path'], self.local_base_path)]
        
        raise ValueError("配置中缺少source_path或source_paths")
        
    async def _check_sync_mode(self) -> str:
        """检查并确认同步模式"""
        sync_config = self.config.get('sync', {})
        mode = sync_config.get('mode', 'incremental')
        confirm = sync_config.get('confirm_mode', True)
        
        if not confirm:
            logger.info(f"使用配置的同步模式: {mode}")
            return mode
        
        print(f"\n当前配置的同步模式: {mode}")
        print("可用模式:")
        print("  full        - 完全覆盖（删除所有本地文件，重新生成）")
        print("  incremental - 增量更新（智能更新，删除失效文件，添加新文件）")
        print("  update-only - 仅添加（只添加新文件，不删除任何本地文件）")
        
        while True:
            user_input = input(f"请选择同步模式 [{mode}]: ").strip().lower()
            if not user_input:
                return mode
            if user_input in ['full', 'incremental', 'update-only']:
                return user_input
            print("无效输入，请选择: full, incremental, update-only")
    
    async def _scan_local_files(self, source_paths: List[Tuple[str, Path]]) -> Dict[str, str]:
        """扫描本地STRM文件，返回本地文件路径到远程路径的映射"""
        local_files = {}
        
        for _, local_path in source_paths:
            if not local_path.exists():
                continue
                
            for strm_file in local_path.rglob("*.strm"):
                try:
                    async with aiofiles.open(strm_file, 'r', encoding='utf-8') as f:
                        content = (await f.read()).strip()
                        
                        # 从STRM内容中提取远程路径
                        remote_path = self._extract_remote_path_from_strm(content)
                        if remote_path:
                            local_files[str(strm_file)] = remote_path
                        else:
                            logger.debug(f"无法从STRM文件提取远程路径: {strm_file}")
                except Exception as e:
                    logger.warning(f"读取STRM文件失败 {strm_file}: {e}")
        
        return local_files
    
    def _extract_remote_path_from_strm(self, strm_content: str) -> str:
        """从STRM文件内容中提取远程路径"""
        try:
            import urllib.parse
            
            # 处理各种可能的STRM格式
            if strm_content.startswith('http'):
                # 完整URL格式
                parsed = urllib.parse.urlparse(strm_content)
                path = parsed.path
                
                # 移除 /d 前缀（如果存在）
                if path.startswith('/d/'):
                    path = path[2:]  # 移除 '/d'
                elif path.startswith('/d'):
                    path = path[2:]  # 移除 '/d'
                    
                return path
            else:
                # 相对路径格式，直接返回
                return strm_content
                
        except Exception as e:
            logger.debug(f"解析STRM内容失败 '{strm_content}': {e}")
            return ""
    
    async def _cleanup_invalid_files(self, local_files: Dict[str, str], remote_files: Set[str]):
        """清理失效的本地文件"""
        if 'files_cleaned' not in self.stats:
            self.stats['files_cleaned'] = 0
        
        files_to_remove = []
        
        for local_path, remote_path in local_files.items():
            if remote_path not in remote_files:
                files_to_remove.append(local_path)
        
        if not files_to_remove:
            logger.info("没有发现失效的STRM文件")
            return
        
        logger.info(f"发现 {len(files_to_remove)} 个失效的STRM文件，开始清理...")
        
        for local_path in files_to_remove:
            try:
                Path(local_path).unlink()
                self.stats['files_cleaned'] += 1
                logger.debug(f"删除失效文件: {local_path}")
            except Exception as e:
                logger.warning(f"删除文件失败 {local_path}: {e}")
                self.stats['errors'] += 1
        
        # 清理空目录
        await self._cleanup_empty_directories()
        
        logger.info(f"清理完成，删除了 {self.stats['files_cleaned']} 个失效文件")
    
    async def _cleanup_empty_directories(self):
        """清理空目录"""
        # 获取所有本地目录路径
        source_paths = self._get_source_paths()
        
        for _, local_path in source_paths:
            if not local_path.exists():
                continue
                
            # 从底层开始删除空目录
            for root, dirs, files in os.walk(local_path, topdown=False):
                for dir_name in dirs:
                    dir_path = Path(root) / dir_name
                    try:
                        if not any(dir_path.iterdir()):  # 目录为空
                            dir_path.rmdir()
                            logger.debug(f"删除空目录: {dir_path}")
                    except Exception as e:
                        logger.debug(f"删除空目录失败 {dir_path}: {e}")


def load_config(config_path: str) -> Dict:
    """加载配置文件"""
    with open(config_path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)


async def run_download_config(
    config: Dict,
    interactive: bool = False,
    progress_callback: Optional[Callable[[Dict], None]] = None,
    cancel_checker: Optional[Callable[[], bool]] = None,
) -> Dict:
    """使用已经加载好的配置运行一次下载任务，并返回统计结果。

    Flask 后台任务默认传入 interactive=False，避免同步模式确认阻塞后台线程。
    CLI 入口保持 interactive=True，兼容原有手动选择同步模式的体验。
    """
    if not interactive:
        config.setdefault('sync', {})['confirm_mode'] = False

    downloader = StrmDownloader(
        config,
        progress_callback=progress_callback,
        cancel_checker=cancel_checker,
    )
    async with OpenListClient(config) as client:
        return await downloader.download(client)


async def run_download(config_path: str = 'config.yaml', interactive: bool = False) -> Dict:
    """从配置文件运行一次下载任务，并返回统计结果。"""
    config = load_config(config_path)
    return await run_download_config(config, interactive=interactive)


async def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='OpenList STRM下载工具')
    parser.add_argument(
        '-c', '--config',
        default='config.yaml',
        help='配置文件路径 (默认: config.yaml)'
    )
    args = parser.parse_args()
    
    try:
        await run_download(args.config, interactive=True)
    except Exception as e:
        logger.error(f"任务执行失败: {e}")
        return
        
    logger.info("所有任务完成")


if __name__ == '__main__':
    asyncio.run(main())
