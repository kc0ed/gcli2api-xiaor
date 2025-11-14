"""
凭证管理器 - 完全基于统一存储中间层
"""
import asyncio
import time
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional, Tuple
from contextlib import asynccontextmanager

from config import get_calls_per_rotation, is_mongodb_mode
from log import log
from .storage_adapter import get_storage_adapter
from .google_oauth_api import fetch_user_email_from_file, Credentials
from .task_manager import task_manager


class CredentialManager:
    """
    统一凭证管理器
    所有存储操作通过storage_adapter进行
    """
    
    def __init__(self):
        # 核心状态
        self._initialized = False
        self._storage_adapter = None
        
        # 凭证轮换相关
        self._call_count = 0
        self._last_scan_time = 0
        
        # 凭证轮换与缓存
        self._current_credential_index = 0
        self._available_creds: List[str] = []  # 可用凭证的有序列表，用于轮换
        self._ratelimited_creds: Dict[str, float] = {}  # 限流凭证: { name: next_reset_timestamp }
        
        # 当前使用的凭证信息 (由 get_valid_credential 临时填充)
        self._current_credential_file: Optional[str] = None
        self._current_credential_data: Optional[Dict[str, Any]] = None
        
        # 并发控制
        self._state_lock = asyncio.Lock()  # 用于初始化和关闭
        self._operation_lock = asyncio.Lock() # 保证 get_valid_credential 的调用原子性
        self._cache_lock = asyncio.Lock()  # 用于所有内存缓存的读写操作
        
        # 工作线程控制
        self._shutdown_event = asyncio.Event()
        self._write_worker_running = False
        self._write_worker_task = None
        
        # 原子操作计数器
        self._atomic_counter = 0
        self._atomic_lock = asyncio.Lock()
        
        # Onboarding state
        self._onboarding_complete = False
        self._onboarding_checked = False
    
    async def initialize(self):
        """初始化凭证管理器"""
        async with self._state_lock:
            if self._initialized:
                return
            
            # 初始化统一存储适配器
            self._storage_adapter = await get_storage_adapter()
            
            # 启动后台工作线程
            await self._start_background_workers()
            
            # 发现并加载凭证
            await self._discover_credentials()
            
            self._initialized = True
            storage_type = "MongoDB" if await is_mongodb_mode() else "File"
            log.debug(f"Credential manager initialized with {storage_type} storage backend")
    
    async def close(self):
        """清理资源"""
        log.debug("Closing credential manager...")
        
        # 设置关闭标志
        self._shutdown_event.set()
        
        # 等待后台任务结束
        if self._write_worker_task:
            try:
                await asyncio.wait_for(self._write_worker_task, timeout=5.0)
            except asyncio.TimeoutError:
                log.warning("Write worker task did not finish within timeout")
                if not self._write_worker_task.done():
                    self._write_worker_task.cancel()
            except asyncio.CancelledError:
                # 任务被取消是正常的关闭流程
                log.debug("Background worker task was cancelled during shutdown")

        self._initialized = False
        log.debug("Credential manager closed")
    
    async def _start_background_workers(self):
        """启动后台工作线程"""
        if not self._write_worker_running:
            self._write_worker_running = True
            self._write_worker_task = task_manager.create_task(
                self._background_worker(), 
                name="credential_background_worker"
            )
    
    async def _background_worker(self):
        """
        后台工作线程，处理定期任务：
        - 定期重新发现可用凭证（热更新）
        - 自动恢复达到重置时间的限流凭证（24小时滚动窗口解封）
        """
        RECOVER_INTERVAL = 60  # 秒，检查频率，适中即可
        try:
            while not self._shutdown_event.is_set():
                try:
                    # 等待一段时间或直到收到关闭信号
                    await asyncio.wait_for(self._shutdown_event.wait(), timeout=RECOVER_INTERVAL)
                    if self._shutdown_event.is_set():
                        break

                    # 1) 重新发现可用凭证（处理新增/删除/禁用）
                    await self._discover_credentials()

                    # 2) 自动恢复已过期的限流凭证
                    await self._recover_rate_limited_credentials()

                except asyncio.TimeoutError:
                    # 正常超时，进入下一轮循环
                    continue
                except asyncio.CancelledError:
                    # 任务被取消，正常退出
                    log.debug("Background worker cancelled, exiting gracefully")
                    break
                except Exception as e:
                    log.error(f"Background worker error: {e}")
                    await asyncio.sleep(5)  # 错误后等待5秒再继续
        except asyncio.CancelledError:
            # 外层捕获取消，确保干净退出
            log.debug("Background worker received cancellation")
        finally:
            log.debug("Background worker exited")
            self._write_worker_running = False
    
    async def _discover_credentials(self):
        """
        从持久化存储中发现所有凭证，并更新内存缓存。
        这是连接持久层和内存缓存的桥梁。
        """
        try:
            all_credential_names = await self._storage_adapter.list_credentials()
            if not all_credential_names:
                log.warning("No credential files found in storage.")
                async with self._cache_lock:
                    self._available_creds.clear()
                    self._ratelimited_creds.clear()
                return

            all_states = await self._storage_adapter.get_all_credential_states()
            
            new_available = []
            new_ratelimited = {}
            
            for name in all_credential_names:
                # 标准化文件名以匹配状态键
                state_key = name
                if hasattr(self._storage_adapter._backend, '_normalize_filename'):
                    state_key = self._storage_adapter._backend._normalize_filename(name)
                
                state = all_states.get(state_key, {})

                if state.get("disabled", False):
                    continue  # 跳过禁用的

                if state.get("is_rate_limited", False):
                    # 如果被限流，但有重置时间，则加入限流列表等待恢复
                    reset_ts = state.get("next_reset_timestamp")
                    if reset_ts:
                        new_ratelimited[name] = float(reset_ts)
                else:
                    # 否则加入可用列表
                    new_available.append(name)

            # 原子化更新内存缓存
            async with self._cache_lock:
                # 对比变化，打印日志
                old_available_set = set(self._available_creds)
                new_available_set = set(new_available)
                
                if old_available_set != new_available_set:
                    added = new_available_set - old_available_set
                    removed = old_available_set - new_available_set
                    log.info(f"可用凭证缓存更新。新增: {list(added) if added else '无'}, 移除: {list(removed) if removed else '无'}")
                    
                    self._available_creds = new_available
                    # 如果列表变化，重置索引
                    if self._current_credential_index >= len(self._available_creds):
                        self._current_credential_index = 0
                        self._call_count = 0
                        log.info("凭证索引已重置。")
                
                self._ratelimited_creds = new_ratelimited
            
            log.debug(f"凭证缓存刷新完成。可用: {len(self._available_creds)}, 限流: {len(self._ratelimited_creds)}")

        except Exception as e:
            log.error(f"Failed to discover credentials and update cache: {e}")
    
    async def _load_credential_data(self, credential_name: str) -> Optional[Dict[str, Any]]:
        """为指定的凭证名称加载其完整数据，包含token过期检测和自动刷新"""
        try:
            # 从存储适配器加载凭证数据
            credential_data = await self._storage_adapter.get_credential(credential_name)
            if not credential_data:
                log.error(f"Failed to load credential data for: {credential_name}")
                return None
            
            # 检查refresh_token
            if "refresh_token" not in credential_data or not credential_data["refresh_token"]:
                log.warning(f"No refresh token in {credential_name}")
                return None
                
            # Auto-add 'type' field if missing but has required OAuth fields
            if 'type' not in credential_data and all(key in credential_data for key in ['client_id', 'refresh_token']):
                credential_data['type'] = 'authorized_user'
                log.debug(f"Auto-added 'type' field to credential from file {credential_name}")
            
            # 兼容不同的token字段格式
            if "access_token" in credential_data and "token" not in credential_data:
                credential_data["token"] = credential_data["access_token"]
            if "scope" in credential_data and "scopes" not in credential_data:
                credential_data["scopes"] = credential_data["scope"].split()
            
            # token过期检测和刷新
            should_refresh = await self._should_refresh_token(credential_data)
            
            if should_refresh:
                log.debug(f"Token需要刷新 - 文件: {credential_name}")
                refreshed_data = await self._refresh_token(credential_data, credential_name)
                if refreshed_data:
                    credential_data = refreshed_data
                    log.debug(f"Token刷新成功: {credential_name}")
                else:
                    log.error(f"Token刷新失败: {credential_name}")
                    return None
            
            return credential_data
            
        except Exception as e:
            log.error(f"Error loading credential data for {credential_name}: {e}")
            return None
    
    async def get_valid_credential(self) -> Optional[Tuple[str, Dict[str, Any]]]:
        """
        【高性能】获取一个有效的凭证。
        此方法经过优化，优先使用内存缓存进行选择，仅在最后加载数据时执行I/O。
        """
        async with self._operation_lock:
            # 1. 从内存缓存中快速选择一个凭证名称
            async with self._cache_lock:
                if not self._available_creds:
                    # 缓存为空，尝试从持久层同步一次
                    log.info("可用凭证缓存为空，尝试从持久层同步...")
                    await self._discover_credentials()
                    if not self._available_creds:
                        log.error("没有可用的凭证。")
                        return None

                # 检查是否需要轮换 (纯内存操作)
                if await self._should_rotate():
                    await self._rotate_credential()
                
                # 选择当前凭证名称 (纯内存操作)
                selected_name = self._available_creds[self._current_credential_index]
            
            log.info(f"API 调用，准备获取凭证。缓存选择: {selected_name} (索引: {self._current_credential_index})")

            # 2. 加载选中凭证的数据 (执行I/O)
            credential_data = await self._load_credential_data(selected_name)
            
            # 3. 处理加载结果
            if not credential_data:
                log.warning(f"凭证 {selected_name} 加载失败，可能已失效。将其禁用并等待后台任务清理。")
                # 快速失败：禁用凭证，本次请求失败，下一个请求将自动使用新的凭证列表。
                await self.set_cred_disabled(selected_name, True)
                return None

            # 4. 检查并更新24小时使用周期 (可能执行I/O)
            # 注意：状态信息需要单独获取
            credential_state = await self._storage_adapter.get_credential_state(selected_name)
            await self._check_and_update_usage_cycle(selected_name, credential_state)

            # 填充当前凭证信息，用于日志记录或兼容旧代码
            self._current_credential_file = selected_name
            self._current_credential_data = credential_data
            
            return selected_name, credential_data
    
    async def _should_rotate(self) -> bool:
        """检查是否需要轮换凭证"""
        if not self._available_creds or len(self._available_creds) <= 1:
            return False
        
        current_calls_per_rotation = await get_calls_per_rotation()
        return self._call_count >= current_calls_per_rotation
    
    async def _rotate_credential(self):
        """轮换到下一个凭证 (纯内存操作)"""
        if len(self._available_creds) <= 1:
            return
        
        self._current_credential_index = (self._current_credential_index + 1) % len(self._available_creds)
        self._call_count = 0
        
        log.info(f"凭证缓存轮换到索引: {self._current_credential_index}")
    
    async def force_rotate_credential(self):
        """强制轮换到下一个凭证（用于429错误处理）"""
        async with self._cache_lock:
            if len(self._available_creds) <= 1:
                log.warning("只有一个可用凭证，无法轮换。")
                return
            
            await self._rotate_credential()
            log.info("因速率限制强制轮换凭证。")
    
    def increment_call_count(self):
        """增加调用计数"""
        self._call_count += 1
    
    async def update_credential_state(self, credential_name: str, state_updates: Dict[str, Any]):
        """更新凭证状态"""
        try:
            # 直接通过存储适配器更新状态
            success = await self._storage_adapter.update_credential_state(credential_name, state_updates)
            
            # 如果是当前使用的凭证，更新缓存
            if success:
                log.debug(f"Updated credential state: {credential_name}")
            else:
                log.warning(f"Failed to update credential state: {credential_name}")
                
            return success
            
        except Exception as e:
            log.error(f"Error updating credential state {credential_name}: {e}")
            return False
    
    async def set_cred_disabled(self, credential_name: str, disabled: bool):
        """设置凭证的启用/禁用状态"""
        try:
            # 1. 更新持久化存储
            state_updates = {"disabled": disabled}
            success = await self.update_credential_state(credential_name, state_updates)
            if not success:
                return False

            # 2. 更新内存缓存
            async with self._cache_lock:
                action = "禁用" if disabled else "启用"
                log.info(f"正在更新缓存以 {action} 凭证: {credential_name}")
                
                # 从两个缓存中都尝试移除
                if credential_name in self._available_creds:
                    self._available_creds.remove(credential_name)
                if credential_name in self._ratelimited_creds:
                    del self._ratelimited_creds[credential_name]
                
                # 如果是启用，则加回可用列表
                if not disabled:
                    if credential_name not in self._available_creds:
                        self._available_creds.append(credential_name)

                # 确保索引不会越界
                if self._current_credential_index >= len(self._available_creds):
                    self._current_credential_index = 0

            log.info(f"凭证 {credential_name} 已{action}，缓存已同步。")
            return True
            
        except Exception as e:
            log.error(f"Error setting credential disabled state {credential_name}: {e}")
            return False
    
    async def get_creds_status(self) -> Dict[str, Dict[str, Any]]:
        """获取所有凭证的状态"""
        try:
            # 从存储适配器获取所有状态
            all_states = await self._storage_adapter.get_all_credential_states()
            return all_states
            
        except Exception as e:
            log.error(f"Error getting credential statuses: {e}")
            return {}

    async def _check_and_update_usage_cycle(self, credential_name: str, credential_state: Dict[str, Any]):
        """
        检查并更新凭证的24小时使用周期。

        逻辑：
        - 如果没有周期信息，或当前时间超过 next_reset_timestamp：
          - 启动/重置一个24小时窗口，从“当前调用”算起。
        """
        now = time.time()
        next_reset = credential_state.get("next_reset_timestamp")

        # 没有周期信息或已过期 -> 开新周期
        if next_reset is None or now > next_reset:
            updates = {
                "first_use_timestamp": now,
                "next_reset_timestamp": now + 24 * 3600,
                # 调用计数交给上层使用统计模块，这里只提供安全重置
            }
            await self.update_credential_state(credential_name, updates)
            # 同步当前缓存（如果这是当前凭证）
            if credential_name == self._current_credential_file:
                self._current_credential_state.update(updates)
            log.info(f"凭证 {credential_name} 启动新的24小时周期: first_use={now}, next_reset={updates['next_reset_timestamp']}")

    async def _recover_rate_limited_credentials(self):
        """
        【基于内存缓存】自动恢复已达重置时间的限流凭证。
        """
        try:
            now = time.time()
            recovered_creds = []

            # 1. 从内存缓存中识别可恢复的凭证
            async with self._cache_lock:
                # 创建副本以安全遍历
                ratelimited_copy = list(self._ratelimited_creds.items())
                for name, reset_ts in ratelimited_copy:
                    if now >= reset_ts:
                        recovered_creds.append(name)
                        # 从限流缓存中移除
                        del self._ratelimited_creds[name]
            
            if not recovered_creds:
                return

            log.info(f"发现 {len(recovered_creds)} 个可恢复的限流凭证: {recovered_creds}")

            # 2. 批量更新持久化存储
            for name in recovered_creds:
                updates = {
                    "is_rate_limited": False,
                    "first_use_timestamp": now,
                    "next_reset_timestamp": now + 24 * 3600,
                }
                await self.update_credential_state(name, updates)
                log.info(f"自动恢复限流凭证 {name}，启动新的24小时周期。")

            # 3. 将恢复的凭证移回可用缓存
            async with self._cache_lock:
                for name in recovered_creds:
                    if name not in self._available_creds:
                        self._available_creds.append(name)
            
            log.info("可用凭证缓存已更新。")

        except Exception as e:
            log.error(f"Error while recovering rate-limited credentials from cache: {e}")

    async def mark_rate_limited(self, credential_name: str, is_limited: bool):
        """
        标记凭证的速率限制状态。

        - is_limited=True: 标记为限流，_discover_credentials 会将其从可用池中剔除。
        - is_limited=False: 手动解除限流（通常由自动恢复逻辑或管理员操作触发）。
        """
        try:
            # 1. 准备状态更新并写入持久存储
            state_updates = {"is_rate_limited": is_limited}
            now = time.time()
            reset_timestamp = now + 24 * 3600
            
            if is_limited:
                state_updates["first_use_timestamp"] = now
                state_updates["next_reset_timestamp"] = reset_timestamp

            success = await self.update_credential_state(credential_name, state_updates)
            if not success:
                return False

            # 2. 更新内存缓存
            async with self._cache_lock:
                # 从可用列表中移除
                if credential_name in self._available_creds:
                    self._available_creds.remove(credential_name)
                
                if is_limited:
                    # 加入限流列表
                    self._ratelimited_creds[credential_name] = reset_timestamp
                    log.warning(f"[429 Rate Limit] 凭证 {credential_name} 已被限流，缓存已更新。")
                else:
                    # 如果是解除限流，从限流列表移除并加回可用列表
                    if credential_name in self._ratelimited_creds:
                        del self._ratelimited_creds[credential_name]
                    if credential_name not in self._available_creds:
                        self._available_creds.append(credential_name)
                    log.info(f"凭证 {credential_name} 已从限流中清除，缓存已更新。")

                # 如果当前索引指向的凭证被移除了，重置索引
                if self._current_credential_index >= len(self._available_creds):
                    self._current_credential_index = 0

            # 3. 如果是当前凭证被限制，立即轮换
            if is_limited:
                await self.force_rotate_credential()
            
            return True
                
        except Exception as e:
            log.error(f"Error marking rate limit for {credential_name}: {e}")
            return False
    
    async def get_or_fetch_user_email(self, credential_name: str) -> Optional[str]:
        """获取或获取用户邮箱地址"""
        try:
            # 首先检查缓存的状态
            state = await self._storage_adapter.get_credential_state(credential_name)
            cached_email = state.get("user_email")
            
            if cached_email:
                return cached_email
            
            # 如果没有缓存，从凭证数据获取
            credential_data = await self._storage_adapter.get_credential(credential_name)
            if not credential_data:
                return None
            
            # 尝试获取邮箱
            email = await fetch_user_email_from_file(credential_data)
            
            if email:
                # 缓存邮箱地址
                await self.update_credential_state(credential_name, {"user_email": email})
                return email
            
            return None
            
        except Exception as e:
            log.error(f"Error fetching user email for {credential_name}: {e}")
            return None
    
    async def record_api_call_result(self, credential_name: str, success: bool, error_code: Optional[int] = None):
        """记录API调用结果"""
        try:
            state_updates = {}
            
            if success:
                state_updates["last_success"] = time.time()
                # 清除错误码（如果之前有的话）
                state_updates["error_codes"] = []
            elif error_code:
                # 记录错误码
                current_state = await self._storage_adapter.get_credential_state(credential_name)
                error_codes = current_state.get("error_codes", [])
                
                if error_code not in error_codes:
                    error_codes.append(error_code)
                    # 限制错误码列表长度
                    if len(error_codes) > 10:
                        error_codes = error_codes[-10:]
                    
                state_updates["error_codes"] = error_codes
            
            if state_updates:
                await self.update_credential_state(credential_name, state_updates)
                
        except Exception as e:
            log.error(f"Error recording API call result for {credential_name}: {e}")
    
    # 原子操作支持
    @asynccontextmanager
    async def _atomic_operation(self, operation_name: str):
        """原子操作上下文管理器"""
        async with self._atomic_lock:
            self._atomic_counter += 1
            operation_id = self._atomic_counter
            log.debug(f"开始原子操作[{operation_id}]: {operation_name}")
            
            try:
                yield operation_id
                log.debug(f"完成原子操作[{operation_id}]: {operation_name}")
            except Exception as e:
                log.error(f"原子操作[{operation_id}]失败: {operation_name} - {e}")
                raise
    
    async def _should_refresh_token(self, credential_data: Dict[str, Any]) -> bool:
        """检查token是否需要刷新"""
        try:
            # 如果没有access_token或过期时间，需要刷新
            if not credential_data.get("access_token") and not credential_data.get("token"):
                log.debug("没有access_token，需要刷新")
                return True
                
            expiry_str = credential_data.get("expiry")
            if not expiry_str:
                log.debug("没有过期时间，需要刷新")
                return True
                
            # 解析过期时间
            try:
                if isinstance(expiry_str, str):
                    if "+" in expiry_str:
                        file_expiry = datetime.fromisoformat(expiry_str)
                    elif expiry_str.endswith("Z"):
                        file_expiry = datetime.fromisoformat(expiry_str.replace('Z', '+00:00'))
                    else:
                        file_expiry = datetime.fromisoformat(expiry_str)
                else:
                    log.debug("过期时间格式无效，需要刷新")
                    return True
                    
                # 确保时区信息
                if file_expiry.tzinfo is None:
                    file_expiry = file_expiry.replace(tzinfo=timezone.utc)
                    
                # 检查是否还有至少5分钟有效期
                now = datetime.now(timezone.utc)
                time_left = (file_expiry - now).total_seconds()
                
                log.debug(f"Token剩余时间: {int(time_left/60)}分钟")
                
                if time_left > 300:  # 5分钟缓冲
                    return False
                else:
                    log.debug(f"Token即将过期（剩余{int(time_left/60)}分钟），需要刷新")
                    return True
                    
            except Exception as e:
                log.warning(f"解析过期时间失败: {e}，需要刷新")
                return True
                
        except Exception as e:
            log.error(f"检查token过期时出错: {e}")
            return True
    
    async def _refresh_token(self, credential_data: Dict[str, Any], filename: str) -> Optional[Dict[str, Any]]:
        """刷新token并更新存储"""
        try:
            # 创建Credentials对象
            creds = Credentials.from_dict(credential_data)
            
            # 检查是否可以刷新
            if not creds.refresh_token:
                log.error(f"没有refresh_token，无法刷新: {filename}")
                return None
                
            # 刷新token
            log.debug(f"正在刷新token: {filename}")
            await creds.refresh()
            
            # 更新凭证数据
            if creds.access_token:
                credential_data["access_token"] = creds.access_token
                # 保持兼容性
                credential_data["token"] = creds.access_token
                
            if creds.expires_at:
                credential_data["expiry"] = creds.expires_at.isoformat()
                
            # 保存到存储
            await self._storage_adapter.store_credential(filename, credential_data)
            log.info(f"Token刷新成功并已保存: {filename}")
            
            return credential_data
            
        except Exception as e:
            error_msg = str(e)
            log.error(f"Token刷新失败 {filename}: {error_msg}")
            
            # 检查是否是凭证永久失效的错误
            is_permanent_failure = self._is_permanent_refresh_failure(error_msg)
            
            if is_permanent_failure:
                log.warning(f"检测到凭证永久失效: {filename}")
                # 记录失效状态，但不在这里禁用凭证，让上层调用者处理
                await self.record_api_call_result(filename, False, 400)
            
            return None
    
    def _is_permanent_refresh_failure(self, error_msg: str) -> bool:
        """判断是否是凭证永久失效的错误"""
        # 常见的永久失效错误模式
        permanent_error_patterns = [
            "400 Bad Request",
            "invalid_grant",
            "refresh_token_expired", 
            "invalid_refresh_token",
            "unauthorized_client",
            "access_denied"
        ]
        
        error_msg_lower = error_msg.lower()
        for pattern in permanent_error_patterns:
            if pattern.lower() in error_msg_lower:
                return True
                
        return False

    # 兼容性方法 - 保持与现有代码的接口兼容
    async def _update_token_in_file(self, file_path: str, new_token: str, expires_at=None):
        """更新凭证令牌（兼容性方法）"""
        try:
            credential_data = await self._storage_adapter.get_credential(file_path)
            if not credential_data:
                log.error(f"Credential not found for token update: {file_path}")
                return False
            
            # 更新令牌数据
            credential_data["token"] = new_token
            if expires_at:
                credential_data["expiry"] = expires_at.isoformat() if hasattr(expires_at, 'isoformat') else expires_at
            
            # 保存更新后的凭证
            success = await self._storage_adapter.store_credential(file_path, credential_data)
            
            if success:
                log.debug(f"Token updated for credential: {file_path}")
            else:
                log.error(f"Failed to update token for credential: {file_path}")
            
            return success
            
        except Exception as e:
            log.error(f"Error updating token for {file_path}: {e}")
            return False


# 全局实例管理（保持兼容性）
_credential_manager: Optional[CredentialManager] = None

async def get_credential_manager() -> CredentialManager:
    """获取全局凭证管理器实例"""
    global _credential_manager
    
    if _credential_manager is None:
        _credential_manager = CredentialManager()
        await _credential_manager.initialize()
    
    return _credential_manager