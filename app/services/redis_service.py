"""Redis服务类"""
from contextlib import asynccontextmanager
from typing import Optional, Dict, Any, List
from datetime import datetime
import asyncio
import redis
from redis.exceptions import LockError, LockNotOwnedError
from app.models.events import Event, EventType
from app.utils.get_audio_devices import get_audio_devices
from app.core.config import settings
from app.core.event_bus import ProductionEventBus
from app.models.call_record import CallRecord, DialogEntry, DialogRecord

# 使用主应用的logger
from app.core.logger import get_logger
from app.models.device_info import ConfigAudioDeviceInfo, DeviceInfo
from app.services.base_service import BaseService

logger = get_logger(__name__)


class RedisService(BaseService):
    """Redis 服务类 - 统一管理设备信息和电话记录（支持分布式锁）"""

    # Redis 键名常量
    DEVICE_INFO_KEY = "device_info"
    CALL_RECORD_PREFIX = "call_record:"
    LOCK_PREFIX = "lock:"
    CONFIG_INPUT_AUDIO_KEY = "config_input_audio"
    DIALOG_RECORD_PREFIX = "dialog_record:"

    def __init__(self, event_bus: Optional[ProductionEventBus] = None):
        """初始化 Redis 服务（支持事件总线注入）"""
        super().__init__(event_bus, "RedisService")
        self.redis_client: Optional[redis.Redis] = None
        self._connection_pool: Optional[redis.ConnectionPool] = None
        self._initialized = False
        self.event_bus = event_bus
        self.device_info: Optional[DeviceInfo] = None

        # 锁配置
        self.lock_timeout = 10  # 锁超时时间（秒）
        self.blocking_timeout = 5  # 获取锁的阻塞超时时间（秒）

    async def initialize(self) -> bool:
        """异步初始化 Redis 连接"""
        if self._initialized:
            return True

        try:
            # 创建连接池
            self._connection_pool = redis.ConnectionPool(
                host=settings.redis_host,
                port=settings.redis_port,
                username=settings.redis_username,
                password=settings.redis_password,
                db=settings.redis_db,
                connection_class=redis.SSLConnection,
                # 连接配置
                decode_responses=True,
                socket_connect_timeout=5,
                socket_timeout=5,
                retry_on_timeout=True,
                health_check_interval=30,
                # 连接池配置
                max_connections=20,  # 添加最大连接数
                retry_on_error=[redis.ConnectionError],  # 添加重试错误类型
            )

            # 创建 Redis 客户端
            self.redis_client = redis.Redis(connection_pool=self._connection_pool)

            # 测试连接
            self.redis_client.ping()
            self._initialized = True
            logger.info(
                "✅ Redis service initialized successfully with distributed lock support"
            )

            return True
        except Exception as e: # pylint: disable=broad-except
            logger.error("❌ Redis service initialization failed: %s", e)
            return False

    async def register_event_listeners(self):
        """注册事件监听器"""
        if self.event_bus:
            await self._register_listener(
                EventType.REDIS_ADD_DIALOG_RECORD, self.add_dialog_record
            )
            await self._register_listener(
                EventType.REDIS_BIND_DIALOG_RECORD_TO_CALL_RECORD,
                self.bind_dialog_record_to_call_record,
            )

    async def shutdown(self):
        """异步关闭 Redis 连接"""
        try:
            if self.redis_client:
                self.redis_client.close()
                self.redis_client = None
                logger.info("Redis client closed")

            if self._connection_pool:
                self._connection_pool.disconnect()
                self._connection_pool = None
                logger.info("Redis connection pool closed")

            self._initialized = False

            logger.info("✅ Redis service shutdown completed")
        except Exception as e: # pylint: disable=broad-except
            logger.error("❌ Error during Redis shutdown: %s", e)

    async def health_check(self) -> Dict[str, Any]:
        """健康检查"""
        try:
            if not self._initialized or not self.redis_client:
                return {"status": "unhealthy", "error": "Not initialized"}

            # 测试连接
            self.redis_client.ping()

            # 获取连接信息
            info = await self.redis_client.info()
            return {
                "status": "healthy",
                "connected_clients": info.get("connected_clients", 0),
                "used_memory": info.get("used_memory_human", "unknown"),
                "redis_version": info.get("redis_version", "unknown"),
            }
        except Exception as e: # pylint: disable=broad-except
            return {"status": "unhealthy", "error": str(e)}

    def _ensure_connected(self):
        """确保 Redis 已连接"""
        if not self._initialized or not self.redis_client:
            raise RuntimeError("Redis service not initialized")

    # ==================== 分布式锁管理 ====================

    @asynccontextmanager
    async def acquire_lock(
        self,
        resource_id: str,
        timeout: Optional[int] = None,
        blocking_timeout: Optional[int] = None,
    ):
        """
        获取Redis分布式锁的异步上下文管理器

        Args:
            resource_id: 资源ID，用于生成锁键名
            timeout: 锁超时时间（秒），默认使用实例配置
            blocking_timeout: 获取锁的阻塞超时时间（秒），默认使用实例配置
        """
        self._ensure_connected()

        lock_key = f"{self.LOCK_PREFIX}{resource_id}"
        lock_timeout = timeout or self.lock_timeout
        lock_blocking_timeout = blocking_timeout or self.blocking_timeout

        lock = None
        try:
            # 创建锁对象
            lock = self.redis_client.lock(
                lock_key,
                timeout=lock_timeout,
                blocking_timeout=lock_blocking_timeout,
                thread_local=False,
            )

            # 在线程池中获取锁（避免阻塞事件循环）
            loop = asyncio.get_event_loop()
            acquired = await loop.run_in_executor(
                None, lambda: lock.acquire(blocking=True)
            )

            if not acquired:
                raise TimeoutError(f"Failed to acquire lock: {lock_key}")

            logger.debug("🔒 Lock acquired: %s", lock_key)
            yield lock

        except (LockError, LockNotOwnedError) as e:
            logger.error("❌ Lock error for %s: %s", lock_key, e)
            raise
        except TimeoutError as e:
            logger.error("⏰ Lock timeout for %s: %s", lock_key, e)
            raise
        except Exception as e:  # pylint: disable=broad-except
            logger.error("❌ Unexpected error acquiring lock %s: %s", lock_key, e)
            raise
        finally:
            if lock:
                try:
                    # 在线程池中释放锁
                    loop = asyncio.get_event_loop()
                    await loop.run_in_executor(None, lock.release)
                    logger.debug("🔓 Lock released: %s", lock_key)
                except (LockError, LockNotOwnedError) as e:
                    logger.error("❌ Failed to release lock %s: %s", lock_key, e)
                except Exception as e:  # pylint: disable=broad-except
                    logger.error("❌ Unexpected error releasing lock %s: %s", lock_key, e)

    @asynccontextmanager
    async def _acquire_multiple_locks(
        self, resource_ids: List[str], timeout: Optional[int] = None
    ):
        """
        获取多个Redis分布式锁的异步上下文管理器

        Args:
            resource_ids: 资源ID列表
            timeout: 锁超时时间
        """
        # 对资源ID排序，避免死锁
        sorted_resource_ids = sorted(resource_ids)
        acquired_locks = []

        try:
            # 按顺序获取所有锁
            for resource_id in sorted_resource_ids:
                lock_key = f"{self.LOCK_PREFIX}{resource_id}"
                lock_timeout = timeout or self.lock_timeout

                lock = self.redis_client.lock(
                    lock_key,
                    timeout=lock_timeout,
                    blocking_timeout=self.blocking_timeout,
                    thread_local=False,
                )

                # 在线程池中获取锁
                loop = asyncio.get_event_loop()
                acquired = await loop.run_in_executor(
                    None, lambda lock_obj=lock: lock_obj.acquire(blocking=True)
                )

                if not acquired:
                    raise TimeoutError(f"Failed to acquire lock: {lock_key}")

                acquired_locks.append(lock)
                logger.debug("🔒 Multi-lock acquired: %s", lock_key)

            yield acquired_locks

        except Exception as e:  # pylint: disable=broad-except
            logger.error("❌ Error acquiring multiple locks: %s", e)
            # 释放已获取的锁
            for lock in acquired_locks:
                try:
                    loop = asyncio.get_event_loop()
                    await loop.run_in_executor(None, lock.release)
                except Exception as release_error:  # pylint: disable=broad-except
                    logger.error(
                        "❌ Failed to release lock during cleanup: %s", release_error
                    )
            raise
        finally:
            # 释放所有锁
            for lock in acquired_locks:
                try:
                    loop = asyncio.get_event_loop()
                    await loop.run_in_executor(None, lock.release)
                    logger.debug("🔓 Multi-lock released")
                except (LockError, LockNotOwnedError) as e:
                    logger.error("❌ Failed to release multi-lock: %s", e)
                except Exception as e:  # pylint: disable=broad-except
                    logger.error("❌ Unexpected error releasing multi-lock: %s", e)

    # ==================== 设备信息管理 ====================

    async def set_device_info(self, device_info: DeviceInfo) -> bool:
        """
        设置设备信息（创建或更新）- 使用分布式锁

        Args:
            device_info: 设备信息对象

        Returns:
            bool: 操作是否成功
        """
        self._ensure_connected()
        try:
            async with self.acquire_lock("device_info"):
                self.redis_client.set(
                    self.DEVICE_INFO_KEY, device_info.model_dump_json()
                )
                self.device_info = device_info
                logger.info("Device info saved with lock: devid=%s", device_info.devid)
                return True
        except Exception as e:  # pylint: disable=broad-except
            logger.error("Failed to save device info: %s", e)
            return False

    # ==================== 电话记录管理（使用分布式锁） ====================

    async def create_call_record(self, call_record: CallRecord) -> str:
        """
        创建新的电话记录 - 使用分布式锁

        Args:
            call_record: 电话记录对象

        Returns:
            str: 生成的UUID
        """
        self._ensure_connected()
        try:
            async with self.acquire_lock(f"call_record:{call_record.call_id}"):
                # 检查记录是否已存在
                existing_key = f"{self.CALL_RECORD_PREFIX}{call_record.call_id}"
                if self.redis_client.exists(existing_key):
                    logger.warning("Call record already exists: %s", call_record.call_id)
                    return call_record.call_id

                self.redis_client.set(
                    f"{self.CALL_RECORD_PREFIX}{call_record.call_id}",
                    call_record.model_dump_json(),
                    ex=86400,  # 24小时过期
                )

                logger.info(
                    "Call record created with lock: %s, phone: %s", call_record.call_id, call_record.phone_number
                )
                return call_record.call_id
        except Exception as e:  # pylint: disable=broad-except
            logger.error("Failed to create call record: %s", e)
            raise

    async def update_call_record(self, call_record: CallRecord) -> bool:
        """
        更新电话记录（使用 CallRecord 对象）- 使用分布式锁

        Args:
            call_record: 包含更新数据的 CallRecord 对象，call_id 必须存在，status 必须存在

        Returns:
            bool: 操作是否成功
        """
        self._ensure_connected()
        try:
            if not call_record.call_id:
                logger.error("CallRecord object missing call_id")
                return False

            async with self.acquire_lock(f"call_record:{call_record.call_id}"):
                # 获取现有记录以确保存在
                existing_record = await self._get_call_record_without_lock(
                    call_record.call_id
                )
                if not existing_record:
                    logger.warning("Call record not found: %s", call_record.call_id)
                    return False

                # 更新时间戳
                call_record.updated_at = datetime.now()

                # 保存更新后的记录
                self.redis_client.set(
                    f"{self.CALL_RECORD_PREFIX}{call_record.call_id}",
                    call_record.model_dump_json(),
                    ex=86400,  # 24小时过期
                )

                logger.info("Call record updated with lock: %s", call_record.call_id)
                return True
        except Exception as e:  # pylint: disable=broad-except
            logger.error("Failed to update call record: %s", e)
            return False

    async def update_call_record_status(self, call_id: str, status: str) -> bool:
        """
        更新电话记录状态 - 使用分布式锁
        """
        self._ensure_connected()
        try:
            async with self.acquire_lock(f"call_record:{call_id}"):
                call_record = await self._get_call_record_without_lock(call_id)
                if not call_record:
                    logger.warning("Call record not found: %s", call_id)
                    return False

                call_record.status = status
                call_record.updated_at = datetime.now()

                # 直接保存，避免递归锁
                self.redis_client.set(
                    f"{self.CALL_RECORD_PREFIX}{call_record.call_id}",
                    call_record.model_dump_json(),
                    ex=86400,
                )

                logger.info(
                    "Call record status updated with lock: %s -> %s", call_id, status
                )
                return True
        except Exception as e:  # pylint: disable=broad-except
            logger.error("Failed to update call record status: %s", e)
            return False

    async def update_call_record_call_id(
        self, old_call_id: str, new_call_id: str
    ) -> bool:
        """
        安全更新电话记录call_id - 使用分布式锁和原子操作

        Args:
            old_call_id: 当前的call_id
            new_call_id: 新的call_id

        Returns:
            bool: 操作是否成功
        """
        self._ensure_connected()

        try:
            # 使用多个锁来确保原子性，按顺序获取避免死锁
            async with self._acquire_multiple_locks(
                [f"call_record:{old_call_id}", f"call_record:{new_call_id}"]
            ):
                # 1. 获取原始通话记录
                call_record = await self._get_call_record_without_lock(old_call_id)
                if not call_record:
                    logger.warning("Call record not found: %s", old_call_id)
                    return False

                # 3. 使用Redis Pipeline确保原子性
                pipeline = self.redis_client.pipeline()

                try:
                    # 更新通话记录的call_id
                    call_record.call_id = new_call_id
                    call_record.updated_at = datetime.now()

                    # 原子操作：创建新记录、删除旧记录
                    pipeline.set(
                        f"{self.CALL_RECORD_PREFIX}{new_call_id}",
                        call_record.model_dump_json(),
                        ex=86400,  # 24小时过期
                    )
                    pipeline.delete(f"{self.CALL_RECORD_PREFIX}{old_call_id}")

                    # 执行所有操作
                    results = pipeline.execute()

                    # 验证所有操作都成功
                    if all(results):
                        logger.info(
                            "Call record ID updated atomically: %s -> %s", old_call_id, new_call_id
                        )
                        return True
                    else:
                        logger.error("Pipeline execution failed: %s", results)
                        return False

                except Exception as pipeline_error:  # pylint: disable=broad-except
                    logger.error("Pipeline execution error: %s", pipeline_error)
                    return False

        except Exception as e:  # pylint: disable=broad-except
            logger.error("Failed to update call record ID: %s", e)
            return False

    async def get_call_record(
        self, call_id: str, lock: bool = True
    ) -> Optional[CallRecord]:
        """
        获取电话记录

        Args:
            call_id: 电话记录ID

        Returns:
            CallRecord: 电话记录对象，不存在时返回 None
        """
        self._ensure_connected()
        try:
            if lock:
                async with self.acquire_lock(f"{self.CALL_RECORD_PREFIX}{call_id}"):
                    return await self._get_call_record_without_lock(call_id)
            else:
                return await self._get_call_record_without_lock(call_id)
        except Exception as e:  # pylint: disable=broad-except
            logger.error("Failed to get call record: %s", e)
            return None

    async def _get_call_record_without_lock(self, call_id: str) -> Optional[CallRecord]:
        """
        内部方法：获取电话记录
        """
        self._ensure_connected()
        try:
            key = f"{self.CALL_RECORD_PREFIX}{call_id}"
            data = self.redis_client.get(key)

            if not data:
                logger.warning("Call record not found: %s", call_id)
                return None

            call_record = CallRecord.model_validate_json(data)
            logger.info("Call record retrieved: %s", call_id)
            return call_record
        except Exception as e:  # pylint: disable=broad-except
            logger.error("Failed to get call record: %s", e)
            return None

    async def get_all_call_records(self) -> List[CallRecord]:
        """获取所有电话记录"""
        self._ensure_connected()
        try:
            pattern = f"{self.CALL_RECORD_PREFIX}*"
            keys = self.redis_client.keys(pattern)

            records = []
            for key in keys:
                data = self.redis_client.get(key)
                if data:
                    records.append(CallRecord.model_validate_json(data))

            logger.info("All call records retrieved, total: %s", len(records))
            return records
        except Exception as e:  # pylint: disable=broad-except
            logger.error("Failed to get all call records: %s", e)
            return []

    # ==================== 对话记录管理 ====================

    async def create_dialog_record(self, dialog_record: DialogRecord) -> bool:
        """
        创建对话记录 - 使用分布式锁
        """
        self._ensure_connected()
        try:
            async with self.acquire_lock(
                f"{self.DIALOG_RECORD_PREFIX}{dialog_record.call_id}"
            ):
                self.redis_client.set(
                    f"{self.DIALOG_RECORD_PREFIX}{dialog_record.call_id}",
                    dialog_record.model_dump_json(),
                    ex=86400,
                )
                return True
        except Exception as e:  # pylint: disable=broad-except
            logger.error("Failed to create dialog record: %s", e)
            return False

    async def _get_dialog_record_without_lock(
        self, call_id: str
    ) -> Optional[DialogRecord]:
        """
        内部方法：获取对话记录（不使用锁，避免递归锁）
        """
        self._ensure_connected()
        try:
            key = f"{self.DIALOG_RECORD_PREFIX}{call_id}"
            data = self.redis_client.get(key)
            if not data:
                logger.warning("Dialog record not found: %s", call_id)
                return None
            dialog_record = DialogRecord.model_validate_json(data)
            return dialog_record
        except Exception as e:  # pylint: disable=broad-except
            logger.error("Failed to get dialog record: %s", e)
            return None

    async def bind_dialog_record_to_call_record(self, event: Event) -> bool:
        """
        更新电话记录对话记录 - 使用分布式锁
        """
        self._ensure_connected()
        try:
            call_id = event.data.get("call_id")
            async with self.acquire_lock(f"{self.CALL_RECORD_PREFIX}{call_id}"):
                dialog_record = await self._get_dialog_record_without_lock(call_id)
                call_record = await self._get_call_record_without_lock(call_id)
                if not call_record:
                    logger.warning("Call record not found: %s", call_id)
                    return False
                call_record.dialog_record = dialog_record.dialog_record
                self.redis_client.set(
                    f"{self.CALL_RECORD_PREFIX}{call_id}",
                    call_record.model_dump_json(),
                    ex=86400,
                )
                return True
        except Exception as e:  # pylint: disable=broad-except
            logger.error("Failed to update call record dialog record: %s", e)
            return False

    async def add_dialog_record(self, event: Event) -> bool:
        """
        添加对话记录 - 使用分布式锁
        """
        self._ensure_connected()
        try:
            call_id = event.data.get("call_id")
            dialog_entry = event.data.get("dialog_entry")
            async with self.acquire_lock(f"{self.DIALOG_RECORD_PREFIX}{call_id}"):
                dialog_record: Optional[
                    DialogRecord
                ] = await self._get_dialog_record_without_lock(call_id)
                if not dialog_record:
                    logger.warning("Dialog record not found: %s", call_id)
                    return False
                dialog_record.dialog_record.append(dialog_entry)
                self.redis_client.set(
                    f"{self.DIALOG_RECORD_PREFIX}{call_id}",
                    dialog_record.model_dump_json(),
                    ex=86400,
                )
                return True
        except Exception as e:  # pylint: disable=broad-except
            logger.error("Failed to add dialog record: %s", e)
            return False

    async def get_dialog_after_marking(self, call_id: str) -> str:
        """
        获取标记后的对话记录并更新标记 - 使用分布式锁
        """
        self._ensure_connected()
        try:
            async with self.acquire_lock(f"{self.DIALOG_RECORD_PREFIX}{call_id}"):
                # 从Redis获取对话记录
                dialog_record = await self._get_dialog_record_without_lock(call_id)

                if not dialog_record or not dialog_record.dialog_record:
                    return "客户没有说话"

                # 查找最后一个speaker为"agent"的记录索引
                last_agent_index = -1
                for i in range(len(dialog_record.dialog_record) - 1, -1, -1):
                    if dialog_record.dialog_record[i].speaker == "agent":
                        last_agent_index = i
                        break

                # 如果没有找到agent记录，或者agent记录后没有新内容
                if (
                    last_agent_index == -1
                    or last_agent_index >= len(dialog_record.dialog_record) - 1
                ):
                    return "客户没有说话"

                # 获取最后一个agent记录之后的所有对话内容
                new_records = dialog_record.dialog_record[last_agent_index + 1 :]

                # 如果没有新记录
                if not new_records:
                    return "客户没有说话"

                # 合并新记录的内容
                dialog_text = ""
                for entry in new_records:
                    dialog_text += entry.content

                # 删除原记录中的new_records（从last_agent_index + 1开始的所有记录）
                dialog_record.dialog_record = dialog_record.dialog_record[
                    : last_agent_index + 1
                ]

                # 添加合并后的用户对话记录来替换被删除的记录
                dialog_record.dialog_record.append(
                    DialogEntry(
                        speaker="user", content=dialog_text, timestamp=datetime.now()
                    )
                )

                # 更新回复标记为下一个位置
                dialog_record.dialog_record_reply_marking = len(
                    dialog_record.dialog_record
                )

                # 保存更新后的记录到 Redis
                self.redis_client.set(
                    f"{self.DIALOG_RECORD_PREFIX}{call_id}",
                    dialog_record.model_dump_json(),
                    ex=86400,
                )

                logger.info("Dialog after marking retrieved with lock: %s", call_id)
                return dialog_text.strip()
        except Exception as e:  # pylint: disable=broad-except
            logger.error("Failed to get dialog after marking: %s", e)
            return "客户没有说话"

    async def set_device_info_input_audio_and_output_audio(
        self, config_audio_device_info: ConfigAudioDeviceInfo
    ):
        """
        设置设备信息输入音频和输出音频
        """
        self._ensure_connected()
        try:
            async with self.acquire_lock(
                f"config_audio_device_info:{config_audio_device_info.id}"
            ):
                device_info = self.device_info
                if not self.device_info:
                    logger.error("Device info not found")
                    raise ValueError("Device info not found")

                input_devices, output_devices = get_audio_devices(deduplicate=True)

                for device in device_info.devices:
                    if (
                        config_audio_device_info.config_audio_device_info_mapping.device.instance
                        == device.instance
                    ):
                        config_audio_device_info.config_audio_device_info_mapping.device = device

                    elif (
                        device.deviceId
                        == config_audio_device_info.config_audio_device_info_mapping.device.deviceId
                    ):
                        config_audio_device_info.config_audio_device_info_mapping.device = device

                    for input_device in input_devices:
                        if (
                            input_device.index
                            == config_audio_device_info.config_audio_device_info_mapping.input_audio.index
                        ):
                            config_audio_device_info.config_audio_device_info_mapping.input_audio = input_device
                        elif (
                            input_device.name
                            == config_audio_device_info.config_audio_device_info_mapping.input_audio.name
                        ):
                            config_audio_device_info.config_audio_device_info_mapping.input_audio = input_device

                    for output_device in output_devices:
                        if (
                            output_device.index
                            == config_audio_device_info.config_audio_device_info_mapping.output_audio.index
                        ):
                            config_audio_device_info.config_audio_device_info_mapping.output_audio = output_device
                        elif (
                            output_device.name
                            == config_audio_device_info.config_audio_device_info_mapping.output_audio.name
                        ):
                            config_audio_device_info.config_audio_device_info_mapping.output_audio = output_device

                self.redis_client.set(
                    f"{self.CONFIG_INPUT_AUDIO_KEY}",
                    config_audio_device_info.model_dump_json(),
                    ex=None,  # 不过期
                )
                logger.info(
                    "Config audio device info saved with lock: %s", config_audio_device_info.id
                )
        except Exception as e:  # pylint: disable=broad-except
            logger.error("Failed to save config audio device info: %s", e)
            raise e
