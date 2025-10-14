"""Redis服务类"""
from contextlib import asynccontextmanager
from typing import Optional, Dict, Any, List
import json
from datetime import datetime
import asyncio
import redis
from sqlalchemy import text
from redis.exceptions import LockError, LockNotOwnedError
from app.models.events import Event, EventType
from app.utils.get_audio_devices import get_audio_devices
from app.core.config import settings
from app.core.event_bus import ProductionEventBus
from app.models.call_record import CallRecord, DialogEntry, DialogRecord
from app.db.database import Database
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
    PHONE_QUEUE_PREFIX = "phone_queue:"
    PHONE_QUEUE_LOCK = "phone_queue_lock"

    def __init__(self, event_bus: Optional[ProductionEventBus] = None, db: Optional[Database] = None):
        """初始化 Redis 服务（支持事件总线和数据库注入）"""
        super().__init__(event_bus, "RedisService")
        self.redis_client: Optional[redis.Redis] = None
        self._connection_pool: Optional[redis.ConnectionPool] = None
        self._initialized = False
        self.event_bus = event_bus
        self.device_info: Optional[DeviceInfo] = None
        self.db = db

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
                ssl_cert_reqs=settings.redis_ssl,  # 禁用 SSL 证书验证
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
        await self._register_listener(EventType.REDIS_ADD_DIALOG_RECORD, self.add_dialog_record)

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

    async def bind_dialog_record_to_call_record(self, call_id: str) -> bool:
        """
        更新电话记录对话记录 - 使用分布式锁
        """
        self._ensure_connected()
        try:
            async with self.acquire_lock(f"{self.CALL_RECORD_PREFIX}{call_id}"):
                dialog_record = await self._get_dialog_record_without_lock(call_id)
                call_record = await self._get_call_record_without_lock(call_id)
                if not call_record:
                    logger.warning("Call record not found: %s", call_id)
                    return False
                if not dialog_record:
                    logger.warning("Dialog record not found: %s", call_id)
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
        logger.debug("Adding dialog record: %s", event.data)
        self._ensure_connected()
        try:
            call_id : str = event.data.get("call_id")
            dialog_entry : DialogEntry = event.data.get("dialog_entry")
            async with self.acquire_lock(f"{self.DIALOG_RECORD_PREFIX}{call_id}"):
                dialog_record: DialogRecord | None = await self._get_dialog_record_without_lock(call_id)
                if not dialog_record:
                    logger.debug("Dialog record not found, creating new one for call_id: %s", call_id)
                    dialog_record = DialogRecord(
                        call_id=call_id,
                        dialog_record=[]
                    )
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

                for config_device in config_audio_device_info.device:
                    for device in device_info.devices:
                        if config_device.instance == device.instance:
                            config_device = device
                            break
                        elif device.deviceId == config_device.deviceId:
                            config_device = device
                            break

                    if config_device.input_devices:
                        for input_device in input_devices:
                            if input_device.index == config_device.input_devices.index:
                                config_device.input_devices = input_device
                                break
                            elif input_device.name == config_device.input_devices.name:
                                config_device.input_devices = input_device
                                break

                    if config_device.output_devices:
                        for output_device in output_devices:
                            if output_device.index == config_device.output_devices.index:
                                config_device.output_devices = output_device
                                break
                            elif output_device.name == config_device.output_devices.name:
                                config_device.output_devices = output_device
                                break

                self.redis_client.set(
                    f"{self.CONFIG_INPUT_AUDIO_KEY}",
                    config_audio_device_info.model_dump_json(),
                    ex=None,
                )
                logger.info(
                    "Config audio device info saved with lock: %s", config_audio_device_info.id
                )
        except Exception as e:
            logger.error("Failed to save config audio device info: %s", e)
            raise e

    # ==================== 电话队列管理 ====================

    async def get_phone_queue_batch(self, test_mode: bool = False) -> List[Dict[str, Any]]:
        """
        原子性地从数据库获取待打列表，保证多实例并发安全
        每次只返回一个号码，但确保Redis缓存中保持100个号码
        
        Args:
            test_mode: 测试模式，为True时仅返回13189300627
            
        Returns:
            List[Dict[str, Any]]: 电话列表，包含id和phone字段
        """
        self._ensure_connected()
        
        if test_mode:
            logger.info("测试模式：仅返回13189300627")
            return [{"id": "test_001", "phone": "13189300627"}]
        
        try:
            async with self.acquire_lock(self.PHONE_QUEUE_LOCK, timeout=30):
                # 1. 检查Redis缓存中的数量
                cached_count = self.redis_client.llen(self.PHONE_QUEUE_PREFIX + "pending")
                
                # 2. 如果缓存不足100个，从数据库补充到100个
                if cached_count < 100:
                    needed_count = 100 - cached_count
                    logger.info("Redis缓存中只有 %d 个号码，需要从数据库补充 %d 个", cached_count, needed_count)
                    
                    db_phones = await self._fetch_phones_from_db(needed_count)
                    if db_phones:
                        await self._add_phones_to_cache(db_phones)
                        logger.info("从数据库补充了 %d 条电话到Redis缓存", len(db_phones))
                    else:
                        logger.warning("数据库中没有更多待打列表")
                
                # 3. 从Redis缓存中获取一个号码
                phone_data = self.redis_client.lpop(self.PHONE_QUEUE_PREFIX + "pending")
                if phone_data:
                    phone_info = json.loads(phone_data)
                    logger.info("从Redis缓存获取到1个号码: %s", phone_info.get("phone"))
                    return [phone_info]
                else:
                    logger.warning("Redis缓存中没有号码")
                    return []
                
        except Exception as e:  # pylint: disable=broad-except
            logger.error("获取电话队列失败: %s", e)
            return []


    async def _fetch_phones_from_db(self, count: int) -> List[Dict[str, Any]]:
        """从数据库获取待打列表并标记为已拨打"""
        try:
            
            if not self.db:
                logger.error("数据库连接未注入")
                return []
            
            async with self.db.get_session() as session:
                select_sql = text("""
                    SELECT id, phone 
                    FROM phone_call_queue 
                    WHERE is_called = FALSE 
                    ORDER BY created_at ASC 
                    LIMIT :count
                    FOR UPDATE
                """)
                
                result = await session.execute(select_sql, {"count": count})
                rows = result.fetchall()
                
                if not rows:
                    logger.info("数据库中没有更多待打列表")
                    return []
                
                # 提取电话信息
                phones = []
                phone_ids = []
                
                for row in rows:
                    phone_info = {
                        "id": row[0],
                        "phone": row[1]
                    }
                    phones.append(phone_info)
                    phone_ids.append(row[0])
                
                # 批量更新为已拨打状态
                if phone_ids:
                    update_sql = text("""
                        UPDATE phone_call_queue 
                        SET is_called = TRUE, updated_at = CURRENT_TIMESTAMP 
                        WHERE id IN :phone_ids
                    """)
                    await session.execute(update_sql, {"phone_ids": tuple(phone_ids)})
                    await session.commit()
                    
                    logger.info("数据库更新了 %d 条电话为已拨打状态", len(phone_ids))
                
                return phones
                    
        except Exception as e:  # pylint: disable=broad-except
            logger.error("从数据库获取电话失败: %s", e)
            return []

    async def _add_phones_to_cache(self, phones: List[Dict[str, Any]]):
        """将电话添加到Redis缓存"""
        try:
            # 使用Redis Pipeline批量操作
            pipeline = self.redis_client.pipeline()
            
            for phone in phones:
                phone_data = json.dumps(phone, ensure_ascii=False)
                pipeline.rpush(self.PHONE_QUEUE_PREFIX + "pending", phone_data)
            
            # 设置过期时间（24小时）
            pipeline.expire(self.PHONE_QUEUE_PREFIX + "pending", 86400)
            
            pipeline.execute()
            logger.info("成功添加 %d 条电话到Redis缓存", len(phones))
            
        except Exception as e:  # pylint: disable=broad-except
            logger.error("添加电话到Redis缓存失败: %s", e)

    async def get_queue_stats(self) -> Dict[str, Any]:
        """获取队列统计信息"""
        self._ensure_connected()
        
        cached_count = 0
        try:
            # Redis缓存中的待打数量
            cached_count = self.redis_client.llen(self.PHONE_QUEUE_PREFIX + "pending")
            
            if not self.db:
                logger.error("数据库连接未注入")
                return {
                    "total": 0,
                    "uncalled": 0,
                    "called": 0,
                    "cached_pending": cached_count
                }
            
            async with self.db.get_session() as session:
                stats_query = text("""
                    SELECT 
                        COUNT(*) as total,
                        SUM(CASE WHEN is_called = FALSE THEN 1 ELSE 0 END) as uncalled,
                        SUM(CASE WHEN is_called = TRUE THEN 1 ELSE 0 END) as called
                    FROM phone_call_queue
                """)
                result = await session.execute(stats_query)
                row = result.fetchone()
                
                stats = {
                    "total": row[0] if row[0] else 0,
                    "uncalled": row[1] if row[1] else 0,
                    "called": row[2] if row[2] else 0,
                    "cached_pending": cached_count
                }
                
                logger.info("队列统计: 总计 %d, 未拨打 %d, 已拨打 %d, 缓存待打 %d", 
                          stats['total'], stats['uncalled'], stats['called'], stats['cached_pending'])
                return stats
                
        except Exception as e:  # pylint: disable=broad-except
            logger.error("获取队列统计失败: %s", e)
            return {
                "total": 0,
                "uncalled": 0,
                "called": 0,
                "cached_pending": cached_count
            }

    async def clear_phone_queue_cache(self) -> bool:
        """清空电话队列缓存"""
        self._ensure_connected()
        
        try:
            async with self.acquire_lock(self.PHONE_QUEUE_LOCK):
                result = self.redis_client.delete(self.PHONE_QUEUE_PREFIX + "pending")
                logger.info("清空电话队列缓存: %s", "成功" if result else "缓存不存在")
                return bool(result)
                
        except Exception as e:  # pylint: disable=broad-except
            logger.error("清空电话队列缓存失败: %s", e)
            return False
