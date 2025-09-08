from datetime import datetime
import redis
from redis.exceptions import LockError, LockNotOwnedError
import json
import asyncio
from contextlib import asynccontextmanager
from typing import Optional, Dict, Any, List
from app.core.config import settings
from app.core.event_bus import ProductionEventBus
from app.schemas import call_record
from app.schemas.call_record import CallRecord, CurrentCallInfo, DialogEntry

# 使用主应用的logger
from app.core.logger import get_logger
from app.schemas.device_info import DeviceInfo
from app.services.base_service import BaseService

logger = get_logger(__name__)

class RedisService(BaseService):
    """Redis 服务类 - 统一管理设备信息和电话记录（支持分布式锁）"""
    
    # Redis 键名常量
    DEVICE_INFO_KEY = "device_info"
    CALL_RECORD_PREFIX = "call_record:"
    CURRENT_CALL_INFO_PREFIX = "current_call_info:"
    LOCK_PREFIX = "lock:"
    
    def __init__(self, event_bus: Optional[ProductionEventBus] = None):
        """初始化 Redis 服务（支持事件总线注入）"""
        super().__init__(event_bus, "RedisService")
        self.redis_client: Optional[redis.Redis] = None
        self._connection_pool: Optional[redis.ConnectionPool] = None
        self._initialized = False
        self.event_bus = event_bus
        self.device_info = None
        
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
                password=settings.redis_password,
                db=settings.redis_db,
                decode_responses=True,
                socket_connect_timeout=5,
                socket_timeout=5,
                retry_on_timeout=True,
                health_check_interval=30,
            )
            
            # 创建 Redis 客户端
            self.redis_client = redis.Redis(connection_pool=self._connection_pool)
            
            # 测试连接
            self.redis_client.ping()
            self._initialized = True
            logger.info("✅ Redis service initialized successfully with distributed lock support")
            
            return True
        except Exception as e:
            logger.error(f"❌ Redis service initialization failed: {e}")
            return False
    
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
        except Exception as e:
            logger.error(f"❌ Error during Redis shutdown: {e}")
    
    async def health_check(self) -> Dict[str, Any]:
        """健康检查"""
        try:
            if not self._initialized or not self.redis_client:
                return {"status": "unhealthy", "error": "Not initialized"}
            
            # 测试连接
            self.redis_client.ping()
            
            # 获取连接信息
            info = self.redis_client.info()
            return {
                "status": "healthy",
                "connected_clients": info.get("connected_clients", 0),
                "used_memory": info.get("used_memory_human", "unknown"),
                "redis_version": info.get("redis_version", "unknown")
            }
        except Exception as e:
            return {"status": "unhealthy", "error": str(e)}
    
    def _ensure_connected(self):
        """确保 Redis 已连接"""
        if not self._initialized or not self.redis_client:
            raise RuntimeError("Redis service not initialized")
    
    # ==================== 分布式锁管理 ====================
    
    @asynccontextmanager
    async def acquire_lock(self, resource_id: str, timeout: Optional[int] = None, blocking_timeout: Optional[int] = None):
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
                thread_local=False
            )
            
            # 在线程池中获取锁（避免阻塞事件循环）
            loop = asyncio.get_event_loop()
            acquired = await loop.run_in_executor(
                None, 
                lambda: lock.acquire(blocking=True)
            )
            
            if not acquired:
                raise TimeoutError(f"Failed to acquire lock: {lock_key}")
                
            logger.debug(f"🔒 Lock acquired: {lock_key}")
            yield lock
            
        except (LockError, LockNotOwnedError) as e:
            logger.error(f"❌ Lock error for {lock_key}: {e}")
            raise
        except TimeoutError as e:
            logger.error(f"⏰ Lock timeout for {lock_key}: {e}")
            raise
        except Exception as e:
            logger.error(f"❌ Unexpected error acquiring lock {lock_key}: {e}")
            raise
        finally:
            if lock:
                try:
                    # 在线程池中释放锁
                    loop = asyncio.get_event_loop()
                    await loop.run_in_executor(None, lock.release)
                    logger.debug(f"🔓 Lock released: {lock_key}")
                except (LockError, LockNotOwnedError) as e:
                    logger.error(f"❌ Failed to release lock {lock_key}: {e}")
                except Exception as e:
                    logger.error(f"❌ Unexpected error releasing lock {lock_key}: {e}")

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
                    self.DEVICE_INFO_KEY, 
                    device_info.model_dump_json()
                )
                logger.info(f"Device info saved with lock: devid={device_info.devid}")
                return True
        except Exception as e:
            logger.error(f"Failed to save device info: {e}")
            return False
    
    async def get_device_info(self) -> Optional[DeviceInfo]:
        """
        获取设备信息
        
        Returns:
            DeviceInfo: 设备信息对象，不存在时返回 None
        """
        self._ensure_connected()
        try:
            data = self.redis_client.get(self.DEVICE_INFO_KEY)
            if not data:
                logger.warning("Device info not found")
                return None
            
            device_info = DeviceInfo.model_validate_json(data)
            self.device_info = device_info
            logger.info(f"Device info retrieved: devid={device_info.devid}")
            return device_info
        except Exception as e:
            logger.error(f"Failed to get device info: {e}")
            return None
    
    async def get_default_device_instance(self, device_index: int|None = None) -> Optional[int]:
        """
        获取默认设备的 instance 值
        
        Args:
            device_index: 设备索引，默认为 0（第一个设备）
            
        Returns:
            int: 设备的 instance 值，不存在时返回 None
        """
        if device_index is None:
            device_index = 0
        
        self._ensure_connected()
        try:
            if self.device_info:
                logger.info(f"Default device instance retrieved: {self.device_info.devices[device_index].instance}")
                return self.device_info.devices[device_index].instance
            
            device_info = await self.get_device_info()
            if not device_info or not device_info.devices:
                logger.warning("Device info not found or device list is empty")
                return None
            
            if device_index >= len(device_info.devices):
                logger.warning(f"Device index {device_index} out of range, total devices: {len(device_info.devices)}")
                return None
            
            instance = device_info.devices[device_index].instance
            self.device_info = device_info
            logger.info(f"Default device instance retrieved: {instance}")
            return instance
        except Exception as e:
            logger.error(f"Failed to get default device instance: {e}")
            return None
    
    async def device_exists(self) -> bool:
        """
        检查设备信息是否存在
        
        Returns:
            bool: 设备信息是否存在
        """
        self._ensure_connected()
        try:
            return self.redis_client.exists(self.DEVICE_INFO_KEY) > 0
        except Exception as e:
            logger.error(f"Failed to check device info existence: {e}")
            return False
    
    async def delete_device_info(self) -> bool:
        """
        删除设备信息 - 使用分布式锁
        
        Returns:
            bool: 操作是否成功
        """
        self._ensure_connected()
        try:
            async with self.acquire_lock("device_info"):
                result = self.redis_client.delete(self.DEVICE_INFO_KEY)
                if result:
                    logger.info("Device info deleted with lock")
                    return True
                else:
                    logger.warning("Device info not found, no need to delete")
                    return False
        except Exception as e:
            logger.error(f"Failed to delete device info: {e}")
            return False
    
    async def get_device_count(self) -> int:
        """
        获取设备数量
        
        Returns:
            int: 设备数量
        """
        device_info = await self.get_device_info()
        return len(device_info.devices) if device_info else 0
    
    async def get_all_device_instances(self) -> List[int]:
        """
        获取所有设备的 instance 列表
        
        Returns:
            List[int]: 所有设备的 instance 值列表
        """
        device_info = await self.get_device_info()
        if not device_info:
            return []
        return [device.instance for device in device_info.devices]

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
                    logger.warning(f"Call record already exists: {call_record.call_id}")
                    return call_record.call_id
                
                self.redis_client.set(
                    f"{self.CALL_RECORD_PREFIX}{call_record.call_id}",
                    call_record.model_dump_json(),
                    ex=86400  # 24小时过期
                )
                
                logger.info(f"Call record created with lock: {call_record.call_id}, phone: {call_record.phone_number}")
                return call_record.call_id
        except Exception as e:
            logger.error(f"Failed to create call record: {e}")
            raise

    async def update_call_record(self, call_record: CallRecord = None) -> bool:
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
                existing_record = await self._get_call_record_without_lock(call_record.call_id)
                if not existing_record:
                    logger.warning(f"Call record not found: {call_record.call_id}")
                    return False
                
                # 更新时间戳
                call_record.updated_at = datetime.now()
                
                # 保存更新后的记录
                self.redis_client.set(
                    f"{self.CALL_RECORD_PREFIX}{call_record.call_id}",
                    call_record.model_dump_json(),
                    ex=86400  # 24小时过期
                )
                            
                logger.info(f"Call record updated with lock: {call_record.call_id}")
                return True
        except Exception as e:
            logger.error(f"Failed to update call record: {e}")
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
                    logger.warning(f"Call record not found: {call_id}")
                    return False
                
                call_record.status = status
                call_record.updated_at = datetime.now()
                
                # 直接保存，避免递归锁
                self.redis_client.set(
                    f"{self.CALL_RECORD_PREFIX}{call_record.call_id}",
                    call_record.model_dump_json(),
                    ex=86400
                )
                
                logger.info(f"Call record status updated with lock: {call_id} -> {status}")
                return True
        except Exception as e:
            logger.error(f"Failed to update call record status: {e}")
            return False
    
    async def update_call_record_call_id(self, instance: int, new_call_id: str) -> bool:
        """
        更新电话记录call_id - 使用分布式锁
        """
        self._ensure_connected()
        try:
            current_call_info = await self.get_current_call_info(instance)

            if not current_call_info:
                logger.warning(f"Current call info not found: {instance}")
                return False

            call_record = await self.get_call_record(current_call_info.uuid_call_record)

            if not call_record:
                logger.warning(f"Call record not found: {current_call_info.uuid_call_id}")
                return False
                
            call_record.call_id = new_call_id
            call_record.updated_at = datetime.now()
            self.redis_client.set(
                f"{self.CALL_RECORD_PREFIX}{call_record.call_id}",
                call_record.model_dump_json(),
                ex=86400
            )
            self.redis_client.delete(f"{self.CALL_RECORD_PREFIX}{current_call_info.uuid_call_record}")
            current_call_info.uuid_call_id = new_call_id
            self.redis_client.set(
                f"{self.CURRENT_CALL_INFO_PREFIX}{current_call_info.instance}",
                current_call_info.model_dump_json(),
                ex=3600
            )
            logger.info(f"Call record ID updated with lock: {instance} -> {new_call_id}")
            return True

        except Exception as e:
            logger.error(f"Failed to update call record ID: {e}")
            return False
    
    async def update_call_record_dialog_record(self, call_id: str, dialog_entry: DialogEntry) -> bool:
        """
        更新电话记录对话记录 - 使用分布式锁
        """
        self._ensure_connected()
        try:
            async with self.acquire_lock(f"call_record:{call_id}"):
                call_record = await self._get_call_record_without_lock(call_id)
                if not call_record:
                    logger.warning(f"Call record not found: {call_id}")
                    return False
                call_record.dialog_record.append(dialog_entry)
                call_record.updated_at = datetime.now()
                await self.update_call_record(call_record)
                return True
        except Exception as e:
            logger.error(f"Failed to update call record dialog record: {e}")
            return False

    async def _get_call_record_without_lock(self, call_id: str) -> Optional[CallRecord]:
        """
        内部方法：获取电话记录（不使用锁，避免递归锁）
        
        Args:
            call_id: 电话记录ID
            
        Returns:
            CallRecord: 电话记录对象，不存在时返回 None
        """
        try:
            key = f"{self.CALL_RECORD_PREFIX}{call_id}"
            data = self.redis_client.get(key)
            
            if not data:
                return None
            
            call_record = CallRecord.model_validate_json(data)
            return call_record
        except Exception as e:
            logger.error(f"Failed to get call record without lock: {e}")
            return None

    async def get_call_record(self, call_id: str) -> Optional[CallRecord]:
        """
        获取电话记录
        
        Args:
            call_id: 电话记录ID
            
        Returns:
            CallRecord: 电话记录对象，不存在时返回 None
        """
        self._ensure_connected()
        try:
            key = f"{self.CALL_RECORD_PREFIX}{call_id}"
            data = self.redis_client.get(key)
            
            if not data:
                logger.warning(f"Call record not found: {call_id}")
                return None

            call_record = CallRecord.model_validate_json(data)
            logger.info(f"Call record retrieved: {call_id}")
            return call_record
        except Exception as e:
            logger.error(f"Failed to get call record: {e}")
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
            
            logger.info(f"All call records retrieved, total: {len(records)}")
            return records
        except Exception as e:
            logger.error(f"Failed to get all call records: {e}")
            return []
    
    async def get_dialog_after_marking(self, call_id: str) -> str:
        """
        获取标记后的对话记录并更新标记 - 使用分布式锁
        """
        self._ensure_connected()
        try:
            async with self.acquire_lock(f"call_record:{call_id}"):
                # 从Redis获取对话记录
                call_record = await self._get_call_record_without_lock(call_id)
                
                if not call_record or not call_record.dialog_record:
                    return "客户没有说话"
                
                # 如果对话记录为空，返回"客户没有说话"
                if not len(call_record.dialog_record) > call_record.dialog_record_reply_marking:
                    return "客户没有说话"
                
                # 获取标记位置之后的所有记录
                new_records = call_record.dialog_record[call_record.dialog_record_reply_marking:]
                
                # 组合对话记录为字符串
                dialog_text = ""
                for entry in new_records:
                    dialog_text += entry.content
                
                # 更新回复标记为下一个位置
                call_record.dialog_record_reply_marking = call_record.dialog_record_reply_marking + len(new_records)
                call_record.updated_at = datetime.now()
                
                # 保存更新后的记录到 Redis
                self.redis_client.set(
                    call_record.call_id,
                    call_record.model_dump_json(),
                    ex=86400
                )
                
                logger.info(f"Dialog after marking retrieved with lock: {call_id}")
                return dialog_text.strip()
        except Exception as e:
            logger.error(f"Failed to get dialog after marking: {e}")
            return "客户没有说话"
    
    # ==================== 当前拨打电话信息管理（使用分布式锁） ====================
    
    async def set_current_call_info(self, current_call_info: CurrentCallInfo) -> Optional[CurrentCallInfo]:
        """
        设置当前拨打电话信息（创建新的通话信息）- 使用分布式锁
        
        Args:
            current_call_info: 当前拨打电话信息对象
            
        Returns:
            CurrentCallInfo: 创建的通话信息对象，失败时返回 None
        """
        self._ensure_connected()
        try:
            if not current_call_info.instance:
                logger.error("CurrentCallInfo object missing instance")
                return None

            async with self.acquire_lock(f"current_call_info:{current_call_info.instance}"):
                # 保存到 Redis
                self.redis_client.set(
                    f"{self.CURRENT_CALL_INFO_PREFIX}{current_call_info.instance}",
                    current_call_info.model_dump_json(),
                    ex=3600  # 1小时过期，防止数据残留
                )
                
                logger.info(f"Current call info set with lock: phone={current_call_info.phone}, instance={current_call_info.instance}")
                return current_call_info
        except Exception as e:
            logger.error(f"Failed to set current call info: {e}")
            return None
    
    async def get_current_call_info(self, instance: int) -> Optional[CurrentCallInfo]:
        """
        获取当前拨打电话信息
        
        Returns:
            CurrentCallInfo: 当前拨打电话信息对象，不存在时返回 None
        """
        self._ensure_connected()
        try:
            key = f"{self.CURRENT_CALL_INFO_PREFIX}{instance}"
            data = self.redis_client.get(key)
            if not data:
                logger.info("No current call info found")
                return None
            
            current_call_info = CurrentCallInfo.model_validate_json(data)
            logger.info(f"Current call info retrieved: uuid={current_call_info.uuid_call_id}, phone={current_call_info.phone}, instance={current_call_info.instance}")
            return current_call_info
        except Exception as e:
            logger.error(f"Failed to get current call info: {e}")
            return None
    
    async def update_current_call_info(self, current_call_info: CurrentCallInfo) -> bool:
        """
        更新当前拨打电话信息 - 使用分布式锁
        
        Args:
            current_call_info: 要更新的通话信息对象
            
        Returns:
            bool: 操作是否成功
        """
        self._ensure_connected()
        try:
            async with self.acquire_lock(f"current_call_info:{current_call_info.instance}"):
                self.redis_client.set(
                    f"{self.CURRENT_CALL_INFO_PREFIX}{current_call_info.instance}", 
                    current_call_info.model_dump_json(),
                    ex=3600  # 1小时过期，防止数据残留
                )
                
                logger.info(f"Current call info updated with lock: instance={current_call_info.instance}")
                return True
        except Exception as e:
            logger.error(f"Failed to update current call info: {e}")
            return False
    
    async def clear_current_call_info(self, instance: int) -> bool:
        """
        清除当前拨打电话信息 - 使用分布式锁
        
        Returns:
            bool: 操作是否成功
        """
        self._ensure_connected()
        try:
            async with self.acquire_lock(f"current_call_info:{instance}"):
                result = self.redis_client.delete(f"{self.CURRENT_CALL_INFO_PREFIX}{instance}")
                if result:
                    logger.info("Current call info cleared with lock")
                    return True
                else:
                    logger.info("No current call info to clear")
                    return True  # 没有数据也算成功
        except Exception as e:
            logger.error(f"Failed to clear current call info: {e}")
            return False