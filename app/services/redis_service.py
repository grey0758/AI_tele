
from datetime import datetime
import redis
import json
from typing import Optional, Dict, Any, List
from app.core.config import settings
from app.schemas.call_record import CallRecord


# 使用主应用的logger
from app.core.logger import get_logger
from app.schemas.device_info import DeviceInfo

logger = get_logger(__name__)

class RedisService:
    """Redis 服务类 - 统一管理设备信息和电话记录"""
    
    # Redis 键名常量
    DEVICE_INFO_KEY = "device_info"
    CALL_RECORD_PREFIX = "call_record:"
    
    def __init__(self, event_bus=None):
        """初始化 Redis 服务（支持事件总线注入）"""
        self.redis_client: Optional[redis.Redis] = None
        self._connection_pool: Optional[redis.ConnectionPool] = None
        self._initialized = False
        self.event_bus = event_bus

        self.default_device_instance = None
    
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
            logger.info("✅ Redis service initialized successfully")
            
            # 发布事件（如果有事件总线）
            if self.event_bus:
                await self.event_bus.publish("redis.connected", {"status": "connected"})
            
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
            
            # 发布事件（如果有事件总线）
            if self.event_bus:
                await self.event_bus.publish("redis.disconnected", {"status": "disconnected"})
                
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
    
    # ==================== 设备信息管理 ====================
    
    async def set_device_info(self, device_info: DeviceInfo) -> bool:
        """
        设置设备信息（创建或更新）
        
        Args:
            device_info: 设备信息对象
            
        Returns:
            bool: 操作是否成功
        """
        self._ensure_connected()
        try:
            self.redis_client.set(
                self.DEVICE_INFO_KEY, 
                device_info.model_dump_json(ensure_ascii=False)
            )
            logger.info(f"Device info saved: devid={device_info.devid}")
            
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
            logger.info(f"Device info retrieved: devid={device_info.devid}")
            return device_info
        except Exception as e:
            logger.error(f"Failed to get device info: {e}")
            return None
    
    async def get_default_device_instance(self, device_index: int = 0) -> Optional[int]:
        """
        获取默认设备的 instance 值
        
        Args:
            device_index: 设备索引，默认为 0（第一个设备）
            
        Returns:
            int: 设备的 instance 值，不存在时返回 None
        """
        if self.default_device_instance:
            logger.info(f"Default device instance retrieved: {self.default_device_instance}")
            return self.default_device_instance
        
        self._ensure_connected()
        try:
            device_info = await self.get_device_info()
            if not device_info or not device_info.devices:
                logger.warning("Device info not found or device list is empty")
                return None
            
            if device_index >= len(device_info.devices):
                logger.warning(f"Device index {device_index} out of range, total devices: {len(device_info.devices)}")
                return None
            
            instance = device_info.devices[device_index].instance
            self.default_device_instance = instance
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
        删除设备信息
        
        Returns:
            bool: 操作是否成功
        """
        self._ensure_connected()
        try:
            result = self.redis_client.delete(self.DEVICE_INFO_KEY)
            if result:
                logger.info("Device info deleted")
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

    # ==================== 电话记录管理 ====================
    
    async def create_call_record(self, call_record: CallRecord) -> str:
        """
        创建新的电话记录
        
        Args:
            call_record: 电话记录对象
            
        Returns:
            str: 生成的UUID
        """
        self._ensure_connected()
        try:
            key = f"{self.CALL_RECORD_PREFIX}{call_record.call_id}"
            self.redis_client.set(
                key,
                call_record.model_dump_json(ensure_ascii=False),
                ex=86400  # 24小时过期
            )
            
            logger.info(f"Call record created: {call_record.call_id}, phone: {call_record.phone_number}")

            return call_record.call_id
        except Exception as e:
            logger.error(f"Failed to create call record: {e}")
            raise

    async def update_call_record(self, call_record: CallRecord) -> bool:
        """
        更新电话记录（使用 CallRecord 对象）
        
        Args:
            call_record: 包含更新数据的 CallRecord 对象，call_id 必须存在
            
        Returns:
            bool: 操作是否成功
        """
        self._ensure_connected()
        try:
            if not call_record.call_id:
                logger.error("CallRecord object missing call_id")
                return False
            
            # 获取现有记录
            existing_record = await self.get_call_record(call_record.call_id)
            if not existing_record:
                logger.warning(f"Call record not found: {call_record.call_id}")
                return False
            
            # 更新字段（只更新非 None 的字段）
            for field in ['status', 'phone_number', 'call_type', 'device_instance', 'dialog_record', 'dialog_record_reply_marking', 'notes']:
                if hasattr(call_record, field):
                    new_value = getattr(call_record, field)
                    if new_value is not None:  # 只更新非 None 值
                        setattr(existing_record, field, new_value)
            
            # 如果状态是已挂断，设置结束时间和计算通话时长
            if existing_record.status == "已挂断" and not existing_record.end_time:
                existing_record.end_time = datetime.now()
                if existing_record.start_time:
                    # 这里可以添加时间计算逻辑
                    pass
            
            # 保存更新后的记录
            key = f"{self.CALL_RECORD_PREFIX}{existing_record.call_id}"
            self.redis_client.set(
                key,
                json.dumps(existing_record.to_dict(), ensure_ascii=False),
                ex=86400  # 24小时过期
            )
            
            logger.info(f"Call record updated: {existing_record.call_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to update call record: {e}")
            return False

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
            
            call_dict = json.loads(data)
            call_record = CallRecord.from_dict(call_dict)
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
                    call_dict = json.loads(data)
                    records.append(CallRecord.from_dict(call_dict))
            
            logger.info(f"All call records retrieved, total: {len(records)}")
            return records
        except Exception as e:
            logger.error(f"Failed to get all call records: {e}")
            return []
    
    async def get_dialog_after_marking(self, call_id: str) -> str:
        """获取标记后的对话记录并更新标记"""
        self._ensure_connected()
        
        # 从Redis获取对话记录
        call_record = await self.get_call_record(call_id)
        
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
        
        # 保存更新后的记录到 Redis
        await self.update_call_record(call_record)
        
        return dialog_text.strip()
    
