"""Redis服务类"""
from typing import Optional, Dict, Any
import redis.asyncio as redis
from app.models.events import Event, EventType
from app.utils.get_audio_devices import get_audio_devices
from app.core.config import settings
from app.core.event_bus import ProductionEventBus
from app.models.call_record import CallRecord
from app.db.database import Database
from app.core.logger import get_logger
from app.models.device_info import ConfigAudioDeviceInfo, DeviceInfo
from app.services.base_service import BaseService

logger = get_logger(__name__)


class RedisService(BaseService):
    """Redis 服务类 - 统一管理设备信息和电话记录"""

    # Redis 键名常量
    DEVICE_INFO_KEY = "device_info"
    CALL_RECORD_PREFIX = "call_record:"
    CONFIG_INPUT_AUDIO_KEY = "config_input_audio"
    DIALOG_RECORD_PREFIX = "dialog_record:"

    def __init__(self, event_bus: Optional[ProductionEventBus] = None, db: Optional[Database] = None):
        """初始化 Redis 服务（支持事件总线和数据库注入）"""
        super().__init__(event_bus, "RedisService")
        self.redis_client: Optional[redis.Redis] = None
        self._initialized = False
        self.event_bus = event_bus
        self.device_info: Optional[DeviceInfo] = None
        self.db = db

    async def initialize(self) -> bool:
        """异步初始化 Redis 连接"""
        if self._initialized:
            return True

        try:
            # 检查是否是Upstash Redis（需要SSL）
            is_upstash = "upstash.io" in settings.redis_host
            
            # 构建Redis连接URL
            if settings.redis_password:
                redis_url = f"redis://{settings.redis_username}:{settings.redis_password}@{settings.redis_host}:{settings.redis_port}/{settings.redis_db}"
            else:
                redis_url = f"redis://{settings.redis_host}:{settings.redis_port}/{settings.redis_db}"
            
            # 如果是Upstash，使用rediss://
            if settings.redis_ssl or is_upstash:
                redis_url = redis_url.replace("redis://", "rediss://")
            
            # 创建异步 Redis 连接
            self.redis_client = await redis.from_url(redis_url, decode_responses=True)
            
            # 测试连接
            assert self.redis_client is not None
            await self.redis_client.ping()
            self._initialized = True
            logger.info("✅ Redis service initialized successfully")
            return True
        except Exception as e: # pylint: disable=broad-except
            logger.error("❌ Redis service initialization failed: %s", e)
            return False

    async def register_event_listeners(self):
        """注册事件监听器"""
        await self._register_listener(EventType.REDIS_CREATE_CALL_RECORD, self.create_call_record)

    async def shutdown(self):
        """异步关闭 Redis 连接"""
        try:
            if self.redis_client:
                await self.redis_client.close()
                self.redis_client = None

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
            assert self.redis_client is not None
            await self.redis_client.ping()

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
            assert self.redis_client is not None
            await self.redis_client.set(
                self.DEVICE_INFO_KEY, device_info.model_dump_json()
            )
            self.device_info = device_info
            logger.info("Device info saved: devid=%s", device_info.devid)
            return True
        except Exception as e:  # pylint: disable=broad-except
            logger.error("Failed to save device info: %s", e)
            return False

    # ==================== 电话记录管理 ====================

    async def create_call_record(self, event: Event) -> str:
        """
        创建新的电话记录

        Args:
            event: 包含call_record的事件

        Returns:
            str: 生成的UUID
        """
        call_record = event.data
        if not isinstance(call_record, CallRecord):
            logger.error("Event data is not a CallRecord")
            return ""
        self._ensure_connected()
        try:
            if not call_record or not call_record.call_id:
                logger.error("CallRecord object missing or missing call_id")
                return ""

            # 检查记录是否已存在
            existing_key = f"{self.CALL_RECORD_PREFIX}{call_record.call_id}"
            assert self.redis_client is not None
            if await self.redis_client.exists(existing_key):
                logger.warning("Call record already exists: %s", call_record.call_id)
                return call_record.call_id

            await self.redis_client.set(
                f"{self.CALL_RECORD_PREFIX}{call_record.call_id}",
                call_record.model_dump_json(),
                ex=86400,  # 24小时过期
            )

            logger.info(
                "Call record created: %s, phone: %s", call_record.call_id, call_record.phone_number
            )
            return call_record.call_id
        except Exception as e:  # pylint: disable=broad-except
            logger.error("Failed to create call record: %s", e)
            raise

    async def set_device_info_input_audio_and_output_audio(
        self, config_audio_device_info: ConfigAudioDeviceInfo
    ):
        """
        设置设备信息输入音频和输出音频
        """
        self._ensure_connected()
        try:
            device_info = self.device_info
            if not device_info:
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

            assert self.redis_client is not None
            await self.redis_client.set(
                f"{self.CONFIG_INPUT_AUDIO_KEY}",
                config_audio_device_info.model_dump_json(),
                ex=None,
            )
            logger.info(
                "Config audio device info saved: %s", config_audio_device_info.id
            )
        except Exception as e:
            logger.error("Failed to save config audio device info: %s", e)
            raise e
