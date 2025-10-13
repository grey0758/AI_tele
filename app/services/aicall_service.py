"""AI电话服务类 - 处理实际的电话拨打逻辑"""

# app/services/aicall_service.py
from datetime import datetime
from fastapi import HTTPException
import asyncio
from app.models.events import Event
from app.models.events import EventType
from app.schemas.aicall import CallRequest
from app.core.logger import get_logger
from app.models.call_record import CallRecord
from app.services.base_service import BaseService
from app.core.event_bus import ProductionEventBus
from app.services.redis_service import RedisService

logger = get_logger(__name__)


class AicallService(BaseService):
    """AI电话服务类 - 处理实际的电话拨打逻辑"""

    def __init__(self, event_bus: ProductionEventBus, redis_service: RedisService):
        super().__init__(event_bus=event_bus, service_name="AicallService")
        self.redis_service = redis_service
        self.is_ended = True  # 是否通话结束

    async def initialize(self) -> bool:
        """初始化"""
        return True

    async def register_event_listeners(self):
        """注册事件监听器"""
        # await self._register_listener(EventType.PHONE_SERVICE_ONHANGUP, self.reset_to_initialized_state)
        await self._register_listener(EventType.PHONE_SERVICE_ONHANGUP_AUTO_CALL, self.auto_call_next_phone, timeout=30.0)

    async def make_call(self, call_request: CallRequest):
        """
        拨打电话服务函数 - 阻塞函数，一次只能有一个电话在拨打

        Args:
            call_request: 通话请求对象

        Returns:
            Dict: 包含通话状态和信息的字典
        """
        try:
            # 检查是否已有通话在进行
            if not self.is_ended:
                logger.info("Another call is already in progress")
                raise HTTPException(status_code=429, detail="Another call is already in progress")

            self.is_ended = False

            logger.info(
                "Starting call to %s with TTS opening...", call_request.phone_number
            )

            call_record = CallRecord(
                **call_request.model_dump(),
                status="to_be_dialed",
                instance=call_request.device_index,
                start_time=datetime.now(),
                call_type="呼出",
            )

            await self.redis_service.create_call_record(call_record)

            await self.emit_event(EventType.PHONE_SERVICE_CALL_OUT, call_record)
            logger.info("Call initiated to %s", call_record.phone_number)

        except Exception as e:  # pylint: disable=broad-except
            logger.error("Error making call to %s: %s", call_request.phone_number, e)
            raise e

    async def reset_to_initialized_state(self, _: Event | None = None):
        """重置到初始化状态"""
        self.is_ended = True
        logger.info("Reset to initialized state completed successfully")

    async def auto_call_next_phone(self, _: Event | None = None):
        """从Redis获取下一个电话号码并自动拨打"""
        try:
            # 测试模式：使用固定电话号码
            phone_number = "13189300627"
            phone_id = "test_001"
            
            logger.info("测试模式 - 开始自动拨打: ID=%s, 电话=%s", phone_id, phone_number)
            
            # 创建拨打电话请求
            call_request = CallRequest(
                phone_number=phone_number,
                device_index=0,
                tts_opening="你好老板，我是广州大麦的月月，我们在寻找联合运营的合作伙伴，共同投入共同分成的方式，问您目前有考虑联合运营的需求吗？",
                custom_id=phone_id
            )

            # 发起拨打
            self.is_ended = True  # 是否通话结束
            await self.make_call(call_request)
            
        except Exception as e:  # pylint: disable=broad-except
            logger.error("自动拨打失败: %s", e)
            # 发生错误时重置状态，允许手动拨打
            self.is_ended = True

    async def start_auto_calling(self):
        """启动自动拨打模式"""
        try:
            if not self.is_ended:
                logger.warning("已有通话在进行，无法启动自动拨打")
                return False
            
            logger.info("启动自动拨打模式（测试模式）")
            await self.auto_call_next_phone()
            return True
            
        except Exception as e:  # pylint: disable=broad-except
            logger.error("启动自动拨打失败: %s", e)
            return False
