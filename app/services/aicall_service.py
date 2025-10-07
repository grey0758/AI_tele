"""AI电话服务类 - 处理实际的电话拨打逻辑"""

# app/services/aicall_service.py
from datetime import datetime
from fastapi import HTTPException
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
        await self._register_listener(
            EventType.AICALL_CALL_END, self.reset_to_initialized_state
        )

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
                instance=6,
                start_time=datetime.now(),
                call_type="呼出",
            )

            await self.redis_service.create_call_record(call_record)

            # 发出通话开始事件
            await self.emit_event(EventType.CALL_OUT, call_record)
            logger.info("Call initiated to %s", call_record.phone_number)

        except Exception as e:  # pylint: disable=broad-except
            logger.error("Error making call to %s: %s", call_request.phone_number, e)
            raise e

    async def reset_to_initialized_state(self, _: Event | None = None):
        """重置到初始化状态"""
        self.is_ended = True
        logger.info("Reset to initialized state completed successfully")
