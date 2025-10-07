# app/services/aicall_service.py

from datetime import datetime
from app.models.events import Event
from app.models.events import EventType
from app.schemas.aicall import CallRequest, CallResponse
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
        self.is_ended = True      # 是否通话结束
    

    async def initialize(self) -> bool:
        return True

    async def register_event_listeners(self):
        await self._register_listener(EventType.AICALL_CALL_END, self.reset_to_initialized_state)
    
    async def make_call(self, call_request: CallRequest) -> CallResponse:
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
                logger.warning("Another call is already in progress")
                return CallResponse(
                    success=False,
                    message="系统繁忙，请稍后再试",
                    phone_number=call_request.phone_number,
                    error="Another call is already in progress"
                )
            
            # 设置通话结束状态        
            self.is_ended = False

            await self.emit_event(EventType.TTS_CONNECT, None)
            
            # 发出通话开始事件
            logger.info(f"Starting call to {call_request.phone_number} with TTS opening...")
            
            # 生成通话信息
            call_record = CallRecord(
                **call_request.model_dump(),
                status="to_be_dialed",
                instance=6,
                start_time=datetime.now(),
                call_type="呼出"
            )

            await self.redis_service.create_call_record(call_record)

            # 发出通话开始事件
            await self.emit_event(EventType.CALL_OUT, call_record)

            logger.info(f"Call initiated to {call_record.phone_number}")


            result = CallResponse(
                success=True,
                phone_number=call_record.phone_number,
                task_id=call_record.call_id,
                message="Call completed successfully"
            )
            
            logger.info(f"Call completed successfully")
            return result
            
        except Exception as e:
            logger.error(f"Error making call to {call_request.phone_number}: {e}")
            
            result = CallResponse(
                success=False,
                phone_number=call_request.phone_number,
                error=str(e),
                message="Failed to complete call"
            )
            return result

    async def reset_to_initialized_state(self, _ : Event = None):
        """重置到初始化状态"""
        try:
            self.is_ended = True

            logger.info("Reset to initialized state completed successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error resetting to initialized state: {e}")
            return False