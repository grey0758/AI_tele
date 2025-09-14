# app/services/aicall_service.py
import asyncio
from datetime import datetime
from re import T
from typing import Dict, Any

from click.core import F

from app.models.events import EventListener, EventPriority, EventType
from app.models.aicall import CallRequest
from app.core.logger import get_logger
from app.models.call_record import CallRecord, CurrentCallInfo
from app.services.base_service import BaseService
from app.core.event_bus import ProductionEventBus
from app.services.redis_service import RedisService

logger = get_logger(__name__)

class AicallService(BaseService):
    """AI电话服务类 - 处理实际的电话拨打逻辑"""
    def __init__(self, event_bus: ProductionEventBus, redis_service: RedisService):
        super().__init__(event_bus=event_bus, service_name="AicallService")
        self.redis_service = redis_service
        self.call_finished = False      # 是否正在通话中
    

    async def initialize(self):
        return True

    async def register_event_listeners(self):
        await self._register_listener(EventType.CALL_TELEPHONY, self.reset_to_initialized_state, wait_for_result=False)
    
    async def _register_listener(self, event_type, handler, priority=EventPriority.NORMAL, **kwargs):
        """辅助方法：减少重复代码"""
        try:
            self.event_bus.register_listener(
                EventListener(
                    event_type=event_type,
                    handler=handler, 
                    priority=priority,
                    name=f"{self.service_name}_{handler.__name__}",
                    **kwargs
                )
            )
            logger.info(f"✅ {self.service_name}: 注册监听器 {event_type.value}")
        except Exception as e:
            logger.error(f"❌ {self.service_name}: 注册监听器失败 {event_type.value} | error={str(e)}")
            raise
    
    async def make_call(self, call_request: CallRequest) -> Dict[str, Any]:
        """
        拨打电话服务函数 - 阻塞函数，一次只能有一个电话在拨打
        
        Args:
            call_request: 通话请求对象
            
        Returns:
            Dict: 包含通话状态和信息的字典
        """
        try:
            # 检查是否已有通话在进行
            if self.call_finished:
                logger.warning("Another call is already in progress")
                return {
                    "success": False,
                    "message": "系统繁忙，请稍后再试",
                    "phone_number": call_request.phone_number,
                    "error": "Another call is already in progress"
                }
            
            # 设置通话结束状态        
            self.call_finished = False
            
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


            result = {
                "success": True,
                "phone_number": call_record.phone_number,
                "status": "completed",
                "message": "Call completed successfully"
            }
            
            logger.info(f"Call completed successfully")
            return result
            
        except Exception as e:
            logger.error(f"Error making call to {call_request.phone_number}: {e}")
            
            result = {
                "success": False,
                "phone_number": call_request.phone_number,
                "error": str(e),
                "message": "Failed to complete call"
            }
            return result
        finally:
            # 清理状态
            self.call_finished = False

    async def reset_to_initialized_state(self, event):
        """重置到初始化状态"""
        try:
            # 异步后台运行这两个重置函数，等待future返回成功后，再设置call_finished为True
            tts_task = self.emit_event(EventType.TTS_CALL_FINISHED, None, wait_for_result=True)
            rtasr_task = self.emit_event(EventType.RTASR_CALL_FINISHED, None, wait_for_result=True)
            
            # 等待两个任务都完成
            results = await asyncio.gather(tts_task, rtasr_task, return_exceptions=True)
            
            # 检查是否有异常
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    task_name = "TTS_CALL_FINISHED" if i == 0 else "RTASR_CALL_FINISHED"
                    logger.error(f"Error in {task_name} event: {result}")
            
            self.call_finished = True
            logger.info("Reset to initialized state completed successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error resetting to initialized state: {e}")
            return False