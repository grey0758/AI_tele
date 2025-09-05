# app/services/aicall_service.py
from datetime import datetime
import time
from typing import Dict, Optional, Any
from app.api.v1.endpoints.aicall import CallResponse
from app.schemas.aicall import CallRequest
from celery import current_app
from app.core.logger import get_logger
from app.schemas.call_record import CallRecord, CurrentCallInfo
from app.services.base_service import BaseService
from app.core.event_bus import ProductionEventBus
from app.services.redis_service import RedisService

logger = get_logger(__name__)


class AicallService(BaseService):
    """AI电话服务类 - 处理实际的电话拨打逻辑"""
    
    def __init__(self, event_bus: Optional[ProductionEventBus] = None, redis_service: Optional[RedisService] = None):
        super().__init__(event_bus=event_bus, service_name="AicallService")
        self.redis_service = redis_service
        self.current_call = None  # 当前通话状态
        self.is_busy = False      # 是否正在通话中
    
    def make_call(self, call_request: CallRequest) -> Dict[str, Any]:
        """
        拨打电话服务函数 - 阻塞函数，一次只能有一个电话在拨打
        
        Args:
            call_request: 通话请求对象
            
        Returns:
            Dict: 包含通话状态和信息的字典
        """
        try:
            # 检查是否已有通话在进行
            if self.is_busy:
                logger.warning("Another call is already in progress")
                return CallResponse(
                    success=False,
                    message="系统繁忙，请稍后再试",
                    phone_number=call_request.phone_number,
                    error="Another call is already in progress"
                )
            
            # 设置忙碌状态
            self.is_busy = True
            
            logger.info(f"Starting call to {call_request.phone_number} with TTS opening...")

            call_request.instance = self.redis_service.get_default_device_instance(call_request.instance)
            
            # 生成通话信息
            call_record = CallRecord(
                **call_request.model_dump(),
                status="to_be_dialed",
                start_time=datetime.now(),
                call_type="呼出"
            )

            self.redis_service.create_call_record(call_record)

            current_call_info = CurrentCallInfo(
                uuid_call_record=call_record.call_id,
                phone=call_record.phone_number,
                instance=call_record.instance
            )
            
            self.redis_service.set_current_call_info(current_call_info)

            self.emit_event("call.out", call_record)

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
            logger.error(f"Error making call to {call_record.phone_number}: {e}")
            # 异步更新通话状态为失败
            current_app.send_task('app.tasks.aicall_tasks.update_call_status_task', args=[call_record.phone_number, "failed", {"error": str(e)}])
            
            result = {
                "success": False,
                "phone_number": call_record.phone_number,
                "error": str(e),
                "message": "Failed to complete call"
            }
            return result
        finally:
            # 清理状态
            self.is_busy = False
            self.current_call = None
