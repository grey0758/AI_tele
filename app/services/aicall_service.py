# app/services/aicall_service.py
import logging
import uuid
import time
import asyncio
from typing import Dict, Optional, Any
from app.services.celery_service import celery_app

logger = logging.getLogger(__name__)


class AICallService:
    """AI电话服务类 - 处理实际的电话拨打逻辑"""
    
    def __init__(self):
        self.service_id = str(uuid.uuid4())
        self.current_call = None  # 当前通话状态
        self.is_busy = False      # 是否正在通话中
        logger.info(f"AI Call Service initialized with ID: {self.service_id}")
    
    def make_call(self, phone_number: str, tts_opening: str, call_config: Optional[Dict] = None) -> Dict[str, Any]:
        """
        拨打电话服务函数 - 阻塞函数，一次只能有一个电话在拨打
        
        Args:
            phone_number: 要拨打的电话号码
            tts_opening: TTS开场白内容
            call_config: 通话配置参数（可选）
            
        Returns:
            Dict: 包含通话状态和信息的字典
        """
        try:
            # 检查是否已有通话在进行
            if self.is_busy:
                logger.warning("Another call is already in progress")
                return {
                    "success": False,
                    "phone_number": phone_number,
                    "error": "Another call is already in progress",
                    "message": "系统繁忙，请稍后再试"
                }
            
            # 设置忙碌状态
            self.is_busy = True
            call_id = str(uuid.uuid4())
            
            logger.info(f"Starting call to {phone_number} with TTS opening: {tts_opening[:50]}...")
            
            # 生成通话信息
            call_info = {
                "call_id": call_id,
                "phone_number": phone_number,
                "tts_opening": tts_opening,
                "status": "initiated",
                "service_id": self.service_id,
                "start_time": time.time(),
                "config": call_config or {}
            }
            
            self.current_call = call_info
            
            
            
            logger.info(f"Call {call_id} initiated to {phone_number}")
            
            celery_app.send_task('app.services.phone_service.make_call', args=[phone_number, tts_opening, call_config])
            # 更新通话状态
            self.current_call["status"] = "connected"
            
            # TODO: 播放TTS开场白
            logger.info(f"Playing TTS opening: {tts_opening}")
            
            # 模拟通话进行中（实际实现时替换为真实逻辑）
            time.sleep(5)  # 模拟通话时间
            
            # 更新通话状态
            self.current_call["status"] = "completed"
            self.current_call["end_time"] = time.time()
            self.current_call["duration"] = self.current_call["end_time"] - self.current_call["start_time"]
            
            result = {
                "success": True,
                "call_id": call_id,
                "phone_number": phone_number,
                "status": "completed",
                "duration": self.current_call["duration"],
                "message": "Call completed successfully"
            }
            
            logger.info(f"Call {call_id} completed successfully")
            return result
            
        except Exception as e:
            logger.error(f"Error making call to {phone_number}: {e}")
            result = {
                "success": False,
                "phone_number": phone_number,
                "error": str(e),
                "message": "Failed to complete call"
            }
            return result
        finally:
            # 清理状态
            self.is_busy = False
            self.current_call = None
    
    def get_call_status(self, call_id: str = None) -> Dict[str, Any]:
        """
        获取通话状态
        
        Args:
            call_id: 通话ID（可选，如果不提供则返回当前通话状态）
            
        Returns:
            Dict: 通话状态信息
        """
        try:
            if call_id and self.current_call and self.current_call.get("call_id") == call_id:
                return {
                    "call_id": call_id,
                    "status": self.current_call.get("status", "unknown"),
                    "phone_number": self.current_call.get("phone_number"),
                    "duration": self.current_call.get("duration", 0),
                    "is_busy": self.is_busy,
                    "message": "Call status retrieved"
                }
            elif self.current_call:
                return {
                    "call_id": self.current_call.get("call_id"),
                    "status": self.current_call.get("status", "unknown"),
                    "phone_number": self.current_call.get("phone_number"),
                    "duration": self.current_call.get("duration", 0),
                    "is_busy": self.is_busy,
                    "message": "Current call status retrieved"
                }
            else:
                return {
                    "status": "idle",
                    "is_busy": self.is_busy,
                    "message": "No active call"
                }
                
        except Exception as e:
            logger.error(f"Error getting call status: {e}")
            return {
                "error": str(e),
                "message": "Failed to get call status"
            }
    
    def hang_up_call(self, call_id: str = None) -> Dict[str, Any]:
        """
        挂断电话
        
        Args:
            call_id: 通话ID（可选）
            
        Returns:
            Dict: 挂断结果
        """
        try:
            if not self.is_busy or not self.current_call:
                return {
                    "success": False,
                    "message": "No active call to hang up"
                }
            
            if call_id and self.current_call.get("call_id") != call_id:
                return {
                    "success": False,
                    "call_id": call_id,
                    "message": "Call ID does not match current call"
                }
            
            logger.info(f"Hanging up call {self.current_call.get('call_id')}")
            
            # TODO: 在这里实现实际的挂断逻辑
            # 例如：发送挂断信号到phone_service
            
            # 更新状态
            self.current_call["status"] = "ended"
            self.current_call["end_time"] = time.time()
            self.current_call["duration"] = self.current_call["end_time"] - self.current_call["start_time"]
            
            result = {
                "success": True,
                "call_id": self.current_call.get("call_id"),
                "status": "ended",
                "duration": self.current_call["duration"],
                "message": "Call ended successfully"
            }
            
            # 清理状态
            self.is_busy = False
            self.current_call = None
            
            return result
            
        except Exception as e:
            logger.error(f"Error hanging up call: {e}")
            return {
                "success": False,
                "error": str(e),
                "message": "Failed to end call"
            }


# 全局服务实例
aicall_service = AICallService()


# Celery任务定义
@celery_app.task(bind=True, name="aicall_service.make_call_task")
def make_call_task(self, phone_number: str, tts_opening: str, call_config: Optional[Dict] = None) -> Dict[str, Any]:
    """
    Celery任务：拨打电话
    
    Args:
        phone_number: 要拨打的电话号码
        tts_opening: TTS开场白内容
        call_config: 通话配置参数
        
    Returns:
        Dict: 通话结果
    """
    try:
        logger.info(f"Processing make_call task {self.request.id} for {phone_number}")
        
        # 使用全局服务实例
        result = aicall_service.make_call(phone_number, tts_opening, call_config)
        
        logger.info(f"Make call task {self.request.id} completed")
        return result
        
    except Exception as e:
        logger.error(f"Error in make_call task {self.request.id}: {e}")
        raise


@celery_app.task(bind=True, name="aicall_service.get_call_status_task")
def get_call_status_task(self, call_id: str = None) -> Dict[str, Any]:
    """
    Celery任务：获取通话状态
    
    Args:
        call_id: 通话ID（可选）
        
    Returns:
        Dict: 通话状态
    """
    try:
        logger.info(f"Processing get_call_status task {self.request.id}")
        
        result = aicall_service.get_call_status(call_id)
        
        logger.info(f"Get call status task {self.request.id} completed")
        return result
        
    except Exception as e:
        logger.error(f"Error in get_call_status task {self.request.id}: {e}")
        raise


@celery_app.task(bind=True, name="aicall_service.hang_up_call_task")
def hang_up_call_task(self, call_id: str = None) -> Dict[str, Any]:
    """
    Celery任务：挂断电话
    
    Args:
        call_id: 通话ID（可选）
        
    Returns:
        Dict: 挂断结果
    """
    try:
        logger.info(f"Processing hang_up_call task {self.request.id}")
        
        result = aicall_service.hang_up_call(call_id)
        
        logger.info(f"Hang up call task {self.request.id} completed")
        return result
        
    except Exception as e:
        logger.error(f"Error in hang_up_call task {self.request.id}: {e}")
        raise


# 导出服务类和任务
__all__ = [
    "AICallService",
    "aicall_service",
    "make_call_task",
    "get_call_status_task", 
    "hang_up_call_task"
]
