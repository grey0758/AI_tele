# app/services/aicall_service.py
import logging
import uuid
import time
import asyncio
from contextlib import asynccontextmanager
from typing import Dict, Optional, Any
from app.schemas.aicall import CallRequest
from celery import current_app
from fastapi import FastAPI

# 使用主应用的logger
from app.core.logger import get_logger

# 获取模块级别的logger
logger = get_logger(__name__)


class AICallService:
    """AI电话服务类 - 处理实际的电话拨打逻辑"""
    
    def __init__(self):
        self.current_call = None  # 当前通话状态
        self.is_busy = False      # 是否正在通话中
        logger.info(f"AI Call Service initialized")
    
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
                return {
                    "success": False,
                    "phone_number": call_request.phone_number,
                    "error": "Another call is already in progress",
                    "message": "系统繁忙，请稍后再试"
                }
            
            # 设置忙碌状态
            self.is_busy = True
            
            logger.info(f"Starting call to {call_request.phone_number} with TTS opening...")
            
            # 生成通话信息
            call_info = {
                "phone_number": call_request.phone_number,
                "status": "initiated",
                "start_time": time.time(),
            }
            
            self.current_call = call_info

            logger.info(f"Call initiated to {call_request.phone_number}")
            
            # 异步更新通话状态为已发起
            # current_app.send_task('app.tasks.aicall_tasks.update_call_status_task', args=[call_request.phone_number, "initiated"])
            
            # 发送电话拨打任务
            current_app.send_task('app.tasks.aicall_tasks.make_call_task', args=[call_request.model_dump()])
            
            result = {
                "success": True,
                "phone_number": call_request.phone_number,
                "status": "completed",
                "message": "Call completed successfully"
            }
            
            logger.info(f"Call completed successfully")
            return result
            
        except Exception as e:
            logger.error(f"Error making call to {call_request.phone_number}: {e}")
            # 异步更新通话状态为失败
            current_app.send_task('app.tasks.aicall_tasks.update_call_status_task', args=[call_request.phone_number, "failed", {"error": str(e)}])
            
            result = {
                "success": False,
                "phone_number": call_request.phone_number,
                "error": str(e),
                "message": "Failed to complete call"
            }
            return result
        finally:
            # 清理状态
            self.is_busy = False
            self.current_call = None

    def update_call_status(self, phone_number: str, status: str, extra_data: Optional[Dict] = None):
        """
        更新通话状态的内部方法
        
        Args:
            phone_number: 电话号码
            status: 新状态
            extra_data: 额外数据
        """
        try:
            if self.current_call and self.current_call["phone_number"] == phone_number:
                self.current_call["status"] = status
                if extra_data:
                    self.current_call.update(extra_data)
                
                logger.info(f"Call status updated for {phone_number}: {status}")
                
                # 这里可以添加状态持久化逻辑，比如保存到数据库
                # await self.save_call_status_to_db(self.current_call)
                
        except Exception as e:
            logger.error(f"Error updating call status for {phone_number}: {e}")

# 全局服务实例
aicall_service = AICallService()

def get_aicall_service() -> AICallService:
    return aicall_service           

@asynccontextmanager
async def lifespan( app: FastAPI):
    yield
    aicall_service.shutdown()
