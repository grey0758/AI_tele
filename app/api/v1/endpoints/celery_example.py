from fastapi import APIRouter, HTTPException, BackgroundTasks
from pydantic import BaseModel
from typing import Dict, Any, Optional
from app.services.celery_service import celery_app
from app.services.phone_service import process_call, send_sms, record_call
import logging

logger = logging.getLogger(__name__)
router = APIRouter()


class CallRequest(BaseModel):
    call_id: str
    phone_number: str
    user_id: str
    metadata: Optional[Dict[str, Any]] = None


class SMSRequest(BaseModel):
    phone_number: str
    message: str


class RecordingRequest(BaseModel):
    call_id: str
    audio_data: str  # Base64 encoded audio data


@router.post("/process-call")
async def submit_call_task(call_request: CallRequest):
    """提交电话处理任务"""
    try:
        # 提交异步任务
        task = process_call.delay({
            "call_id": call_request.call_id,
            "phone_number": call_request.phone_number,
            "user_id": call_request.user_id,
            "metadata": call_request.metadata or {}
        })
        
        return {
            "task_id": task.id,
            "status": "submitted",
            "message": "Call processing task submitted successfully"
        }
    except Exception as e:
        logger.error(f"Error submitting call task: {e}")
        raise HTTPException(status_code=500, detail="Failed to submit call task")


@router.post("/send-sms")
async def submit_sms_task(sms_request: SMSRequest):
    """提交短信发送任务"""
    try:
        # 提交异步任务
        task = send_sms.delay(sms_request.phone_number, sms_request.message)
        
        return {
            "task_id": task.id,
            "status": "submitted",
            "message": "SMS task submitted successfully"
        }
    except Exception as e:
        logger.error(f"Error submitting SMS task: {e}")
        raise HTTPException(status_code=500, detail="Failed to submit SMS task")


@router.post("/record-call")
async def submit_recording_task(recording_request: RecordingRequest):
    """提交通话录制任务"""
    try:
        # 将 Base64 音频数据转换为字节
        import base64
        audio_bytes = base64.b64decode(recording_request.audio_data)
        
        # 提交异步任务
        task = record_call.delay(recording_request.call_id, audio_bytes)
        
        return {
            "task_id": task.id,
            "status": "submitted",
            "message": "Call recording task submitted successfully"
        }
    except Exception as e:
        logger.error(f"Error submitting recording task: {e}")
        raise HTTPException(status_code=500, detail="Failed to submit recording task")


@router.get("/task-status/{task_id}")
async def get_task_status(task_id: str):
    """获取任务状态"""
    try:
        task_result = celery_app.AsyncResult(task_id)
        
        response = {
            "task_id": task_id,
            "status": task_result.status,
        }
        
        if task_result.ready():
            if task_result.successful():
                response["result"] = task_result.result
            else:
                response["error"] = str(task_result.info)
        
        return response
    except Exception as e:
        logger.error(f"Error getting task status: {e}")
        raise HTTPException(status_code=500, detail="Failed to get task status")


@router.get("/celery-stats")
async def get_celery_stats():
    """获取 Celery 统计信息"""
    try:
        # 获取 Celery 检查器
        inspector = celery_app.control.inspect()
        
        stats = {
            "active_tasks": inspector.active(),
            "registered_tasks": inspector.registered(),
            "stats": inspector.stats(),
        }
        
        return stats
    except Exception as e:
        logger.error(f"Error getting Celery stats: {e}")
        raise HTTPException(status_code=500, detail="Failed to get Celery stats")
