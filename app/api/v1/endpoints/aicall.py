# app/api/v1/endpoints/aicall.py
from fastapi import APIRouter, HTTPException, BackgroundTasks
from pydantic import BaseModel
from typing import Optional, Dict, Any
import logging

from app.services.aicall_service import aicall_service, make_call_task, get_call_status_task, hang_up_call_task

logger = logging.getLogger(__name__)

router = APIRouter()


class MakeCallRequest(BaseModel):
    """拨打电话请求模型"""
    phone_number: str
    tts_opening: str
    call_config: Optional[Dict[str, Any]] = None


class CallResponse(BaseModel):
    """通话响应模型"""
    success: bool
    call_id: Optional[str] = None
    phone_number: str
    status: Optional[str] = None
    duration: Optional[float] = None
    message: str
    error: Optional[str] = None


class CallStatusResponse(BaseModel):
    """通话状态响应模型"""
    call_id: Optional[str] = None
    status: str
    phone_number: Optional[str] = None
    duration: Optional[float] = None
    is_busy: bool
    message: str
    error: Optional[str] = None


@router.post("/make-call", response_model=CallResponse, summary="拨打电话")
async def make_call(request: MakeCallRequest, background_tasks: BackgroundTasks):
    """
    拨打电话端点
    
    - **phone_number**: 要拨打的电话号码
    - **tts_opening**: TTS开场白内容
    - **call_config**: 通话配置参数（可选）
    
    返回通话结果信息
    """
    try:
        logger.info(f"Received make call request for {request.phone_number}")
        
        # 检查服务是否忙碌
        if aicall_service.is_busy:
            raise HTTPException(
                status_code=409,
                detail="系统繁忙，已有通话在进行中"
            )
        
        # 启动后台任务进行拨号
        task = make_call_task.delay(
            request.phone_number,
            request.tts_opening,
            request.call_config
        )
        
        # 等待任务完成（阻塞调用）
        result = task.get()
        
        logger.info(f"Call completed: {result}")
        
        return CallResponse(
            success=result.get("success", False),
            call_id=result.get("call_id"),
            phone_number=result.get("phone_number", request.phone_number),
            status=result.get("status"),
            duration=result.get("duration"),
            message=result.get("message", ""),
            error=result.get("error")
        )
        
    except Exception as e:
        logger.error(f"Error in make_call endpoint: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"拨打电话失败: {str(e)}"
        )


@router.get("/call-status", response_model=CallStatusResponse, summary="获取通话状态")
async def get_call_status(call_id: Optional[str] = None):
    """
    获取通话状态端点
    
    - **call_id**: 通话ID（可选，如果不提供则返回当前通话状态）
    
    返回通话状态信息
    """
    try:
        logger.info(f"Getting call status for call_id: {call_id}")
        
        # 启动后台任务获取状态
        task = get_call_status_task.delay(call_id)
        result = task.get()
        
        logger.info(f"Call status retrieved: {result}")
        
        return CallStatusResponse(
            call_id=result.get("call_id"),
            status=result.get("status", "unknown"),
            phone_number=result.get("phone_number"),
            duration=result.get("duration"),
            is_busy=result.get("is_busy", False),
            message=result.get("message", ""),
            error=result.get("error")
        )
        
    except Exception as e:
        logger.error(f"Error in get_call_status endpoint: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"获取通话状态失败: {str(e)}"
        )


@router.post("/hang-up", response_model=CallResponse, summary="挂断电话")
async def hang_up_call(call_id: Optional[str] = None):
    """
    挂断电话端点
    
    - **call_id**: 通话ID（可选）
    
    返回挂断结果
    """
    try:
        logger.info(f"Hanging up call with call_id: {call_id}")
        
        # 启动后台任务进行挂断
        task = hang_up_call_task.delay(call_id)
        result = task.get()
        
        logger.info(f"Call hang up completed: {result}")
        
        return CallResponse(
            success=result.get("success", False),
            call_id=result.get("call_id"),
            phone_number=result.get("phone_number", ""),
            status=result.get("status"),
            duration=result.get("duration"),
            message=result.get("message", ""),
            error=result.get("error")
        )
        
    except Exception as e:
        logger.error(f"Error in hang_up_call endpoint: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"挂断电话失败: {str(e)}"
        )


@router.get("/service-status", summary="获取服务状态")
async def get_service_status():
    """
    获取AI电话服务状态端点
    
    返回服务当前状态信息
    """
    try:
        status = {
            "service_id": aicall_service.service_id,
            "is_busy": aicall_service.is_busy,
            "current_call": aicall_service.current_call,
            "status": "running"
        }
        
        return status
        
    except Exception as e:
        logger.error(f"Error in get_service_status endpoint: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"获取服务状态失败: {str(e)}"
        )
