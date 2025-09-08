from dataclasses import dataclass
from typing import Annotated
from celery import current_app
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel, Field
from app.schemas.aicall import CallRequest
from app.core.dependencies import get_aicall_service
from app.services.aicall_service import AicallService
from app.core.logger import get_logger


router = APIRouter()
logger = get_logger(__name__)

class CallResponse(BaseModel):
    success: Annotated[bool, Field(description="是否成功")]
    message: Annotated[str | None, Field(default=None, description="消息")]
    task_id: Annotated[str | None, Field(default=None, description="任务ID")]
    phone_number: Annotated[str | None, Field(default=None, description="电话号码")]
    error: Annotated[str | None, Field(default=None, description="错误")]

@router.post("/make_call", response_model=CallResponse)
async def make_call(request: CallRequest, aicall_service: AicallService = Depends(get_aicall_service)):
    """
    发起AI电话呼叫
    
    Args:
        request: 包含电话号码和配置的请求
        
    Returns:
        CallResponse: 呼叫结果
    """
    try:
        # 检查服务是否可用
        if aicall_service.is_busy:
            return CallResponse(
                success=False,
                message="系统繁忙，请稍后再试",
                phone_number=request.phone_number,
                error="Another call is already in progress"
            )
        
        await aicall_service.make_call(request)
        
        return CallResponse(
            success=True,
            message="呼叫任务已提交",
            phone_number=request.phone_number
        )
        
    except Exception as e:
        logger.error(f"make_call 接口异常: {e}", exc_info=True)
        return CallResponse(
            success=False,
            message=f"发起呼叫失败: {str(e)}",
            phone_number=request.phone_number,
            error=str(e)
        )


@router.get("/call_status/{task_id}")
async def get_call_status(task_id: str):
    """
    获取呼叫任务状态
    
    Args:
        task_id: Celery 任务ID
        
    Returns:
        Dict: 任务状态信息
    """
    try:
        task_result = current_app.AsyncResult(task_id)
        
        if task_result.ready():
            if task_result.successful():
                result = task_result.result
                return {
                    "task_id": task_id,
                    "status": "completed",
                    "result": result
                }
            else:
                return {
                    "task_id": task_id,
                    "status": "failed",
                    "error": str(task_result.result)
                }
        else:
            return {
                "task_id": task_id,
                "status": "pending",
                "info": task_result.info
            }
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取任务状态失败: {str(e)}")


@router.get("/service_status")
async def get_service_status(aicall_service: AicallService = Depends(get_aicall_service)):
    """
    获取AI呼叫服务状态
    
    Returns:
        Dict: 服务状态信息
    """
    try:
        return {
            "service_running": True,
            "current_call": aicall_service.current_call,
            "is_busy": aicall_service.is_busy,
            "message": "AI呼叫服务运行正常"
        }
    except Exception as e:
        return {
            "service_running": False,
            "error": str(e),
            "message": "获取服务状态失败"
        }


@router.post("/cancel_call")
async def cancel_call(aicall_service: AicallService = Depends(get_aicall_service)):
    """
    取消当前呼叫（如果正在进行）
    
    Returns:
        Dict: 取消结果
    """
    try:
        if not aicall_service.is_busy:
            return {
                "success": False,
                "message": "当前没有正在进行的呼叫"
            }
        
        # 重置服务状态
        aicall_service.is_busy = False
        aicall_service.current_call = None
        
        return {
            "success": True,
            "message": "呼叫已取消"
        }
        
    except Exception as e:
        return {
            "success": False,
            "message": f"取消呼叫失败: {str(e)}"
        }
