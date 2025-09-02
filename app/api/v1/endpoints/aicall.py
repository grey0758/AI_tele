from dataclasses import dataclass
from celery import current_app
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from app.schemas.aicall import CallRequest
from app.services.aicall_service import get_aicall_service

aicall_service = get_aicall_service()


router = APIRouter()

class CallResponse(BaseModel):
    success: bool
    message: str
    task_id: str = None
    phone_number: str = None
    error: str = None


@router.post("/make_call", response_model=CallResponse)
async def make_call(request: CallRequest):
    """
    发起AI电话呼叫
    
    Args:
        request: 包含电话号码和配置的请求
        
    Returns:
        CallResponse: 呼叫结果
    """
    try:
        # 验证电话号码
        if not request.phone_number or len(request.phone_number) < 11:
            raise HTTPException(status_code=400, detail="无效的电话号码")
        
        # 检查服务是否可用
        if aicall_service.is_busy:
            return CallResponse(
                success=False,
                message="系统繁忙，请稍后再试",
                phone_number=request.phone_number,
                error="Another call is already in progress"
            )
        
        aicall_service.make_call(request)
        
        return CallResponse(
            success=True,
            message="呼叫任务已提交",
            phone_number=request.phone_number
        )
        
    except Exception as e:
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
        task_result = current_app.s
        
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
async def get_service_status():
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
async def cancel_call():
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
