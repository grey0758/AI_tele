from dataclasses import dataclass
from typing import Annotated
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
