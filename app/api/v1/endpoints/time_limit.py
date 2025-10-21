"""时间限制管理API端点"""
from fastapi import APIRouter, HTTPException, status, Depends
from app.core.dependencies import get_phone_service
from app.services.phone_service import PhoneService
from app.models.events import EventType
from app.schemas.base import ResponseBuilder
from app.core.logger import get_logger

logger = get_logger(__name__)
router = APIRouter()


@router.post("/activate-wait")
async def activate_time_limit_wait(
    phone_service: PhoneService = Depends(get_phone_service)
):
    """
    激活时间限制等待
    
    当系统因为时间限制（超过晚上9点）而等待时，
    可以通过此接口手动激活等待，继续拨号流程。
    
    Returns:
        激活结果
    """
    try:
        # 通过事件总线发送激活事件
        await phone_service.emit_event(
            EventType.PHONE_SERVICE_ACTIVATE_TIME_LIMIT_WAIT,
            {"message": "手动激活时间限制等待"}
        )
        
        logger.info("时间限制等待激活事件已发送")
        
        return ResponseBuilder.success(
            data={"activated": True},
            message="时间限制等待已激活，拨号流程将继续"
        )
        
    except Exception as e:
        logger.error("激活时间限制等待失败: %s", e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"激活时间限制等待失败: {str(e)}"
        )


@router.get("/status")
async def get_time_limit_status(
    phone_service: PhoneService = Depends(get_phone_service)
):
    """
    获取时间限制状态
    
    Returns:
        当前时间限制状态
    """
    try:
        from datetime import datetime
        current_time = datetime.now()
        is_waiting = current_time.hour >= 21
        
        return ResponseBuilder.success(
            data={
                "current_time": current_time.strftime("%H:%M:%S"),
                "is_waiting": is_waiting,
                "wait_event_set": phone_service.time_limit_wait_event.is_set(),
                "time_limit_hour": 21
            },
            message="时间限制状态获取成功"
        )
        
    except Exception as e:
        logger.error("获取时间限制状态失败: %s", e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"获取时间限制状态失败: {str(e)}"
        )


@router.post("/reset-wait")
async def reset_time_limit_wait(
    phone_service: PhoneService = Depends(get_phone_service)
):
    """
    重置时间限制等待状态
    
    清除等待事件，用于测试或重置状态。
    
    Returns:
        重置结果
    """
    try:
        # 重置等待事件
        phone_service.time_limit_wait_event.clear()
        
        logger.info("时间限制等待事件已重置")
        
        return ResponseBuilder.success(
            data={"reset": True},
            message="时间限制等待状态已重置"
        )
        
    except Exception as e:
        logger.error("重置时间限制等待失败: %s", e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"重置时间限制等待失败: {str(e)}"
        )
