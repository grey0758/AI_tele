# app/api/phone.py
import asyncio
import json
from fastapi import APIRouter, HTTPException, Depends, BackgroundTasks
from typing import List, Optional
from pydantic import BaseModel
from app.services.phone_service import PhoneService

router = APIRouter()

# 全局电话服务实例
phone_service = None

def get_phone_service() -> PhoneService:
    """获取电话服务实例"""
    global phone_service
    if phone_service is None:
        phone_service = PhoneService()
    return phone_service

class PhoneNumberRequest(BaseModel):
    phone_number: str
    device_instance: Optional[int] = None

class PhoneResponse(BaseModel):
    success: bool
    message: str
    data: Optional[dict] = None

@router.post("/dial", response_model=PhoneResponse)
async def dial_phone(
    request: PhoneNumberRequest,
    background_tasks: BackgroundTasks,
    phone_service: PhoneService = Depends(get_phone_service)
):
    """拨打电话"""
    
    # 启动电话服务（如果尚未启动）
    background_tasks.add_task(phone_service.start_async)
    
    # 等待连接建立（简单等待，实际应用中可能需要更复杂的逻辑）
    import asyncio
    await asyncio.sleep(1)
    
    state = await phone_service.get_connection_state()
    if not state['is_connected']:
        raise HTTPException(status_code=503, detail="Phone system not connected")
    
    try:
        # 构建拨号消息
        message = {
            "method": "call",
            "instance": request.device_instance or 0,
            "phone": request.phone_number
        }
        
        success = await phone_service.send_message(message)
        
        if success:
            return PhoneResponse(
                success=True,
                message=f"Dialing {request.phone_number}",
                data={
                    "phone_number": request.phone_number,
                    "connection_id": phone_service.connection_id
                }
            )
        else:
            raise HTTPException(status_code=500, detail="Failed to send dial command")
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error dialing phone: {str(e)}")

@router.get("/status")
async def get_phone_status(
    phone_service: PhoneService = Depends(get_phone_service)
):
    """获取电话系统状态"""
    
    # 获取所有设备
    devices = await phone_service.get_all_devices()
    
    # 获取连接状态
    connection_state = await phone_service.get_connection_state()
    
    return {
        "devices": devices,
        "connection_state": connection_state,
        "total_devices": len(devices),
        "is_connected": connection_state.get('is_connected', False)
    }

@router.get("/devices")
async def get_devices(
    phone_service: PhoneService = Depends(get_phone_service)
):
    """获取所有设备列表"""
    devices = await phone_service.get_all_devices()
    return {
        "devices": devices,
        "total_count": len(devices)
    }

@router.get("/calls")
async def get_calls(
    phone_service: PhoneService = Depends(get_phone_service)
):
    """获取所有通话状态"""
    # 这里需要添加获取通话状态的方法
    return {
        "message": "Call status endpoint - to be implemented",
        "connection_id": phone_service.connection_id
    }

@router.post("/stop")
async def stop_phone_service(
    phone_service: PhoneService = Depends(get_phone_service)
):
    """停止电话服务"""
    try:
        await phone_service.stop()
        return {
            "success": True,
            "message": "Phone service stopped successfully"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error stopping phone service: {str(e)}")

@router.post("/restart")
async def restart_phone_service(
    phone_service: PhoneService = Depends(get_phone_service)
):
    """重启电话服务"""
    try:
        await phone_service.stop()
        await asyncio.sleep(1)
        await phone_service.start_async()
        return {
            "success": True,
            "message": "Phone service restarted successfully"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error restarting phone service: {str(e)}")
