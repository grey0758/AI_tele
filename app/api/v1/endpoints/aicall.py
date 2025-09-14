from typing import Annotated, List
from fastapi import APIRouter, Depends
from pydantic import BaseModel, Field
from app.utils.get_audio_devices import get_audio_devices
from app.models.aicall import CallRequest
from app.models.device_info import ConfigAudioDeviceInfo, InputDevice, OutputDevice
from app.core.dependencies import get_aicall_service, get_redis_service
from app.services.aicall_service import AicallService
from app.services.redis_service import RedisService
from app.core.logger import get_logger


router = APIRouter()
logger = get_logger(__name__)

class CallResponse(BaseModel):
    success: Annotated[bool, Field(description="是否成功")]
    message: Annotated[str | None, Field(default=None, description="消息")]
    task_id: Annotated[str | None, Field(default=None, description="任务ID")]
    phone_number: Annotated[str | None, Field(default=None, description="电话号码")]
    error: Annotated[str | None, Field(default=None, description="错误")]

class DeviceConfigResponse(BaseModel):
    success: Annotated[bool, Field(description="是否成功")]
    message: Annotated[str | None, Field(default=None, description="消息")]
    config_id: Annotated[str | None, Field(default=None, description="配置ID")]
    error: Annotated[str | None, Field(default=None, description="错误")]

class DeviceConfigListResponse(BaseModel):
    success: Annotated[bool, Field(description="是否成功")]
    message: Annotated[str | None, Field(default=None, description="消息")]
    error: Annotated[str | None, Field(default=None, exclude=True, description="错误")]
    input_devices: Annotated[List[InputDevice], Field(default_factory=list, description="输入设备列表")]
    output_devices: Annotated[List[OutputDevice], Field(default_factory=list, description="输出设备列表")]

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
        if aicall_service.call_finished:
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

@router.post("/set_device_config", response_model=DeviceConfigResponse)
async def set_device_config(
    config: ConfigAudioDeviceInfo, 
    redis_service: RedisService = Depends(get_redis_service)
):
    """
    设置设备音频输入输出配置
    
    Args:
        config: 音频设备配置信息
        
    Returns:
        DeviceConfigResponse: 配置结果
    """
    try:
        success = await redis_service.set_device_info_input_audio_and_output_audio(config)
        
        if success:
            return DeviceConfigResponse(
                success=True,
                message="设备音频配置设置成功",
                config_id=config.id
            )
        else:
            return DeviceConfigResponse(
                success=False,
                message="设备音频配置设置失败",
                config_id=config.id,
                error="配置保存失败"
            )
        
    except Exception as e:
        logger.error(f"set_device_config 接口异常: {e}", exc_info=True)
        return DeviceConfigResponse(
            success=False,
            message=f"设备音频配置设置失败: {str(e)}",
            config_id=config.id,
            error=str(e)
        )

#当前设备音频设备列表查询
@router.get("/get_device_config", response_model=DeviceConfigListResponse)
async def get_device_config(
):
    """
    查询设备音频输入输出配置
    """
    try:
        # 查询当前设备音频设备列表
        input_devices, output_devices = get_audio_devices(deduplicate=True)
        return DeviceConfigListResponse(
            success=True,
            message="设备音频配置查询成功",
            input_devices=input_devices,
            output_devices=output_devices
        )
    except Exception as e:
        logger.error(f"get_device_config 接口异常: {e}", exc_info=True)
        return DeviceConfigListResponse(
            success=False,
            message=f"设备音频配置查询失败: {str(e)}",
            error=str(e),
            input_devices=None,
            output_devices=None,    
        )