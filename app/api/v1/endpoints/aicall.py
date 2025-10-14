"""AI电话相关接口"""
from fastapi import APIRouter, Depends
from app.schemas.base import ResponseBuilder
from app.utils.get_audio_devices import get_audio_devices
from app.core.dependencies import get_aicall_service
from app.schemas.aicall import (
    CallRequest,
    DeviceConfigListResponse,
)
from app.services.aicall_service import AicallService
from app.core.logger import get_logger


router = APIRouter()
logger = get_logger(__name__)


@router.post("/make_call")
async def make_call(
    request: CallRequest, aicall_service: AicallService = Depends(get_aicall_service)
):
    """
    发起AI电话呼叫

    Args:
        request: 包含电话号码和配置的请求

    Returns:
        ResponseBuilder: 呼叫结果
    """
    logger.info("make_call 接口请求:%s", request)

    await aicall_service.make_call(request)
    return ResponseBuilder.success(data=None, message="Call completed successfully")


# @router.post("/set_device_config")
# async def set_device_config(
#     config: ConfigAudioDeviceInfo,
#     redis_service: RedisService = Depends(get_redis_service),
# ):
#     """
#     设置设备音频输入输出配置

#     Args:
#         config: 音频设备配置信息

#     Returns:
#         DeviceConfigResponse: 配置结果
#     """
#     await redis_service.set_device_info_input_audio_and_output_audio(config)
#     return ResponseBuilder.success(data=None, message="设备音频配置设置成功")


# 当前设备音频设备列表查询
@router.get("/get_device_config")
async def get_device_config():
    """
    查询设备音频输入输出配置
    """

    input_devices, output_devices = get_audio_devices(deduplicate=True)
    return ResponseBuilder.success(data=DeviceConfigListResponse(
        input_devices=input_devices,
        output_devices=output_devices,
    ), message="设备音频配置查询成功")


@router.post("/start_auto_calling")
async def start_auto_calling(
    aicall_service: AicallService = Depends(get_aicall_service)
):
    """
    启动自动拨打模式
    
    从Redis队列中获取电话号码并自动拨打，每次电话结束后会自动拨打下一个
    
    Returns:
        ResponseBuilder: 启动结果
    """
    logger.info("启动自动拨打模式请求")
    
    try:
        success = await aicall_service.start_auto_calling()
        
        if success:
            return ResponseBuilder.success(
                data=None, 
                message="自动拨打模式启动成功"
            )
        else:
            return ResponseBuilder.error(
                code=409,
                message="自动拨打模式启动失败，可能有通话正在进行"
            )
            
    except Exception as e:
        logger.error("启动自动拨打模式失败: %s", e)
        return ResponseBuilder.error(
            code=500,
            message=f"启动自动拨打模式失败: {str(e)}"
        )
