"""AI电话相关数据类"""
from typing import Annotated

from pydantic import BaseModel, Field

from app.models.device_info import InputDevice, OutputDevice


class CallRequest(BaseModel):
    """呼叫请求数据类"""
    phone_number: Annotated[str, Field(
        min_length=11,
        max_length=11,
        pattern=r'^1[3-9]\d{9}$',
        description="手机号码"
    )]
    device_index: Annotated[int , Field(default=0, description="设备索引")]
    tts_opening: Annotated[str, Field(default="您好，我是广州大麦的月月，我们自研了一款帮老板做生意的AI智能眼镜，您感兴趣了解一下吗？", description="TTS开场白")]
    custom_id: Annotated[str | None, Field(default=None, description="自定义ID")]

class CallResponse(BaseModel):
    """呼叫响应数据类"""
    success: Annotated[bool, Field(description="是否成功")]
    message: Annotated[str | None, Field(default=None, description="消息")]
    task_id: Annotated[str | None, Field(default=None, description="任务ID")]
    phone_number: Annotated[str | None, Field(default=None, description="电话号码")]
    error: Annotated[str | None, Field(default=None, description="错误")]

class DeviceConfigResponse(BaseModel):
    """设备配置响应数据类"""
    success: Annotated[bool, Field(description="是否成功")]
    message: Annotated[str | None, Field(default=None, description="消息")]
    config_id: Annotated[str | None, Field(default=None, description="配置ID")]
    error: Annotated[str | None, Field(default=None, description="错误")]

class DeviceConfigListResponse(BaseModel):
    """设备配置列表响应数据类"""
    input_devices: Annotated[list[InputDevice], Field(default_factory=list, description="输入设备列表")]
    output_devices: Annotated[list[OutputDevice], Field(default_factory=list, description="输出设备列表")]
