# app/models/device_info.py
"""设备信息数据类"""
import uuid
from typing import Annotated, List
from pydantic import BaseModel, Field

class InputDevice(BaseModel):
    """输入设备信息数据类"""
    index: Annotated[int, Field(description="设备索引")]
    name: Annotated[str | None, Field(default=None, description="设备名称")]
    maxInputChannels: Annotated[int | None, Field(default=None, description="最大输入声道数")]
    maxOutputChannels: Annotated[int | None, Field(default=None, description="最大输出声道数")]
    defaultSampleRate: Annotated[float | None, Field(default=None, description="默认采样率")]
    deviceType: Annotated[str | None, Field(default=None, description="设备类型")]

class OutputDevice(BaseModel):
    """输出设备信息数据类"""
    index: Annotated[int, Field( description="设备索引")]
    name: Annotated[str | None, Field(default=None, description="设备名称")]
    maxInputChannels: Annotated[int | None, Field(default=None, description="最大输入声道数")]
    maxOutputChannels: Annotated[int | None, Field(default=None, description="最大输出声道数")]
    defaultSampleRate: Annotated[float | None, Field(default=None, description="默认采样率")]
    deviceType: Annotated[str | None, Field(default=None, description="设备类型")]


class Device(BaseModel):
    """设备信息数据类 单个设备信息"""    
    id: Annotated[int | None, Field(default=None, description="设备ID")]
    instance: Annotated[int, Field(description="实例ID")]
    model: Annotated[int | None, Field(default=None, description="设备型号")]
    btConnect: Annotated[int | None, Field(default=None, description="蓝牙连接状态")]
    btDeviceName: Annotated[str | None, Field(default=None, description="蓝牙设备名称")]
    phoneName: Annotated[str | None, Field(default=None, description="手机名称")]
    deviceId: Annotated[str | None, Field(default=None, description="设备唯一标识")]
    userId: Annotated[str | None, Field(default=None, description="用户ID")]
    firmWareVer: Annotated[str | None, Field(default=None, description="固件版本")]
    valid: Annotated[int | None, Field(default=None, description="设备有效性")]
    error: Annotated[str | None, Field(default=None, description="错误信息")]
    input_devices: Annotated[InputDevice | None, Field(default=None, description="输入设备列表")]
    output_devices: Annotated[OutputDevice | None, Field(default=None, description="输出设备列表")]

class DeviceInfo(BaseModel):
    """设备信息数据类 设备列表"""
    notify: Annotated[str | None, Field(default=None, description="通知类型")]
    devid: Annotated[str | None, Field(default=None, description="设备ID")]
    version: Annotated[str | None, Field(default=None, description="版本号")]
    recordmode: Annotated[int | None, Field(default=None, description="录音模式")]
    devices: Annotated[List[Device], Field(default_factory=list, description="设备列表")]


class ConfigAudioDeviceInfo(BaseModel):
    """配置音频设备信息数据类"""
    id: Annotated[str, Field(default_factory=lambda: str(uuid.uuid4()), description="UUID")]
    device: Annotated[List[Device], Field(default_factory=list, description="设备")]
