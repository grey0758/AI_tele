from pydantic import BaseModel, Field
from typing import Optional, Dict, Any
from datetime import datetime
from enum import Enum


class TTSStatus(str, Enum):
    """TTS播放状态枚举"""
    PENDING = "pending"      # 等待播放
    PLAYING = "playing"      # 正在播放
    PAUSED = "paused"       # 暂停播放
    COMPLETED = "completed"  # 播放完成
    ERROR = "error"          # 播放错误
    STOPPED = "stopped"      # 停止播放


class TTSRequest(BaseModel):
    """TTS请求模型"""
    text: str = Field(..., description="要合成的文本")
    session_id: Optional[str] = Field(None, description="会话ID，不提供则自动生成")
    user_id: Optional[str] = Field(default="default", description="用户ID，默认为default")
    priority: int = Field(default=1, description="播放优先级，数字越小优先级越高")
    voice_config: Optional[Dict[str, Any]] = Field(default=None, description="语音配置参数")


class TTSState(BaseModel):
    """TTS播放状态模型"""
    session_id: str = Field(..., description="会话ID")
    user_id: Optional[str] = Field(None, description="用户ID")
    status: TTSStatus = Field(..., description="播放状态")
    text: str = Field(..., description="要播放的文本")
    current_position: int = Field(default=0, description="当前播放位置")
    total_length: int = Field(default=0, description="总长度")
    start_time: Optional[datetime] = Field(None, description="开始时间")
    end_time: Optional[datetime] = Field(None, description="结束时间")
    error_message: Optional[str] = Field(None, description="错误信息")
    
    # 分布式扩展字段
    node_id: str = Field(..., description="节点ID")
    instance_id: str = Field(..., description="实例ID")
    created_at: datetime = Field(default_factory=datetime.now, description="创建时间")
    updated_at: datetime = Field(default_factory=datetime.now, description="更新时间")
    ttl: int = Field(default=3600, description="过期时间(秒)")
    
    # 播放控制字段
    volume: float = Field(default=1.0, description="音量")
    speed: float = Field(default=1.0, description="播放速度")
    is_loop: bool = Field(default=False, description="是否循环播放")
    
    # 队列管理字段
    queue_position: Optional[int] = Field(None, description="队列位置")
    priority: int = Field(default=1, description="优先级")
    
    # 统计字段
    play_count: int = Field(default=0, description="播放次数")
    total_play_time: float = Field(default=0.0, description="总播放时间")


class TTSControl(BaseModel):
    """TTS控制命令模型"""
    session_id: str = Field(..., description="会话ID")
    action: str = Field(..., description="控制动作: play, pause, stop, resume, seek")
    params: Optional[Dict[str, Any]] = Field(default=None, description="控制参数")
    timestamp: datetime = Field(default_factory=datetime.now, description="时间戳")


class TTSResponse(BaseModel):
    """TTS响应模型"""
    success: bool = Field(..., description="是否成功")
    message: str = Field(..., description="响应消息")
    data: Optional[Dict[str, Any]] = Field(default=None, description="响应数据")
    session_id: Optional[str] = Field(default=None, description="会话ID")
    timestamp: datetime = Field(default_factory=datetime.now, description="响应时间")
