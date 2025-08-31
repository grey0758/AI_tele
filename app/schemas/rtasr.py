from pydantic import BaseModel, Field
from typing import Optional, Dict, Any
from datetime import datetime


class RTASRRequest(BaseModel):
    """RTASR请求模型"""
    session_id: Optional[str] = Field(None, description="会话ID，如果不提供则自动生成")
    user_id: str = Field(default="default", description="用户ID")
    debounce_delay: float = Field(default=1.0, description="防抖延迟时间（秒）")


class RTASRResponse(BaseModel):
    """RTASR响应模型"""
    code: int = Field(200, description="响应状态码")
    message: str = Field(description="响应消息")
    data: Dict[str, Any] = Field(description="响应数据")
    timestamp: datetime = Field(default_factory=datetime.now, description="响应时间戳")


class RTASRStatus(BaseModel):
    """RTASR状态模型"""
    session_id: str = Field(description="会话ID")
    user_id: str = Field(description="用户ID")
    status: str = Field(description="状态")
    timestamp: datetime = Field(description="时间戳")
    error_message: Optional[str] = Field(None, description="错误信息")


class RTASRTextResult(BaseModel):
    """RTASR文本识别结果"""
    type: str = Field(description="消息类型")
    data: str = Field(description="识别文本")
    timestamp: datetime = Field(description="时间戳")


class RTASRSentenceResult(BaseModel):
    """RTASR句子识别结果"""
    type: str = Field(description="消息类型")
    data: str = Field(description="识别句子")
    timestamp: datetime = Field(description="时间戳")


class RTASRWebSocketMessage(BaseModel):
    """RTASR WebSocket消息模型"""
    action: str = Field(description="操作类型")
    data: Optional[Dict[str, Any]] = Field(None, description="消息数据")


class RTASRWebSocketResponse(BaseModel):
    """RTASR WebSocket响应模型"""
    type: str = Field(description="响应类型")
    session_id: Optional[str] = Field(None, description="会话ID")
    data: Optional[str] = Field(None, description="响应数据")
    error: Optional[str] = Field(None, description="错误信息")
    timestamp: datetime = Field(default_factory=datetime.now, description="时间戳")
