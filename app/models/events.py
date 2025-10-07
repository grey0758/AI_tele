import asyncio
import uuid
from datetime import datetime
from enum import Enum
from typing import Annotated, Callable, Dict, Any, Optional

from pydantic import BaseModel, Field, ConfigDict

from app.core.config import settings


# Pydantic数据类生成规范

## 基本要求
# 1. 使用Pydantic的BaseModel作为基类
# 2. 从pydantic导入BaseModel和Field
# 3. 从typing导入必要的类型注解（Optional, List, Dict等）
# 4. 从datetime导入datetime用于时间字段

## 导入规范
# ```python
# from pydantic import BaseModel, Field
# from typing import Optional, List, Dict, Union
# from datetime import datetime

class EventType(Enum):
    # 电话事件
    CALL_OUT = "call.out"
    CALL_IN = "call.in"
    CALL_TELEPHONY = "call.telephony"
    CALL_END = "call.end"
    CALL_RING = "call.ring"
    CALL_ANSWER = "call.answer"
    CALL_MISSED = "call.missed"
    CALL_BLOCKED = "call.blocked"

    AICALL_CALL_END = "aicall.call_end"
    RECORD_CALL_END = "record.call_end"
    TTS_CALL_END = "tts.call_end"

    TTS_SEND_TEXT = "tts.send_text"
    TTS_CONNECT = "tts.connect"

    RTASR_START = "rtasr.start"
    RTASR_STOP = "rtasr.stop"
    RTASR_START_AUDIO = "rtasr.start_audio"
    RTASR_STOP_AUDIO = "rtasr.stop_audio"
    RTASR_CALL_END = "rtasr.call_end"

    RECORD_UPLOAD_RECORD = "record.upload_record"

    REDIS_ADD_DIALOG_RECORD = "redis.add_dialog_record"
    REDIS_BIND_DIALOG_RECORD_TO_CALL_RECORD = "redis.bind_dialog_record_to_call_record"

    CONVERSATION_ANSWER = "conversation.answer"

    # 自定义事件
    CUSTOM = "custom"

class EventPriority(Enum):
    LOW = 0
    NORMAL = 1
    HIGH = 2
    CRITICAL = 3

class EventStatus(Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    TIMEOUT = "timeout"
    CANCELLED = "cancelled"

class Event(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    type: Annotated[EventType, Field(description="事件类型")]
    data: Annotated[Any | None, Field(default=None,description="事件数据")]        
    event_id: Annotated[str, Field(default_factory=lambda: str(uuid.uuid4()),description="事件ID")]
    created_at: Annotated[datetime, Field(default_factory=datetime.now,description="事件创建时间")]
    priority: Annotated[EventPriority, Field(default=EventPriority.NORMAL,description="事件优先级")]
    wait_for_result: Annotated[bool, Field(default=settings.default_wait_for_result,description="是否等待结果")]
    timeout: Annotated[float, Field(default=settings.default_timeout,description="事件超时时间")]
    retry_count: Annotated[int, Field(default=0,description="重试次数")]
    max_retries: Annotated[int, Field(default=settings.max_retry_count,description="最大重试次数")]
    correlation_id: Annotated[str | None, Field(default=None,description="关联ID")]
    source: Annotated[str | None, Field(default=None,description="事件来源")]
    tags: Annotated[Dict[str, str], Field(default_factory=dict,description="事件标签")]
    
    # 内部字段
    result_future: Annotated[asyncio.Future | None, Field(default=None, exclude=True, description="结果Future")]
    status: Annotated[EventStatus, Field(default=EventStatus.PENDING,description="事件状态")]
    error_message: Annotated[str | None, Field(default=None,description="错误信息")]
    processing_start_time: Annotated[datetime | None, Field(default=None,description="事件处理开始时间")]
    processing_end_time: Annotated[datetime | None, Field(default=None,description="事件处理结束时间")]


class EventMetrics(BaseModel):
    """事件指标"""
    model_config = ConfigDict(arbitrary_types_allowed=True)
    total_events: Annotated[int, Field(default=0,description="总事件数")]   
    completed_events: Annotated[int, Field(default=0,description="完成事件数")]
    failed_events: Annotated[int, Field(default=0,description="失败事件数")]
    timeout_events: Annotated[int, Field(default=0,description="超时事件数")]
    cancelled_events: Annotated[int, Field(default=0,description="取消事件数")]
    average_processing_time: Annotated[float, Field(default=0.0,description="平均处理时间")]    
    events_per_second: Annotated[float, Field(default=0.0,description="每秒事件数")]
    queue_size: Annotated[int, Field(default=0,description="队列大小")]
    active_workers: Annotated[int, Field(default=0,description="活跃工作线程数")]
    dead_letter_queue_size: Annotated[int, Field(default=0,description="死信队列大小")]
    last_updated: Annotated[datetime, Field(default_factory=datetime.now,description="最后更新时间")]
    

class EventListener(BaseModel):
    """事件监听器包装类"""
    model_config = ConfigDict(arbitrary_types_allowed=True)
    event_type: Annotated[EventType, Field(description="监听的事件类型")]
    handler: Annotated[Callable, Field(description="处理函数")]
    priority: Annotated[EventPriority, Field(default=EventPriority.NORMAL,description="优先级")]
    max_concurrent: Annotated[int, Field(default=1,description="最大并发数")]
    timeout: Annotated[float, Field(default=5.0,description="超时时间")]
    name: Annotated[str, Field(description="名称")]
    active_count: Annotated[int, Field(default=0,description="活跃计数")]
    total_processed: Annotated[int, Field(default=0,description="处理总数")]
    total_failed: Annotated[int, Field(default=0,description="失败总数")]
    semaphore: Annotated[asyncio.Semaphore, Field(default_factory=lambda: asyncio.Semaphore(1), exclude=True, description="信号量")]
    created_at: Annotated[datetime, Field(default_factory=datetime.now,description="创建时间")]