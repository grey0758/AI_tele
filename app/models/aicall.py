from typing import Annotated
from pydantic import BaseModel, Field

class CallRequest(BaseModel):
    phone_number: Annotated[str, Field(
        min_length=11, 
        max_length=11, 
        pattern=r'^1[3-9]\d{9}$',
        description="手机号码"
    )]
    device_index: Annotated[int , Field(default=0, description="设备索引")]
    tts_opening: Annotated[str, Field(default="您好，我是广州大麦的小婷，我们自研了一款帮老板做生意的AI智能眼镜，您感兴趣了解一下吗？", description="TTS开场白")]
    custom_id: Annotated[str | None, Field(default=None, description="自定义ID")]
    

