"""录音服务相关数据类"""
from typing import Annotated
from pydantic import BaseModel, Field, ConfigDict

class FileUploadRequest(BaseModel):
    """文件上传请求数据类"""
    file: Annotated[bytes, Field(description="文件")]
    file_uuid: Annotated[str, Field(min_length=36, max_length=36, description="文件唯一标识UUID")]
    description: Annotated[str | None, Field(default=None, description="文件描述")]

    model_config = ConfigDict(arbitrary_types_allowed=True)

class FileUploadResponse(BaseModel):
    """文件上传响应数据类"""
    file_url: Annotated[str, Field(description="文件URL")]
