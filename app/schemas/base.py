"""
通用响应数据结构
"""

from typing import TypeVar, Generic, Any, List, Annotated
from pydantic import BaseModel, Field, ConfigDict

# 定义泛型类型变量
T = TypeVar("T")


class ResponseData(BaseModel, Generic[T]):
    """通用响应数据结构"""

    code: Annotated[int, Field(description="响应状态码，200或0表示成功")]
    data: Annotated[T | None, Field(description="响应数据")]
    message: Annotated[str, Field(description="响应消息")]
    model_config = ConfigDict(arbitrary_types_allowed=True)


# 分页信息模型
class PaginationInfo(BaseModel):
    """分页信息"""

    page: Annotated[int, Field(description="当前页码")]
    page_size: Annotated[int, Field(description="每页数量")]
    total: Annotated[int, Field(description="总记录数")]
    total_pages: Annotated[int, Field(description="总页数")]


class PaginatedData(BaseModel, Generic[T]):
    """分页数据"""

    items: Annotated[List[T], Field(description="数据列表")]
    pagination: Annotated[PaginationInfo, Field(description="分页信息")]


# 分页响应类型
PaginatedResponse = ResponseData[PaginatedData[T]]


# 状态码常量
class ResponseCode:
    """响应状态码常量"""

    SUCCESS = 200
    CREATED = 201
    BAD_REQUEST = 400
    UNAUTHORIZED = 401
    FORBIDDEN = 403
    NOT_FOUND = 404
    INTERNAL_ERROR = 500

    # 业务状态码
    BUSINESS_ERROR = 1001
    VALIDATION_ERROR = 1002
    DATA_NOT_FOUND = 1003


class ResponseBuilder:
    """响应构造器"""

    @staticmethod
    def success(data: T, message: str = "操作成功", code: int = 0) -> ResponseData[T]:
        """构造成功响应"""
        return ResponseData[T](code=code, data=data, message=message)

    @staticmethod
    def error(message: str, code: int = 500, data: Any = None) -> ResponseData[Any]:
        """构造错误响应"""
        return ResponseData[Any](code=code, data=data, message=message)

    @staticmethod
    def not_found(message: str = "资源不存在") -> ResponseData[None]:
        """构造404响应"""
        return ResponseData[None](code=404, data=None, message=message)

    @staticmethod
    def unauthorized(message: str = "身份验证失败") -> ResponseData[None]:
        """构造401响应"""
        return ResponseData[None](code=401, data=None, message=message)

    @staticmethod
    def forbidden(message: str = "没有操作权限") -> ResponseData[None]:
        """构造403响应"""
        return ResponseData[None](code=403, data=None, message=message)

    @staticmethod
    def bad_request(message: str = "请求参数错误") -> ResponseData[None]:
        """构造400响应"""
        return ResponseData[None](code=400, data=None, message=message)

    @staticmethod
    def internal_error(message: str = "服务器内部错误") -> ResponseData[None]:
        """构造500响应"""
        return ResponseData[None](code=500, data=None, message=message)

    @staticmethod
    def paginated(
        items: List[T],
        page: int,
        page_size: int,
        total: int,
        message: str = "获取数据成功",
    ) -> ResponseData[PaginatedData[T]]:
        """构造分页响应"""
        total_pages = (total + page_size - 1) // page_size
        pagination_info = PaginationInfo(
            page=page, page_size=page_size, total=total, total_pages=total_pages
        )
        paginated_data = PaginatedData[T](items=items, pagination=pagination_info)
        return ResponseData[PaginatedData[T]](
            code=200, data=paginated_data, message=message
        )


# 向后兼容的别名
BaseResponse = ResponseData
