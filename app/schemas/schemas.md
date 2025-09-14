"""
Pydantic数据类生成规范 (v2)

基本要求:
1. 使用Pydantic的BaseModel作为基类
2. 按需导入模块，只导入实际使用的内容
3. 所有字段必须使用统一的定义格式
4. 类名使用PascalCase，字段名使用snake_case
5. 优先使用Annotated语法和现代Python类型注解

导入规范:
- from pydantic import BaseModel, Field
- from typing import Annotated, List, Dict  # 根据需要选择导入
- from datetime import datetime  # 仅在有时间字段时导入
- 不再使用Optional，改用 | None 语法

字段定义格式:
- 必填字段: field_name: Annotated[FieldType, Field(description="字段描述")]
- 可选字段(可为None): field_name: Annotated[FieldType | None, Field(default=None, description="字段描述")]
- 可选字段(有默认值): field_name: Annotated[FieldType, Field(default=默认值, description="字段描述")]
- 所有字段都必须提供description参数，清晰描述字段用途
- 必填字段不设置default参数
- 可选字段根据情况设置default或default_factory

类定义规范:
- 继承BaseModel
- 添加类文档字符串说明模型用途
- 字段按逻辑分组排列
- 使用model_config配置模型行为(如需要)

默认值设置规范:
- 静态不可变值使用default: Field(default=值)
  * 数字: Field(default=0)
  * 字符串: Field(default="默认值")
  * 布尔值: Field(default=True)
  * None: Field(default=None)
  * 枚举值: Field(default=EnumType.VALUE)
  
- 动态生成值或可变对象使用default_factory: Field(default_factory=工厂函数)
  * UUID生成: Field(default_factory=lambda: str(uuid.uuid4()))
  * 当前时间: Field(default_factory=datetime.now)
  * 空列表: Field(default_factory=list)
  * 空字典: Field(default_factory=dict)
  * 复杂对象: Field(default_factory=lambda: {"key": "value"})

字段验证规范:
- 字符串长度: Field(min_length=1, max_length=100)
- 数值范围: Field(ge=0, le=100)  # ge=大于等于, le=小于等于, gt=大于, lt=小于
- 正则模式: Field(pattern=r"^[a-zA-Z0-9]+$")
- 示例值: Field(examples=["example1", "example2"])

示例:
class UserModel(BaseModel):
    \"\"\"用户信息模型\"\"\"
    # 必填字段
    user_id: Annotated[str, Field(
        min_length=1,
        max_length=50,
        description="用户唯一标识"
    )]
    
    # 可选字段，可为None
    name: Annotated[str | None, Field(
        default=None,
        max_length=100,
        description="用户姓名"
    )]
    
    # 可选字段，有默认值
    age: Annotated[int, Field(
        default=0,
        ge=0,
        le=150,
        description="用户年龄"
    )]
    
    # 动态默认值
    created_at: Annotated[datetime, Field(
        default_factory=datetime.now,
        description="创建时间"
    )]
    
    # 列表字段
    tags: Annotated[List[str], Field(
        default_factory=list,
        max_length=10,
        description="用户标签列表"
    )]

类型别名复用(可选):
# 定义可复用的类型别名
UserId = Annotated[str, Field(min_length=1, max_length=50, description="用户ID")]
PhoneNumber = Annotated[str, Field(pattern=r"^1[3-9]\d{9}$", description="手机号码")]

class UserModel(BaseModel):
    user_id: UserId
    phone: PhoneNumber | None = Field(default=None)
"""
