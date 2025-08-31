# AI Tele FastAPI Project

一个基于FastAPI的现代化Web API项目，包含完整的项目结构和最佳实践。

## 功能特性

- 🚀 基于FastAPI框架
- 🗄️ SQLAlchemy ORM + Alembic数据库迁移
- 📝 结构化日志记录
- 🔒 异常处理和错误管理
- 📄 分页支持
- 🐳 Docker容器化部署
- 🧪 测试框架支持

## 项目结构

```
├── main.py                    # 应用入口
├── app/
│   ├── api/                   # API层
│   │   ├── v1/               # API v1版本
│   │   │   └── endpoints/    # 具体端点
│   ├── core/                 # 核心配置
│   ├── db/                   # 数据库
│   ├── models/               # SQLAlchemy模型
│   ├── schemas/              # Pydantic模型
│   ├── services/             # 业务逻辑层
│   ├── utils/                # 工具函数
│   └── middleware/           # 中间件
├── alembic/                  # 数据库迁移
├── uploads/                  # 上传文件目录
├── logs/                     # 日志文件目录
└── docker-compose.yml        # Docker部署配置
```

## 快速开始

### 环境要求

- Python 3.8+
- PostgreSQL 12+

### 安装依赖

```bash
pip install -r requirements.txt
```

### 环境配置

复制环境变量示例文件并配置：

```bash
cp env.example .env
```

编辑 `.env` 文件，配置数据库连接等信息。

### 数据库迁移

```bash
# 初始化迁移
alembic init alembic

# 创建迁移文件
alembic revision --autogenerate -m "Initial migration"

# 执行迁移
alembic upgrade head
```

### 运行应用

```bash
# 开发模式
uvicorn main:app --reload

# 生产模式
uvicorn main:app --host 0.0.0.0 --port 8000
```

### Docker部署

```bash
# 构建并启动服务
docker-compose up -d

# 查看日志
docker-compose logs -f app

# 停止服务
docker-compose down
```

## API文档

启动应用后，访问以下地址查看API文档：

- Swagger UI: http://localhost:8000/docs
- ReDoc: http://localhost:8000/redoc

## 开发指南

### 添加新的API端点

1. 在 `app/api/v1/endpoints/` 目录下创建新的端点文件
2. 在 `app/api/v1/api.py` 中注册路由
3. 在 `app/services/` 中添加业务逻辑
4. 在 `app/schemas/` 中定义数据模型

### 数据库模型

- 继承 `Base` 类
- 使用 `IDMixin` 和 `TimestampMixin` 获取通用字段
- 在 `alembic/env.py` 中导入新模型

### 日志记录

使用 `app.core.logger.logger` 记录日志：

```python
from app.core.logger import logger

logger.info("信息日志")
logger.error("错误日志")
logger.debug("调试日志")
```

## 测试

```bash
# 运行测试
pytest

# 运行测试并显示覆盖率
pytest --cov=app

# 运行特定测试文件
pytest tests/test_api.py
```

## 部署

### 生产环境配置

1. 设置 `DEBUG=False`
2. 配置生产数据库
3. 设置强密钥
4. 配置CORS策略
5. 启用HTTPS

### 性能优化

- 使用连接池
- 启用缓存
- 异步处理
- 负载均衡

## 贡献

1. Fork 项目
2. 创建功能分支
3. 提交更改
4. 推送到分支
5. 创建 Pull Request

## 许可证

MIT License

## 联系方式

如有问题或建议，请提交 Issue 或联系项目维护者。
