# -*- coding: utf-8 -*-
"""
文件上传系统主模块

这是一个基于 FastAPI 的文件上传系统，支持文件上传和下载功能。
提供了完整的生命周期管理、健康检查和静态文件服务。
"""
from contextlib import asynccontextmanager
import uvicorn
from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from app.core.config import settings
from app.api.v1.api import api_router
from app.core.dependencies import service_container, check_services_health
from app.middleware.logging import logging_middleware
from app.schemas.base import ResponseBuilder
from app.core.logger import get_logger

logger = get_logger(__name__)


@asynccontextmanager
async def lifespan(_app: FastAPI):
    """应用生命周期管理"""
    print("🚀 Starting application")

    try:
        # 初始化服务容器（包含事件总线和所有服务）
        await service_container.initialize()
        print("✅ All services initialized")

        # 设备初始化完成后会自动启动测试呼叫

    except Exception as e:
        print(f"❌ Startup failed: {e}")
        await service_container.shutdown()
        raise

    yield

    # 关闭阶段
    print("🛑 Shutting down application")
    await service_container.shutdown()
    print("👋 Application shutdown completed")


# 创建FastAPI应用
app = FastAPI(
    title="AI电话系统",
    description="智能电话控制系统，支持通话控制和录音管理",
    version="1.0.0",
    lifespan=lifespan
)

# 注册请求/响应日志中间件
app.middleware("http")(logging_middleware)

# 添加CORS中间件
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 注册API路由
app.include_router(api_router, prefix="/api/v1")

@app.exception_handler(HTTPException)
async def http_exception_handler(_request: Request, exc: HTTPException):
    """HTTP异常处理器 - 统一使用ResponseBuilder格式"""
    response_data = ResponseBuilder.error(
        message=exc.detail,
        code=exc.status_code
    )
    return JSONResponse(
        status_code=exc.status_code,
        content=response_data.model_dump()
    )


@app.exception_handler(Exception)
async def general_exception_handler(_request: Request, exc: Exception):
    """通用异常处理器 - 统一使用ResponseBuilder格式"""
    response_data = ResponseBuilder.internal_error(f"服务器内部错误: {str(exc)}")
    return JSONResponse(
        status_code=500,
        content=response_data.model_dump()
    )


@app.get("/")
async def root():
    """根路径"""
    return {
        "message": "record upload system running",
        "version": "1.0.0",
        "docs": "/docs",
    }


@app.get("/health")
async def health_check():
    """健康检查"""
    return await check_services_health()


if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host=settings.host,
        port=settings.port
    )
