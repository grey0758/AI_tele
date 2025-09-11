# -*- coding: utf-8 -*-
from contextlib import asynccontextmanager
import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.core.config import settings
from app.api.v1.api import api_router
from app.core.dependencies import service_container, check_services_health
from app.middleware.logging import logging_middleware

# 全局处理器存储
app_processors = {}

@asynccontextmanager
async def lifespan(app: FastAPI):
    """应用生命周期管理"""
    print("🚀 Starting application")
    
    try:
        # 初始化服务容器（包含事件总线和所有服务）
        await service_container.initialize()
        print("✅ All services initialized")
        
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
app.middleware('http')(logging_middleware)

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

@app.get("/")
async def root():
    """根路径"""
    return {
        "message": "AI电话系统运行中",
        "version": "1.0.0",
        "docs": "/docs"
    }

@app.get("/health")
async def health_check():
    """健康检查"""
    return await check_services_health()

if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host=settings.host,
        port=settings.port,
        reload=settings.debug,
        reload_dirs=["."],
        reload_excludes=["logs/*", "*.log", "uploads/*", "**/__pycache__/*"],
        log_level="info"
    )
