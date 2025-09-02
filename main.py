# -*- coding: utf-8 -*-
import asyncio
import uvicorn
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.core.config import settings
from app.api.v1.api import api_router
from app.core.logger import logger
from app.services.celery_service import lifespan

# 全局变量定义
celery_initialized = False
phone_controller_task = None

# 导入任务模块以确保任务被注册
import app.tasks.phone_tasks
import app.tasks.tts_tasks
import app.tasks.rtasr_tasks
import app.tasks.aicall_tasks

# 创建FastAPI应用
app = FastAPI(
    title="AI电话系统",
    description="智能电话控制系统，支持通话控制和录音管理",
    version="1.0.0",
    lifespan=lifespan
)

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

# 添加系统状态检查端点
@app.get("/system/status")
async def get_system_status():
    """获取系统状态"""
    from app.services.celery_service import get_celery_manager
    
    celery_manager = get_celery_manager()
    celery_status = celery_manager.get_status()
    
    status = {
        "celery": celery_status,
        "phone_service": {
            "task_id": phone_controller_task.id if phone_controller_task else None,
            "status": "running" if phone_controller_task else "not_started"
        }
    }
    
    return status

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
    return {
        "status": "healthy",
        "service": "AI电话系统"
    }

if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host=settings.host,
        port=settings.port,
        reload=settings.debug,
        log_level="info"
    )
