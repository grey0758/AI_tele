import asyncio
import uvicorn
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.core.config import settings
from app.api.v1.api import api_router
from app.core.logger import logger
from app.services.celery_service import celery_app

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup 部分
    logger.info("Starting AI电话系统...")
    

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
    global celery_initialized, phone_controller_task
    
    status = {
        "celery": {
            "initialized": celery_initialized,
            "status": "running" if celery_initialized else "not_initialized"
        },
        "phone_service": {
            "task_id": phone_controller_task.id if phone_controller_task else None,
            "status": "running" if phone_controller_task else "not_started"
        }
    }
    
    # 如果celery已初始化，尝试获取更详细的状态
    if celery_initialized:
        try:
            inspector = celery_app.control.inspect()
            active_tasks = inspector.active()
            registered_tasks = inspector.registered()
            
            status["celery"]["active_tasks"] = active_tasks
            status["celery"]["registered_tasks"] = registered_tasks
        except Exception as e:
            status["celery"]["error"] = str(e)
    
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
