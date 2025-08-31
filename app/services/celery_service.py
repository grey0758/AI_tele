from celery import Celery
from app.core.config import settings
from typing import Optional
import logging

logger = logging.getLogger(__name__)


def build_redis_url(
    host: str = settings.redis_host,
    port: int = settings.redis_port,
    db: int = settings.redis_db,
    password: Optional[str] = settings.redis_password,
    ssl: bool = settings.redis_ssl
) -> str:
    """构建 Redis URL"""
    if password:
        url = f"redis://:{password}@{host}:{port}/{db}"
    else:
        url = f"redis://{host}:{port}/{db}"
    
    if ssl:
        url = url.replace("redis://", "rediss://")
    
    return url


def create_celery_app() -> Celery:
    """创建并配置 Celery 应用"""
    # 构建 Redis URL
    broker_url = build_redis_url()
    
    # 创建 Celery 实例
    celery_app = Celery(
        "ai_tele_worker",
        broker=broker_url,
        backend=broker_url,
        include=[
            "app.services.phone_service",
            "app.services.tts_service", 
            "app.services.rtasr_service",
            "app.services.aicall_service"
        ]
    )
    
    # 基础配置
    celery_app.conf.update(
        # 序列化配置
        task_serializer="json",
        accept_content=["json"],
        result_serializer="json",
        
        # 时区配置
        timezone="UTC",
        enable_utc=True,
        
        # 任务执行配置
        task_always_eager=False,  # 生产环境设为 False
        task_eager_propagates=True,
        
        # 结果配置
        result_expires=3600,  # 结果过期时间（秒）
        result_backend_transport_options={
            "master_name": "mymaster",
            "visibility_timeout": 3600,
        },
        
        # 工作进程配置
        worker_prefetch_multiplier=1,
        worker_max_tasks_per_child=1000,
        
        # 任务路由配置
        task_routes={
            "app.services.phone_service.*": {"queue": "phone_queue"},
            "app.services.tts_service.*": {"queue": "tts_queue"},
            "app.services.rtasr_service.*": {"queue": "rtasr_queue"},
            "app.services.aicall_service.*": {"queue": "aicall_queue"},
        },
        
        # 队列配置
        task_default_queue="default",
        task_default_exchange="default",
        task_default_routing_key="default",
        
        # 日志配置
        worker_log_format="[%(asctime)s: %(levelname)s/%(processName)s] %(message)s",
        worker_task_log_format="[%(asctime)s: %(levelname)s/%(processName)s][%(task_name)s(%(task_id)s)] %(message)s",
    )
    
    return celery_app


# 创建全局 Celery 实例
celery_app = create_celery_app()


# 自动发现任务（如果存在任务模块）
try:
    celery_app.autodiscover_tasks([
        "app.services.phone_service",
        "app.services.tts_service", 
        "app.services.rtasr_service",
        "app.services.aicall_service"
    ])
    logger.info("Celery tasks auto-discovered successfully")
except Exception as e:
    logger.warning(f"Failed to auto-discover tasks: {e}")


# 导出 Celery 实例
__all__ = ["celery_app", "create_celery_app"]
