# app/core/container.py
from dependency_injector import containers, providers
from app.services.phone_service import PhoneService
from app.services.celery_service import celery_app
from app.core.config import settings
import redis


class Container(containers.DeclarativeContainer):
    """依赖注入容器"""
    
    # 配置
    config = providers.Configuration()
    
    # Redis 客户端
    redis_client = providers.Singleton(
        redis.Redis,
        host=settings.redis_host,
        port=settings.redis_port,
        db=settings.redis_db,
        password=settings.redis_password,
        decode_responses=True,
        socket_connect_timeout=5,
        socket_timeout=5
    )
    
    # PhoneService 单例
    phone_service = providers.Singleton(
        PhoneService
    )
    
    # Celery 应用
    celery_app = providers.Singleton(
        lambda: celery_app
    )


# 创建全局容器实例
container = Container()
container.config.from_dict({
    "redis": {
        "host": settings.redis_host,
        "port": settings.redis_port,
        "db": settings.redis_db,
        "password": settings.redis_password
    }
})
