from celery import Celery
from app.core.config import settings
from typing import Optional, List, Dict, Any
import logging

logger = logging.getLogger(__name__)


class CeleryManager:
    """Celery 管理器类，统一管理 Celery 应用的创建和配置"""
    
    def __init__(self, app_name: str = "ai_tele_worker"):
        self.app_name = app_name
        self._celery_app: Optional[Celery] = None
        
    def build_redis_url(
        self,
        host: str = None,
        port: int = None,
        db: int = None,
        password: Optional[str] = None,
        ssl: bool = None
    ) -> str:
        """构建 Redis URL"""
        # 使用传入参数或默认配置
        host = host or settings.redis_host
        port = port or settings.redis_port
        db = db or settings.redis_db
        password = password or settings.redis_password
        ssl = ssl if ssl is not None else settings.redis_ssl
        
        if password:
            url = f"redis://:{password}@{host}:{port}/{db}"
        else:
            url = f"redis://{host}:{port}/{db}"
        
        if ssl:
            url = url.replace("redis://", "rediss://")
        
        return url
    
    def get_default_config(self) -> Dict[str, Any]:
        """获取默认的 Celery 配置"""
        return {
            # 序列化配置
            "task_serializer": "json",
            "accept_content": ["json"],
            "result_serializer": "json",
            
            # 时区配置
            "timezone": "UTC",
            "enable_utc": True,
            
            # 任务执行配置
            "task_always_eager": False,
            "task_eager_propagates": True,
            
            # 结果配置
            "result_expires": 3600,
            "result_backend_transport_options": {
                "master_name": "mymaster",
                "visibility_timeout": 3600,
            },
            
            # 工作进程配置
            "worker_prefetch_multiplier": 1,
            "worker_max_tasks_per_child": 1000,
            
            # 任务路由配置
            "task_routes": {
                "app.services.phone_service.*": {"queue": "phone_queue"},
                "app.services.tts_service.*": {"queue": "tts_queue"},
                "app.services.rtasr_service.*": {"queue": "rtasr_queue"},
                "app.services.aicall_service.*": {"queue": "aicall_queue"},
            },
            
            # 队列配置
            "task_default_queue": "default",
            "task_default_exchange": "default",
            "task_default_routing_key": "default",
            
            # 日志配置
            "worker_log_format": "[%(asctime)s: %(levelname)s/%(processName)s] %(message)s",
            "worker_task_log_format": "[%(asctime)s: %(levelname)s/%(processName)s][%(task_name)s(%(task_id)s)] %(message)s",
        }
    
    def get_default_includes(self) -> List[str]:
        """获取默认的任务模块列表"""
        return [
            "app.services.phone_service",
            "app.services.tts_service", 
            "app.services.rtasr_service",
            "app.services.aicall_service",
        ]
    
    def create_celery_app(
        self,
        broker_url: str = None,
        backend_url: str = None,
        includes: List[str] = None,
        config: Dict[str, Any] = None,
        auto_discover: bool = True
    ) -> Celery:
        """创建并配置 Celery 应用"""
        
        # 构建 Redis URL
        if not broker_url:
            broker_url = self.build_redis_url()
        if not backend_url:
            backend_url = broker_url
            
        # 获取包含的任务模块
        if includes is None:
            includes = self.get_default_includes()
            
        # 创建 Celery 实例
        celery_app = Celery(
            self.app_name,
            broker=broker_url,
            backend=backend_url,
            include=includes
        )
        
        # 合并配置
        final_config = self.get_default_config()
        if config:
            final_config.update(config)
            
        # 应用配置
        celery_app.conf.update(final_config)
        
        # 自动发现任务
        if auto_discover:
            self._auto_discover_tasks(celery_app, includes)
        
        self._celery_app = celery_app
        return celery_app
    
    def _auto_discover_tasks(self, celery_app: Celery, task_modules: List[str]):
        """自动发现任务"""
        try:
            celery_app.autodiscover_tasks(task_modules)
            logger.info("Celery tasks auto-discovered successfully")
        except Exception as e:
            logger.warning(f"Failed to auto-discover tasks: {e}")
    
    def get_celery_app(self) -> Celery:
        """获取 Celery 应用实例，如果不存在则创建"""
        if self._celery_app is None:
            self._celery_app = self.create_celery_app()
        return self._celery_app
    
    def recreate_celery_app(self, **kwargs) -> Celery:
        """重新创建 Celery 应用"""
        self._celery_app = None
        return self.create_celery_app(**kwargs)


# 创建全局管理器实例
celery_manager = CeleryManager()

# 创建全局 Celery 实例（保持向后兼容）
celery_app = celery_manager.get_celery_app()

# 导出
__all__ = ["celery_app"]
