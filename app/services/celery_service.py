# app/services/celery_service.py
from typing import Optional, List, Dict, Any, Protocol
import redis
import threading
from celery import Celery
from app.core.config import settings
from app.core.logger import get_logger

logger = get_logger(__name__)

class CeleryServiceInterface(Protocol):
    """Celery 服务接口"""
    def get_app(self) -> Celery: ...
    def startup(self) -> bool: ...
    def shutdown(self) -> None: ...
    def send_task(self, name: str, args: list = None, kwargs: dict = None, **options) -> Any: ...

class CeleryService:
    """Celery 服务实现"""
    
    def __init__(
        self,
        app_name: str = "ai_tele_worker",
        redis_host: str = None,
        redis_port: int = None,
        redis_db: int = None,
        redis_password: str = None,
        redis_ssl: bool = None
    ):
        self.app_name = app_name
        self.redis_host = redis_host or settings.redis_host
        self.redis_port = redis_port or settings.redis_port
        self.redis_db = redis_db or settings.redis_db
        self.redis_password = redis_password or settings.redis_password
        self.redis_ssl = redis_ssl if redis_ssl is not None else settings.redis_ssl
        
        self._celery_app: Optional[Celery] = None
        self._redis_client: Optional[redis.Redis] = None
        self._initialized = False
        self._lock = threading.Lock()
        
    def build_redis_url(self) -> str:
        """构建 Redis URL"""
        if self.redis_password:
            url = f"redis://:{self.redis_password}@{self.redis_host}:{self.redis_port}/{self.redis_db}"
        else:
            url = f"redis://{self.redis_host}:{self.redis_port}/{self.redis_db}"
        
        if self.redis_ssl:
            url = url.replace("redis://", "rediss://")
        
        return url
    
    def get_default_config(self) -> Dict[str, Any]:
        """获取默认的 Celery 配置"""
        return {
            "task_serializer": "json",
            "accept_content": ["json"],
            "result_serializer": "json",
            "timezone": "UTC",
            "enable_utc": True,
            "task_always_eager": False,
            "task_eager_propagates": True,
            "result_expires": 3600,
            "worker_prefetch_multiplier": 1,
            "worker_max_tasks_per_child": 1000,
            "task_routes": {
                "app.services.phone_service.*": {"queue": "phone_queue"},
                "app.services.tts_service.*": {"queue": "tts_queue"},
                "app.services.rtasr_service.*": {"queue": "rtasr_queue"},
                "app.services.aicall_service.*": {"queue": "aicall_queue"},
            },
            "task_default_queue": "default",
            "task_default_exchange": "default",
            "task_default_routing_key": "default",
        }
    
    def get_default_includes(self) -> List[str]:
        """获取默认的任务模块列表"""
        return [
            "app.tasks.phone_tasks",
            "app.tasks.tts_tasks", 
            "app.tasks.rtasr_tasks",
            "app.tasks.aicall_tasks",
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
        
        with self._lock:
            if not broker_url:
                broker_url = self.build_redis_url()
            if not backend_url:
                backend_url = broker_url
                
            if includes is None:
                includes = self.get_default_includes()
                
            celery_app = Celery(
                self.app_name,
                broker=broker_url,
                backend=backend_url,
                include=includes
            )
            
            final_config = self.get_default_config()
            if config:
                final_config.update(config)
                
            celery_app.conf.update(final_config)
            
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
    
    def startup(self) -> bool:
        """启动时初始化 Celery"""
        if self._initialized:
            logger.info("Celery 已经初始化，跳过重复初始化")
            return True
            
        try:
            logger.info("=== 开始初始化 Celery 应用 ===")
            
            self.create_celery_app()
            
            if self._celery_app:
                broker_url = self._celery_app.conf.broker_url
                logger.info(f"Celery 应用初始化成功！")
                logger.info(f"  - Broker URL: {broker_url}")
                logger.info(f"  - 应用名称: {self._celery_app.main}")
                
                self._initialized = True
                logger.info("=== Celery 初始化完成 ===")
                return True
            
            logger.error("Celery 应用创建失败")
            return False
            
        except Exception as e:
            logger.error(f"Celery 启动失败: {e}")
            return False
    
    def shutdown(self):
        """关闭时清理 Celery"""
        try:
            with self._lock:
                if self._celery_app:
                    try:
                        if hasattr(self._celery_app, 'close'):
                            self._celery_app.close()
                    except Exception as e:
                        logger.warning(f"关闭 Celery 应用时出错: {e}")
                    
                    self._celery_app = None
                    logger.info("Celery 应用已关闭")
                self._initialized = False
        except Exception as e:
            logger.error(f"关闭 Celery 应用时出错: {e}")
    
    def get_app(self) -> Celery:
        """获取 Celery 应用实例"""
        if not self._celery_app:
            raise RuntimeError("Celery 应用未初始化")
        return self._celery_app
    
    def send_task(self, name: str, args: list = None, kwargs: dict = None, **options):
        """发送任务"""
        return self.get_app().send_task(name, args=args, kwargs=kwargs, **options)
