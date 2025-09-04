# app/core/dependencies.py
from functools import lru_cache
from typing import Optional, Dict, Any
from fastapi import HTTPException, status
import asyncio

from app.core.event_bus import ProductionEventBus, create_event_bus
from app.core.config import settings
from app.core.logger import get_logger
from app.services.phone_service import PhoneService
from app.services.tts_service import TtsService
from app.services.rtasr_service import RtasrService
from app.services.aicall_service import AicallService
from app.services.redis_service import RedisService

logger = get_logger(__name__)

class EnhancedServiceContainer:
    """增强的服务容器 - 支持事件总线和生命周期管理"""
    
    def __init__(self):
        self._services = {}
        self._event_bus: Optional[ProductionEventBus] = None
        self._initialized = False
        self._lock = asyncio.Lock()
    
    async def initialize(self, config=None):
        """初始化所有服务"""
        if self._initialized:
            return
        
        async with self._lock:
            if self._initialized:  # 双重检查
                return
            
            try:
                logger.info("Initializing service container...")
                
                # 1. 首先初始化事件总线
                self._event_bus = create_event_bus(config or settings)
                await self._event_bus.start()
                logger.info("✅ EventBus initialized")
                
                # 2. 初始化业务服务（注入事件总线）
                await self._initialize_services()
                logger.info("✅ Business services initialized")
                
                # 3. 注册事件监听器
                await self._register_event_listeners()
                logger.info("✅ Event listeners registered")
                
                self._initialized = True
                logger.info("🎉 Service container initialization completed")
                
            except Exception as e:
                logger.error("❌ Service container initialization failed", error=str(e))
                await self._cleanup()
                raise
    
    async def _initialize_services(self):
        """初始化业务服务"""
        # 创建服务实例并注入事件总线
        self._services['redis_service'] = RedisService(self._event_bus)
        self._services['phone_service'] = PhoneService(self._event_bus, self._services['redis_service'])
        self._services['tts_service'] = TtsService(self._event_bus)
        self._services['rtasr_service'] = RtasrService(self._event_bus)
        self._services['aicall_service'] = AicallService(self._event_bus, self._services['redis_service'])
        
        # 如果服务有异步初始化方法，调用它们
        for name, service in self._services.items():
            if hasattr(service, 'initialize'):
                success = await service.initialize()
                if success:
                    logger.info(f"✅ Service {name} initialized")
                else:
                    logger.error(f"❌ Service {name} initialization failed")
                    # 根据需要决定是否抛出异常
                    if name == 'redis_service':  # Redis 是关键服务
                        raise RuntimeError(f"Critical service {name} failed to initialize")
    
    async def _register_event_listeners(self):
        """注册所有服务的事件监听器"""
        for name, service in self._services.items():
            if hasattr(service, 'register_event_listeners'):
                await service.register_event_listeners()
                logger.info(f"Event listeners registered for {name}")
    
    async def shutdown(self):
        """关闭所有服务"""
        if not self._initialized:
            return
        
        logger.info("Shutting down service container...")
        
        try:
            # 1. 关闭业务服务
            for name, service in self._services.items():
                if hasattr(service, 'shutdown'):
                    try:
                        await service.shutdown()
                        logger.info(f"Service {name} shutdown completed")
                    except Exception as e:
                        logger.error(f"Error shutting down {name}", error=str(e))
            
            # 2. 关闭事件总线
            if self._event_bus and self._event_bus.running:
                await self._event_bus.stop()
                logger.info("EventBus shutdown completed")
            
        except Exception as e:
            logger.error("Error during shutdown", error=str(e))
        finally:
            await self._cleanup()
    
    async def _cleanup(self):
        """清理资源"""
        self._services.clear()
        self._event_bus = None
        self._initialized = False
    
    def register_service(self, name: str, service):
        """注册服务"""
        self._services[name] = service
    
    def get_service(self, name: str):
        """获取服务"""
        if not self._initialized:
            raise RuntimeError("Service container not initialized")
        return self._services.get(name)
    
    def get_event_bus(self) -> ProductionEventBus:
        """获取事件总线"""
        if not self._initialized or not self._event_bus:
            raise RuntimeError("EventBus not initialized")
        return self._event_bus
    
    def get_all_services(self):
        """获取所有服务"""
        return self._services.copy()
    
    @property
    def is_initialized(self) -> bool:
        """检查是否已初始化"""
        return self._initialized
    
    @property
    def is_event_bus_running(self) -> bool:
        """检查事件总线是否运行中"""
        return self._initialized and self._event_bus and self._event_bus.running

# 全局服务容器
service_container = EnhancedServiceContainer()

# === FastAPI 依赖注入函数 ===

def get_event_bus() -> ProductionEventBus:
    """获取事件总线依赖"""
    try:
        if not service_container.is_event_bus_running:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Event bus is not running"
            )
        return service_container.get_event_bus()
    except RuntimeError as e:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=str(e)
        )

def get_phone_service() -> PhoneService:
    """获取电话服务"""
    try:
        service = service_container.get_service('phone_service')
        if not service:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Phone service not available"
            )
        return service
    except RuntimeError as e:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=str(e)
        )

def get_tts_service() -> TtsService:
    """获取TTS服务"""
    try:
        service = service_container.get_service('tts_service')
        if not service:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="TTS service not available"
            )
        return service
    except RuntimeError as e:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=str(e)
        )

def get_rtasr_service() -> RtasrService:
    """获取RTASR服务"""
    try:
        service = service_container.get_service('rtasr_service')
        if not service:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="RTASR service not available"
            )
        return service
    except RuntimeError as e:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=str(e)
        )

def get_aicall_service() -> AicallService:
    """获取AI电话服务"""
    try:
        service = service_container.get_service('aicall_service')
        if not service:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="AI call service not available"
            )
        return service
    except RuntimeError as e:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=str(e)
        )

def get_redis_service() -> RedisService:
    """获取Redis服务"""
    try:
        service = service_container.get_service('redis_service')
        if not service:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Redis service not available"
            )
        return service
    except RuntimeError as e:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=str(e)
        )

# === 批量获取服务 ===

def get_all_services() -> Dict[str, Any]:
    """获取所有服务"""
    if not service_container.is_initialized:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Services not initialized"
        )
    return service_container.get_all_services()

# === 健康检查 ===

async def check_services_health() -> Dict[str, Any]:
    """检查所有服务健康状态"""
    if not service_container.is_initialized:
        return {"status": "not_initialized", "services": {}}
    
    health_status = {"status": "healthy", "services": {}}
    
    # 检查事件总线
    try:
        event_bus = service_container.get_event_bus()
        bus_health = await event_bus.get_health_status()
        health_status["services"]["event_bus"] = bus_health
    except Exception as e:
        health_status["services"]["event_bus"] = {"status": "unhealthy", "error": str(e)}
        health_status["status"] = "degraded"
    
    # 检查业务服务
    for name, service in service_container.get_all_services().items():
        try:
            if hasattr(service, 'health_check'):
                service_health = await service.health_check()
                health_status["services"][name] = service_health
            else:
                health_status["services"][name] = {"status": "unknown"}
        except Exception as e:
            health_status["services"][name] = {"status": "unhealthy", "error": str(e)}
            health_status["status"] = "degraded"
    
    return health_status
