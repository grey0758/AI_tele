# app/services/base_service.py
"""
服务基类 - 支持事件总线
"""
from abc import ABC
from typing import Dict, Any, Optional
from datetime import datetime

from app.core.event_bus import ProductionEventBus
from app.models.events import EventListener, EventType, EventPriority, Event
from app.core.logger import get_logger

logger = get_logger(__name__)


class BaseService(ABC):
    """服务基类 - 支持事件总线"""

    def __init__(
        self,
        event_bus: Optional[ProductionEventBus] = None,
        service_name: Optional[str] = None,
    ):
        self.event_bus = event_bus
        self.service_name = service_name or self.__class__.__name__
        self.start_time = datetime.now()

        # 统计信息
        self.stats = {
            "total_processed": 0,
            "total_failed": 0,
            "events_emitted": 0,
            "events_handled": 0,
        }

        logger.info(
            "%s initialized | has_event_bus=%s",
            self.service_name,
            self.event_bus is not None,
        )

    async def initialize(self):
        """异步初始化方法（子类可重写）"""

    async def shutdown(self):
        """关闭方法（子类可重写）"""
        logger.info("%s shutting down", self.service_name)

    async def emit_event(
        self,
        event_type: EventType,
        data: Any = None,
        wait_for_result: bool = False,
        priority: EventPriority = EventPriority.NORMAL,
        **kwargs,
    ) -> Any:
        """发送事件的便捷方法"""
        if not self.event_bus:
            logger.warning("No event bus available in %s", self.service_name)
            return None

        try:
            # 构造 Event 实例后发送，匹配事件总线的签名要求
            event = Event(
                type=event_type,
                data=data,
                priority=priority,
                wait_for_result=wait_for_result,
                source=self.service_name,
                **kwargs,
            )
            result = await self.event_bus.emit(event)

            self.stats["events_emitted"] += 1
            logger.debug(
                "Event emitted from %s | event_type=%s",
                self.service_name,
                event_type.value,
            )

            return result

        except Exception as e:  # pylint: disable=broad-except
            logger.error(
                "Failed to emit event from %s | event_type=%s, error=%s",
                self.service_name,
                event_type.value,
                str(e),
            )
            raise

    async def _register_listener(
        self, event_type, handler, priority=EventPriority.NORMAL, **kwargs
    ):
        """辅助方法：减少重复代码"""
        try:
            self.event_bus.register_listener(
                EventListener(
                    event_type=event_type,
                    handler=handler,
                    priority=priority,
                    name=f"{self.service_name}_{handler.__name__}",
                    **kwargs,
                )
            )
            logger.info("✅ %s: 注册监听器 %s", self.service_name, event_type.value)
        except Exception as e:  # pylint: disable=broad-except
            logger.error(
                "❌ %s: 注册监听器失败 %s | error=%s",
                self.service_name,
                event_type.value,
                str(e),
            )
            raise

    async def health_check(self) -> Dict[str, Any]:
        """健康检查（子类可重写）"""
        uptime = (datetime.now() - self.start_time).total_seconds()
        success_rate = (
            self.stats["total_processed"]
            / max(self.stats["total_processed"] + self.stats["total_failed"], 1)
            * 100
        )

        return {
            "service_name": self.service_name,
            "status": "healthy",
            "uptime_seconds": uptime,
            "success_rate": success_rate,
            "has_event_bus": self.event_bus is not None,
            "stats": self.stats,
        }
