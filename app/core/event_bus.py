import asyncio
import json
import time
from collections import defaultdict, deque
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from app.core.config import settings
from app.core.logger import get_logger
from app.models.events import Event, EventType, EventPriority, EventStatus, EventMetrics, EventListener

logger = get_logger(__name__)

class ProductionEventBus:
    """生产级事件总线"""
    
    def __init__(self, config=None):
        # 使用传入的配置或默认配置
        self.config = config or settings
        
        # 核心组件
        self.listeners: Dict[EventType, List[EventListener]] = defaultdict(list)
        self.pending_events: Dict[str, Event] = {}
        self.event_queues: List[asyncio.Queue] = []
        self.dead_letter_queue: deque = deque(maxlen=self.config.dead_letter_queue_size)
        
        # 状态管理
        self.running = False
        self.workers: List[asyncio.Task] = []
        self.health_check_task: Optional[asyncio.Task] = None
        self.metrics_task: Optional[asyncio.Task] = None
        
        # 指标和监控
        self.metrics = EventMetrics()
        self.event_history: deque = deque(maxlen=1000)
        self.processing_times: deque = deque(maxlen=100)
        self.events_per_second_counter = deque(maxlen=60)
        
        # 线程安全
        self.lock = asyncio.Lock()
        self.metrics_lock = asyncio.Lock()
        
        # 持久化
        self.persistence_enabled = self.config.enable_persistence
        self.persistence_path = Path(self.config.persistence_path)
        
        # 初始化队列（按优先级）
        for _ in EventPriority:
            queue = asyncio.Queue(maxsize=self.config.max_queue_size)
            self.event_queues.append(queue)
        
        logger.info(
            "EventBus initialized | worker_count=%s, max_queue_size=%s, persistence_enabled=%s",
            self.config.worker_count,
            self.config.max_queue_size,
            self.persistence_enabled,
        )
    
    async def start(self):
        """启动事件总线"""
        if self.running:
            logger.warning("EventBus is already running")
            return
        
        logger.info("Starting EventBus...")
        self.running = True
        
        # 创建持久化目录
        if self.persistence_enabled:
            self.persistence_path.mkdir(parents=True, exist_ok=True)
            logger.info(f"Persistence enabled | path={str(self.persistence_path)}")
        
        # 启动工作线程
        for i in range(self.config.worker_count):
            worker = asyncio.create_task(self._event_worker(f"worker-{i}"))
            self.workers.append(worker)
        
        # 启动健康检查
        self.health_check_task = asyncio.create_task(self._health_check_worker())
        
        # 启动指标收集
        self.metrics_task = asyncio.create_task(self._metrics_worker())
        
        logger.info(
            f"EventBus started successfully | workers={len(self.workers)}, queues={len(self.event_queues)}"
        )
    
    async def stop(self, timeout: float = 10.0):
        """停止事件总线"""
        if not self.running:
            logger.info("EventBus is not running")
            return
        
        logger.info(f"Stopping EventBus... | timeout={timeout}")
        self.running = False
        
        # 停止健康检查和指标收集
        if self.health_check_task:
            self.health_check_task.cancel()
        if self.metrics_task:
            self.metrics_task.cancel()
        
        # 等待工作线程完成
        try:
            await asyncio.wait_for(
                asyncio.gather(*self.workers, return_exceptions=True),
                timeout=timeout
            )
            logger.info("All workers stopped gracefully")
        except asyncio.TimeoutError:
            logger.warning(f"Some workers did not stop gracefully | timeout={timeout}")
            # 强制取消剩余的工作线程
            for worker in self.workers:
                if not worker.done():
                    worker.cancel()
        
        # 取消所有待处理的事件
        async with self.lock:
            cancelled_count = 0
            for event in self.pending_events.values():
                if event.result_future and not event.result_future.done():
                    event.result_future.cancel()
                    event.status = EventStatus.CANCELLED
                    cancelled_count += 1
            self.pending_events.clear()
            
            if cancelled_count > 0:
                logger.info(f"Cancelled pending events | count={cancelled_count}")
        
        logger.info("EventBus stopped successfully")
    
    def register_listener(self, listener: EventListener):
        """注册事件监听器"""
        
        self.listeners[listener.event_type].append(listener)
        # 按优先级排序（高优先级在前）
        self.listeners[listener.event_type].sort(key=lambda x: x.priority, reverse=True)
        
        logger.info(
            f"Listener registered | event_type={listener.event_type.value}, "
            f"handler_name={listener.name}, priority={listener.priority}, "
            f"max_concurrent={listener.max_concurrent}"
        )
    
    async def emit(self, event: Event) -> Any:
        """发送事件"""
        if not self.running:
            raise RuntimeError("EventBus is not running")
        
        
        logger.debug(
            f"Emitting event | event_id={event.event_id}, event_type={event.type.value}, "
            f"priority={event.priority.name}, wait_for_result={event.wait_for_result}"
        )
        
        # 如果需要等待结果，创建Future
        if event.wait_for_result:
            event.result_future = asyncio.Future()
            async with self.lock:
                self.pending_events[event.event_id] = event
        
        # 放入对应优先级的队列
        try:
            queue_index = event.priority.value
            await self.event_queues[queue_index].put(event)
            
            # 更新指标
            await self._update_metrics(total_events=1)
            
            # 记录事件
            await self._log_event(event, "emitted")
            
        except Exception as e:
            error_msg = f"Failed to queue event: {e}"
            logger.error(f"Event queue error | event_id={event.event_id}, error={error_msg}")
            if event.wait_for_result and event.result_future:
                event.result_future.set_exception(RuntimeError(error_msg))
            raise RuntimeError(error_msg)
        
        # 如果需要等待结果
        if event.wait_for_result:
            try:
                result = await asyncio.wait_for(
                    event.result_future, 
                    timeout=event.timeout
                )
                logger.debug(
                    f"Event completed | event_id={event.event_id}, "
                    f"processing_time={self._get_processing_time(event)}"
                )
                return result
            except asyncio.TimeoutError:
                await self._handle_timeout(event)
                raise TimeoutError(f"Event {event.type.value} timed out after {event.timeout}s")
            finally:
                async with self.lock:
                    self.pending_events.pop(event.event_id, None)
        
        return None
    
    async def _event_worker(self, worker_id: str):
        """事件处理工作线程"""
        logger.info(f"Worker started | worker_id={worker_id}")
        
        while self.running:
            try:
                event = await self._get_next_event()
                if event:
                    await self._process_event(event, worker_id)
                else:
                    # 没有事件时短暂休息
                    await asyncio.sleep(0.1)
            except asyncio.CancelledError:
                logger.info(f"Worker cancelled | worker_id={worker_id}")
                break
            except Exception as e:
                logger.error(f"Worker error | worker_id={worker_id}, error={str(e)}")
                await asyncio.sleep(1)
        
        logger.info(f"Worker stopped | worker_id={worker_id}")
    
    async def _get_next_event(self) -> Optional[Event]:
        """获取下一个事件（按优先级）"""
        # 按优先级从高到低检查队列
        for queue in reversed(self.event_queues):
            try:
                event = await asyncio.wait_for(queue.get(), timeout=0.1)
                return event
            except asyncio.TimeoutError:
                continue
        return None
    
    async def _process_event(self, event: Event, worker_id: str):
        """处理单个事件"""
        event.status = EventStatus.PROCESSING
        event.processing_start_time = datetime.now()
        
        try:
            await self._log_event(event, "processing", worker_id)
            
            # 获取监听器
            listeners = self.listeners.get(event.type, [])
            if not listeners:
                raise ValueError(f"No listeners for event type: {event.type.value}")
            
            # 执行所有监听器
            results = []
            for listener in listeners:
                try:
                    result = await self._execute_listener(listener, event)
                    results.append(result)
                    listener.total_processed += 1
                except Exception as e:
                    listener.total_failed += 1
                    logger.error(
                        f"Listener execution failed | event_id={event.event_id}, "
                        f"listener_name={listener.name}, error={str(e)}"
                    )
                    # 如果是关键监听器，抛出异常
                    if listener.priority >= 2:  # HIGH or CRITICAL
                        raise
            
            # 设置结果
            final_result = results[0] if len(results) == 1 else results
            await self._complete_event(event, final_result)
            
        except Exception as e:
            await self._handle_event_error(event, e)
    
    async def _execute_listener(self, listener: EventListener, event: Event):
        """执行监听器"""
        # 并发控制
        if listener.semaphore:
            async with listener.semaphore:
                listener.active_count += 1
                try:
                    return await self._call_listener(listener, event)
                finally:
                    listener.active_count -= 1
        else:
            return await self._call_listener(listener, event)
    
    async def _call_listener(self, listener: EventListener, event: Event):
        """调用监听器函数"""
        timeout = listener.timeout or event.timeout
        
        try:
            if asyncio.iscoroutinefunction(listener.handler):
                result = await asyncio.wait_for(
                    listener.handler(event), 
                    timeout=timeout
                )
            else:
                # 同步函数在线程池中执行
                loop = asyncio.get_event_loop()
                result = await loop.run_in_executor(
                    None, listener.handler, event
                )
            return result
        except asyncio.TimeoutError:
            raise TimeoutError(f"Listener {listener.name} timeout after {timeout}s")
    
    async def _complete_event(self, event: Event, result: Any):
        """完成事件处理"""
        event.status = EventStatus.COMPLETED
        event.processing_end_time = datetime.now()
        
        # 计算处理时间
        processing_time = self._get_processing_time(event)
        if processing_time is not None:
            self.processing_times.append(processing_time)
        
        # 设置Future结果
        if event.wait_for_result and event.result_future and not event.result_future.done():
            event.result_future.set_result(result)
        
        # 更新指标
        await self._update_metrics(completed_events=1)
        await self._log_event(event, "completed")
    
    async def _handle_event_error(self, event: Event, error: Exception):
        """处理事件错误"""
        event.error_message = str(error)
        
        # 重试逻辑
        if event.retry_count < event.max_retries:
            event.retry_count += 1
            event.status = EventStatus.PENDING
            
            # 延迟重试
            retry_delay = self.config.retry_delay * (2 ** (event.retry_count - 1))  # 指数退避
            await asyncio.sleep(retry_delay)
            
            # 重新放入队列
            queue_index = event.priority.value
            await self.event_queues[queue_index].put(event)
            
            logger.warning(
                f"Retrying event | event_id={event.event_id}, retry_count={event.retry_count}, "
                f"retry_delay={retry_delay}"
            )
            return
        
        # 重试次数用完，标记为失败
        event.status = EventStatus.FAILED
        event.processing_end_time = datetime.now()
        
        # 添加到死信队列
        self.dead_letter_queue.append(event)
        
        # 设置Future异常
        if event.wait_for_result and event.result_future and not event.result_future.done():
            event.result_future.set_exception(error)
        
        # 更新指标
        await self._update_metrics(failed_events=1)
        await self._log_event(event, "failed", error=str(error))
        
        logger.error(
            f"Event failed permanently | event_id={event.event_id}, "
            f"retry_count={event.retry_count}, error={str(error)}"
        )
    
    async def _handle_timeout(self, event: Event):
        """处理事件超时"""
        event.status = EventStatus.TIMEOUT
        event.processing_end_time = datetime.now()
        
        # 添加到死信队列
        self.dead_letter_queue.append(event)
        
        # 更新指标
        await self._update_metrics(timeout_events=1)
        await self._log_event(event, "timeout")
        
        logger.warning(
            f"Event timed out | event_id={event.event_id}, timeout={event.timeout}"
        )
    
    def _get_processing_time(self, event: Event) -> Optional[float]:
        """获取事件处理时间"""
        if event.processing_start_time and event.processing_end_time:
            return (event.processing_end_time - event.processing_start_time).total_seconds()
        return None
    
    async def _update_metrics(self, **kwargs):
        """更新指标"""
        async with self.metrics_lock:
            for key, value in kwargs.items():
                current_value = getattr(self.metrics, key, 0)
                setattr(self.metrics, key, current_value + value)
            
            # 计算平均处理时间
            if self.processing_times:
                self.metrics.average_processing_time = sum(self.processing_times) / len(self.processing_times)
            
            # 计算队列大小
            self.metrics.queue_size = sum(queue.qsize() for queue in self.event_queues)
            
            # 计算活跃工作线程数
            self.metrics.active_workers = len([w for w in self.workers if not w.done()])
            
            # 计算死信队列大小
            self.metrics.dead_letter_queue_size = len(self.dead_letter_queue)
            
            self.metrics.last_updated = datetime.now()
    
    async def _log_event(self, event: Event, action: str, worker_id: str = None, error: str = None):
        """记录事件日志"""
        log_data = {
            "event_id": event.event_id,
            "event_type": event.type.value,
            "action": action,
            "timestamp": datetime.now().isoformat(),
            "correlation_id": event.correlation_id,
            "source": event.source,
            "worker_id": worker_id,
            "error": error,
            "retry_count": event.retry_count,
            "priority": event.priority.name,
            "status": event.status.value
        }
        
        # 添加到历史记录
        self.event_history.append(log_data)
        
        # 持久化日志
        if self.persistence_enabled:
            await self._persist_log(log_data)
    
    async def _persist_log(self, log_data: dict):
        """持久化日志到文件"""
        try:
            log_file = self.persistence_path / f"events_{datetime.now().strftime('%Y%m%d')}.log"
            # 使用aiofiles进行异步文件写入
            import aiofiles
            async with aiofiles.open(log_file, 'a', encoding='utf-8') as f:
                await f.write(json.dumps(log_data) + '\n')
        except Exception as e:
            logger.error(f"Failed to persist log | error={str(e)}")
    
    async def _health_check_worker(self):
        """健康检查工作线程"""
        logger.info("Health check worker started")
        
        while self.running:
            try:
                await self._perform_health_check()
                await asyncio.sleep(self.config.health_check_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Health check error | error={str(e)}")
                await asyncio.sleep(5)
        
        logger.info("Health check worker stopped")
    
    async def _perform_health_check(self):
        """执行健康检查"""
        # 检查工作线程状态
        dead_workers = [w for w in self.workers if w.done()]
        if dead_workers:
            logger.warning(f"Found dead workers, restarting... | count={len(dead_workers)}")
            
            # 重启死掉的工作线程
            for i, worker in enumerate(self.workers):
                if worker.done():
                    new_worker = asyncio.create_task(self._event_worker(f"worker-{i}"))
                    self.workers[i] = new_worker
        
        # 检查队列大小
        total_queue_size = sum(queue.qsize() for queue in self.event_queues)
        if total_queue_size > self.config.max_queue_size * 0.8:
            logger.warning(f"Queue size is high | size={total_queue_size}")
        
        # 检查死信队列
        if len(self.dead_letter_queue) > self.config.dead_letter_queue_size * 0.8:
            logger.warning(f"Dead letter queue is high | size={len(self.dead_letter_queue)}")
        
        logger.debug(
            f"Health check completed | active_workers={len([w for w in self.workers if not w.done()])}, "
            f"queue_size={total_queue_size}, dead_letter_size={len(self.dead_letter_queue)}"
        )
    
    async def _metrics_worker(self):
        """指标收集工作线程"""
        logger.info("Metrics worker started")
        
        last_event_count = 0
        
        while self.running:
            try:
                # 计算每秒事件数
                current_time = time.time()
                current_event_count = self.metrics.total_events
                
                # 记录时间窗口内的事件数
                self.events_per_second_counter.append((current_time, current_event_count))
                
                # 计算过去60秒的平均事件数
                if len(self.events_per_second_counter) >= 2:
                    oldest_time, oldest_count = self.events_per_second_counter[0]
                    time_diff = current_time - oldest_time
                    event_diff = current_event_count - oldest_count
                    
                    if time_diff > 0:
                        async with self.metrics_lock:
                            self.metrics.events_per_second = event_diff / time_diff
                
                await asyncio.sleep(1)  # 每秒更新一次
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Metrics worker error | error={str(e)}")
                await asyncio.sleep(1)
        
        logger.info("Metrics worker stopped")
    
    # 公共API方法
    async def get_metrics(self) -> EventMetrics:
        """获取事件指标"""
        async with self.metrics_lock:
            # 创建指标副本
            return EventMetrics(
                total_events=self.metrics.total_events,
                completed_events=self.metrics.completed_events,
                failed_events=self.metrics.failed_events,
                timeout_events=self.metrics.timeout_events,
                cancelled_events=self.metrics.cancelled_events,
                average_processing_time=self.metrics.average_processing_time,
                events_per_second=self.metrics.events_per_second,
                queue_size=self.metrics.queue_size,
                active_workers=self.metrics.active_workers,
                dead_letter_queue_size=self.metrics.dead_letter_queue_size,
                last_updated=self.metrics.last_updated
            )
    
    async def get_health_status(self) -> Dict[str, Any]:
        """获取健康状态"""
        metrics = await self.get_metrics()
        
        # 计算健康分数
        health_score = 100
        if metrics.total_events > 0:
            failure_rate = (metrics.failed_events + metrics.timeout_events) / metrics.total_events
            health_score = max(0, 100 - (failure_rate * 100))
        
        return {
            "status": "healthy" if self.running and health_score > 80 else "degraded" if self.running else "stopped",
            "health_score": health_score,
            "running": self.running,
            "uptime": (datetime.now() - self.metrics.last_updated).total_seconds() if self.running else 0,
            "workers": {
                "total": len(self.workers),
                "active": metrics.active_workers,
                "dead": len(self.workers) - metrics.active_workers
            },
            "queues": {
                "total_size": metrics.queue_size,
                "dead_letter_size": metrics.dead_letter_queue_size,
                "queue_utilization": metrics.queue_size / (self.config.max_queue_size * len(self.event_queues)) * 100
            },
            "events": {
                "total": metrics.total_events,
                "completed": metrics.completed_events,
                "failed": metrics.failed_events,
                "timeout": metrics.timeout_events,
                "success_rate": metrics.completed_events / max(metrics.total_events, 1) * 100,
                "failure_rate": (metrics.failed_events + metrics.timeout_events) / max(metrics.total_events, 1) * 100
            },
            "performance": {
                "average_processing_time": metrics.average_processing_time,
                "events_per_second": metrics.events_per_second
            }
        }
    
    async def get_event_history(self, limit: int = 100) -> List[Dict[str, Any]]:
        """获取事件历史"""
        return list(self.event_history)[-limit:]
    
    async def get_dead_letter_events(self, limit: int = 100) -> List[Dict[str, Any]]:
        """获取死信队列事件"""
        events = list(self.dead_letter_queue)[-limit:]
        return [event.to_dict() for event in events]
    
    async def get_listeners_info(self) -> Dict[str, List[Dict[str, Any]]]:
        """获取监听器信息"""
        result = {}
        for event_type, listeners in self.listeners.items():
            result[event_type.value] = [listener.to_dict() for listener in listeners]
        return result
    
    async def clear_dead_letter_queue(self):
        """清空死信队列"""
        cleared_count = len(self.dead_letter_queue)
        self.dead_letter_queue.clear()
        logger.info(f"Dead letter queue cleared | count={cleared_count}")
        return cleared_count
    
    def listen(self, event_type: EventType, priority: EventPriority = EventPriority.NORMAL, 
               max_concurrent: Optional[int] = None, timeout: Optional[float] = None, 
               name: Optional[str] = None):
        """装饰器：注册事件监听器"""
        def decorator(func):
            listener = EventListener(
                event_type=event_type,
                handler=func,
                priority=priority,
                max_concurrent=max_concurrent,
                timeout=timeout,
                name=name
            )
            self.register_listener(listener)
            return func
        return decorator

def create_event_bus(config=None) -> ProductionEventBus:
    """创建事件总线实例"""
    return ProductionEventBus(config)
