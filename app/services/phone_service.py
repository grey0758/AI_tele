# app/services/phone_service.py
import asyncio
from calendar import c
from contextlib import asynccontextmanager
import json
import logging
from tkinter import EventType
from typing import Dict, Any, Optional
from celery import current_app
from fastapi import FastAPI
from websockets import connect
from websockets.exceptions import ConnectionClosed
from app.api.v1.endpoints.aicall import CallRequest
from app.core.event_bus import ProductionEventBus
from app.services.base_service import BaseService
from app.services.redis_service import DeviceInfo, Device, RedisService
from app.services.redis_service import get_redis_service

redis_service = get_redis_service()

# 使用统一日志管理器
from app.core.logger import get_logger

# 获取模块级别的logger
logger = get_logger(__name__)


class PhoneService(BaseService):
    """电话服务"""
    
    def __init__(self, event_bus: Optional[ProductionEventBus] = None, redis_service: Optional[RedisService] = None):
        super().__init__(event_bus, "phone_service")
        self.ws_url = "ws://127.0.0.1:9898/ws"
        self.websocket = None
        self.should_stop = False
        self._service_task = None
        self.tts_opening = ""
        self._is_running = False
        self.redis_service = redis_service
    
    async def initialize(self):
        if not self._is_running:
            self._service_task = asyncio.create_task(self._start_service())
            self._is_running = True
            logger.info("PhoneService 已启动")
        
    async def register_event_listeners(self):
        """推荐：直接注册模式"""
        if not self.event_bus:
            return
        
        # 清晰、直接、易维护
        await self._register_listener(EventType.TTS_COMPLETED, self.handle_tts_completed, 1)
        await self._register_listener(EventType.AI_RESPONSE_READY, self.handle_ai_response, 2)
        await self._register_listener(EventType.CALL_ENDED, self.handle_call_ended, 1)
    
        
    async def _register_listener(self, event_type, handler, priority, **kwargs):
        """辅助方法：减少重复代码"""
        try:
            self.event_bus.register_listener(
                event_type=event_type,
                handler=handler,
                priority=priority,
                name=f"{self.service_name}_{handler.__name__}",
                **kwargs
            )
            logger.info(f"✅ {self.service_name}: 注册监听器 {event_type.value}")
        except Exception as e:
            logger.error(f"❌ {self.service_name}: 注册监听器失败 {event_type.value}", error=str(e))
            raise
    
    async def _start_service(self):
        """启动服务：连接 WebSocket 并开始监听消息"""
        try:
            logger.info(f"PhoneService 正在启动")
            
            # 连接 WebSocket
            if await self.connect():
                logger.info(f"PhoneService 已连接 WebSocket，开始监听消息")
                # 开始监听消息
                await self.listen_messages()
            else:
                logger.error(f"PhoneService 连接 WebSocket 失败")
                
        except Exception as e:
            logger.error(f"PhoneService 启动失败: {e}")
    
    async def stop(self):
        """停止服务"""
        if self._is_running:
            await self._stop_service()
            self._is_running = False
            logger.info("PhoneService 已停止")
    
    async def _stop_service(self):
        """停止服务"""
        try:
            self.should_stop = True
            await self.disconnect()
            if self._service_task:
                self._service_task.cancel()
            logger.info(f"PhoneService 已停止")
        except Exception as e:
            logger.error(f"停止 PhoneService 时出错: {e}")
    
    def is_service_running(self) -> bool:
        """检查服务是否正在运行"""
        return self._is_running and self.websocket is not None and not self.should_stop
    
    def get_service_status(self) -> Dict[str, Any]:
        """获取服务状态信息"""
        return {
            "websocket_connected": self.websocket is not None,
            "service_running": self.is_service_running(),
            "should_stop": self.should_stop,
            "ws_url": self.ws_url
        }
    
    async def restart_service(self):
        """重启服务"""
        try:
            logger.info(f"正在重启 PhoneService")
            await self._stop_service()
            self.should_stop = False
            await self._start_service()
            logger.info(f"PhoneService 重启完成")
        except Exception as e:
            logger.error(f"重启 PhoneService 失败: {e}")
    
    async def connect(self) -> bool:
        """连接到 WebSocket 服务器"""
        try:
            logger.info(f"Connecting to WebSocket: {self.ws_url}")
            self.websocket = await connect(self.ws_url)
            logger.info("WebSocket connected successfully")
            return True
        except Exception as e:
            logger.error(f"WebSocket connection failed: {e}")
            return False
    
    async def disconnect(self):
        """断开 WebSocket 连接"""
        if self.websocket:
            try:
                await self.websocket.close()
                logger.info("WebSocket disconnected")
            except Exception as e:
                logger.error(f"Error disconnecting WebSocket: {e}")
    
    async def send_message(self, message_data: Dict) -> bool:
        """发送消息到 WebSocket 服务器"""
        if not self.websocket:
            logger.error("WebSocket not connected")
            return False
        
        try:
            message_json = json.dumps(message_data)
            await self.websocket.send(message_json)
            logger.debug(f"Sent message: {message_data}")
            return True
        except Exception as e:
            logger.error(f"Failed to send message: {e}")
            return False
    
    def handle_call_out(self, call_request: CallRequest) -> Dict[str, Any]:
        """
        拨打电话
        
        Args:
            phone_number: 要拨打的电话号码
            instance: 设备实例值
            custom_id: 自定义ID（可选）
            
        Returns:
            Dict: 拨号结果
        """
        try:
            # 构建拨号消息
            call_request.instance = redis_service.get_default_device_instance(call_request.instance) if call_request.instance == 0 else call_request.instance
            dial_message = {
                "method": "call",
                "instance": call_request.instance,
                "phone": call_request.phone_number
            }
            
            # 如果提供了自定义ID，添加到消息中
            if call_request.custom_id:
                dial_message["CustomId"] = call_request.custom_id
            
            self.tts_opening = call_request.tts_opening
            
            logger.info(f"Dialing {call_request.phone_number} with instance {call_request.instance}")
            
            # 检查WebSocket连接状态
            if not self.websocket:
                logger.error("WebSocket not connected")
                return {
                    "success": False,
                    "phone_number": call_request.phone_number,
                    "error": "WebSocket未连接",
                    "message": "拨号失败，请检查连接状态"
                }
            
            # 同步发送消息（这里需要重新设计为同步方式）
            # 暂时返回成功，实际发送逻辑需要重构
            logger.info(f"拨号消息已准备: {dial_message}")
            
            return {
                "success": True,
                "phone_number": call_request.phone_number,
                "instance": call_request.instance,
                "custom_id": call_request.custom_id,
                "message": f"拨号请求已准备: {call_request.phone_number}"
            }
                
        except Exception as e:
            logger.error(f"Error dialing phone {call_request.phone_number}: {e}")
            return {
                "success": False,
                "phone_number": call_request.phone_number,
                "error": str(e),
                "message": f"拨号异常: {str(e)}"
            }
    
    async def hang_up(self, instance: int = 0) -> Dict[str, Any]:
        """
        挂断电话
        
        Args:
            instance: 设备实例值
            
        Returns:
            Dict: 挂断结果
        """
        try:
            hang_up_message = {
                "method": "hangup",
                "instance": instance
            }
            
            logger.info(f"Hanging up call on instance {instance}")
            
            success = await self.send_message(hang_up_message)
            
            if success:
                return {
                    "success": True,
                    "instance": instance,
                    "message": "挂断请求已发送"
                }
            else:
                return {
                    "success": False,
                    "instance": instance,
                    "error": "发送挂断消息失败"
                }
                
        except Exception as e:
            logger.error(f"Error hanging up call on instance {instance}: {e}")
            return {
                "success": False,
                "instance": instance,
                "error": str(e)
            }
    
    def handle_on_connect_message(self, message_data: Dict) -> Dict[str, Any]:
        try:
            logger.info("Processing OnConnect message")

            device_info = DeviceInfo(
                notify=message_data.get("notify"),
                devid=message_data.get("devid"),
                version=message_data.get("version"),
                recordmode=message_data.get("recordmode", 0),
                devices=[Device(**device) for device in message_data.get("devices", [])]
            )

            redis_service.set_device_info(device_info)
            
            return {
                "success": True,
                "message": "连接成功，设备信息已保存",
                "device_info": device_info.to_dict()
            }
            
        except Exception as e:
            logger.error(f"Error handling OnConnect message: {e}")
            return {
                "success": False,
                "error": str(e),
                "message": "处理连接消息失败"
            }
    
    async def listen_messages(self):
        """监听 WebSocket 消息"""
        if not self.websocket:
            logger.error("WebSocket not connected")
            return
        
        try:
            async for message in self.websocket:
                if self.should_stop:
                    break
                
                try:
                    data = json.loads(message)
                    logger.debug(f"Received message: {data}")
                    
                    # 处理不同类型的消息
                    notify_type = data.get("notify")
                    
                    if notify_type == "OnConnect":
                        # 处理连接成功消息
                        result = self.handle_on_connect_message(data)
                        logger.info(f"OnConnect handled: {result}")

                    elif notify_type == "OnAnswer":
                        # 处理接听事件
                        current_app.send_task('app.tasks.phone_tasks.handle_call_answer', args=[data, self.tts_opening])
                        
                    elif notify_type == "OnCallOut":
                        # 处理呼出事件
                        current_app.send_task('app.tasks.phone_tasks.handle_call_out', args=[data])
                        
                    elif notify_type == "OnCallIn":
                        # 处理呼入事件
                        current_app.send_task('app.tasks.phone_tasks.handle_call_in', args=[data])

                    elif notify_type == "OnHangUp":
                        # 处理挂断事件
                        current_app.send_task('app.tasks.phone_tasks.handle_hang_up', args=[data])
                        
                    else:
                        logger.debug(f"Unknown notify type: {notify_type}")
                        
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to parse message as JSON: {e}")
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
                    
        except ConnectionClosed:
            logger.info("WebSocket connection closed")
        except Exception as e:
            logger.error(f"Error in message listener: {e}")
        finally:
            logger.info("Message listener stopped")

phone_service = PhoneService()

def get_phone_service() -> PhoneService:
    return phone_service

@asynccontextmanager
async def lifespan(app: FastAPI):
    await phone_service.start()
    yield
    await phone_service.stop()

# ==================== Celery 任务 ====================

# @celery_app.task(queue='phone_queue')
# def make_call(call_request: CallRequest):
#     """拨打电话"""
#     try:
#         if call_request.instance == 0:
#             call_request.instance = redis_service.get_default_device_instance(call_request.instance) if call_request.instance == 0 else call_request.instance
#         asyncio.run(phone_service.dial_phone(call_request))
#     except Exception as e:
#         logger.error(f"Error calling phone: {e}")
#         raise

# @celery_app.task(queue='phone_queue')
# def handle_call_out(message_data: Dict):
#     """处理呼出事件"""
#     try:
#         call_uuid = message_data.get('call_uuid')
#         if not call_uuid:
#             logger.warning("Call out message missing call_uuid")
#             return {"status": "error", "message": "Missing call_uuid"}

#         # 获取设备实例ID
#         device_instance = message_data.get('device_instance')
        
#         call_data = CallRecord(
#             call_id=call_uuid,
#             phone_number=message_data.get('phone_number'),
#             call_type='呼出',
#             status='呼出',
#             device_instance=device_instance
#         )
        
#         save_call_state.delay(call_uuid, call_data)
        
#         logger.info(f"Call out initiated: {call_uuid}")
#         return {"status": "success", "call_uuid": call_uuid}
#     except Exception as e:
#         logger.error(f"Error handling call out: {e}")
#         raise

# @celery_app.task(queue='phone_queue')
# def handle_call_in(message_data: Dict):
#     """处理呼入事件"""
#     try:
#         call_uuid = message_data.get('call_uuid')
#         if not call_uuid:
#             logger.warning("Call in message missing call_uuid")
#             return {"status": "error", "message": "Missing call_uuid"}
        
#         # 获取设备实例ID
#         device_instance = message_data.get('device_instance')
        
#         call_data = CallRecord(
#             call_id=call_uuid,
#             phone_number=message_data.get('phone_number'),
#             call_type='呼入',
#             status='呼入',
#             device_instance=device_instance
#         )

#         save_call_state.delay(call_uuid, call_data)
        
#         logger.info(f"Call in received: {call_uuid}")
#         return {"status": "success", "call_uuid": call_uuid}
#     except Exception as e:
#         logger.error(f"Error handling call in: {e}")
#         raise

# @celery_app.task(queue='phone_queue')
# def handle_hang_up(message_data: Dict):
#     """处理挂断事件"""
#     try:
#         call_uuid = message_data.get('call_uuid')
#         if not call_uuid:
#             logger.warning("Hang up message missing call_uuid")
#             return {"status": "error", "message": "Missing call_uuid"}
        
#         # 获取设备实例ID
#         device_instance = message_data.get('device_instance')
        
#         call_data = CallRecord(
#             call_id=call_uuid,
#             phone_number=message_data.get('phone_number'),
#             call_type='呼出',
#             status='已挂断',
#             device_instance=device_instance
#         )

#         update_call_state.delay(call_uuid, call_data)
        
#         logger.info(f"Call ended: {call_uuid}")
#         return {"status": "success", "call_uuid": call_uuid}
#     except Exception as e:
#         logger.error(f"Error handling hang up: {e}")
#         raise

# @celery_app.task(bind=True, retry_backoff=True, max_retries=3)
# def add_dialog_to_call_record(self, call_id: str, dialog_entry_data: DialogEntry):
#     try:
        
#         # 验证输入参数
#         if not call_id:
#             raise ValueError("call_id 不能为空")
        
#         if not dialog_entry_data.get("speaker") or not dialog_entry_data.get("content"):
#             raise ValueError("speaker 和 content 不能为空")
        
#         # 获取现有通话记录
#         existing_record = redis_service.get_call_record(call_id)
#         if not existing_record:
#             raise ValueError(f"通话记录不存在: {call_id}")
        
#         # 创建对话记录
#         timestamp = dialog_entry_data.get("timestamp")
#         if not timestamp:
#             timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
#         dialog_entry = DialogEntry(
#             speaker=dialog_entry_data["speaker"],
#             content=dialog_entry_data["content"],
#             timestamp=timestamp
#         )
        
#         # 添加到现有记录
#         existing_record.dialog_record.append(dialog_entry)
        
#         # 更新记录
#         success = redis_service.update_call_record(existing_record)
        
#         if success:
#             logger.info(f"成功添加对话记录到通话 {call_id}")
#             return {
#                 "success": True,
#                 "message": "对话记录添加成功",
#                 "call_id": call_id,
#                 "dialog_count": len(existing_record.dialog_record)
#             }
#         else:
#             raise Exception("更新通话记录失败")
            
#     except Exception as e:
#         logger.error(f"添加对话记录失败: {e}")
#         # Celery 重试机制
#         if self.request.retries < self.max_retries:
#             logger.info(f"任务重试中... ({self.request.retries + 1}/{self.max_retries})")
#             raise self.retry(countdown=60, exc=e)
        
#         return {
#             "success": False,
#             "message": f"添加对话记录失败: {str(e)}",
#             "call_id": call_id
#         }

# @celery_app.task(queue='phone_queue')
# def handle_call_answer(message_data: Dict, tts_opening: str):
#     """处理接听事件"""
#     try:
#         call_uuid = message_data.get('call_uuid')
#         if not call_uuid:
#             logger.warning("Call answer message missing call_uuid")
#             return {"status": "error", "message": "Missing call_uuid"}

#         celery_app.send_task('app.services.tts_service.add_tts_text', args=[tts_opening, call_uuid])
#         celery_app.send_task('app.services.rtasr_service.start_rtasr_session', args=[call_uuid])
#         time.sleep(5)
#         celery_app.send_task('app.services.rtasr_service.start_audio', args=[call_uuid])
#         return {"status": "success", "call_uuid": call_uuid}
#     except Exception as e:
#         logger.error(f"Error handling call answer: {e}")
#         raise

# # ==================== 数据存储任务 ====================

# @celery_app.task(queue='phone_queue')
# def save_call_state(call_uuid: str, call_data: CallRecord):
#     """保存通话状态"""
#     try:
#         # 从 CallRecord 对象中提取数据
#         phone_number = call_data.phone_number
#         call_type = call_data.call_type
#         device_instance = call_data.device_instance
        
#         redis_service.create_call_record(
#             call_uuid, 
#             phone_number, 
#             call_type, 
#             device_instance
#         )
#         logger.debug(f"Saving call state: {call_uuid}")
#         return {"status": "saved", "call_uuid": call_uuid}
#     except Exception as e:
#         logger.error(f"Error saving call state: {e}")
#         raise

# @celery_app.task(queue='phone_queue')
# def update_call_state(call_uuid: str, update_data: CallRecord):
#     """更新通话状态"""
#     try:
#         # 从 CallRecord 对象中提取数据
#         update_dict = {
#             'status': update_data.status,
#             'device_instance': update_data.device_instance
#         }
        
#         # 如果有其他字段需要更新，也可以添加
#         if hasattr(update_data, 'dialog_record') and update_data.dialog_record:
#             update_dict['dialog_record'] = update_data.dialog_record
#         if hasattr(update_data, 'notes') and update_data.notes:
#             update_dict['notes'] = update_data.notes
        
#         redis_service.update_call_state(call_uuid, update_dict)
#         logger.debug(f"Updating call state: {call_uuid}")
#         return {"status": "updated", "call_uuid": call_uuid}
#     except Exception as e:
#         logger.error(f"Error updating call state: {e}")
#         raise