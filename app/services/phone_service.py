# app/services/phone_service.py
import asyncio
from datetime import datetime
import json
from app.models.events import Event, EventPriority

from pydantic import BaseModel, Field
from app.models.events import EventListener, EventType
from typing import Annotated, Dict, Any, Optional

from websockets import connect
from websockets.exceptions import ConnectionClosed
from app.core.event_bus import ProductionEventBus
from app.models.call_record import CallRecord, DialogEntry, DialogRecord
from app.models.device_info import Device
from app.services.base_service import BaseService
from app.services.redis_service import DeviceInfo, RedisService


# 使用统一日志管理器
from app.core.logger import get_logger

# 获取模块级别的logger
logger = get_logger(__name__)

# class SendMessageType(Enum):
#     CALL = "call"
#     HANGUP = "hangup"

class SendMessage(BaseModel):
    method: Annotated[str, Field(description="方法")]
    instance: Annotated[int, Field(description="设备实例")]
    phone: Annotated[str | None, Field(default=None, description="电话号码")]
    CustomId: Annotated[str | None, Field(default=None, description="自定义ID", exclude=True)]

class OnMessageType(BaseModel):
    notify: Annotated[str | None, Field(default=None, description="通知类型")]
    id: Annotated[int | None, Field(default=None, description="设备ID")]
    instance: Annotated[int | None, Field(default=None, description="设备实例")]
    uuid: Annotated[str, Field(description="通话唯一标识")]

class PhoneService(BaseService):
    """电话服务"""
    
    def __init__(self, event_bus: Optional[ProductionEventBus] = None, redis_service: Optional[RedisService] = None):
        super().__init__(event_bus, "PhoneService")
        self.ws_url = "ws://127.0.0.1:9898/ws"
        self.websocket = None
        self.should_stop = False
        self._service_task = None
        self._is_running = False
        self.redis_service = redis_service

        self.call_id = None
        self.instance = None
        self.call_finished = False  
    
    async def initialize(self):
        try:
            if not self._is_running:
                self._service_task = asyncio.create_task(self._start_service())
                self._is_running = True
                logger.info("PhoneService 已启动")
                return True
        except Exception as e:
            logger.error("PhoneService 启动失败 | error=%s", str(e))
            return False
        
    async def register_event_listeners(self):
        """推荐：直接注册模式"""
        if not self.event_bus:
            return
        
        await self._register_listener(EventType.CALL_OUT, self.handle_call_out, wait_for_result=False)
        await self._register_listener(EventType.CALL_END, self.hang_up, wait_for_result=False)
        
    async def _register_listener(self, event_type, handler, priority=EventPriority.NORMAL, **kwargs):
        """辅助方法：减少重复代码"""
        try:
            self.event_bus.register_listener(
                EventListener(
                    event_type=event_type,
                    handler=handler,
                    priority=priority,
                    name=f"{self.service_name}_{handler.__name__}",
                    **kwargs
                )
            )
            logger.info(f"✅ {self.service_name}: 注册监听器 {event_type.value}")
        except Exception as e:
            logger.error(f"❌ {self.service_name}: 注册监听器失败 {event_type.value} | error={str(e)}")
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
    
    async def send_message(self, message_data: SendMessage) -> bool:
        """发送消息到 WebSocket 服务器"""
        if not self.websocket:
            logger.error("WebSocket not connected")
            return False
        
        try:
            await self.websocket.send(message_data.model_dump_json())
            logger.debug(f"Sent message: {message_data}")
            return True
        except Exception as e:
            logger.error(f"Failed to send message: {e}")
            return False
    
    async def handle_call_out(self, event: Event) -> Dict[str, Any]:
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
            # 从事件中提取数据
            call_record: CallRecord = event.data
            self.call_id = call_record.call_id

            # 构建拨号消息
            dial_message = SendMessage(
                method="call",
                instance=call_record.instance,
                phone=call_record.phone_number,
                CustomId=call_record.custom_id
            )
            
            logger.info(f"Dialing {call_record.phone_number} with instance {call_record.instance}")
            
            # 检查WebSocket连接状态
            if not self.websocket:
                logger.error("WebSocket not connected")
                return {
                    "success": False,
                    "phone_number": call_record.phone_number,
                    "error": "WebSocket未连接",
                    "message": "拨号失败，请检查连接状态"
                }

            await self.send_message(dial_message)
            
            call_record.status = "already_dialed"  
            call_record.start_time = datetime.now()
            await self.redis_service.update_call_record(call_record)
            
            logger.info(f"拨号消息已准备: {dial_message}")
            
            return {
                "success": True,
                "phone_number": call_record.phone_number,
                "instance": call_record.instance,
                "custom_id": call_record.custom_id,
                "message": f"拨号请求已准备: {call_record.phone_number}"
            }
                
        except Exception as e:
            logger.error(f"Error dialing phone {call_record.phone_number}: {e}")
            call_record.status = "无法拨打"
            call_record.end_time = datetime.now()
            call_record.duration = 0
            call_record.notes = str(e)
            await self.redis_service.update_call_record(call_record)
            return {
                "success": False,
                "phone_number": call_record.phone_number,
                "error": str(e),
                "message": f"拨号异常: {str(e)}"
            }
    
    async def hang_up(self, event: Event = None) -> Dict[str, Any]:
        """
        挂断电话
        
        Args:
            event: 事件
            
        Returns:
            Dict: 挂断结果
        """
        try:
            instance = self.instance

            hang_up_message = SendMessage(
                method="terminateCall",
                instance=instance,
            )
            
            logger.info(f"Hanging up call on instance {instance}")
            
            await self.send_message(hang_up_message)
            
            if True:
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
    
    async def handle_on_connect_message(self, message_data: Dict) -> Dict[str, Any]:
        try:
            logger.info("Processing OnConnect message")

            device_info = DeviceInfo(
                notify=message_data.get("notify"),
                devid=message_data.get("devid"),
                version=message_data.get("version"),
                recordmode=message_data.get("recordmode", 0),
                devices=[Device(**device) for device in message_data.get("devices", [])]
            )

            await self.redis_service.set_device_info(device_info)
            
            return {
                "success": True,
                "message": "连接成功，设备信息已保存",
                "device_info": device_info.model_dump_json()
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
                        result = await self.handle_on_connect_message(data)
                        logger.info(f"OnConnect handled: {result}")

                    elif notify_type == "OnAnswer":
                        # 处理接听事件
                        on_message = OnMessageType(         
                            notify=notify_type,
                            id=data.get("id"),
                            instance=data.get("instance"),
                            uuid=data.get("uuid")
                        )

                        await self.redis_service.update_call_record_call_id(self.call_id,on_message.uuid)

                        self.call_id = on_message.uuid
                        self.instance = on_message.instance

                        await self.redis_service.update_call_record_status(call_id=self.call_id, status="已接听")

                        record = await self.redis_service.get_call_record(self.call_id)

                        tts_opening = record.tts_opening if record else ""

                        dialog_record = DialogRecord(
                            call_id=self.call_id,
                            dialog_record=[DialogEntry(speaker="agent", content=tts_opening, timestamp=datetime.now().isoformat())],
                            dialog_record_reply_marking=0
                        )

                        await self.redis_service.create_dialog_record(dialog_record)

                        tts_send_text = {
                            "text": tts_opening,
                            "call_id": self.call_id,
                            "instance": self.instance
                        }

                        await self.emit_event(EventType.TTS_SEND_TEXT, tts_send_text)
                        await self.emit_event(EventType.RTASR_START, self.call_id)
                        await asyncio.sleep(5)
                        await self.emit_event(EventType.RTASR_START_AUDIO, self.call_id)

                    elif notify_type == "OnCallOut":
                        # 处理呼出事件
                        pass
                        
                    elif notify_type == "OnCallIn":
                        # 处理呼入事件
                        pass

                    elif notify_type == "OnHangUp":
                        # 处理挂断事件
                        await self._call_finished()
                        
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
    
    async def _call_finished(self):
        """通话结束"""
        self.call_finished = True
        self.call_id = None
        self.instance = None
        await self.emit_event(EventType.CALL_END, None)