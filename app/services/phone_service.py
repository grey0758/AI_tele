# app/services/phone_service.py
import asyncio
from datetime import datetime
import json
from re import A
import websocket
import threading
from app.models.events import Event

from pydantic import BaseModel, Field
from app.models.events import EventType
from typing import Annotated, Dict, Any, Optional

from app.core.event_bus import ProductionEventBus
from app.models.call_record import CallRecord, DialogEntry, DialogRecord
from app.models.device_info import Device
from app.services.base_service import BaseService
from app.services.redis_service import DeviceInfo, RedisService


# 使用统一日志管理器
from app.core.logger import get_logger

# 获取模块级别的logger
logger = get_logger(__name__)

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
        self.ws = None
        self.ws_thread = None
        self.should_stop = False
        self._is_running = False
        self.redis_service = redis_service

        self.call_id = None
        self.instance = None
        self.call_finished = False
        
        # 连接状态管理
        self._connection_event = threading.Event()
        self._connection_lock = threading.Lock()

        self.agent_hang_up = threading.Event()
        self.agent_hang_up.clear()
    
    async def initialize(self) -> bool:
        try:
            if not self._is_running:
                self._connect()
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
        
        await self._register_listener(EventType.CALL_OUT, self.handle_call_out)
        await self._register_listener(EventType.CALL_END, self.hang_up)
    
    def _connect(self):
        """建立 WebSocket 连接"""
        if self.ws is not None:
            logger.warning("WebSocket 连接已存在")
            return 
        
        try:
            self.ws = websocket.WebSocketApp(
                self.ws_url,
                on_open=self._on_open,
                on_message=self._on_message,
                on_error=self._on_error,
                on_close=self._on_close
            )
            
            def run_websocket():
                self.ws.run_forever()
            
            self.ws_thread = threading.Thread(target=run_websocket, daemon=True)
            self.ws_thread.start()
            
        except Exception as e:
            logger.error(f"建立 WebSocket 连接异常: {e}")

    def _on_open(self, ws):
        """连接建立"""
        self._connection_event.set()
        logger.info("PhoneService WebSocket 连接已建立")

    def _on_message(self, ws, message):
        """处理消息"""
        try:
            data = json.loads(message)
            logger.debug(f"收到消息: {data}")
            
            # 处理不同类型的消息
            notify_type = data.get("notify")
            
            if notify_type == "OnConnect":
                # 处理连接成功消息
                asyncio.run(self.handle_on_connect_message(data))
                logger.info("OnConnect 消息已处理")

            elif notify_type == "OnAnswer":
                # 处理接听事件
                on_message = OnMessageType(         
                    notify=notify_type,
                    id=data.get("id"),
                    instance=data.get("instance"),
                    uuid=data.get("uuid")
                )

                asyncio.run(self.redis_service.update_call_record_call_id(self.call_id, on_message.uuid))

                self.call_id = on_message.uuid
                self.instance = on_message.instance

                asyncio.run(self.redis_service.update_call_record_status(call_id=self.call_id, status="已接听"))

                record = asyncio.run(self.redis_service.get_call_record(self.call_id))

                tts_opening = record.tts_opening if record else ""

                dialog_record = DialogRecord(
                    call_id=self.call_id,
                    dialog_record=[DialogEntry(speaker="agent", content=tts_opening, timestamp=datetime.now().isoformat())],
                    dialog_record_reply_marking=0
                )

                asyncio.run(self.redis_service.create_dialog_record(dialog_record))

                tts_send_text = {
                    "text": tts_opening,
                    "call_id": self.call_id,
                    "instance": self.instance
                }

                asyncio.run(self.emit_event(EventType.TTS_SEND_TEXT, tts_send_text))
                asyncio.run(self.emit_event(EventType.RTASR_START, self.call_id))
                asyncio.run(asyncio.sleep(5))
                asyncio.run(self.emit_event(EventType.RTASR_START_AUDIO, self.call_id))

            elif notify_type == "OnCallOut":
                # 处理呼出事件
                pass
                
            elif notify_type == "OnCallIn":
                # 处理呼入事件
                pass

            elif notify_type == "OnHangUp":
                # 处理挂断事件
                if self.agent_hang_up.is_set():
                    self.agent_hang_up.clear()
                else:
                    logger.error(f"挂断事件，call_id: {self.call_id}, agent_hang_up: False")
                    asyncio.run(self.emit_event(EventType.CALL_END, {"call_id": self.call_id, "agent_hang_up": False}))
                asyncio.run(self.emit_event(EventType.AICALL_CALL_END, data={"call_id": self.call_id, "agent_hang_up": True}))    
            else:
                logger.debug(f"未知通知类型: {notify_type}")
                
        except json.JSONDecodeError as e:
            logger.error(f"解析消息为 JSON 失败: {e}")
        except Exception as e:
            logger.error(f"处理消息时出错: {e}")

    def _on_error(self, ws, error):
        """错误处理"""
        logger.error(f"PhoneService WebSocket 错误: {error}")

    def _on_close(self, ws, close_status_code, close_msg):
        """连接关闭"""
        self._connection_event.clear()
        logger.info(f"PhoneService WebSocket 连接关闭: {close_status_code}, 消息: {close_msg}")
    
    def is_service_running(self) -> bool:
        """检查服务是否正在运行"""
        return self._is_running and self.ws is not None and not self.should_stop
    
    def get_service_status(self) -> Dict[str, Any]:
        """获取服务状态信息"""
        return {
            "websocket_connected": self.ws is not None,
            "service_running": self.is_service_running(),
            "should_stop": self.should_stop,
            "ws_url": self.ws_url
        }
    
    def send_message(self, message_data: SendMessage) -> bool:
        """发送消息到 WebSocket 服务器"""
        if not self.ws:
            logger.error("WebSocket not connected")
            return False
        
        try:
            self.ws.send(message_data.model_dump_json(exclude_none=True))
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
            if not self.ws:
                logger.error("WebSocket not connected")
                return {
                    "success": False,
                    "phone_number": call_record.phone_number,
                    "error": "WebSocket未连接",
                    "message": "拨号失败，请检查连接状态"
                }

            self.send_message(dial_message)
            
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

        if event.data.get("agent_hang_up"):
            self.agent_hang_up.set()
            try:
                instance = self.instance

                hang_up_message = SendMessage(
                    method="terminateCall",
                    instance=instance
                )
                
                logger.info(f"Hanging up call on instance {instance}")
                
                self.send_message(hang_up_message)
                await self.emit_event(EventType.TTS_CALL_END, data={"call_id": self.call_id, "agent_hang_up": True})
                await self.emit_event(EventType.RECORD_CALL_END, data={"call_id": self.call_id, "agent_hang_up": True})
                await self.emit_event(EventType.RTASR_CALL_END, data={"call_id": self.call_id, "agent_hang_up": True})
                await self._call_finished()
                return True

            except Exception as e:
                logger.error(f"Error hanging up call on instance {e}")
                return False

        await self.emit_event(EventType.TTS_CALL_END, data={"call_id": self.call_id, "agent_hang_up": False})
        await self.emit_event(EventType.RECORD_CALL_END, data={"call_id": self.call_id, "agent_hang_up": False})
        await self.emit_event(EventType.RTASR_CALL_END, data={"call_id": self.call_id, "agent_hang_up": False})
        await self._call_finished()
        return True
    
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
    
    
    async def _call_finished(self):
        """通话结束"""
        self.call_finished = True
        self.call_id = None
        self.instance = None
    
    def stop(self):
        """停止 PhoneService"""
        logger.info("停止 PhoneService...")
        
        self.should_stop = True
        
        if self.ws:
            try:
                self.ws.close()
            except Exception as e:
                logger.warning("关闭 WebSocket 时出错: %s", e)
        
        if hasattr(self, 'ws_thread') and self.ws_thread and self.ws_thread.is_alive():
            self.ws_thread.join(timeout=5.0)
            if self.ws_thread.is_alive():
                logger.warning("WebSocket 线程未能在5秒内正常退出")
        
        self._is_running = False
        logger.info("PhoneService 已停止")
        