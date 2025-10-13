"""电话服务"""
# app/services/phone_service.py
from typing import Annotated, Dict, Any, Optional
import threading
import asyncio
import time
from datetime import datetime
import json
import websocket
from pydantic import BaseModel, Field
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.date import DateTrigger
from app.models.events import Event
from app.models.events import EventType
from app.core.event_bus import ProductionEventBus
from app.models.call_record import DialogEntry, DialogRecord, CallRecord
from app.models.device_info import Device
from app.services.base_service import BaseService
from app.services.redis_service import DeviceInfo, RedisService
from app.core.config import settings


# 使用统一日志管理器
from app.core.logger import get_logger

# 获取模块级别的logger
logger = get_logger(__name__)


class SendMessage(BaseModel):
    """SendMessage 数据类"""

    method: Annotated[str, Field(description="方法")]
    instance: Annotated[int, Field(description="设备实例")]
    phone: Annotated[str | None, Field(default=None, description="电话号码")]
    CustomId: Annotated[
        str | None, Field(default=None, description="自定义ID", exclude=True)
    ]


class OnMessageType(BaseModel):
    """OnMessageType 数据类"""

    notify: Annotated[str | None, Field(default=None, description="通知类型")]
    id: Annotated[int | None, Field(default=None, description="设备ID")]
    instance: Annotated[int | None, Field(default=None, description="设备实例")]
    uuid: Annotated[str, Field(description="通话唯一标识")]


class PhoneService(BaseService):
    """电话服务"""

    def __init__(
        self,
        event_bus: Optional[ProductionEventBus] = None,
        redis_service: Optional[RedisService] = None,
    ):
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

        self.device_info = None

        # 连接状态管理
        self._connection_event = threading.Event()
        self._connection_lock = threading.Lock()

        self.agent_hang_up = threading.Event()
        self.agent_hang_up.clear()
        
        # 定时任务调度
        self._scheduler = AsyncIOScheduler()
        self._scheduler_started = False

    async def initialize(self) -> bool:
        try:
            if not self._is_running:
                self._connect()
                self._is_running = True
                logger.info("PhoneService 已启动")
                return True
            return True
        except Exception as e:  # pylint: disable=broad-except
            logger.error("PhoneService 启动失败 | error=%s", str(e))
            return False

    async def register_event_listeners(self):
        """推荐：直接注册模式"""
        if not self.event_bus:
            return

        await self._register_listener(EventType.PHONE_SERVICE_CALL_OUT, self.handle_call_out, timeout=30.0)
        await self._register_listener(EventType.PHONE_SERVICE_ONHANGUP, self._call_finished)
        await self._register_listener(EventType.PHONE_SERVICE_TERMINATECALL, self.hang_up, timeout=30.0)

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
                on_close=self._on_close,
            )

            def run_websocket():
                self.ws.run_forever()

            self.ws_thread = threading.Thread(target=run_websocket, daemon=True)
            self.ws_thread.start()

        except Exception as e:  # pylint: disable=broad-except
            logger.error("建立 WebSocket 连接异常: %s", e)

    def _on_open(self, ws):  # pylint: disable=unused-argument
        """连接建立"""
        self._connection_event.set()
        logger.info("PhoneService WebSocket 连接已建立")

    def _on_message(self, ws, message):  # pylint: disable=unused-argument
        """处理消息"""
        try:
            data = json.loads(message)
            logger.debug("收到消息: %s", data)

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
                    uuid=data.get("uuid"),
                )

                # 取消拨号超时定时器
                self._cancel_timer("call_timeout")
                logger.info("接听事件收到，取消拨号超时定时器")

                # 启动通话时长定时器（5分钟）
                self._start_timer("call_duration", 300, "call_duration_timeout")
                logger.info("启动通话时长定时器（5分钟）")

                asyncio.run(self.emit_event(EventType.PHONE_SERVICE_ONANSWER, on_message))

                asyncio.run(self.redis_service.update_call_record_call_id(self.call_id, on_message.uuid))

                self.call_id = on_message.uuid
                self.instance = on_message.instance

                asyncio.run(self.redis_service.update_call_record_status(call_id=self.call_id, status="已接听"))

                record = asyncio.run(self.redis_service.get_call_record(self.call_id))

                tts_opening = record.tts_opening if record else ""

                dialog_record = DialogRecord(
                    call_id=self.call_id,
                    dialog_record=[
                        DialogEntry(
                            speaker="agent",
                            content=tts_opening,
                            timestamp=datetime.now().isoformat(),
                        )
                    ]
                )

                asyncio.run(self.redis_service.create_dialog_record(dialog_record))

            elif notify_type == "OnCallOut":
                # 处理呼出事件
                pass

            elif notify_type == "OnCallIn":
                # 处理呼入事件
                pass

            elif notify_type == "OnHangUp":
                # 处理挂断事件
                agent_hang_up = False
                if self.agent_hang_up.is_set():
                    self.agent_hang_up.clear()
                    agent_hang_up = True
                self._cancel_timer("call_duration")
                logger.info("挂断事件收到，取消通话时长定时器")
                asyncio.run(self.redis_service.update_call_record_status(call_id=self.call_id, status="已挂断"))
                asyncio.run(self.redis_service.bind_dialog_record_to_call_record(call_id=self.call_id))
                asyncio.run(self.emit_event(EventType.PHONE_SERVICE_ONHANGUP,{"call_id": self.call_id, "instance": self.instance, "agent_hang_up": agent_hang_up}))
            else:
                logger.debug("未知通知类型: %s", notify_type)

        except json.JSONDecodeError as e:
            logger.error("解析消息为 JSON 失败: %s", e)
        except Exception as e:  # pylint: disable=broad-except
            logger.error("处理消息时出错: %s", e)

    def _on_error(self, ws, error):  # pylint: disable=unused-argument
        """错误处理"""
        logger.error("PhoneService WebSocket 错误: %s", error)

    def _on_close(self, ws, close_status_code, close_msg):  # pylint: disable=unused-argument
        """连接关闭"""
        self._connection_event.clear()
        logger.info(
            "PhoneService WebSocket 连接关闭: %s, 消息: %s",
            close_status_code,
            close_msg,
        )

    def is_service_running(self) -> bool:
        """检查服务是否正在运行"""
        return self._is_running and self.ws is not None and not self.should_stop

    def get_service_status(self) -> Dict[str, Any]:
        """获取服务状态信息"""
        return {
            "websocket_connected": self.ws is not None,
            "service_running": self.is_service_running(),
            "should_stop": self.should_stop,
            "ws_url": self.ws_url,
        }

    def send_message(self, message_data: SendMessage) -> bool:
        """发送消息到 WebSocket 服务器"""
        if not self.ws:
            logger.error("WebSocket not connected")
            return False

        try:
            self.ws.send(message_data.model_dump_json(exclude_none=True))
            logger.debug("Sent message: %s", message_data)
            return True
        except Exception as e:  # pylint: disable=broad-except
            logger.error("Failed to send message: %s", e)
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
            if not event.data:
                logger.error("Event data is None")
                return {
                    "success": False,
                    "phone_number": None,
                    "error": "Event data is None",
                    "message": "拨号失败，请检查事件数据",
                }

            call_record : CallRecord = event.data
            self.call_id = call_record.call_id
            self.instance = self.device_info.devices[call_record.instance].instance

            # 构建拨号消息
            dial_message = SendMessage(
                method="call",
                instance=self.instance,
                phone=call_record.phone_number,
                CustomId=call_record.custom_id,
            )

            logger.info("Dialing %s with instance %s", call_record.phone_number, call_record.instance,)

            # 检查WebSocket连接状态
            if not self.ws:
                logger.error("WebSocket not connected")
                return {
                    "success": False,
                    "phone_number": call_record.phone_number,
                    "error": "WebSocket未连接",
                    "message": "拨号失败，请检查连接状态",
                }

            self.send_message(dial_message)

            call_record.status = "already_dialed"
            call_record.start_time = datetime.now()
            await self.redis_service.update_call_record(call_record)

            # 启动15秒定时器，如果超时则挂断电话
            self._start_timer("call_timeout", 15, "call_timeout")

            logger.info("拨号消息已准备: %s", dial_message)

            return {
                "success": True,
                "phone_number": call_record.phone_number,
                "instance": call_record.instance,
                "custom_id": call_record.custom_id,
                "message": f"拨号请求已准备: {call_record.phone_number}",
            }

        except Exception as e:  # pylint: disable=broad-except
            logger.error("Error dialing phone %s: %s", call_record.phone_number, e)
            call_record.status = "无法拨打"
            call_record.end_time = datetime.now()
            call_record.duration = 0
            call_record.notes = str(e)
            await self.redis_service.update_call_record(call_record)
            return {
                "success": False,
                "phone_number": call_record.phone_number,
                "error": str(e),
                "message": f"拨号异常: {str(e)}",
            }

    async def hang_up(self, event: Event = None) -> bool:
        """挂断电话"""
        
        hang_up_message = SendMessage(method="terminateCall", instance=self.instance if self.instance else settings.instance)
        if event.data.get("terminate_type") == "chat_ended":
            await asyncio.sleep(7)
        self.agent_hang_up.set()
        self.send_message(hang_up_message)
        logger.info("Hanging up call on instance %s", self.instance)
        return True

    async def handle_on_connect_message(self, message_data: Dict) -> Dict[str, Any]:
        """处理连接消息"""
        try:
            logger.info("Processing OnConnect message")

            device_info = DeviceInfo(
                notify=message_data.get("notify"),
                devid=message_data.get("devid"),
                version=message_data.get("version"),
                recordmode=message_data.get("recordmode", 0),
                devices=[
                    Device(**device) for device in message_data.get("devices", [])
                ],
            )

            self.device_info = device_info

            await self.redis_service.set_device_info(device_info)

            return {
                "success": True,
                "message": "连接成功，设备信息已保存",
                "device_info": device_info.model_dump_json(),
            }

        except Exception as e:  # pylint: disable=broad-except
            logger.error("Error handling OnConnect message: %s", e)
            return {"success": False, "error": str(e), "message": "处理连接消息失败"}

    async def _call_finished(self, _: Event = None):
        """通话结束"""
        self.call_finished = True
        self.call_id = None
        self.instance = None
        await self.emit_event(EventType.PHONE_SERVICE_ONHANGUP_AUTO_CALL)

    def stop(self):
        """停止 PhoneService"""
        logger.info("停止 PhoneService...")

        self.should_stop = True

        if self.ws:
            try:
                self.ws.close()
            except Exception as e:  # pylint: disable=broad-except
                logger.warning("关闭 WebSocket 时出错: %s", e)

        if hasattr(self, "ws_thread") and self.ws_thread and self.ws_thread.is_alive():
            self.ws_thread.join(timeout=5.0)
            if self.ws_thread.is_alive():
                logger.warning("WebSocket 线程未能在5秒内正常退出")

        # 清理定时任务
        if hasattr(self, "_scheduler") and self._scheduler_started:
            try:
                self._scheduler.shutdown(wait=False)
            except Exception as e:  # pylint: disable=broad-except
                logger.warning("关闭调度器时出错: %s", e)

        self._is_running = False
        logger.info("PhoneService 已停止")

    def _ensure_scheduler(self):
        if not self._scheduler_started:
            try:
                self._scheduler.start()
                self._scheduler_started = True
            except Exception as e:  # pylint: disable=broad-except
                logger.error("启动调度器失败: %s", e)

    def _start_timer(self, timer_id: str, duration: int, terminate_type: str):
        self._ensure_scheduler()
        try:
            run_date = datetime.fromtimestamp(time.time() + duration)
            if self._scheduler.get_job(timer_id):
                self._scheduler.remove_job(timer_id)
            self._scheduler.add_job(
                self._handle_timer_timeout,
                trigger=DateTrigger(run_date=run_date),
                id=timer_id,
                args=[terminate_type],
                replace_existing=True,
                coalesce=True,
                misfire_grace_time=5,
            )
            logger.info("启动定时器: %s, 时长: %d秒", timer_id, duration)
        except Exception as e:  # pylint: disable=broad-except
            logger.error("添加定时任务失败: %s", e)

    def _cancel_timer(self, timer_id: str):
        try:
            if self._scheduler.get_job(timer_id):
                self._scheduler.remove_job(timer_id)
                logger.info("取消定时器: %s", timer_id)
        except Exception as e:  # pylint: disable=broad-except
            logger.warning("取消定时器失败: %s", e)

    # 移除自研轮询管理线程，改由调度器触发

    async def _handle_timer_timeout(self, terminate_type: str):
        try:
            await self.emit_event(EventType.PHONE_SERVICE_TERMINATECALL, {"terminate_type": terminate_type})
        except Exception as e:  # pylint: disable=broad-except
            logger.warning("无法发送超时事件，直接发送挂断消息: %s", e)
            hang_up_message = SendMessage(method="terminateCall", instance=self.instance if self.instance else settings.instance)
            self.send_message(hang_up_message)
