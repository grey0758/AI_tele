# app/services/phone_service.py
import asyncio
import json
import logging
import uuid
from typing import Dict, List, Optional, Any
from websockets import connect
from websockets.exceptions import ConnectionClosed
from app.services.celery_service import celery_app
from app.services.redis_service import redis_service, DeviceInfo, Device, CallRecord

logger = logging.getLogger(__name__)

class PhoneService:
    """简化的电话控制器 - 使用 Celery 任务管理"""
    
    def __init__(self):
        self.ws_url = "ws://127.0.0.1:9898/ws"
        self.websocket = None
        self.should_stop = False
        self._service_task = None
        
        # 实例化时自动启动服务
        asyncio.create_task(self._start_service())
    
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
        return self.websocket is not None and not self.should_stop
    
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
    
    async def dial_phone(self, phone_number: str, instance: int = 0, custom_id: str = None) -> Dict[str, Any]:
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
            dial_message = {
                "method": "call",
                "instance": instance,
                "phone": phone_number
            }
            
            # 如果提供了自定义ID，添加到消息中
            if custom_id:
                dial_message["CustomId"] = custom_id
            
            logger.info(f"Dialing {phone_number} with instance {instance}")
            
            # 发送拨号消息
            success = await self.send_message(dial_message)
            
            if success:
                return {
                    "success": True,
                    "phone_number": phone_number,
                    "instance": instance,
                    "custom_id": custom_id,
                    "message": f"拨号请求已发送: {phone_number}"
                }
            else:
                return {
                    "success": False,
                    "phone_number": phone_number,
                    "error": "发送拨号消息失败",
                    "message": "拨号失败，请检查连接状态"
                }
                
        except Exception as e:
            logger.error(f"Error dialing phone {phone_number}: {e}")
            return {
                "success": False,
                "phone_number": phone_number,
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
                        
                    elif notify_type == "OnCallOut":
                        # 处理呼出事件
                        handle_call_out.delay(data)
                        
                    elif notify_type == "OnCallIn":
                        # 处理呼入事件
                        handle_call_in.delay(data)

                    elif notify_type == "OnHangUp":
                        # 处理挂断事件
                        handle_hang_up.delay(data)
                        
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

# ==================== Celery 任务 ====================

@celery_app.task(queue='phone_queue')
def call_phone(phone_number: str, instance: int = 0, custom_id: str = None):
    """拨打电话"""
    try:
        phone_service.dial_phone(phone_number, instance, custom_id)
    except Exception as e:
        logger.error(f"Error calling phone: {e}")
        raise

@celery_app.task(queue='phone_queue')
def handle_call_out(message_data: Dict):
    """处理呼出事件"""
    try:
        call_uuid = message_data.get('call_uuid')
        if not call_uuid:
            logger.warning("Call out message missing call_uuid")
            return {"status": "error", "message": "Missing call_uuid"}

        call_data = CallRecord(
            call_id=call_uuid,
            phone_number=message_data.get('phone_number'),
            call_type='outgoing',
            status='initiated',
            data=message_data
        )
        
        save_call_state.delay(call_uuid, call_data)
        
        logger.info(f"Call out initiated: {call_uuid}")
        return {"status": "success", "call_uuid": call_uuid}
    except Exception as e:
        logger.error(f"Error handling call out: {e}")
        raise

@celery_app.task(queue='phone_queue')
def handle_call_in(message_data: Dict):
    """处理呼入事件"""
    try:
        call_uuid = message_data.get('call_uuid')
        if not call_uuid:
            logger.warning("Call in message missing call_uuid")
            return {"status": "error", "message": "Missing call_uuid"}
        
        call_data = CallRecord(
            call_id=call_uuid,
            phone_number=message_data.get('phone_number'),
            call_type='incoming',
            status='ringing',
            data=message_data
        )

        save_call_state.delay(call_uuid, call_data)
        
        logger.info(f"Call in received: {call_uuid}")
        return {"status": "success", "call_uuid": call_uuid}
    except Exception as e:
        logger.error(f"Error handling call in: {e}")
        raise

@celery_app.task(queue='phone_queue')
def handle_hang_up(message_data: Dict):
    """处理挂断事件"""
    try:
        call_uuid = message_data.get('call_uuid')
        if not call_uuid:
            logger.warning("Hang up message missing call_uuid")
            return {"status": "error", "message": "Missing call_uuid"}
        
        call_data = CallRecord(
            call_id=call_uuid,
            phone_number=message_data.get('phone_number'),
            call_type='outgoing',
            status='ended',
            data=message_data
        )

        update_call_state.delay(call_uuid, call_data)
        
        logger.info(f"Call ended: {call_uuid}")
        return {"status": "success", "call_uuid": call_uuid}
    except Exception as e:
        logger.error(f"Error handling hang up: {e}")
        raise

# ==================== 数据存储任务 ====================

@celery_app.task(queue='phone_queue')
def save_call_state(call_uuid: str, call_data: Dict):
    """保存通话状态"""
    try:
        redis_service.create_call_record(call_uuid, call_data)
        logger.debug(f"Saving call state: {call_uuid}")
        return {"status": "saved", "call_uuid": call_uuid}
    except Exception as e:
        logger.error(f"Error saving call state: {e}")
        raise

@celery_app.task(queue='phone_queue')
def update_call_state(call_uuid: str, update_data: Dict):
    """更新通话状态"""
    try:
        redis_service.update_call_state(call_uuid, update_data)
        logger.debug(f"Updating call state: {call_uuid}")
        return {"status": "updated", "call_uuid": call_uuid}
    except Exception as e:
        logger.error(f"Error updating call state: {e}")
        raise