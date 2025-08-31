# app/services/phone_service.py
import asyncio
import json
import logging
import uuid
from typing import Dict, List, Optional, Callable
from websockets import connect
from websockets.exceptions import ConnectionClosed

logger = logging.getLogger(__name__)

class PhoneDevice:
    """电话设备信息"""
    def __init__(self, device_data: Dict):
        self.id = device_data.get("id", 0)
        self.instance = device_data.get("instance", 0)
        self.model = device_data.get("model", 1)
        self.bt_connect = device_data.get("btConnect", 0)
        self.bt_device_name = device_data.get("btDeviceName", "")
        self.phone_name = device_data.get("phoneName", "")
        self.device_id = device_data.get("deviceId", "")
        self.user_id = device_data.get("userId", "")
        self.firmware_ver = device_data.get("firmWareVer", "")
        self.valid = device_data.get("valid", 0)
        self.error = device_data.get("error", "")
    
    def to_dict(self) -> Dict:
        """转换为字典"""
        return {
            'id': self.id,
            'instance': self.instance,
            'model': self.model,
            'bt_connect': self.bt_connect,
            'bt_device_name': self.bt_device_name,
            'phone_name': self.phone_name,
            'device_id': self.device_id,
            'user_id': self.user_id,
            'firmware_ver': self.firmware_ver,
            'valid': self.valid,
            'error': self.error
        }

class PhoneService:
    """电话控制器 - 使用内存管理状态"""
    
    def __init__(self, connection_id: str = None):
        self.connection_id = connection_id or str(uuid.uuid4())
        self.ws_url = "ws://127.0.0.1:9898/ws"
        self.websocket = None
        self.reconnect_interval = 5
        self.max_reconnect_attempts = 10
        
        # 控制状态
        self.should_stop = False
        self._connection_task = None
        self._listener_task = None
        
        # 内存状态存储
        self._connection_state = {
            'is_connected': False,
            'reconnect_attempts': 0,
            'started_at': None,
            'connected_at': None,
            'last_error': None
        }
        self._devices = {}
        self._call_states = {}
        
        # 事件回调
        self._event_callbacks = []
    
    def add_event_callback(self, callback: Callable):
        """添加事件回调函数"""
        self._event_callbacks.append(callback)
    
    async def _publish_event(self, event_type: str, data: Dict):
        """发布事件到所有回调函数"""
        for callback in self._event_callbacks:
            try:
                await callback(event_type, data)
            except Exception as e:
                logger.error(f"Event callback error: {e}")
    
    async def update_connection_state(self, updates: Dict):
        """更新连接状态"""
        self._connection_state.update(updates)
    
    async def get_connection_state(self) -> Dict:
        """获取连接状态"""
        return self._connection_state.copy()
    
    async def set_device_info(self, device_id: str, device_data: Dict):
        """设置设备信息"""
        self._devices[device_id] = device_data
    
    async def get_device_info(self, device_id: str) -> Optional[Dict]:
        """获取设备信息"""
        return self._devices.get(device_id)
    
    async def get_all_devices(self) -> Dict:
        """获取所有设备信息"""
        return self._devices.copy()
    
    async def set_call_state(self, call_uuid: str, call_data: Dict):
        """设置通话状态"""
        self._call_states[call_uuid] = call_data
    
    async def get_call_state(self, call_uuid: str) -> Optional[Dict]:
        """获取通话状态"""
        return self._call_states.get(call_uuid)
    
    async def start_async(self):
        """异步启动电话控制器"""
        if self._connection_task and not self._connection_task.done():
            logger.warning("Phone controller is already starting/running")
            return
            
        logger.info(f"Starting phone controller with connection_id: {self.connection_id}")
        
        # 初始化连接状态
        await self.update_connection_state({
            'is_connected': False,
            'reconnect_attempts': 0,
            'started_at': asyncio.get_event_loop().time()
        })
        
        # 创建后台连接任务
        self._connection_task = asyncio.create_task(self._connect_with_retry())
        
        # 发布启动事件
        await self._publish_event('controller_started', {
            'connection_id': self.connection_id
        })
        
        logger.info("Phone controller start task created")
    
    async def _connect_with_retry(self):
        """带重试的连接逻辑"""
        state = await self.get_connection_state()
        
        while not self.should_stop and state['reconnect_attempts'] < self.max_reconnect_attempts:
            try:
                await self._connect_once()
                
                # 连接成功后启动监听任务
                if (await self.get_connection_state())['is_connected']:
                    self._listener_task = asyncio.create_task(self._listen_messages())
                    await self._listener_task
                    
            except Exception as e:
                logger.error(f"Connection attempt failed: {e}")
                state = await self.get_connection_state()
                await self.update_connection_state({
                    'is_connected': False,
                    'reconnect_attempts': state['reconnect_attempts'] + 1,
                    'last_error': str(e)
                })
                
                state = await self.get_connection_state()
                if state['reconnect_attempts'] < self.max_reconnect_attempts:
                    logger.info(f"Retrying connection in {self.reconnect_interval}s... "
                              f"(attempt {state['reconnect_attempts']}/{self.max_reconnect_attempts})")
                    await asyncio.sleep(self.reconnect_interval)
                else:
                    logger.error("Max reconnection attempts reached")
                    await self._publish_event('max_reconnect_reached', {
                        'connection_id': self.connection_id
                    })
                    break
    
    async def _connect_once(self):
        """单次连接尝试"""
        logger.info(f"Connecting to WebSocket server: {self.ws_url}")
        
        try:
            self.websocket = await connect(self.ws_url)
            await self.update_connection_state({
                'is_connected': True,
                'connected_at': asyncio.get_event_loop().time(),
                'reconnect_attempts': 0
            })
            
            logger.info("WebSocket connected successfully")
            
            # 发布连接成功事件
            await self._publish_event('websocket_connected', {
                'connection_id': self.connection_id
            })
                    
        except Exception as e:
            logger.error(f"WebSocket connection failed: {e}")
            await self.update_connection_state({
                'is_connected': False,
                'last_error': str(e)
            })
            raise
    
    async def _listen_messages(self):
        """监听WebSocket消息"""
        logger.info("Starting message listener")
        
        try:
            async for message in self.websocket:
                if self.should_stop:
                    break
                    
                try:
                    await self._handle_message(message)
                except Exception as e:
                    logger.error(f"Error handling message: {e}")
                    
        except ConnectionClosed:
            logger.info("WebSocket connection closed")
            await self.update_connection_state({
                'is_connected': False,
                'last_error': 'Connection closed'
            })
        except Exception as e:
            logger.error(f"Message listener error: {e}")
            await self.update_connection_state({
                'is_connected': False,
                'last_error': str(e)
            })
        finally:
            # 发布断开连接事件
            await self._publish_event('websocket_disconnected', {
                'connection_id': self.connection_id
            })
    
    async def _handle_message(self, message):
        """处理接收到的消息"""
        try:
            data = json.loads(message)
            logger.debug(f"Received message: {data}")
            
            # 发布消息接收事件
            await self._publish_event('message_received', {
                'connection_id': self.connection_id,
                'data': data
            })
            
            # 处理不同类型的消息
            msg_type = data.get('type')
            
            if msg_type == 'device_list':
                await self._handle_device_list(data)
            elif msg_type == 'call_out':
                await self._handle_call_out(data)
            elif msg_type == 'call_in':
                await self._handle_call_in(data)
            elif msg_type == 'hang_up':
                await self._handle_hang_up(data)
            elif msg_type == 'message':
                await self._handle_message_data(data)
            else:
                logger.debug(f"Unknown message type: {msg_type}")
                
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse message as JSON: {e}")
        except Exception as e:
            logger.error(f"Error handling message: {e}")
    
    async def _handle_device_list(self, data):
        """处理设备列表消息"""
        devices = data.get('devices', [])
        
        for device_data in devices:
            device = PhoneDevice(device_data)
            device_id = device.device_id or str(device.id)
            
            # 存储设备信息到内存
            await self.set_device_info(device_id, device.to_dict())
        
        # 发布设备更新事件
        await self._publish_event('devices_updated', {
            'connection_id': self.connection_id,
            'device_count': len(devices)
        })
    
    async def _handle_call_out(self, data):
        """处理呼出消息"""
        call_uuid = data.get('call_uuid')
        if not call_uuid:
            logger.warning("Call out message missing call_uuid")
            return
        
        # 存储通话状态到内存
        await self.set_call_state(call_uuid, {
            'type': 'outgoing',
            'status': 'initiated',
            'timestamp': asyncio.get_event_loop().time(),
            'data': data
        })
        
        # 发布呼出事件
        await self._publish_event('call_out', {
            'connection_id': self.connection_id,
            'call_uuid': call_uuid,
            'data': data
        })
    
    async def _handle_call_in(self, data):
        """处理呼入消息"""
        call_uuid = data.get('call_uuid')
        if not call_uuid:
            logger.warning("Call in message missing call_uuid")
            return
        
        # 存储通话状态到内存
        await self.set_call_state(call_uuid, {
            'type': 'incoming',
            'status': 'ringing',
            'timestamp': asyncio.get_event_loop().time(),
            'data': data
        })
        
        # 发布呼入事件
        await self._publish_event('call_in', {
            'connection_id': self.connection_id,
            'call_uuid': call_uuid,
            'data': data
        })
    
    async def _handle_hang_up(self, data):
        """处理挂断消息"""
        call_uuid = data.get('call_uuid')
        if not call_uuid:
            logger.warning("Hang up message missing call_uuid")
            return
        
        # 更新通话状态
        call_state = await self.get_call_state(call_uuid)
        if call_state:
            call_state['status'] = 'ended'
            call_state['end_timestamp'] = asyncio.get_event_loop().time()
            await self.set_call_state(call_uuid, call_state)
        
        # 发布挂断事件
        await self._publish_event('hang_up', {
            'connection_id': self.connection_id,
            'call_uuid': call_uuid,
            'data': data
        })
    
    async def _handle_message_data(self, data):
        """处理消息数据"""
        # 发布消息发送事件
        await self._publish_event('message_sent', {
            'connection_id': self.connection_id,
            'data': data
        })
    
    async def send_message(self, message_data: Dict):
        """发送消息到WebSocket服务器"""
        if not self.websocket or not (await self.get_connection_state())['is_connected']:
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
    
    async def get_status(self) -> Dict:
        """获取控制器状态"""
        connection_state = await self.get_connection_state()
        devices = await self.get_all_devices()
        
        return {
            'connection_id': self.connection_id,
            'connection_state': connection_state,
            'device_count': len(devices),
            'call_count': len(self._call_states),
            'is_running': self._connection_task and not self._connection_task.done()
        }
    
    async def stop(self):
        """停止控制器"""
        logger.info(f"Stopping phone controller: {self.connection_id}")
        
        self.should_stop = True
        
        # 取消任务
        if self._connection_task and not self._connection_task.done():
            self._connection_task.cancel()
        
        if self._listener_task and not self._listener_task.done():
            self._listener_task.cancel()
        
        # 关闭WebSocket连接
        if self.websocket:
            try:
                await self.websocket.close()
            except Exception as e:
                logger.error(f"Error closing WebSocket: {e}")
        
        # 发布停止事件
        await self._publish_event('controller_stopped', {
            'connection_id': self.connection_id
        })
        
        logger.info(f"Phone controller stopped: {self.connection_id}")
    
    async def wait_for_stop(self):
        """等待控制器停止"""
        if self._connection_task:
            try:
                await self._connection_task
            except asyncio.CancelledError:
                pass
        
        if self._listener_task:
            try:
                await self._listener_task
            except asyncio.CancelledError:
                pass
