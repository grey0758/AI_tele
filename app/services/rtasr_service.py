# app/services/rtasr_service.py
import asyncio
import hashlib
import hmac
import base64
import json, time, threading
from websocket import create_connection
import websocket
from urllib.parse import quote
import pyaudio
from typing import Dict, Any, Optional
from app.core.config import settings
from app.core.event_bus import ProductionEventBus
from app.models.events import Event, EventListener, EventPriority, EventType
from app.models.call_record import DialogEntry
from datetime import datetime
from app.services.base_service import BaseService
from app.services.redis_service import RedisService

# 使用主应用的logger
from app.core.logger import get_logger

# 获取模块级别的logger
logger = get_logger(__name__)

class RtasrService(BaseService):
    """简化的 RTASR 客户端 - 专注于连接管理"""
    
    def __init__(self, event_bus: Optional[ProductionEventBus] = None, redis_service: Optional[RedisService] = None):
        super().__init__(event_bus, "RtasrService")
        self.redis_service = redis_service
        self.event_bus = event_bus
        self._is_ready = False
        self.call_id = None

        self.app_id = settings.rtasr_appid
        self.api_key = settings.rtasr_api_key
        
        # 连接状态
        self.ws = None
        self.ws_connected = False
        self.is_sending_audio = False
        self.silence_data = b'\x00' * 1280
        
        # 线程管理
        self.trecv = None
        self.audio_thread = None

        logger.info(f"RTASR客户端初始化完成，会话ID:")

    async def initialize(self):
        return True

    async def register_event_listeners(self):
        """推荐：直接注册模式"""
        if not self.event_bus:
            return
        
        await self._register_listener(EventType.RTASR_START, self.handle_rtasr_start, wait_for_result=False)
        await self._register_listener(EventType.RTASR_STOP, self.close, wait_for_result=False)
        await self._register_listener(EventType.RTASR_START_AUDIO, self.start_audio, wait_for_result=False)
        await self._register_listener(EventType.RTASR_STOP_AUDIO, self.stop_audio, wait_for_result=False)
        await self._register_listener(EventType.RTASR_CALL_FINISHED, self.reset_to_initialized_state, wait_for_result=False)

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

    async def handle_rtasr_start(self, event: Event) -> bool:
        self.call_id = event.data
        """创建WebSocket连接"""
        try:
            base_url = "ws://rtasr.xfyun.cn/v1/ws"
            ts = str(int(time.time()))
            tt = (self.app_id + ts).encode('utf-8')
            md5 = hashlib.md5()
            md5.update(tt)
            baseString = md5.hexdigest()
            baseString = bytes(baseString, encoding='utf-8')

            apiKey = self.api_key.encode('utf-8')
            signa = hmac.new(apiKey, baseString, hashlib.sha1).digest()
            signa = base64.b64encode(signa)
            signa = str(signa, 'utf-8')
            
            url = base_url + "?appid=" + self.app_id + "&ts=" + ts + "&signa=" + quote(signa) + "&engLangType=2"
            
            self.ws = create_connection(url, timeout=10, enable_multithread=False)
            self.ws_connected = True
            
            # 启动接收线程
            self.trecv = threading.Thread(target=self._recv_loop, daemon=True)
            self.trecv.start()
            
            # 启动音频发送线程
            self.audio_thread = threading.Thread(target=self._audio_loop, daemon=True)
            self.audio_thread.start()
            
            logger.info(f"RTASR连接创建成功")
            return True
            
        except Exception as e:
            logger.error(f"RTASR连接创建失败: {e}")
            return False

    def handle_rtasr_message(self, message_data: Dict):
        try:
            action = message_data.get("action")
            logger.debug(f"处理 RTASR 消息:{action}")
            
            if action == "started":
                # 处理启动消息
                logger.info(f"RTASR 启动成功:")
                
                return {"status": "success", "action": "started"}
                
            elif action == "result":
                # 处理识别结果
                data = message_data.get("data", "{}")
                data_json = json.loads(data)
                
                # 提取识别文本
                words = []
                for rt in data_json.get("cn", {}).get("st", {}).get("rt", []):
                    for ws in rt.get("ws", []):
                        for cw in ws.get("cw", []):
                            words.append(cw.get("w", ""))
                
                text = ''.join(words)
                msg_type = data_json.get("cn", {}).get("st", {}).get("type", "")
                
                if text.strip():
                    logger.info(f"RTASR 识别结果{msg_type} - {text}")
                    
                    # 处理识别结果
                    dialog_entry_data = DialogEntry(
                        speaker="user",
                        content=text,
                        timestamp= datetime.now().isoformat()
                    )

                def save_dialog_async():
                    try:
                        asyncio.run(
                            self.redis_service.add_dialog_record(self.call_id, dialog_entry_data)
                        )
                    except Exception as e:
                        logger.error(f"保存对话记录失败: {e}")
                
                # 启动后台线程，不等待完成
                threading.Thread(target=save_dialog_async, daemon=True).start()
                
                return {"status": "success", "action": "result", "text": text, "type": msg_type}
                
            elif action == "error":
                # 处理错误消息
                error_code = message_data.get("code", "")
                error_desc = message_data.get("desc", "")
                
                logger.error(f"RTASR 错误 [{error_code} - {error_desc}")
                
                return {"status": "error", "action": "error", "code": error_code, "desc": error_desc}
            else:
                logger.debug(f"未知 RTASR 消息类型: {action}")
                return {"status": "unknown", "action": action}
                
        except json.JSONDecodeError as e:
            logger.error(f"RTASR 消息 JSON 解析错误: {e}")
            return {"status": "json_error", "error": str(e)}
        except Exception as e:
            logger.error(f"处理 RTASR 消息错误: {e}")
            raise

    def _recv_loop(self):
        """接收消息循环"""
        try:
            while self.ws_connected and self.ws and self.ws.connected:
                try:
                    result = str(self.ws.recv())
                    if len(result) == 0:
                        break
                    logger.info(f"RTASR接收消息: {result}")
                    result_dict = json.loads(result)

                    if not self.call_id:    
                        logger.error("RTASR接收消息失败: call_id为空")
                        continue
                    
                    self.handle_rtasr_message(result_dict)
                        
                except websocket.WebSocketTimeoutException:
                    continue
                except Exception as e:
                    logger.error(f"RTASR接收异常: {e}")
                    break
                    
        except Exception as e:
            logger.error(f"RTASR接收线程异常: {e}")

    def _audio_loop(self):
        """音频发送循环"""
        p = None
        stream = None
        
        try:
            p = pyaudio.PyAudio()
            stream = p.open(
                format=pyaudio.paInt16,
                channels=1,
                rate=16000,
                input=True,
                frames_per_buffer=1280
            )
            
            while self.ws_connected and self.ws.connected:
                try:
                    if self.is_sending_audio:
                        chunk = stream.read(1280, exception_on_overflow=False)
                        self.ws.send(chunk)
                    else:
                        self.ws.send(self.silence_data)
                    
                    time.sleep(0.04)  # 40ms间隔
                    
                except Exception as e:
                    logger.error(f"RTASR音频发送异常: {e}")
                    break
                    
        except Exception as e:
            logger.error(f"RTASR音频初始化异常: {e}")
        finally:
            logger.info(f"RTASR音频发送线程结束")
            if stream:
                stream.stop_stream()
                stream.close()
            if p:
                p.terminate()

    def start_audio(self, event: Event):
        """开始发送音频"""
        self.is_sending_audio = True
        logger.info(f"RTASR开始发送音频")

    def stop_audio(self, event: Event):
        """停止发送音频"""
        self.is_sending_audio = False
        logger.info(f"RTASR停止发送音频")

    def close(self, event: Event):
        """关闭连接"""
        logger.info(f"RTASR开始关闭")
        self.ws_connected = False
        
        if self.ws and self.ws.connected:
            try:
                end_tag = "{\"end\": true}"
                self.ws.send(bytes(end_tag.encode('utf-8')))
                self.ws.close()
            except:
                pass
        
        logger.info(f"RTASR已关闭")

    def get_status(self) -> Dict[str, Any]:
        """获取状态"""
        return {
            "connected": self.ws_connected and self.ws and self.ws.connected,
            "is_sending_audio": self.is_sending_audio,
            "timestamp": datetime.now().isoformat()
        }

    def reset_to_initialized_state(self, event):
        """将RTASR服务重置到initialize完成后的状态"""
        logger.info("开始重置RTASR服务到initialize后状态...")
        
        try:
            # 1. 停止当前所有活动
            self.ws_connected = False
            self.is_sending_audio = False
            
            # 2. 关闭WebSocket连接
            if self.ws:
                try:
                    # 发送结束标记
                    if self.ws.connected:
                        end_tag = "{\"end\": true}"
                        self.ws.send(bytes(end_tag.encode('utf-8')))
                    self.ws.close()
                except Exception as e:
                    logger.warning(f"关闭RTASR WebSocket时出错: {e}")
                finally:
                    self.ws = None
            
            # 3. 等待接收线程结束
            if self.trecv and self.trecv.is_alive():
                self.trecv.join(timeout=3.0)
                if self.trecv.is_alive():
                    logger.warning("RTASR接收线程未能及时退出")
            
            # 4. 等待音频线程结束
            if self.audio_thread and self.audio_thread.is_alive():
                self.audio_thread.join(timeout=3.0)
                if self.audio_thread.is_alive():
                    logger.warning("RTASR音频线程未能及时退出")
            
            # 5. 重置所有属性到 initialize 后的状态
            self._reset_attributes_to_initialized()
            
            logger.info("RTASR服务已成功重置到initialize后状态")
            return True
            
        except Exception as e:
            logger.error(f"重置RTASR服务失败: {e}")
            return False

    def _reset_attributes_to_initialized(self):
        """重置所有属性到initialize完成后的状态"""
        
        # BaseService相关属性保持不变
        # self.event_bus, self.service_name 由父类管理，不重置
        
        # 构造函数参数保持不变
        # self.redis_service, self.event_bus, self.app_id, self.api_key 不重置
        
        # 重置会话相关状态
        self._is_ready = False  # 重置为初始化后的状态
        self.call_id = None
        
        # 重置连接状态
        self.ws = None
        self.ws_connected = False
        self.is_sending_audio = False
        
        # 重置线程引用
        self.trecv = None
        self.audio_thread = None
        
        # silence_data 保持不变，这是常量数据
        # self.silence_data = b'\x00' * 1280  # 保持不变
