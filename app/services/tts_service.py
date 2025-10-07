# app/services/tts_service.py
import asyncio
from calendar import c
import copy
import ctypes
from math import log
from re import S
import websocket
import hashlib
import base64
import hmac
import json
from urllib.parse import urlencode
import time
import ssl
from wsgiref.handlers import format_date_time
from datetime import datetime
from time import mktime
import pyaudio
import threading
from collections import deque
from typing import Dict, Any, Optional
from enum import Enum
from app.core.config import settings
from app.core.event_bus import ProductionEventBus
from app.models.call_record import DialogEntry
from app.models.events import Event, EventType
from app.core.logger import get_logger
from app.services.base_service import BaseService
from app.services.redis_service import RedisService

logger = get_logger(__name__)

class ConnectionState(Enum):
    """WebSocket连接状态枚举"""
    DISCONNECTED = "disconnected"
    CONNECTING = "connecting" 
    CONNECTED = "connected"
    RECONNECTING = "reconnecting"
    FAILED = "failed"

class AudioBuffer:
    """音频缓冲区管理类"""
    def __init__(self, max_size: int = 1000):
        self.buffer = deque(maxlen=max_size)
        self.lock = threading.RLock()  # 使用RLock避免死锁
        self.condition = threading.Condition(self.lock)  # 添加条件变量
        self.is_active = True
        self._stats = {
            'chunks_added': 0,
            'chunks_played': 0,
            'buffer_overflows': 0,
            'clear_count': 0
        }
        
    def put(self, audio_data: bytes) -> None:
        """添加音频数据到缓冲区"""
        with self.condition:
            if self.is_active and audio_data:
                if len(self.buffer) >= self.buffer.maxlen - 1:
                    self._stats['buffer_overflows'] += 1
                    logger.warning(f"音频缓冲区接近满载: {len(self.buffer)}")
                
                self.buffer.append(audio_data)
                self._stats['chunks_added'] += 1
                self.condition.notify()
                
    def get(self, timeout: float = 0.1) -> Optional[bytes]:
        """支持超时等待的获取方法"""
        with self.condition:
            if not self.buffer and self.is_active:
                self.condition.wait(timeout)
            
            if self.buffer:
                data = self.buffer.popleft()
                self._stats['chunks_played'] += 1
                return data
            return None
            
    def clear(self):
        """清空缓冲区"""
        with self.condition:
            cleared_count = len(self.buffer)
            self.buffer.clear()
            self._stats['clear_count'] += 1
            if cleared_count > 0:
                logger.debug(f"清空音频缓冲区，丢弃 {cleared_count} 个音频块")
            
    def size(self) -> int:
        """获取缓冲区大小"""
        with self.lock:
            return len(self.buffer)
            
    def is_empty(self) -> bool:
        """检查缓冲区是否为空"""
        with self.lock:
            return len(self.buffer) == 0
            
    def deactivate(self):
        """停用缓冲区"""
        with self.condition:
            self.is_active = False
            self.buffer.clear()
            self.condition.notify_all()
            
    def reactivate(self):
        """重新激活缓冲区"""
        with self.lock:
            self.is_active = True
            
    def get_stats(self) -> Dict[str, int]:
        """获取缓冲区统计信息"""
        with self.lock:
            return {
                **self._stats,
                'current_size': len(self.buffer),
                'max_size': self.buffer.maxlen,
                'utilization': len(self.buffer) / self.buffer.maxlen * 100
            }
    
    def reset_to_initialized_state(self, event):
        """重置缓冲区到刚创建时的状态"""
        with self.condition:
            self.buffer.clear()
            
            self.is_active = True
            
            self._stats = {
                'chunks_added': 0,
                'chunks_played': 0,
                'buffer_overflows': 0,
                'clear_count': 0
            }
            
            self.condition.notify_all()

class TtsConfig:
    """TTS配置类"""
    def __init__(self):
        self.appid = settings.tts_appid_c
        self.api_key = settings.tts_apikey_c
        self.api_secret = settings.tts_apisecret_c
        self.host = "cbm01.cn-huabei-1.xf-yun.com"
        self.path = "/v1/private/mcd9m97e6"
        
        # 业务参数
        self.business_params = {
            "oral": {"oral_level": "high", "spark_assist": 1},
            "tts": {
                "vcn": "x5_lingyuyan_flow",
                "volume": 50, "speed": 55, "pitch": 50,
                "audio": {
                    "encoding": "raw", "sample_rate": 24000,
                    "channels": 1, "bit_depth": 16,"frame_size":1024
                }
            }
        }
    
    def create_url(self):
        """生成认证URL"""
        now = datetime.now()
        date = format_date_time(mktime(now.timetuple()))
        
        signature_origin = f"host: {self.host}\ndate: {date}\nGET {self.path} HTTP/1.1"
        signature_sha = hmac.new(
            self.api_secret.encode('utf-8'), 
            signature_origin.encode('utf-8'),
            digestmod=hashlib.sha256
        ).digest()
        signature_sha = base64.b64encode(signature_sha).decode('utf-8')
        
        authorization_origin = (
            f'api_key="{self.api_key}", algorithm="hmac-sha256", '
            f'headers="host date request-line", signature="{signature_sha}"'
        )
        authorization = base64.b64encode(authorization_origin.encode('utf-8')).decode('utf-8')
        
        params = urlencode({
            "authorization": authorization,
            "date": date,
            "host": self.host
        })
        
        return f"wss://{self.host}{self.path}?{params}"

class TtsService(BaseService):
    """TTS服务 - 集成智能音频缓冲器"""
    
    def __init__(self, event_bus: ProductionEventBus, redis_service: RedisService):
        super().__init__(event_bus, "TtsService")
        self.event_bus = event_bus
        self.redis_service = redis_service
        self.config = TtsConfig()

        # WebSocket连接状态管理
        self.ws = None
        self.ws_thread = None
        self.audio_player = None
        self.connection_state = ConnectionState.DISCONNECTED
        self._connection_event = threading.Event()
        
        # 会话状态
        self.seq = 0
        self.call_hang_up = threading.Event()
        self.call_hang_up.clear()
        self.call_is_ended = threading.Event()
        self.call_is_ended.clear()
        
        self.instance = None
        self.call_id = None
        
        # 线程锁
        self.lock = threading.Lock()
    
    async def initialize(self) -> bool:
        """初始化服务"""
        try:
            self.audio_player = SmartAudioPlayer(redis_service=self.redis_service, tts_service=self)
            self.audio_player.initialize()
            logger.info("TTS服务启动成功")
            return True
        except Exception as e:
            logger.error("TTS服务启动失败: %s", e)
            return False
    
    async def register_event_listeners(self):
        """注册事件监听器"""
        if self.event_bus:
            await self._register_listener(EventType.TTS_SEND_TEXT, self.handle_tts_send_text)
            await self._register_listener(EventType.TTS_CONNECT, self._connect)
            await self._register_listener(EventType.TTS_CALL_END, self.end_call)

    def _connect(self, _: Event = None):
        if self.connection_state in [ConnectionState.CONNECTING, ConnectionState.CONNECTED]:
            logger.warning("连接已存在或正在连接中")
            return 
        
        self.connection_state = ConnectionState.CONNECTING
            
        try:
            url = self.config.create_url()
            self.ws = websocket.WebSocketApp(
                url,
                on_open=self._on_open,
                on_message=self._on_message,
                on_error=self._on_error,
                on_close=self._on_close
            )
            
            def run_websocket():
                self.ws.run_forever(sslopt={"cert_reqs": ssl.CERT_NONE})
            
            self.ws_thread = threading.Thread(target=run_websocket, daemon=True)
            self.ws_thread.start()
            
        except Exception as e:
            logger.error(f"建立连接异常: {e}")
            self.connection_state = ConnectionState.FAILED

    
    def _on_open(self, ws):
        """连接建立"""
        self.connection_state = ConnectionState.CONNECTED
        self._connection_event.set()
        logger.info("TTS WebSocket连接已建立")

    def _on_message(self, ws, message):
        """处理消息"""
        try:  
            message_data = json.loads(message)
            code = message_data["header"]["code"]
            sid = message_data["header"]["sid"]
            
            if code != 0:
                errMsg = message_data.get("message", "Unknown error")
                logger.error(f"TTS错误 - sid:{sid}, code:{code}, message:{errMsg}")
                return
            
            message_copy = copy.deepcopy(message_data)
            if "payload" in message_copy and "audio" in message_copy["payload"]:
                message_copy["payload"].pop("audio")
            #计时结束
            logger.warning("收到响应结束计时%s", time.time())
            logger.debug(f"收到响应: {json.dumps(message_copy, indent=2, ensure_ascii=False)}")
            
            if "payload" in message_data and "audio" in message_data["payload"]:
                audio_data = message_data["payload"]["audio"]["audio"]
                if audio_data:
                    audio_bytes = base64.b64decode(audio_data)
                    self.audio_player.add_audio(audio_bytes)
                    logger.debug(f"添加音频数据到缓冲区: {len(audio_bytes)} bytes")
                
                status = message_data["payload"]["audio"]["status"]
                if status == 2:
                    self.ws.close()
                    asyncio.run(self.emit_event(EventType.TTS_CONNECT, None))
                    logger.info("TTS合成完成，WebSocket连接已关闭")
                    
        except Exception as e:
            logger.error(f"处理WebSocket消息异常: {e}")
    
    def _on_error(self, ws, error):
        """错误处理"""
        logger.error(f"TTS WebSocket错误: {error}")
        self.connection_state = ConnectionState.DISCONNECTED
    
    def _on_close(self, ws, close_status_code, close_msg):
        self.connection_state = ConnectionState.DISCONNECTED
        self._connection_event.clear()
        logger.info(f"TTS WebSocket连接关闭: {close_status_code}, 消息: {close_msg}")
    
    
    def _create_frame(self, text: str) -> dict:
        """创建数据帧"""
        common_args = {"app_id": self.config.appid, "status": 2}
        
        business_args = {
            "oral": {
                "oral_level": "mid",
                "spark_assist": 1,
                "stop_split": 0,
                "remain": 0
            },
            "tts": {
                "vcn": "x5_lingxiaoxuan_flow",
                "volume": 50,
                "rhy": 0,
                "speed": 50,
                "pitch": 50,
                "bgs": 0,
                "reg": 0,
                "rdn": 0,
                "audio": {
                    "encoding": "raw",
                    "sample_rate": 24000,
                    "channels": 1,
                    "bit_depth": 16,
                    "frame_size": 0
                }
            }
        }
        
        data = {
            "text": {
                "encoding": "utf8",
                "compress": "raw",
                "format": "plain",
                "status": 2,
                "seq": self.seq,
                "text": str(base64.b64encode(text.encode('utf-8')), "UTF8")
            }
        }
        
        return {
            "header": common_args,
            "parameter": business_args,
            "payload": data
        }
    
    async def handle_tts_send_text(self, event) -> bool:
        if data := event.data:
            if instance := data.get("instance"):
                self.instance = instance
                self.call_hang_up.set()
                
            if call_id := data.get("call_id"):
                self.call_id = call_id
            if text := data.get("text"):
                pass
        else:
            logger.error("事件数据为空")
            return False

        try:
            with self.lock:
                if self.connection_state not in [ConnectionState.CONNECTED, ConnectionState.CONNECTING]:
                    await self.emit_event(EventType.TTS_CONNECT, None) # 发送连接事件
                    if not self._connection_event.wait(timeout=10):
                        raise TimeoutError("连接建立超时")
                
                text = text.strip()
                
                frame = self._create_frame(text)
                
                self.seq += 1   
                    
                logger.info(f"发送第{self.seq}帧文本数据, status=2, seq={self.seq}, text='{text[:50]}...'")
                    
                frame_copy = copy.deepcopy(frame)
                if "payload" in frame_copy and "text" in frame_copy["payload"]:
                    frame_copy["payload"].pop("text")
                logger.debug(f"发送参数: {json.dumps(frame_copy, indent=2, ensure_ascii=False)}")
                #计时开始
                logger.warning("发送文本开始计时%s", time.time())        
                self.ws.send(json.dumps(frame))
                    
                logger.info("文本发送完成")
                return True
        except TimeoutError as e:
            logger.error("连接建立超时 %s", e)
            return False
        except Exception as e:
            logger.error("发送文本异常: %s", e)
            return False
    
    def handle_interrupt(self) -> bool:
        """处理打断请求"""
        try:
            self.audio_player.interrupt_playback()
            logger.info("TTS播放已被打断")
            return True
        except Exception as e:
            logger.error(f"TTS打断处理失败: {e}")
            return False
    
    async def end_call(self, event: Event = None) -> bool:
        """通话结束"""
        if not event.data.get("agent_hang_up"):
            logger.info(f"{event.data}，end_call事件设置call_is_ended.set()")
            self.call_is_ended.set()
        
        self.audio_player.reset_for_call()
        if await self.reset_for_call(None):
            logger.info("TTS服务已成功重置到initialize状态")
            return True
        else:
            logger.error("TTS服务重置到initialize状态失败")
            return False
    
    async def reset_for_call(self, event: Event = None) -> bool:
        """将TTS服务重置到initialize状态"""
        logger.info("开始重置TTS服务到initialize后状态...")
        
        try:
            with self.lock:
                self.ws.close()
                self.connection_state = ConnectionState.DISCONNECTED
                self.seq = 0

                self.call_hang_up.clear()

                self.instance = None
                self.call_id = None

                logger.info("TTS服务已成功重置到initialize后状态")
                return True
        except TimeoutError as e:
            logger.error("WebSocket thread failed to exit within timeout: %s", e)
            return False
        except Exception as e:
            logger.error("重置TTS服务失败: %s", e)
            return False
        
    def stop(self):
        """停止TTS服务"""
        logger.info("停止TTS服务...")
        
        if self.ws:
            try:
                self.ws.close()
            except Exception as e:
                logger.warning("关闭WebSocket时出错: %s", e)
        
        if hasattr(self, 'ws_thread') and self.ws_thread and self.ws_thread.is_alive():
            self.ws_thread.join(timeout=5.0)
            if self.ws_thread.is_alive():
                logger.warning("WebSocket线程未能在5秒内正常退出")
        
        self.audio_player.stop()
        
        self.connection_state = ConnectionState.DISCONNECTED
        logger.info("TTS服务已停止")
    

class SmartAudioPlayer:
    
    def __init__(self, sample_rate: int = 24000, chunk_size: int = 1024, redis_service: RedisService = None, tts_service: TtsService = None):

        self.tts_service = tts_service
        self.sample_rate = sample_rate
        self.chunk_size = chunk_size
        self.redis_service = redis_service
        
        self.call_is_ended = False
        self.p = None
        self.stream = None
        
        self.audio_buffer = AudioBuffer(max_size=200)
        self.open_tts_has_played = threading.Event()
        self.open_tts_has_played.clear()
        
        self.playback_thread = None
        
        self.interrupt_lock = threading.Lock()
        self.is_interrupted = False
        
        
        self.stats = {
            'total_played_chunks': 0,
            'interruptions_count': 0,
            'playback_errors': 0,
            'average_latency': 0.0
        }
    
    def initialize(self):
        """初始化音频播放器"""
        try:
            self.p = pyaudio.PyAudio()
            self.stream = self.p.open(
                format=pyaudio.paInt16,
                channels=1,
                rate=self.sample_rate,
                output=True,
                frames_per_buffer=self.chunk_size
            )
            self._start_playback_worker()
            
            logger.info("智能音频播放器初始化成功")
            
        except Exception as e:
            logger.error(f"音频播放器初始化失败: {e}")
            raise
    
    def _start_playback_worker(self):
        """启动播放工作线程"""
        def playback_worker():
            logger.info("音频播放工作线程已启动")
            
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
            try:
                while self.tts_service.call_hang_up.wait(timeout=300):
                    try:
                        with self.interrupt_lock:
                            if self.is_interrupted:
                                time.sleep(0.01)
                                continue
                        
                        audio_data = self.audio_buffer.get(timeout=0.1)

                        if not audio_data:

                            if self.tts_service.call_is_ended.is_set():
                                logger.info("通话结束is_set，马上clear，并发出通话结束事件")
                                self.tts_service.call_is_ended.clear()
                                asyncio.run(self.tts_service.emit_event(EventType.CALL_END, data={"call_id": self.tts_service.call_id, "agent_hang_up": True}))
                                self.tts_service.call_hang_up.clear()
                                continue

                            if self.open_tts_has_played.is_set():
                                logger.info("缓冲区为空且已播放过首次音频，触发AI决策")
                                
                                try:
                                    dialog = asyncio.run(
                                        self.redis_service.get_dialog_after_marking(self.tts_service.call_id)
                                    )
                                    conversation_answer = self.tts_service.emit_event(EventType.CONVERSATION_ANSWER, data={"chat_log": dialog}, wait_for_result=True)
                                    text = conversation_answer.answer
                                    asyncio.run(self.tts_service.emit_event(EventType.TTS_SEND_TEXT, data={"text": text}))

                                    dialog_entry = DialogEntry(speaker="agent", content=text, timestamp=datetime.now().isoformat())
                                    
                                    asyncio.run(self.tts_service.emit_event(EventType.REDIS_ADD_DIALOG_RECORD, data={"call_id": self.tts_service.call_id, "dialog_entry": dialog_entry}))
                                    if conversation_answer.isCallEnd:
                                        self.tts_service.call_is_ended.set()
                                    
                                    # 等待新的音频数据  
                                    if not (audio_data := self.audio_buffer.get(timeout=10)):
                                        logger.error("AI决策后TTS音频数据获取失败")
                                        continue
                                        
                                except Exception as e:
                                    logger.error(f"AI决策异步调用错误: {e}")
                                    continue
                            else:
                                continue

                        if audio_data and self.stream:
                            start_time = time.time()
                            self.stream.write(audio_data)
                            
                            # 标记已经播放过首次音频
                            if self.tts_service.call_hang_up.is_set():
                                if not self.open_tts_has_played.is_set():
                                    self.open_tts_has_played.set()
                                    logger.info("首次音频播放完成，已设置播放标志")
                                
                            # 更新统计
                            self.stats['total_played_chunks'] += 1
                            latency = time.time() - start_time
                            self.stats['average_latency'] = (
                                self.stats['average_latency'] * 0.9 + latency * 0.1
                            )
                            
                    except Exception as e:
                        logger.error(f"播放工作线程错误: {e}")
                        self.stats['playback_errors'] += 1
                        time.sleep(0.01)
            finally:
                loop.close()
                logger.info("音频播放工作线程已停止")
        
        self.playback_thread = threading.Thread(target=playback_worker, daemon=True)
        self.playback_thread.start()
    
    def add_audio(self, audio_data: bytes):
        """添加音频数据"""
        if not self.is_interrupted:
            self.audio_buffer.put(audio_data)
    
    def get_status(self) -> Dict[str, Any]:
        """获取播放器状态"""
        buffer_stats = self.audio_buffer.get_stats()
        return {
            **buffer_stats,
            # 兼容调用方期望的键名，避免 KeyError
            'buffer_utilization': buffer_stats.get('utilization', 0),
            'is_playing': self.playing,
            'is_interrupted': self.is_interrupted,
            'player_stats': self.stats.copy(),
            # 添加测试期望的直接键名
            'total_played_chunks': self.stats['total_played_chunks'],
            'interruptions_count': self.stats['interruptions_count'],
            'playback_errors': self.stats['playback_errors'],
            'average_latency': self.stats['average_latency']
        }
    
    def stop(self):
        """停止播放器"""
        logger.info("停止智能音频播放器...")
        self.audio_buffer.deactivate()
        
        # 等待线程结束
        if self.playback_thread and self.playback_thread.is_alive():
            self.playback_thread.join(timeout=2)
        
        # 关闭音频设备
        if self.stream:
            try:
                self.stream.close()
            except: pass
        
        if self.p:
            try:
                self.p.terminate()
            except: pass
    
    def reset_for_call(self):
        self.audio_buffer.clear()
        self.open_tts_has_played.clear()
        
    