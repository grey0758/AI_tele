# app/services/tts_service.py
import copy
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
from typing import Dict, Any, Optional, Callable
from enum import Enum
from app.core.config import settings
from app.core.event_bus import ProductionEventBus
from app.models.events import Event, EventListener, EventType
from app.services import conversation_service
from app.core.logger import get_logger
from app.services.base_service import BaseService
from app.services.redis_service import RedisService
from app.services.conversation_service import ConversationService   

logger = get_logger(__name__)

class ConnectionState(Enum):
    """WebSocket连接状态枚举"""
    DISCONNECTED = "disconnected"
    CONNECTING = "connecting" 
    CONNECTED = "connected"
    RECONNECTING = "reconnecting"
    FAILED = "failed"

class AudioBuffer:
    """改进的音频缓冲区管理类 - 基于您的设计"""
    
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
        
    def put(self, audio_data: bytes):
        """添加音频数据到缓冲区"""
        with self.condition:
            if self.is_active and audio_data:
                if len(self.buffer) >= self.buffer.maxlen - 1:
                    self._stats['buffer_overflows'] += 1
                    logger.warning(f"音频缓冲区接近满载: {len(self.buffer)}")
                
                self.buffer.append(audio_data)
                self._stats['chunks_added'] += 1
                self.condition.notify()  # 通知等待的消费者
                
    def get(self, timeout: float = 0.1) -> Optional[bytes]:
        """支持超时等待的获取方法"""
        with self.condition:
            # 如果缓冲区为空且还在活动状态，等待新数据
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
            self.condition.notify_all()  # 通知所有等待的线程
            
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

class SmartAudioPlayer:
    
    def __init__(self, sample_rate: int = 24000, chunk_size: int = 1024, redis_service: RedisService = None, event_bus: ProductionEventBus = None):
        self.sample_rate = sample_rate
        self.chunk_size = chunk_size
        self.redis_service = redis_service
        self.event_bus = event_bus
        # 音频设备
        self.p = None
        self.stream = None
        
        # 缓冲区系统
        self.audio_buffer = AudioBuffer(max_size=200)
        
        # 播放控制
        self.playing = False
        self.playback_active = threading.Event()
        self.playback_active.set()
        
        # 工作线程
        self.playback_thread = None
        self.monitor_thread = None
        
        # 打断处理
        self.interrupt_lock = threading.Lock()
        self.is_interrupted = False
        
        # 回调函数
        self.on_buffer_status: Optional[Callable[[Dict], None]] = None
        
        # 统计信息
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
            
            # 启动工作线程
            self._start_playback_worker()
            self._start_monitor_worker()
            
            logger.info("智能音频播放器初始化成功")
            
        except Exception as e:
            logger.error(f"音频播放器初始化失败: {e}")
            raise
    
    def _start_playback_worker(self):
        """启动播放工作线程"""
        def playback_worker():
            logger.info("音频播放工作线程已启动")
            self.playing = True
            
            while self.playing:
                try:
                    # 等待播放激活信号
                    if not self.playback_active.wait(timeout=0.1):
                        continue
                    
                    # 检查打断状态
                    with self.interrupt_lock:
                        if self.is_interrupted:
                            time.sleep(0.01)
                            continue
                    
                    # 从缓冲区获取音频数据（支持超时等待）
                    audio_data = self.audio_buffer.get(timeout=0.1)
                    if not audio_data:
                        self.call_id = self.redis_service
                        dialog = self.redis_service.get_dialog_after_marking(self.call_id)      
                        text = conversation_service.ai_decision(dialog)
                        event = Event(
                            type=EventType.TTS_SEND_TEXT,
                            data=text,
                            wait_for_result=True
                        )
                        self.event_bus.emit(event)
                        while True:
                            audio_data = self.audio_buffer.get(timeout=0.1)
                            if audio_data:
                                break
                            time.sleep(0.01)
                        logger.info("新的音频数据生成完成")

                    if audio_data and self.stream:
                        start_time = time.time()
                        self.stream.write(audio_data)
                        
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
            
            logger.info("音频播放工作线程已停止")
        
        self.playback_thread = threading.Thread(target=playback_worker, daemon=True)
        self.playback_thread.start()
    
    def _start_monitor_worker(self):
        """启动监控工作线程"""
        def monitor_worker():
            logger.info("缓冲区监控线程已启动")
            
            while self.playing:
                try:
                    status = self.get_status()
                    
                    # 检查缓冲区状态
                    if status['buffer_utilization'] > 90:
                        logger.warning(f"缓冲区使用率过高: {status['buffer_utilization']:.1f}%")
                    
                    # 触发状态回调
                    if self.on_buffer_status:
                        self.on_buffer_status(status)
                    
                    time.sleep(1.0)  # 每秒监控一次
                    
                except Exception as e:
                    logger.error(f"监控线程错误: {e}")
                    time.sleep(1.0)
            
            logger.info("缓冲区监控线程已停止")
        
        self.monitor_thread = threading.Thread(target=monitor_worker, daemon=True)
        self.monitor_thread.start()
    
    def add_audio(self, audio_data: bytes):
        """添加音频数据"""
        if not self.is_interrupted:
            self.audio_buffer.put(audio_data)
    
    def interrupt_playback(self):
        """打断播放 - 改进版"""
        with self.interrupt_lock:
            if self.is_interrupted:
                return
            
            logger.info("执行智能打断...")
            self.is_interrupted = True
            self.stats['interruptions_count'] += 1
            
            # 1. 暂停播放
            self.playback_active.clear()
            
            # 2. 清空缓冲区
            self.audio_buffer.clear()
            
            # 3. 重置音频流
            self._reset_audio_stream()
            
            # 4. 短暂延迟后重新激活
            threading.Timer(0.05, self._reactivate_playback).start()
    
    def _reset_audio_stream(self):
        """重置音频流"""
        try:
            if self.stream:
                self.stream.stop_stream()
                self.stream.close()
            
            self.stream = self.p.open(
                format=pyaudio.paInt16,
                channels=1,
                rate=self.sample_rate,
                output=True,
                frames_per_buffer=self.chunk_size
            )
            logger.debug("音频流已重置")
            
        except Exception as e:
            logger.error(f"重置音频流失败: {e}")
    
    def _reactivate_playback(self):
        """重新激活播放"""
        with self.interrupt_lock:
            self.is_interrupted = False
            self.playback_active.set()
            logger.info("播放已重新激活")
    
    def get_status(self) -> Dict[str, Any]:
        """获取播放器状态"""
        buffer_stats = self.audio_buffer.get_stats()
        return {
            **buffer_stats,
            # 兼容调用方期望的键名，避免 KeyError
            'buffer_utilization': buffer_stats.get('utilization', 0),
            'is_playing': self.playing,
            'is_interrupted': self.is_interrupted,
            'playback_active': self.playback_active.is_set(),
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
        self.playing = False
        self.playback_active.clear()
        self.audio_buffer.deactivate()
        
        # 等待线程结束
        if self.playback_thread and self.playback_thread.is_alive():
            self.playback_thread.join(timeout=2)
        
        if self.monitor_thread and self.monitor_thread.is_alive():
            self.monitor_thread.join(timeout=2)
        
        # 关闭音频设备
        if self.stream:
            try:
                self.stream.close()
            except: pass
        
        if self.p:
            try:
                self.p.terminate()
            except: pass

class TtsService(BaseService):
    """优化的TTS服务 - 集成智能音频缓冲器"""
    
    def __init__(self, event_bus: ProductionEventBus, redis_service: RedisService):
        super().__init__(event_bus, "TtsService")
        self.redis_service = redis_service
        self.config = TtsConfig()
        self.audio_player = SmartAudioPlayer(redis_service=redis_service, event_bus=event_bus)
        
        # WebSocket连接状态管理
        self.ws = None
        self.ws_thread = None
        self.connection_state = ConnectionState.DISCONNECTED
        self.connection_lock = threading.RLock()
        self.stop_event = threading.Event()
        self.stopped = False
        
        # 会话状态
        self.seq = 0
        self.is_first_frame = True
        self.call_id = None
        self.synthesis_complete = False
        
        
        # 线程锁
        self.lock = threading.RLock()  # 使用RLock避免死锁
        
        # 设置音频播放器回调
        self.audio_player.on_buffer_status = self._on_buffer_status_changed
    
    async def initialize(self):
        """初始化服务"""
        try:
            self.audio_player.initialize()
            self._connect()
            logger.info("TTS服务启动成功")
            return True
        except Exception as e:
            logger.error(f"TTS服务启动失败: {e}")
            return False
    
    async def register_event_listeners(self):
        """注册事件监听器"""
        if self.event_bus:
            # TTS文本发送
            self.event_bus.register_listener(
                EventListener(
                    event_type=EventType.TTS_SEND_TEXT,
                    handler=self.handle_tts_send_text,
                    priority=1,
                    name=f"{self.service_name}_handle_tts_send_text"
                )
            )
    
    def _connect(self):
        with self.connection_lock:
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
            
            # 在单独线程中运行WebSocket - 参考 a.py
            def run_websocket():
                self.ws.run_forever(sslopt={"cert_reqs": ssl.CERT_NONE})
            
            self.ws_thread = threading.Thread(target=run_websocket, daemon=True)
            self.ws_thread.start()
            
        except Exception as e:
            logger.error(f"建立连接异常: {e}")
            self.connection_state = ConnectionState.FAILED

    
    def _on_open(self, ws):
        """连接建立 - 参考 a.py 的实现"""
        with self.connection_lock:
            self.connection_state = ConnectionState.CONNECTED
            self.seq = 0
            self.is_first_frame = True
        logger.info("TTS WebSocket连接已建立")
    
    def _on_message(self, ws, message):
        """处理消息 - 参考 a.py 的实现"""
        try:
            message_data = json.loads(message)
            code = message_data["header"]["code"]
            sid = message_data["header"]["sid"]
            
            if code != 0:
                errMsg = message_data.get("message", "Unknown error")
                logger.error(f"TTS错误 - sid:{sid}, code:{code}, message:{errMsg}")
                return
            
            # 使用深层复制message字段删除payload字段再打印debug
            message_copy = copy.deepcopy(message_data)
            if "payload" in message_copy and "audio" in message_copy["payload"]:
                message_copy["payload"].pop("audio")
            logger.debug(f"收到响应: {json.dumps(message_copy, indent=2, ensure_ascii=False)}")
            
            if "payload" in message_data and "audio" in message_data["payload"]:
                audio_data = message_data["payload"]["audio"]["audio"]
                if audio_data:
                    # 解码音频数据并直接添加到播放缓冲区
                    audio_bytes = base64.b64decode(audio_data)
                    self.audio_player.add_audio(audio_bytes)
                    logger.debug(f"添加音频数据到缓冲区: {len(audio_bytes)} bytes")
                
                status = message_data["payload"]["audio"]["status"]
                if status == 2:  # 最后一帧
                    self.ws.close()
                    logger.info("TTS合成完成，WebSocket连接已关闭")
                    self._handle_synthesis_complete()
                    
        except Exception as e:
            logger.error(f"处理WebSocket消息异常: {e}")
    
    def _on_error(self, ws, error):
        """错误处理 - 参考 a.py 的实现"""
        logger.error(f"TTS WebSocket错误: {error}")
        with self.connection_lock:
            self.connection_state = ConnectionState.DISCONNECTED
    
    def _on_close(self, ws, close_status_code, close_msg):
        with self.connection_lock:
            self.connection_state = ConnectionState.DISCONNECTED
        
        logger.info(f"TTS WebSocket连接关闭: {close_status_code}, 消息: {close_msg}")
    
    
    def _handle_synthesis_complete(self):
        """处理合成完成"""
        if self.call_id:
            try:    

                dialog = self.redis_service.get_dialog_after_marking(self.call_id)
                logger.info(f"对话记录: {dialog}")
                text = conversation_service.ai_decision(dialog)
                logger.info(f"AI决策: {text}")
                if text:
                    self.handle_tts_send_text(text)
            except Exception as e:
                logger.error(f"AI决策失败: {e}")
    
    def _on_buffer_status_changed(self, status: Dict):
        """缓冲区状态变化回调"""
        if status['buffer_utilization'] > 95:
            logger.warning(f"TTS缓冲区使用率过高: {status['buffer_utilization']:.1f}%")
    
    def _create_frame(self, text: str) -> dict:
        """创建数据帧 - 参考 a.py 的实现，只使用 status=2"""
        # 参考 a.py 的 create_ws_param 方法
        common_args = {"app_id": self.config.appid, "status": 2}
        
        business_args = {
            "oral": {  # 添加必需的口语化配置
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
                    "encoding": "raw",  # 使用raw格式
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
                "status": 2,  # 只使用 status=2
                "seq": self.seq,
                "text": str(base64.b64encode(text.encode('utf-8')), "UTF8")
            }
        }
        
        return {
            "header": common_args,
            "parameter": business_args,
            "payload": data
        }
    
    async def handle_tts_send_text(self, event_or_text) -> bool:
        """发送文本进行合成 - 参考 a.py 的实现"""
        # 兼容处理：支持Event对象或直接文本
        if isinstance(event_or_text, Event):
            text = event_or_text.data
        else:
            text = event_or_text
            
        if not text or not text.strip():
            logger.error("文本为空")
            return False
        
        try:
            # 如果连接不存在或已断开，重新建立连接
            if self.connection_state != ConnectionState.CONNECTED or self.stopped:
                logger.info("连接不存在，重新建立连接...")
                self._connect()
                
                # 等待连接建立
                timeout = 10  # 10秒超时
                start_time = time.time()
                while self.connection_state != ConnectionState.CONNECTED and (time.time() - start_time) < timeout:
                    time.sleep(0.1)
                
                if self.connection_state != ConnectionState.CONNECTED:
                    logger.error("连接建立超时")
                    return False
            
            with self.lock:
                text = text.strip()
                
                frame = self._create_frame(text)
                
                self.seq += 1   
                    
                logger.info(f"发送第{self.seq}帧文本数据, status=2, seq={self.seq}, text='{text[:50]}...'")
                    
                # 使用深层复制frame字段删除payload字段再打印debug
                frame_copy = copy.deepcopy(frame)
                if "payload" in frame_copy and "text" in frame_copy["payload"]:
                    frame_copy["payload"].pop("text")
                logger.debug(f"发送参数: {json.dumps(frame_copy, indent=2, ensure_ascii=False)}")
                    
                self.ws.send(json.dumps(frame))
                    
                logger.info("文本发送完成")
                return True
                
        except Exception as e:
            logger.error(f"发送文本异常: {e}")
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
    
    def finish_session(self) -> bool:
        """结束会话"""
        if self.connection_state != ConnectionState.CONNECTED:
            return True
        
        try:
            with self.lock:
                # 重置会话状态
                self.is_first_frame = True
                self.seq = 0
                logger.info("TTS会话结束")
                return True
        except Exception as e:
            logger.error(f"结束TTS会话失败: {e}")
            return False
    
    def set_call_id(self, call_id: str):
        """设置通话ID"""
        self.call_id = call_id
    
    def is_connected(self) -> bool:
        """检查连接状态"""
        return self.connection_state == ConnectionState.CONNECTED and not self.stopped
    
    def get_status(self) -> Dict[str, Any]:
        """获取服务状态"""
        audio_status = self.audio_player.get_status()
        return {
            "connection_state": self.connection_state.value,
            "connected": self.is_connected(),
            "seq": self.seq,
            "is_first_frame": self.is_first_frame,
            "call_id": self.call_id,
            "synthesis_complete": self.synthesis_complete,
            "audio_player": audio_status,
            "timestamp": datetime.now().isoformat()
        }
    
    def stop(self):
        """改进的停止方法"""
        logger.info("停止TTS服务...")
        self.stopped = True
        self.stop_event.set()  # 通知所有线程停止
        
        # 关闭WebSocket连接
        if self.ws:
            try:
                self.ws.close()
            except Exception as e:
                logger.warning(f"关闭WebSocket时出错: {e}")
        
        # 等待WebSocket线程结束
        if hasattr(self, 'ws_thread') and self.ws_thread and self.ws_thread.is_alive():
            self.ws_thread.join(timeout=5.0)
            if self.ws_thread.is_alive():
                logger.warning("WebSocket线程未能在5秒内正常退出")
        
        # 停止音频播放器
        self.audio_player.stop()
        
        # 更新连接状态
        with self.connection_lock:
            self.connection_state = ConnectionState.DISCONNECTED
        
        logger.info("TTS服务已停止")
