# app/services/tts_service.py
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
from app.core.config import settings
from app.core.event_bus import ProductionEventBus
from app.models.events import Event, EventListener, EventType
from app.services import conversation_service
from app.core.logger import get_logger
from app.services.base_service import BaseService
from app.services.redis_service import RedisService

logger = get_logger(__name__)

# 常量定义
class TtsStatus:
    FIRST = 0
    CONTINUE = 1
    LAST = 2

class AudioBuffer:
    """改进的音频缓冲区管理类 - 基于您的设计"""
    
    def __init__(self, max_size: int = 200):
        self.buffer = deque(maxlen=max_size)
        self.lock = threading.Lock()
        self.is_active = True
        self._stats = {
            'chunks_added': 0,
            'chunks_played': 0,
            'buffer_overflows': 0,
            'clear_count': 0
        }
        
    def put(self, audio_data: bytes):
        """添加音频数据到缓冲区"""
        with self.lock:
            if self.is_active and audio_data:
                if len(self.buffer) >= self.buffer.maxlen - 1:
                    self._stats['buffer_overflows'] += 1
                    logger.warning(f"音频缓冲区接近满载: {len(self.buffer)}")
                
                self.buffer.append(audio_data)
                self._stats['chunks_added'] += 1
                
    def get(self) -> Optional[bytes]:
        """从缓冲区获取音频数据"""
        with self.lock:
            if self.buffer:
                data = self.buffer.popleft()
                self._stats['chunks_played'] += 1
                return data
            return None
            
    def clear(self):
        """清空缓冲区"""
        with self.lock:
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
        with self.lock:
            self.is_active = False
            self.buffer.clear()
            
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
            "oral": {"oral_level": "mid", "spark_assist": 1},
            "tts": {
                "vcn": "x5_lingyuyan_flow",
                "volume": 50, "speed": 55, "pitch": 50,
                "audio": {
                    "encoding": "raw", "sample_rate": 24000,
                    "channels": 1, "bit_depth": 16
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
    """智能音频播放器 - 基于您的缓冲器设计"""
    
    def __init__(self, sample_rate: int = 24000, chunk_size: int = 512):
        self.sample_rate = sample_rate
        self.chunk_size = chunk_size
        
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
                    
                    # 从缓冲区获取音频数据
                    audio_data = self.audio_buffer.get()
                    if audio_data and self.stream:
                        start_time = time.time()
                        self.stream.write(audio_data)
                        
                        # 更新统计
                        self.stats['total_played_chunks'] += 1
                        latency = time.time() - start_time
                        self.stats['average_latency'] = (
                            self.stats['average_latency'] * 0.9 + latency * 0.1
                        )
                    else:
                        time.sleep(0.001)  # 缓冲区为空时短暂休眠
                        
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
            'player_stats': self.stats.copy()
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
        self.audio_player = SmartAudioPlayer()
        
        # WebSocket连接
        self.ws = None
        self.connected = False
        self.stopped = False
        
        # 会话状态
        self.seq = 0
        self.is_first_frame = True
        self.call_id = None
        self.synthesis_complete = False
        
        # 重连管理
        self.reconnect_attempts = 0
        self.max_reconnect_attempts = 3
        
        # 线程锁
        self.lock = threading.Lock()
        
        # 设置音频播放器回调
        self.audio_player.on_buffer_status = self._on_buffer_status_changed
    
    async def initialize(self):
        """初始化服务"""
        try:
            self.audio_player.initialize()
            self._connect()
            logger.info("优化版TTS服务启动成功")
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
            
            # 可以添加打断事件监听
            # self.event_bus.register_listener(
            #     EventListener(
            #         event_type=EventType.TTS_INTERRUPT,
            #         handler=self.handle_interrupt,
            #         priority=1,
            #         name=f"{self.service_name}_handle_interrupt"
            #     )
            # )
    
    def _connect(self):
        """建立WebSocket连接"""
        if self.connected:
            return
        
        try:
            url = self.config.create_url()
            self.ws = websocket.WebSocketApp(
                url,
                on_open=self._on_open,
                on_message=self._on_message,
                on_error=self._on_error,
                on_close=self._on_close
            )
            
            threading.Thread(
                target=lambda: self.ws.run_forever(sslopt={"cert_reqs": ssl.CERT_NONE}),
                daemon=True,
                name="TTS-WebSocket"
            ).start()
            
        except Exception as e:
            logger.error(f"WebSocket连接失败: {e}")
    
    def _on_open(self, ws):
        """连接建立"""
        with self.lock:
            self.connected = True
            self.reconnect_attempts = 0
            self.seq = 0
            self.is_first_frame = True
        logger.info("TTS WebSocket连接已建立")
    
    def _on_message(self, ws, message):
        """处理消息"""
        try:
            data = json.loads(message)
            
            # 检查错误
            if data.get("header", {}).get("code", 0) != 0:
                logger.error(f"TTS错误: {data['header'].get('message', '未知错误')}")
                return
            
            # 处理音频数据
            audio_data = data.get("payload", {}).get("audio", {}).get("audio")
            if audio_data:
                decoded_audio = base64.b64decode(audio_data)
                self.audio_player.add_audio(decoded_audio)
            
            # 检查合成状态
            if data.get("header", {}).get("status") == TtsStatus.LAST:
                self.synthesis_complete = True
                logger.info("TTS合成完成")
                self._handle_synthesis_complete()
                
        except Exception as e:
            logger.error(f"处理TTS消息异常: {e}")
    
    def _on_error(self, ws, error):
        """错误处理"""
        logger.error(f"TTS WebSocket错误: {error}")
    
    def _on_close(self, ws, close_status_code, close_msg):
        """连接关闭"""
        with self.lock:
            self.connected = False
        
        logger.info(f"TTS WebSocket连接关闭: {close_status_code}")
        
        if not self.stopped and self.reconnect_attempts < self.max_reconnect_attempts:
            self._reconnect()
    
    def _reconnect(self):
        """重连逻辑"""
        self.reconnect_attempts += 1
        delay = min(2 ** self.reconnect_attempts, 30)
        
        def do_reconnect():
            time.sleep(delay)
            if not self.stopped:
                self._connect()
        
        threading.Thread(target=do_reconnect, daemon=True, name="TTS-Reconnect").start()
    
    def _handle_synthesis_complete(self):
        """处理合成完成"""
        if self.call_id:
            try:
                dialog = self.redis_service.get_dialog_after_marking(self.call_id)
                text = conversation_service.ai_decision(dialog)
                if text:
                    self.handle_tts_send_text(text)
            except Exception as e:
                logger.error(f"AI决策失败: {e}")
    
    def _on_buffer_status_changed(self, status: Dict):
        """缓冲区状态变化回调"""
        if status['buffer_utilization'] > 95:
            logger.warning(f"TTS缓冲区使用率过高: {status['buffer_utilization']:.1f}%")
    
    def _create_frame(self, text: str, status: int) -> dict:
        """创建数据帧"""
        return {
            "header": {
                "app_id": self.config.appid,
                "status": status
            },
            "parameter": self.config.business_params if self.seq == 0 else {},
            "payload": {
                "text": {
                    "encoding": "utf8",
                    "compress": "raw", 
                    "format": "plain",
                    "status": status,
                    "seq": self.seq,
                    "text": base64.b64encode(text.encode('utf-8')).decode()
                }
            }
        }
    
    def handle_tts_send_text(self, event: Event) -> bool:
        """发送文本进行合成"""
        if not self.connected or self.stopped:
            logger.warning("TTS服务未连接")
            return False
        
        if not event.data or not event.data.strip():
            logger.error("文本为空")
            return False
        
        try:
            with self.lock:
                status = TtsStatus.FIRST if self.is_first_frame else TtsStatus.CONTINUE
                
                frame = self._create_frame(event.data.strip(), status)
                self.ws.send(json.dumps(frame))
                
                self.seq += 1
                self.is_first_frame = False
                
                logger.info(f"TTS文本发送成功: seq={self.seq-1}, status={status}")
                return True
                
        except Exception as e:
            logger.error(f"发送TTS文本失败: {e}")
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
        if not self.connected:
            return True
        
        try:
            with self.lock:
                frame = self._create_frame("", TtsStatus.LAST)
                self.ws.send(json.dumps(frame))
                self.is_first_frame = True
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
        return self.connected and not self.stopped
    
    def get_status(self) -> Dict[str, Any]:
        """获取服务状态"""
        audio_status = self.audio_player.get_status()
        return {
            "connected": self.is_connected(),
            "seq": self.seq,
            "is_first_frame": self.is_first_frame,
            "call_id": self.call_id,
            "synthesis_complete": self.synthesis_complete,
            "reconnect_attempts": self.reconnect_attempts,
            "audio_player": audio_status,
            "timestamp": datetime.now().isoformat()
        }
    
    def stop(self):
        """停止服务"""
        logger.info("停止优化版TTS服务...")
        self.stopped = True
        
        if self.ws:
            self.ws.close()
        
        self.audio_player.stop()
        
        with self.lock:
            self.connected = False
            self.reconnect_attempts = 0
        
        logger.info("优化版TTS服务已停止")
