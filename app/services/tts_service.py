# app/services/tts_service.py
from contextlib import asynccontextmanager
import os
import queue
from fastapi import FastAPI
import websocket
import datetime
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
from typing import Dict, Any, Callable
from app.core.config import settings
from app.services import conversation_service
from app.services.redis_service import get_redis_service
from celery import current_app

# 使用主应用的logger
from app.core.logger import get_logger

# 获取模块级别的logger
logger = get_logger(__name__)

redis_service = get_redis_service()

STATUS_FIRST_FRAME = 0  # 第一帧的标识
STATUS_CONTINUE_FRAME = 1  # 中间帧标识
STATUS_LAST_FRAME = 2  # 最后一帧的标识


class Ws_Param(object):
    def __init__(self):
        self.APPID = settings.tts_appid_c
        self.APIKey = settings.tts_apikey_c
        self.APISecret = settings.tts_apisecret_c

        # 公共参数(common)
        self.CommonArgs = {
            "app_id": self.APPID,
            "status": 2
        }
        # 业务参数(business)
        self.BusinessArgs = {
            "oral": {
                "oral_level": "mid"
            },
            "tts": {
                "vcn": "x5_lingyuyan_flow",
                "volume": 50,
                "rhy": 0,
                "speed": 55,
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

    def create_url(self):
        url = 'wss://cbm01.cn-huabei-1.xf-yun.com/v1/private/mcd9m97e6'
        now = datetime.now()
        date = format_date_time(mktime(now.timetuple()))

        signature_origin = f"host: cbm01.cn-huabei-1.xf-yun.com\ndate: {date}\nGET /v1/private/mcd9m97e6 HTTP/1.1"
        signature_sha = hmac.new(self.APISecret.encode('utf-8'), signature_origin.encode('utf-8'),
                                 digestmod=hashlib.sha256).digest()
        signature_sha = base64.b64encode(signature_sha).decode(encoding='utf-8')

        authorization_origin = (
            f'api_key="{self.APIKey}", '
            f'algorithm="hmac-sha256", '
            f'headers="host date request-line", '
            f'signature="{signature_sha}"'
        )
        authorization = base64.b64encode(authorization_origin.encode('utf-8')).decode('utf-8')
        
        v = {
            "authorization": authorization,
            "date": date,
            "host": "cbm01.cn-huabei-1.xf-yun.com"
        }
        url = url + '?' + urlencode(v)
        return url

    def create_data_frame(self, text, status, seq):
        """创建数据帧"""
        return {
            "text": {
                "encoding": "utf8",
                "compress": "raw",
                "format": "plain",
                "status": status,
                "seq": seq,
                "text": base64.b64encode(text.encode('utf-8')).decode()
            }
        }


class TtsService:
    """全局TTS服务，保持长连接"""
    
    def __init__(self):
        self.p = None
        self.stream = None
        self.wsParam = Ws_Param()
        self.ws = None
        self.stopped = False
        self.session_started = False
        self.seq = 0
        self.is_first_frame = True
        self.reconnect_attempts = 0
        self.max_reconnect_attempts = settings.websocket_max_reconnect_attempts
        self.reconnect_delay = settings.websocket_reconnect_delay
        self.max_reconnect_delay = settings.websocket_max_reconnect_delay
        self.last_activity = time.time()  # 最后活动时间
        self.heartbeat_interval = settings.websocket_heartbeat_interval
        self.connection_timeout = settings.websocket_connection_timeout

        # 添加缺失的属性
        self.synthesis_complete = False
        self.call_id = None

        self.audio_queue = queue.Queue()
        self.audio_thread = None
        self.audio_playing = False
        
        # 线程锁，确保线程安全
        self.lock = threading.Lock()
        
        # 连接状态回调
        self.on_connected_callbacks = []
        self.on_disconnected_callbacks = []
        
        # 心跳线程
        self.heartbeat_thread = None
        self.heartbeat_running = False
        
        # 自动启动
        self._initialize()

    def _initialize(self):
        """初始化服务"""
        try:
            logger.info("初始化全局TTS服务")
            self._init_audio()
            self._connect()
        except Exception as e:
            logger.error(f"TTS服务初始化失败: {e}")

    def _init_audio(self):
        """初始化音频播放"""
        try:
            if self.p is None:
                self.p = pyaudio.PyAudio()
            
            if self.stream is None or not self.stream.is_active():
                if self.stream:
                    self.stream.close()
                
                self.stream = self.p.open(
                    format=pyaudio.paInt16,
                    channels=1,
                    rate=24000,
                    output=True,
                    frames_per_buffer=512
                )
                logger.info("音频流初始化成功")
        except Exception as e:
            logger.error(f"音频流初始化失败: {e}")
            raise

    def _connect(self):
        """建立WebSocket连接"""
        if self.session_started:
            logger.info("WebSocket已连接，跳过重连")
            return

        try:
            websocket.enableTrace(False)
            wsUrl = self.wsParam.create_url()
            
            self.ws = websocket.WebSocketApp(
                wsUrl,
                on_message=self._on_message,
                on_error=self._on_error,
                on_close=self._on_close,
                on_open=self._on_open
            )
            
            # 在后台线程中启动WebSocket连接
            ws_thread = threading.Thread(target=self._run_websocket, daemon=True, name="TTS-WebSocket")
            ws_thread.start()
            
            logger.info("WebSocket连接启动中...")
            
        except Exception as e:
            logger.error(f"WebSocket连接失败: {e}")
            self._schedule_reconnect()

    def _run_websocket(self):
        """在后台线程中运行WebSocket"""
        try:
            # 优化WebSocket参数，移除可能导致问题的ping_payload
            self.ws.run_forever(
                sslopt={"cert_reqs": ssl.CERT_NONE},
                ping_interval=30,  # 增加ping间隔
                ping_timeout=10,   # 增加ping超时
                skip_utf8_validation=True  # 跳过UTF-8验证以提高性能
            )
        except Exception as e:
            logger.error(f"WebSocket运行异常: {e}")
            if not self.stopped:
                self._schedule_reconnect()

    def _on_open(self, ws):
        """WebSocket连接打开"""
        with self.lock:
            self.session_started = True
            self.reconnect_attempts = 0
            self.seq = 0
            self.is_first_frame = True
            self.last_activity = time.time()
            
        logger.info("WebSocket连接已建立")
        
        # 启动心跳线程
        self._start_heartbeat()
        
        # 调用连接回调
        for callback in self.on_connected_callbacks:
            try:
                callback()
            except Exception as e:
                logger.error(f"连接回调执行失败: {e}")

    def _on_message(self, ws, message):  # 修正：添加ws参数
        try:
            # 更新最后活动时间
            self.last_activity = time.time()
            
            data = json.loads(message)
            logger.debug(f"收到WebSocket消息: {data}")
            
            if "header" in data and "status" in data["header"]:
                status = data["header"]["status"]
                if status == 2:  # 合成结束状态
                    self.synthesis_complete = True
                    logger.info("TTS合成完成")
                    if hasattr(self, 'call_id') and self.call_id:
                        try:
                            text = conversation_service.ai_decision(redis_service.get_dialog_after_marking(self.call_id))
                            current_app.send
                        except Exception as e:
                            logger.error(f"AI决策或发送文本失败: {e}")

            # 修正：检查data而不是message
            if "payload" in data and "audio" in data["payload"]:
                audio_data = data["payload"]["audio"]
                if "audio" in audio_data:
                    audio = base64.b64decode(audio_data["audio"])
                    
                    if not self.stopped and len(audio) > 0:
                        # 将音频数据放入队列而不是直接播放
                        self.audio_queue.put(audio)
                        
                        # 确保播放线程运行
                        if not self.audio_playing:
                            self._start_audio_thread()

        except Exception as e:
            logger.error(f"处理WebSocket消息异常: {e}")

    def _on_error(self, ws, error):
        """WebSocket错误处理"""
        logger.error(f"WebSocket错误: {error}")
        # 记录错误但不立即重连，让on_close处理

    def _on_close(self, ws, close_status_code, close_msg):
        """WebSocket连接关闭"""
        with self.lock:
            self.session_started = False
            
        # 停止心跳
        self._stop_heartbeat()
        
        logger.info(f"WebSocket连接已关闭: {close_status_code} - {close_msg}")
        
        # 调用断连回调
        for callback in self.on_disconnected_callbacks:
            try:
                callback()
            except Exception as e:
                logger.error(f"断连回调执行失败: {e}")
        
        # 如果不是主动停止，则尝试重连
        if not self.stopped:
            self._schedule_reconnect()

    def _start_heartbeat(self):
        """启动心跳线程"""
        if self.heartbeat_running:
            return
            
        self.heartbeat_running = True
        self.heartbeat_thread = threading.Thread(target=self._heartbeat_worker, daemon=True, name="TTS-Heartbeat")
        self.heartbeat_thread.start()
        logger.debug("心跳线程已启动")

    def _stop_heartbeat(self):
        """停止心跳线程"""
        self.heartbeat_running = False
        if self.heartbeat_thread and self.heartbeat_thread.is_alive():
            self.heartbeat_thread.join(timeout=1)
        logger.debug("心跳线程已停止")

    def _heartbeat_worker(self):
        """心跳工作线程 - 移除自定义心跳消息"""
        while self.heartbeat_running and self.session_started:
            try:
                time.sleep(self.heartbeat_interval)
                
                if not self.heartbeat_running or not self.session_started:
                    break
                    
                # 检查连接是否超时
                if time.time() - self.last_activity > self.connection_timeout:
                    logger.warning("连接超时，主动关闭连接")
                    if self.ws:
                        self.ws.close()
                    break
                
                # 移除自定义心跳消息发送，依赖WebSocket的ping/pong机制
                # 这样可以避免服务器不识别的消息格式错误
                        
            except Exception as e:
                logger.error(f"心跳线程异常: {e}")
                break

    def _schedule_reconnect(self):
        """安排重连"""
        if self.stopped or self.reconnect_attempts >= self.max_reconnect_attempts:
            if self.reconnect_attempts >= self.max_reconnect_attempts:
                logger.error(f"重连次数已达上限({self.max_reconnect_attempts})，停止重连")
            return
            
        self.reconnect_attempts += 1
        
        # 使用指数退避策略，但有最大延迟限制
        delay = min(self.reconnect_delay * (2 ** (self.reconnect_attempts - 1)), self.max_reconnect_delay)
        
        logger.info(f"将在 {delay} 秒后进行第 {self.reconnect_attempts} 次重连")
        
        def reconnect():
            time.sleep(delay)
            if not self.stopped:
                logger.info(f"开始第 {self.reconnect_attempts} 次重连...")
                self._connect()
        
        reconnect_thread = threading.Thread(target=reconnect, daemon=True, name=f"TTS-Reconnect-{self.reconnect_attempts}")
        reconnect_thread.start()

    def _start_audio_thread(self):
        """启动音频播放线程"""
        if not self.audio_thread or not self.audio_thread.is_alive():
            self.audio_playing = True
            self.audio_thread = threading.Thread(target=self._audio_player, daemon=True, name="TTS-AudioPlayer")
            self.audio_thread.start()

    def _audio_player(self):
        """音频播放线程"""
        while self.audio_playing:
            try:
                # 从队列中获取音频数据，超时1秒
                audio_data = self.audio_queue.get(timeout=1)
                if audio_data and self.stream:
                    if not self.stream.is_active():
                        self.stream.start_stream()
                    self.stream.write(audio_data)
                self.audio_queue.task_done()
            except queue.Empty:
                continue
            except Exception as e:
                logger.error(f"音频播放异常: {e}")

    def send_text(self, text: str) -> bool:
        """发送文本进行TTS合成"""
        if self.stopped or not self.session_started:
            logger.warning("TTS服务未连接，无法发送文本")
            return False

        try:
            with self.lock:
                # 验证文本
                if not text or not isinstance(text, str):
                    logger.error("输入文本无效")
                    return False
                
                cleaned_text = text.strip().replace('\n', ' ').replace('\r', ' ')
                if len(cleaned_text) < 1:
                    logger.error("文本长度过短")
                    return False

                # 确定帧状态
                if self.is_first_frame:
                    frame_status = STATUS_FIRST_FRAME
                    header_status = STATUS_FIRST_FRAME
                    self.is_first_frame = False
                    logger.info(f"发送第一帧文本: {cleaned_text}")
                else:
                    frame_status = STATUS_CONTINUE_FRAME
                    header_status = STATUS_CONTINUE_FRAME
                    logger.info(f"发送文本: {cleaned_text}")

                # 构建数据帧
                data_frame = {
                    "header": {**self.wsParam.CommonArgs, "status": header_status},
                    "parameter": self.wsParam.BusinessArgs if self.seq == 0 else {},
                    "payload": self.wsParam.create_data_frame(cleaned_text, frame_status, self.seq)
                }
                
                # 发送数据
                self.ws.send(json.dumps(data_frame))
                self.seq += 1
                
                logger.info(f"文本发送成功，序列号: {self.seq - 1}")
                return True
                
        except Exception as e:
            logger.error(f"发送文本失败: {e}")
            return False

    def finish_session(self) -> bool:
        """结束当前会话"""
        if not self.session_started:
            return True

        try:
            with self.lock:
                # 发送结束帧
                end_frame = {
                    "header": {**self.wsParam.CommonArgs, "status": STATUS_LAST_FRAME},
                    "parameter": {},
                    "payload": {
                        "text": {
                            "encoding": "utf8",
                            "compress": "raw",
                            "format": "plain",
                            "status": STATUS_LAST_FRAME,
                            "seq": self.seq,
                            "text": base64.b64encode("END".encode('utf-8')).decode()
                        }
                    }
                }
                
                self.ws.send(json.dumps(end_frame))
                self.seq += 1
                
                # 重置状态，准备下次会话
                self.is_first_frame = True
                
                logger.info("会话结束帧发送成功")
                return True
                
        except Exception as e:
            logger.error(f"结束会话失败: {e}")
            return False

    def is_connected(self) -> bool:
        """检查连接状态"""
        return self.session_started and not self.stopped

    def get_status(self) -> Dict[str, Any]:
        """获取服务状态"""
        return {
            "connected": self.is_connected(),
            "reconnect_attempts": self.reconnect_attempts,
            "seq": self.seq,
            "is_first_frame": self.is_first_frame,
            "synthesis_complete": self.synthesis_complete,
            "call_id": self.call_id,
            "timestamp": datetime.now().isoformat()
        }

    def add_connected_callback(self, callback: Callable):
        """添加连接成功回调"""
        self.on_connected_callbacks.append(callback)

    def add_disconnected_callback(self, callback: Callable):
        """添加断连回调"""
        self.on_disconnected_callbacks.append(callback)

    def set_call_id(self, call_id: str):
        """设置通话ID"""
        self.call_id = call_id
        logger.info(f"设置通话ID: {call_id}")

    def stop(self):
        """停止服务"""
        logger.info("正在停止TTS服务...")
        self.stopped = True
        
        # 停止心跳线程
        self._stop_heartbeat()
        
        # 关闭WebSocket连接
        if self.ws:
            try:
                self.ws.close()
            except Exception as e:
                logger.error(f"关闭WebSocket异常: {e}")
        
        # 停止音频播放
        self.audio_playing = False
        if self.audio_thread and self.audio_thread.is_alive():
            self.audio_thread.join(timeout=2)
        
        # 关闭音频流
        if self.stream:
            try:
                if self.stream.is_active():
                    self.stream.stop_stream()
                self.stream.close()
            except Exception as e:
                logger.error(f"关闭音频流异常: {e}")
        
        # 关闭PyAudio
        if self.p:
            try:
                self.p.terminate()
            except Exception as e:
                logger.error(f"关闭PyAudio异常: {e}")
        
        # 重置状态
        with self.lock:
            self.session_started = False
            self.reconnect_attempts = 0
        
        logger.info("TTS服务已停止")

    def restart(self):
        """重启服务"""
        logger.info("重启TTS服务...")
        self.stop()
        
        # 等待所有资源清理完成
        time.sleep(2)
        
        # 重置所有状态
        with self.lock:
            self.stopped = False
            self.session_started = False
            self.reconnect_attempts = 0
            self.seq = 0
            self.is_first_frame = True
            self.last_activity = time.time()
            self.heartbeat_running = False
            self.synthesis_complete = False
        
        # 重新初始化
        self._initialize()
        logger.info("TTS服务重启完成")


# 创建全局实例
tts_service = TtsService()

@asynccontextmanager
async def lifespan(app: FastAPI):
    """FastAPI生命周期管理"""
    # 启动时的操作
    logger.info("FastAPI应用启动，TTS服务已初始化")
    yield
    # 关闭时的操作
    logger.info("FastAPI应用关闭，正在停止TTS服务")
    tts_service.stop()

def get_tts_service() -> TtsService:
    """获取TTS服务实例"""
    return tts_service

