# app/services/tts_service.py
import os
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
import queue
import uuid
import asyncio
from typing import Optional, Dict, Any, Callable
from app.core.config import settings
import logging

from app.services.celery_service import celery_app

logger = logging.getLogger(__name__)

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
        self.max_reconnect_attempts = 5
        self.reconnect_delay = 2  # 重连延迟秒数
        
        # 线程锁，确保线程安全
        self.lock = threading.Lock()
        
        # 连接状态回调
        self.on_connected_callbacks = []
        self.on_disconnected_callbacks = []
        
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
            ws_thread = threading.Thread(target=self._run_websocket, daemon=True)
            ws_thread.start()
            
            logger.info("WebSocket连接启动中...")
            
        except Exception as e:
            logger.error(f"WebSocket连接失败: {e}")
            self._schedule_reconnect()

    def _run_websocket(self):
        """在后台线程中运行WebSocket"""
        try:
            self.ws.run_forever(
                sslopt={"cert_reqs": ssl.CERT_NONE},
                ping_interval=30,
                ping_timeout=10,
                ping_payload="TTS_KEEPALIVE"
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
            
        logger.info("WebSocket连接已建立")
        
        # 调用连接回调
        for callback in self.on_connected_callbacks:
            try:
                callback()
            except Exception as e:
                logger.error(f"连接回调执行失败: {e}")

    def _on_message(self, ws, message):
        """处理WebSocket消息"""
        try:
            if self.stopped:
                return

            message = json.loads(message)
            code = message["header"]["code"]
            sid = message["header"]["sid"]

            if code != 0:
                errMsg = message["message"]
                logger.error(f"sid:{sid} call error:{errMsg} code is:{code}")
                return

            if "payload" in message and "audio" in message["payload"]:
                audio = base64.b64decode(message["payload"]["audio"]["audio"])
                status = message["payload"]["audio"]["status"]
                
                if status == 0:
                    logger.info("开始接收音频数据")
                elif status == 2:
                    logger.info("音频数据接收完成")

                # 实时播放音频
                if not self.stopped and self.stream and len(audio) > 0:
                    try:
                        if not self.stream.is_active():
                            self.stream.start_stream()
                        self.stream.write(audio)
                    except Exception as e:
                        logger.error(f"音频播放异常: {e}")

        except Exception as e:
            logger.error(f"处理WebSocket消息异常: {e}")

    def _on_error(self, ws, error):
        """WebSocket错误处理"""
        logger.error(f"WebSocket错误: {error}")

    def _on_close(self, ws, close_status_code, close_msg):
        """WebSocket连接关闭"""
        with self.lock:
            self.session_started = False
            
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

    def _schedule_reconnect(self):
        """安排重连"""
        if self.stopped or self.reconnect_attempts >= self.max_reconnect_attempts:
            return
            
        self.reconnect_attempts += 1
        delay = self.reconnect_delay * self.reconnect_attempts
        
        logger.info(f"将在 {delay} 秒后进行第 {self.reconnect_attempts} 次重连")
        
        def reconnect():
            time.sleep(delay)
            if not self.stopped:
                self._connect()
        
        threading.Thread(target=reconnect, daemon=True).start()

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
            "timestamp": datetime.now().isoformat()
        }

    def add_connected_callback(self, callback: Callable):
        """添加连接成功回调"""
        self.on_connected_callbacks.append(callback)

    def add_disconnected_callback(self, callback: Callable):
        """添加断连回调"""
        self.on_disconnected_callbacks.append(callback)

    def stop(self):
        """停止服务"""
        logger.info("正在停止TTS服务...")
        self.stopped = True
        
        if self.ws:
            self.ws.close()
        
        if self.stream:
            try:
                self.stream.stop_stream()
                self.stream.close()
            except Exception as e:
                logger.error(f"关闭音频流异常: {e}")
        
        if self.p:
            self.p.terminate()
        
        logger.info("TTS服务已停止")

    def restart(self):
        """重启服务"""
        logger.info("重启TTS服务...")
        self.stop()
        time.sleep(1)
        self.stopped = False
        self.session_started = False
        self.reconnect_attempts = 0
        self._initialize()


# 创建全局实例
tts_service = TtsService()

# 导出
__all__ = ["tts_service"]

# ==================== Celery 任务 ====================
@celery_app.task(queue='tts_queue')
def add_tts_text(text: str):
    tts_service.send_text(text)
    

