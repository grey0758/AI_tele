# app/services/rtasr_service.py
from contextlib import asynccontextmanager
import hashlib
import hmac
import base64
from datetime import datetime
import json, time, threading
from fastapi import FastAPI
from websocket import create_connection
import websocket
from urllib.parse import quote
import pyaudio
from typing import Dict, Any
from celery import current_app
from app.core.config import settings
from app.services.celery_service import get_celery_app

celery_app = get_celery_app()

# 使用主应用的logger
from app.core.logger import get_logger

# 获取模块级别的logger
logger = get_logger(__name__)

class RtasrService:
    """简化的 RTASR 客户端 - 专注于连接管理"""
    
    def __init__(self):

        self.call_id = None

        self.app_id = settings.rtasr_appid
        self.api_key = settings.rtasr_api_key
        
        # 连接状态
        self.ws = None
        self.running = False
        self.is_sending_audio = False
        self.silence_data = b'\x00' * 1280
        
        # 线程管理
        self.trecv = None
        self.audio_thread = None

        logger.info(f"RTASR客户端初始化完成，会话ID:")

    def create_connection(self, call_id: str) -> bool:
        self.call_id = call_id
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
            
            self.ws = create_connection(url, timeout=10)
            self.running = True
            
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

    def _recv_loop(self):
        """接收消息循环"""
        try:
            while self.running and self.ws and self.ws.connected:
                try:
                    result = str(self.ws.recv())
                    if len(result) == 0:
                        break
                        
                    result_dict = json.loads(result)
                    
                    # 将消息处理委托给 Celery 任务
                    current_app.send_task('app.tasks.rtasr_tasks.handle_rtasr_message', args=[self.call_id, result_dict])
                        
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
            
            while self.running and self.ws and self.ws.connected:
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
            if stream:
                stream.stop_stream()
                stream.close()
            if p:
                p.terminate()

    def start_audio(self):
        """开始发送音频"""
        self.is_sending_audio = True
        logger.info(f"RTASR开始发送音频")

    def stop_audio(self):
        """停止发送音频"""
        self.is_sending_audio = False
        logger.info(f"RTASR停止发送音频")

    def close(self):
        """关闭连接"""
        logger.info(f"RTASR开始关闭")
        self.running = False
        
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
            "connected": self.running and self.ws and self.ws.connected,
            "is_sending_audio": self.is_sending_audio,
            "timestamp": datetime.now().isoformat()
        }


# =================== Celery 任务 ====================

# @celery_app.task(queue='rtasr_queue')
# def start_rtasr_session(call_id: str):
#     """启动 RTASR 会话"""
#     rtasr_client.create_connection(call_id)
#     return {"status": "success", "action": "started"}

# @celery_app.task(queue='rtasr_queue')
# def stop_rtasr_session():
#     """停止 RTASR 会话"""
#     rtasr_client.close()
#     return {"status": "success", "action": "stopped"}

# @celery_app.task(queue='rtasr_queue')
# def start_audio(call_id: str):
#     """开始发送音频"""
#     rtasr_client.start_audio(call_id)
#     return {"status": "success", "action": "started"}

# @celery_app.task(queue='rtasr_queue')
# def stop_audio(call_id: str):
#     """停止发送音频"""
#     rtasr_client.stop_audio(call_id)
#     return {"status": "success", "action": "stopped"}

# @celery_app.task(queue='rtasr_queue', ignore_result=True)
# def handle_rtasr_message(call_id: str, message_data: Dict):
#     """处理 RTASR 消息的统一任务"""
#     try:
#         action = message_data.get("action")
#         logger.debug(f"处理 RTASR 消息:{action}")
        
#         if action == "started":
#             # 处理启动消息
#             logger.info(f"RTASR 启动成功:")
            
#             # 更新会话状态
#             update_rtasr_status.delay({
#                 "status": "started",
#                 "message": message_data,
#                 "timestamp": datetime.now().isoformat()
#             })
            
#             return {"status": "success", "action": "started"}
            
#         elif action == "result":
#             # 处理识别结果
#             data = message_data.get("data", "{}")
#             data_json = json.loads(data)
            
#             # 提取识别文本
#             words = []
#             for rt in data_json.get("cn", {}).get("st", {}).get("rt", []):
#                 for ws in rt.get("ws", []):
#                     for cw in ws.get("cw", []):
#                         words.append(cw.get("w", ""))
            
#             text = ''.join(words)
#             msg_type = data_json.get("cn", {}).get("st", {}).get("type", "")
            
#             if text.strip():
#                 logger.info(f"RTASR 识别结果{msg_type} - {text}")
                
#                 # 处理识别结果
#                 dialog_entry_data = DialogEntry(
#                     speaker="user",
#                     content=text,
#                     timestamp= datetime.now().isoformat()
#                 )
#                 process_rtasr_text.delay(call_id, dialog_entry_data, msg_type)
            
#             return {"status": "success", "action": "result", "text": text, "type": msg_type}
            
#         elif action == "error":
#             # 处理错误消息
#             error_code = message_data.get("code", "")
#             error_desc = message_data.get("desc", "")
            
#             logger.error(f"RTASR 错误 [{error_code} - {error_desc}")
            
#             # 如果是空闲超时，尝试重连
#             # if "37005" in error_desc or "Client idle timeout" in error_desc:
#             #     logger.info(f"RTASR 空闲超时，重启会话")
#             #     restart_rtasr_session.delay()
            
#             return {"status": "error", "action": "error", "code": error_code, "desc": error_desc}
#         else:
#             logger.debug(f"未知 RTASR 消息类型: {action}")
#             return {"status": "unknown", "action": action}
            
#     except json.JSONDecodeError as e:
#         logger.error(f"RTASR 消息 JSON 解析错误: {e}")
#         return {"status": "json_error", "error": str(e)}
#     except Exception as e:
#         logger.error(f"处理 RTASR 消息错误: {e}")
#         raise

# @celery_app.task(queue='rtasr_queue')
# def process_rtasr_text(call_id: str, dialog_entry_data: DialogEntry, msg_type: str):
#     """处理识别的文本"""
#     try:
#         # 这里可以实现文本处理逻辑
#         # 比如：语义分析、意图识别、触发AI回复等
        
#         add_dialog_to_call_record.delay( call_id, dialog_entry_data, msg_type )
        
#         # 如果是完整句子，可以触发AI处理
#         if msg_type == "0":  # 中间结果
#             # 可以触发实时处理
#             pass
        
#         return {"status": "processed", "text": dialog_entry_data.content}
#     except Exception as e:
#         logger.error(f"处理识别文本错误: {e}")
#         raise

# # ==================== 数据存储任务 ====================

# @celery_app.task(queue='rtasr_queue')
# def store_rtasr_client(client_data: Dict):
#     """存储 RTASR 客户端信息"""
#     try:
#         # 实现客户端信息存储逻辑
#         # 比如存储到 Redis 或数据库
#         logger.debug(f"存储 RTASR 客户端信息")
#         return {"status": "stored"}
#     except Exception as e:
#         logger.error(f"存储 RTASR 客户端信息错误: {e}")
#         raise

# @celery_app.task(queue='rtasr_queue')
# def update_rtasr_status(status_data: Dict):
#     """更新 RTASR 状态"""
#     try:
#         # 实现状态更新逻辑
#         logger.debug(f"更新 RTASR 状态")
#         return {"status": "updated"}
#     except Exception as e:
#         logger.error(f"更新 RTASR 状态错误: {e}")
#         raise
