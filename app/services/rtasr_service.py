# app/services/rtasr_service.py
import hashlib
import hmac
import base64
from datetime import datetime
import json, time, threading
from websocket import create_connection
import websocket
from urllib.parse import quote
import pyaudio
import uuid
from typing import Optional, Callable, Dict, Any
import asyncio
from celery import current_app
from app.services.celery_service import celery_app
from app.core.config import settings
import logging
import redis

logger = logging.getLogger(__name__)

# Redis 连接
def get_redis_client():
    """获取 Redis 客户端"""
    try:
        redis_client = redis.Redis(
            host=settings.redis_host,
            port=settings.redis_port,
            db=settings.redis_db,
            password=settings.redis_password,
            decode_responses=True,
            socket_connect_timeout=5,
            socket_timeout=5
        )
        # 测试连接
        redis_client.ping()
        return redis_client
    except Exception as e:
        logger.error(f"Redis 连接失败: {e}")
        return None

class RtasrClient:
    """简化的 RTASR 客户端 - 专注于连接管理"""
    
    def __init__(self, session_id: str = None, user_id: str = "default"):
        self.session_id = session_id or str(uuid.uuid4())
        self.user_id = user_id
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
        
        logger.info(f"RTASR客户端初始化完成，会话ID: {self.session_id}")

    def create_connection(self) -> bool:
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
            
            logger.info(f"RTASR连接创建成功: {self.session_id}")
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
                    handle_rtasr_message.delay(self.session_id, result_dict)
                        
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
        logger.info(f"RTASR开始发送音频: {self.session_id}")

    def stop_audio(self):
        """停止发送音频"""
        self.is_sending_audio = False
        logger.info(f"RTASR停止发送音频: {self.session_id}")

    def close(self):
        """关闭连接"""
        logger.info(f"RTASR开始关闭: {self.session_id}")
        self.running = False
        
        if self.ws and self.ws.connected:
            try:
                end_tag = "{\"end\": true}"
                self.ws.send(bytes(end_tag.encode('utf-8')))
                self.ws.close()
            except:
                pass
        
        logger.info(f"RTASR已关闭: {self.session_id}")

    def get_status(self) -> Dict[str, Any]:
        """获取状态"""
        return {
            "session_id": self.session_id,
            "user_id": self.user_id,
            "connected": self.running and self.ws and self.ws.connected,
            "is_sending_audio": self.is_sending_audio,
            "timestamp": datetime.now().isoformat()
        }


# ==================== Celery 任务 ====================

@celery_app.task(bind=True, queue='rtasr_queue')
def start_rtasr_session(self, session_id: str = None, user_id: str = "default"):
    """启动 RTASR 会话任务"""
    try:
        session_id = session_id or str(uuid.uuid4())
        logger.info(f"启动 RTASR 会话: {session_id}")
        
        # 运行 RTASR 客户端
        asyncio.run(_run_rtasr_client(session_id, user_id))
        
        return {"status": "completed", "session_id": session_id}
    except Exception as e:
        logger.error(f"RTASR 会话任务失败: {e}")
        raise self.retry(countdown=60, max_retries=3)

async def _run_rtasr_client(session_id: str, user_id: str):
    """运行 RTASR 客户端的异步逻辑"""
    client = RtasrClient(session_id, user_id)
    
    # 将客户端存储到缓存中
    store_rtasr_client.delay(session_id, client.get_status())
    
    max_retries = 5
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            if client.create_connection():
                # 保持连接活跃
                while client.running:
                    await asyncio.sleep(1)
                    # 定期更新状态
                    update_rtasr_status.delay(session_id, client.get_status())
            else:
                retry_count += 1
                logger.warning(f"RTASR连接失败，重试 {retry_count}/{max_retries}")
                await asyncio.sleep(5)
                
        except Exception as e:
            logger.error(f"RTASR客户端错误: {e}")
            retry_count += 1
            await asyncio.sleep(5)
        finally:
            client.close()
    
    # 清理会话
    cleanup_rtasr_session.delay(session_id)

@celery_app.task(queue='rtasr_queue', ignore_result=True)
def handle_rtasr_message(session_id: str, message_data: Dict):
    """处理 RTASR 消息任务"""
    try:
        action = message_data.get("action")
        
        if action == "started":
            return handle_rtasr_started.delay(session_id, message_data)
        elif action == "result":
            return handle_rtasr_result.delay(session_id, message_data)
        elif action == "error":
            return handle_rtasr_error.delay(session_id, message_data)
        else:
            logger.debug(f"未知 RTASR 消息类型: {action}")
            
        return {"status": "processed", "action": action}
    except Exception as e:
        logger.error(f"处理 RTASR 消息错误: {e}")
        raise

@celery_app.task(queue='rtasr_queue')
def handle_rtasr_started(session_id: str, message_data: Dict):
    """处理 RTASR 启动消息"""
    try:
        logger.info(f"RTASR 启动成功: {session_id}")
        
        # 更新会话状态
        update_rtasr_status.delay(session_id, {
            "status": "started",
            "message": message_data,
            "timestamp": datetime.now().isoformat()
        })
        
        return {"status": "success", "session_id": session_id}
    except Exception as e:
        logger.error(f"处理 RTASR 启动消息错误: {e}")
        raise

@celery_app.task(queue='rtasr_queue')
def handle_rtasr_result(session_id: str, message_data: Dict):
    """处理 RTASR 识别结果"""
    try:
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
            logger.info(f"RTASR 识别结果 [{session_id}]: {msg_type} - {text}")
            
            # 处理识别结果
            process_rtasr_text.delay(session_id, text, msg_type)
        
        return {"status": "success", "text": text, "type": msg_type}
    except Exception as e:
        logger.error(f"处理 RTASR 结果错误: {e}")
        raise

@celery_app.task(queue='rtasr_queue')
def handle_rtasr_error(session_id: str, message_data: Dict):
    """处理 RTASR 错误消息"""
    try:
        error_code = message_data.get("code", "")
        error_desc = message_data.get("desc", "")
        
        logger.error(f"RTASR 错误 [{session_id}]: {error_code} - {error_desc}")
        
        # 如果是空闲超时，尝试重连
        if "37005" in error_desc or "Client idle timeout" in error_desc:
            logger.info(f"RTASR 空闲超时，重启会话: {session_id}")
            restart_rtasr_session.delay(session_id)
        
        return {"status": "error", "code": error_code, "desc": error_desc}
    except Exception as e:
        logger.error(f"处理 RTASR 错误消息失败: {e}")
        raise

@celery_app.task(queue='rtasr_queue')
def process_rtasr_text(session_id: str, text: str, msg_type: str):
    """处理识别的文本"""
    try:
        # 这里可以实现文本处理逻辑
        # 比如：语义分析、意图识别、触发AI回复等
        
        logger.info(f"处理识别文本 [{session_id}]: {text}")
        
        # 保存识别结果
        save_rtasr_result.delay(session_id, {
            "text": text,
            "type": msg_type,
            "timestamp": datetime.now().isoformat()
        })
        
        # 如果是完整句子，可以触发AI处理
        if msg_type == "0":  # 中间结果
            # 可以触发实时处理
            pass
        
        return {"status": "processed", "text": text}
    except Exception as e:
        logger.error(f"处理识别文本错误: {e}")
        raise

@celery_app.task(queue='rtasr_queue')
def control_rtasr_audio(session_id: str, action: str):
    """控制 RTASR 音频发送"""
    try:
        # 这里需要通过某种方式控制对应的客户端
        # 可以通过 Redis 或其他方式传递控制信号
        
        logger.info(f"控制 RTASR 音频 [{session_id}]: {action}")
        
        # 发送控制信号
        send_rtasr_control_signal.delay(session_id, action)
        
        return {"status": "success", "action": action}
    except Exception as e:
        logger.error(f"控制 RTASR 音频错误: {e}")
        raise

@celery_app.task(queue='rtasr_queue')
def restart_rtasr_session(session_id: str):
    """重启 RTASR 会话"""
    try:
        logger.info(f"重启 RTASR 会话: {session_id}")
        
        # 清理旧会话
        cleanup_rtasr_session.delay(session_id)
        
        # 启动新会话
        start_rtasr_session.delay(session_id)
        
        return {"status": "restarted", "session_id": session_id}
    except Exception as e:
        logger.error(f"重启 RTASR 会话错误: {e}")
        raise

# ==================== 数据存储任务 ====================

@celery_app.task(queue='rtasr_queue')
def store_rtasr_client(session_id: str, client_data: Dict):
    """存储 RTASR 客户端信息到Redis"""
    try:
        redis_client = get_redis_client()
        if not redis_client:
            logger.error("Redis连接失败，无法存储客户端信息")
            return {"status": "error", "message": "Redis连接失败"}
        
        # 生成Redis键名
        client_key = f"rtasr:client:{session_id}"
        
        # 存储客户端信息
        client_data["session_id"] = session_id
        client_data["created_at"] = datetime.now().isoformat()
        
        redis_client.hset(client_key, mapping=client_data)
        redis_client.expire(client_key, 86400)  # 24小时过期
        
        logger.info(f"RTASR客户端信息已存储到Redis: {session_id}")
        return {"status": "stored", "session_id": session_id}
    except Exception as e:
        logger.error(f"存储 RTASR 客户端信息到Redis错误: {e}")
        raise

@celery_app.task(queue='rtasr_queue')
def update_rtasr_status(session_id: str, status_data: Dict):
    """更新 RTASR 状态到Redis"""
    try:
        redis_client = get_redis_client()
        if not redis_client:
            logger.error("Redis连接失败，无法更新状态")
            return {"status": "error", "message": "Redis连接失败"}
        
        # 生成Redis键名
        status_key = f"rtasr:status:{session_id}"
        
        # 更新状态信息
        status_data["session_id"] = session_id
        status_data["updated_at"] = datetime.now().isoformat()
        
        redis_client.hset(status_key, mapping=status_data)
        redis_client.expire(status_key, 86400)  # 24小时过期
        
        logger.debug(f"RTASR状态已更新到Redis: {session_id}")
        return {"status": "updated", "session_id": session_id}
    except Exception as e:
        logger.error(f"更新 RTASR 状态到Redis错误: {e}")
        raise

@celery_app.task(queue='rtasr_queue')
def save_rtasr_result(session_id: str, result_data: Dict):
    """保存识别结果到Redis"""
    try:
        redis_client = get_redis_client()
        if not redis_client:
            logger.error("Redis连接失败，无法保存识别结果")
            return {"status": "error", "message": "Redis连接失败"}
        
        # 生成Redis键名
        result_key = f"rtasr:result:{session_id}"
        session_key = f"rtasr:session:{session_id}"
        
        # 保存识别结果到列表
        result_data_with_id = {
            "id": str(uuid.uuid4()),
            "session_id": session_id,
            **result_data
        }
        
        # 将结果添加到会话的结果列表中
        redis_client.lpush(result_key, json.dumps(result_data_with_id, ensure_ascii=False))
        
        # 设置过期时间（24小时）
        redis_client.expire(result_key, 86400)
        
        # 更新会话信息
        session_info = {
            "session_id": session_id,
            "last_result": result_data_with_id,
            "result_count": redis_client.llen(result_key),
            "updated_at": datetime.now().isoformat()
        }
        redis_client.hset(session_key, mapping=session_info)
        redis_client.expire(session_key, 86400)
        
        logger.info(f"识别结果已保存到Redis [{session_id}]: {result_data.get('text', '')}")
        return {"status": "saved", "session_id": session_id, "result_id": result_data_with_id["id"]}
    except Exception as e:
        logger.error(f"保存 RTASR 结果到Redis错误: {e}")
        raise

@celery_app.task(queue='rtasr_queue')
def cleanup_rtasr_session(session_id: str):
    """清理 RTASR 会话数据"""
    try:
        redis_client = get_redis_client()
        if not redis_client:
            logger.error("Redis连接失败，无法清理会话")
            return {"status": "error", "message": "Redis连接失败"}
        
        # 删除相关的Redis键
        keys_to_delete = [
            f"rtasr:client:{session_id}",
            f"rtasr:status:{session_id}",
            f"rtasr:session:{session_id}",
            f"rtasr:result:{session_id}"
        ]
        
        deleted_count = 0
        for key in keys_to_delete:
            if redis_client.exists(key):
                redis_client.delete(key)
                deleted_count += 1
        
        logger.info(f"RTASR会话数据已清理: {session_id}, 删除了 {deleted_count} 个键")
        return {"status": "cleaned", "session_id": session_id, "deleted_keys": deleted_count}
    except Exception as e:
        logger.error(f"清理 RTASR 会话数据错误: {e}")
        raise

@celery_app.task(queue='rtasr_queue')
def send_rtasr_control_signal(session_id: str, signal: str):
    """发送控制信号到Redis"""
    try:
        redis_client = get_redis_client()
        if not redis_client:
            logger.error("Redis连接失败，无法发送控制信号")
            return {"status": "error", "message": "Redis连接失败"}
        
        # 生成控制信号键名
        control_key = f"rtasr:control:{session_id}"
        
        # 发送控制信号
        control_data = {
            "session_id": session_id,
            "signal": signal,
            "timestamp": datetime.now().isoformat()
        }
        
        # 使用发布订阅模式发送控制信号
        redis_client.publish(f"rtasr:control:{session_id}", json.dumps(control_data, ensure_ascii=False))
        
        # 同时保存到控制历史
        redis_client.lpush(control_key, json.dumps(control_data, ensure_ascii=False))
        redis_client.ltrim(control_key, 0, 99)  # 只保留最近100条控制信号
        redis_client.expire(control_key, 86400)  # 24小时过期
        
        logger.info(f"RTASR控制信号已发送到Redis: {session_id} - {signal}")
        return {"status": "sent", "signal": signal, "session_id": session_id}
    except Exception as e:
        logger.error(f"发送控制信号到Redis错误: {e}")
        raise

# ==================== 服务管理类 ====================

class RtasrService:
    """RTASR 服务管理器 - Celery 版本"""
    
    @staticmethod
    def start_session(session_id: str = None, user_id: str = "default") -> str:
        """启动 RTASR 会话"""
        task = start_rtasr_session.delay(session_id, user_id)
        return task.id
    
    @staticmethod
    def control_audio(session_id: str, action: str) -> str:
        """控制音频发送"""
        task = control_rtasr_audio.delay(session_id, action)
        return task.id
    
    @staticmethod
    def restart_session(session_id: str) -> str:
        """重启会话"""
        task = restart_rtasr_session.delay(session_id)
        return task.id
    
    @staticmethod
    def cleanup_session(session_id: str) -> str:
        """清理会话"""
        task = cleanup_rtasr_session.delay(session_id)
        return task.id
    
    @staticmethod
    def get_session_results(session_id: str, limit: int = 50) -> Dict:
        """获取会话识别结果"""
        try:
            redis_client = get_redis_client()
            if not redis_client:
                return {"error": "Redis连接失败"}
            
            result_key = f"rtasr:result:{session_id}"
            results = redis_client.lrange(result_key, 0, limit - 1)
            
            parsed_results = []
            for result in results:
                try:
                    parsed_results.append(json.loads(result))
                except json.JSONDecodeError:
                    continue
            
            return {
                "session_id": session_id,
                "results": parsed_results,
                "total": redis_client.llen(result_key)
            }
        except Exception as e:
            logger.error(f"获取会话结果错误: {e}")
            return {"error": str(e)}
    
    @staticmethod
    def get_session_status(session_id: str) -> Dict:
        """获取会话状态"""
        try:
            redis_client = get_redis_client()
            if not redis_client:
                return {"error": "Redis连接失败"}
            
            status_key = f"rtasr:status:{session_id}"
            client_key = f"rtasr:client:{session_id}"
            session_key = f"rtasr:session:{session_id}"
            
            status = redis_client.hgetall(status_key)
            client_info = redis_client.hgetall(client_key)
            session_info = redis_client.hgetall(session_key)
            
            return {
                "session_id": session_id,
                "status": status,
                "client_info": client_info,
                "session_info": session_info
            }
        except Exception as e:
            logger.error(f"获取会话状态错误: {e}")
            return {"error": str(e)}
    
    @staticmethod
    def get_all_sessions() -> Dict:
        """获取所有活跃会话"""
        try:
            redis_client = get_redis_client()
            if not redis_client:
                return {"error": "Redis连接失败"}
            
            # 获取所有会话键
            session_keys = redis_client.keys("rtasr:session:*")
            sessions = []
            
            for key in session_keys:
                session_id = key.split(":")[-1]
                session_data = redis_client.hgetall(key)
                if session_data:
                    sessions.append({
                        "session_id": session_id,
                        **session_data
                    })
            
            return {
                "sessions": sessions,
                "total": len(sessions)
            }
        except Exception as e:
            logger.error(f"获取所有会话错误: {e}")
            return {"error": str(e)}


# ==================== 服务工厂函数 ====================

# 全局RTASR服务实例
_rtasr_service_instance = None

def get_rtasr_service() -> RtasrService:
    """获取RTASR服务实例（单例模式）"""
    global _rtasr_service_instance
    if _rtasr_service_instance is None:
        _rtasr_service_instance = RtasrService()
    return _rtasr_service_instance
