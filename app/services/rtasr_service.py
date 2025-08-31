# -*- encoding:utf-8 -*-
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
from app.core.config import settings

class RtasrClient:
    def __init__(self, session_id: str = None, user_id: str = "default", 
                 on_text_received: Optional[Callable] = None, 
                 on_sentence_received: Optional[Callable] = None, 
                 debounce_delay: float = 1.0):
        self.session_id = session_id or str(uuid.uuid4())
        self.user_id = user_id
        self.app_id = settings.rtasr_appid
        self.api_key = settings.rtasr_api_key
        self.on_text_received = on_text_received
        self.on_sentence_received = on_sentence_received
        self.last_text = ""
        self.buffer_text = ""
        self.debounce_timer = None
        self.debounce_delay = debounce_delay
        
        # 音频控制标志
        self.is_sending_audio = False
        self.silence_data = b'\x00' * 1280  # 1280字节空白音频
        self.ws = None
        self.trecv = None
        self.audio_thread = None
        self.running = False
        
        # 缓冲池相关
        self.buffer_start_time: Optional[float] = None
        self.max_buffer_time = 15.0 # 缓冲池最大时间
        self.force_flush_timer: Optional[threading.Timer] = None
        self.is_processing = False
        
        print(f"[{datetime.now()}]RTASR客户端初始化完成，会话ID: {self.session_id}")

    def start(self):
        """启动RTASR客户端"""
        try:
            if self._create_connection():
                return True
            return False
        except Exception as e:
            print(f"[{datetime.now()}]RTASR启动失败: {e}")
            return False

    def _create_connection(self):
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
            
            # 添加engLangType=2参数，使用中文模式
            url = base_url + "?appid=" + self.app_id + "&ts=" + ts + "&signa=" + quote(signa) + "&engLangType=2"
            
            self.ws = create_connection(url, timeout=10)
            self.running = True
            
            # 启动接收线程
            self.trecv = threading.Thread(target=self.recv, daemon=True)
            self.trecv.start()
            
            # 立即启动音频发送线程，防止空闲超时
            self.audio_thread = threading.Thread(target=self.send_mic_audio, daemon=True)
            self.audio_thread.start()
            
            print(f"[{datetime.now()}]RTASR连接创建成功")
            return True
            
        except Exception as e:
            print(f"[{datetime.now()}]RTASR连接创建失败: {e}")
            return False

    def send_mic_audio(self):
        """发送麦克风音频 - 关键改进版本"""
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
            
            print(f"[{datetime.now()}]RTASR开始音频流发送...")
            
            while self.running and self.ws and self.ws.connected:
                try:
                    if self.is_sending_audio:
                        # 发送真实麦克风音频
                        chunk = stream.read(1280, exception_on_overflow=False)
                        self.ws.send(chunk)
                    else:
                        # 发送静音数据保持连接活跃
                        self.ws.send(self.silence_data)
                    
                    # 严格按照官方建议：每40ms发送1280字节
                    time.sleep(0.04)
                    
                except Exception as e:
                    print(f"[{datetime.now()}]RTASR音频发送异常: {e}")
                    break
                    
        except Exception as e:
            print(f"[{datetime.now()}]RTASR音频初始化异常: {e}")
        finally:
            # 清理资源
            if stream:
                stream.stop_stream()
                stream.close()
            if p:
                p.terminate()
            print(f"[{datetime.now()}]RTASR音频资源已清理")

    def recv(self):
        """接收服务器响应"""
        try:
            while self.running and self.ws and self.ws.connected:
                try:
                    result = str(self.ws.recv())
                    if len(result) == 0:
                        print(f"[{datetime.now()}]RTASR接收结束")
                        break
                        
                    result_dict = json.loads(result)
                    
                    if result_dict["action"] == "started":
                        print(f"[{datetime.now()}]RTASR启动成功: {result}")
                        
                    elif result_dict["action"] == "result":
                        if self.on_text_received:
                            self.on_text_received(result_dict["data"])
                        self.on_result_received(result_dict["data"])
                        
                    elif result_dict["action"] == "error":
                        print(f"[{datetime.now()}]RTASR错误: {result}")
                        # 遇到错误时尝试重连
                        self._handle_error(result_dict)
                        break
                        
                except websocket.WebSocketTimeoutException:
                    print(f"[{datetime.now()}]RTASR接收超时，继续监听...")
                    continue
                except Exception as e:
                    print(f"[{datetime.now()}]RTASR接收异常: {e}")
                    break
                    
        except Exception as e:
            print(f"[{datetime.now()}]RTASR接收线程异常: {e}")

    def _handle_error(self, error_dict):
        """处理错误并尝试重连"""
        error_code = error_dict.get("code", "")
        error_desc = error_dict.get("desc", "")
        
        if "37005" in error_desc or "Client idle timeout" in error_desc:
            print(f"[{datetime.now()}]RTASR检测到空闲超时，尝试重连...")
            self._reconnect()
        else:
            print(f"[{datetime.now()}]RTASR其他错误: {error_code} - {error_desc}")

    def _reconnect(self):
        """重连机制"""
        print(f"[{datetime.now()}]RTASR开始重连...")
        self.running = False
        
        if self.ws:
            try:
                self.ws.close()
            except:
                pass
        
        time.sleep(2)  # 等待2秒后重连
        
        if self._create_connection():
            print(f"[{datetime.now()}]RTASR重连成功")
        else:
            print(f"[{datetime.now()}]RTASR重连失败")

    def start_sending_audio(self):
        """开始发送真实麦克风音频"""
        self.is_sending_audio = True
        print(f"[{datetime.now()}]RTASR开始发送麦克风音频")

    def stop_sending_audio(self):
        """停止发送真实音频，但继续发送静音保持连接"""
        self.is_sending_audio = False
        print(f"[{datetime.now()}]RTASR停止发送麦克风音频，切换到静音模式")

    def on_result_received(self, data):
        """处理识别结果"""
        try:
            data_json = json.loads(data)
            words = []
            
            for rt in data_json["cn"]["st"]["rt"]:
                for ws in rt["ws"]:
                    for cw in ws["cw"]:
                        words.append(cw["w"])
            
            current_text = ''.join(words)
            msg_type = data_json["cn"]["st"]["type"]
            
            if msg_type == "0":  # 中间结果
                # 如果缓冲池为空，记录开始时间
                if not self.buffer_text.strip():
                    self.buffer_start_time = time.time()
                    print(f"[{datetime.now()}]RTASR缓冲池开始计时")
                
                self.buffer_text += current_text
                print(f"[{datetime.now()}]RTASR缓冲区增加: {self.buffer_text}")
                
                # 启动强制清空定时器
                self._start_force_flush_timer()
            
            if current_text != self.last_text:
                self.last_text = current_text
                print(f"[{datetime.now()}]RTASR识别结果: {msg_type} - {current_text}")
                
                # 防抖处理
                if self.debounce_timer:
                    self.debounce_timer.cancel()
                
                def trigger_callback():
                    if self.buffer_text.strip() and not self.is_processing:
                        self._flush_buffer()
                
                self.debounce_timer = threading.Timer(self.debounce_delay, trigger_callback)
                self.debounce_timer.start()
                
        except Exception as e:
            print(f"[{datetime.now()}]RTASR结果处理异常: {e}")
    
    def _start_force_flush_timer(self):
        """启动强制清空定时器"""
        if self.force_flush_timer:
            self.force_flush_timer.cancel()
        
        def force_flush():
            if self.buffer_text.strip() and not self.is_processing:
                print(f"[{datetime.now()}]RTASR强制清空缓冲池（15秒超时）")
                self._flush_buffer()
        
        self.force_flush_timer = threading.Timer(self.max_buffer_time, force_flush)
        self.force_flush_timer.start()
    
    def _flush_buffer(self):
        """清空缓冲池并触发AI处理"""
        if not self.buffer_text.strip() or self.is_processing:
            return
        
        self.is_processing = True
        buffer_content = self.buffer_text.strip()
        self.buffer_text = ""
        
        # 取消定时器
        if self.force_flush_timer:
            self.force_flush_timer.cancel()
            self.force_flush_timer = None
        
        if self.debounce_timer:
            self.debounce_timer.cancel()
            self.debounce_timer = None
        
        # 计算缓冲时间
        buffer_duration = 0
        if self.buffer_start_time:
            buffer_duration = time.time() - self.buffer_start_time
            self.buffer_start_time = None
        
        print(f"[{datetime.now()}]RTASR缓冲池清空，内容: {buffer_content}，缓冲时间: {buffer_duration:.2f}秒")
        
        # 触发回调
        if self.on_sentence_received:
            self.on_sentence_received(buffer_content)
        
        # 重置处理状态
        self.is_processing = False

    def close(self):
        """关闭连接"""
        print(f"[{datetime.now()}]RTASR开始关闭...")
        self.running = False
        
        # 清理定时器
        if self.debounce_timer:
            self.debounce_timer.cancel()
            self.debounce_timer = None
        
        if self.force_flush_timer:
            self.force_flush_timer.cancel()
            self.force_flush_timer = None
        
        # 清空缓冲池
        if self.buffer_text.strip():
            print(f"[{datetime.now()}]RTASR关闭时清空剩余缓冲: {self.buffer_text}")
            self.buffer_text = ""
        
        if self.ws and self.ws.connected:
            try:
                # 发送结束标记
                end_tag = "{\"end\": true}"
                self.ws.send(bytes(end_tag.encode('utf-8')))
                print(f"[{datetime.now()}]RTASR已发送结束标记")
            except:
                pass
            
            try:
                self.ws.close()
            except:
                pass
        
        print(f"[{datetime.now()}]RTASR已关闭")

    def get_status(self) -> Dict[str, Any]:
        """获取客户端状态"""
        return {
            "session_id": self.session_id,
            "user_id": self.user_id,
            "connected": self.running and self.ws and self.ws.connected,
            "is_sending_audio": self.is_sending_audio,
            "last_text": self.last_text,
            "buffer_text": self.buffer_text,
            "timestamp": datetime.now().isoformat()
        }
    
    def clear_buffer(self):
        """清空缓冲池"""
        try:
            # 清空缓冲文本
            self.buffer_text = ""
            self.last_text = ""
            
            # 取消定时器
            if self.debounce_timer:
                self.debounce_timer.cancel()
                self.debounce_timer = None
            
            if self.force_flush_timer:
                self.force_flush_timer.cancel()
                self.force_flush_timer = None
            
            # 重置缓冲开始时间
            self.buffer_start_time = None
            
            # 重置处理状态
            self.is_processing = False
            
            print(f"[{datetime.now()}]RTASR缓冲池已清空")
            
        except Exception as e:
            print(f"[{datetime.now()}]清空RTASR缓冲池失败: {e}")


class RtasrService:
    """RTASR服务管理器"""
    
    def __init__(self):
        self.clients: Dict[str, RtasrClient] = {}

    def create_client(self, session_id: str = None, user_id: str = "default", 
                     on_text_received: Optional[Callable] = None, 
                     on_sentence_received: Optional[Callable] = None, 
                     debounce_delay: float = 1.0) -> RtasrClient:
        """创建RTASR客户端"""
        client = RtasrClient(
            session_id=session_id,
            user_id=user_id,
            on_text_received=on_text_received,
            on_sentence_received=on_sentence_received,
            debounce_delay=debounce_delay
        )
        
        self.clients[client.session_id] = client
        return client

    def get_client(self, session_id: str) -> Optional[RtasrClient]:
        """获取客户端"""
        return self.clients.get(session_id)

    def remove_client(self, session_id: str):
        """移除客户端"""
        if session_id in self.clients:
            client = self.clients[session_id]
            client.close()
            del self.clients[session_id]

    def get_all_sessions(self) -> Dict[str, Dict[str, Any]]:
        """获取所有会话状态"""
        sessions = {}
        for session_id, client in self.clients.items():
            sessions[session_id] = client.get_status()
        return sessions

    def cleanup_expired_sessions(self):
        """清理过期的会话"""
        expired_sessions = []
        for session_id, client in self.clients.items():
            # 检查会话是否已停止
            if not client.running:
                expired_sessions.append(session_id)
        
        for session_id in expired_sessions:
            self.remove_client(session_id)


# 全局服务实例
_rtasr_service = None

def get_rtasr_service() -> RtasrService:
    """获取RTASR服务实例"""
    global _rtasr_service
    if _rtasr_service is None:
        _rtasr_service = RtasrService()
    return _rtasr_service


# 使用示例
def on_text_callback(data):
    print(f"[{datetime.now()}]收到文本: {data}")

def on_sentence_callback(sentence):
    print(f"[{datetime.now()}]完整句子: {sentence}")

if __name__ == '__main__':
    # 创建客户端
    rtasr_client = RtasrClient(
        on_text_received=on_text_callback,
        on_sentence_received=on_sentence_callback,
        debounce_delay=1
    )
    
    try:
        # 启动客户端
        if rtasr_client.start():
            # 等待3秒后开始真实录音
            print("等待3秒后开始录音...")
            time.sleep(3)
            rtasr_client.start_sending_audio()
            
            # 录音20秒
            time.sleep(20)
            rtasr_client.stop_sending_audio()
            
            # 再等待5秒观察静音模式
            time.sleep(5)
        else:
            print("RTASR客户端启动失败")
        
    except KeyboardInterrupt:
        print("用户中断")
    finally:
        rtasr_client.close()
