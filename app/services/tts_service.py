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

from app.core.config import settings

STATUS_FIRST_FRAME = 0  # 第一帧的标识
STATUS_CONTINUE_FRAME = 1  # 中间帧标识
STATUS_LAST_FRAME = 2  # 最后一帧的标识


class Ws_Param(object):
    # 初始化
    def __init__(self, APPID, APIKey, APISecret):
        self.APPID = APPID
        self.APIKey = APIKey
        self.APISecret = APISecret

        # 公共参数(common)
        self.CommonArgs = {
            "app_id": self.APPID,
            "status": 2  # 这个会在发送时动态设置
        }
        # 业务参数(business)，更多个性化参数可在官网查看
        self.BusinessArgs = {
            "oral": {
                "oral_level": "mid"
            },
            "tts": {
                "vcn": "x5_lingyuyan_flow",  # 发音人参数，更换不同的发音人会有不同的音色效果
                "volume": 50,  # 设置音量大小
                "rhy": 0,  # 是否返回拼音标注		0:不返回拼音, 1:返回拼音（纯文本格式，utf8编码）
                "speed": 55,  # 设置合成语速，值越大，语速越快
                "pitch": 50,  # 设置振幅高低，可通过该参数调整效果
                "bgs": 0,  # 背景音	0:无背景音, 1:内置背景音1, 2:内置背景音2
                "reg": 0,  # 英文发音方式 	0:自动判断处理，如果不确定将按照英文词语拼写处理（缺省）, 1:所有英文按字母发音, 2:自动判断处理，如果不确定将按照字母朗读
                "rdn": 0,  # 合成音频数字发音方式	0:自动判断, 1:完全数值, 2:完全字符串, 3:字符串优先
                "audio": {
                    "encoding": "raw",  # 合成音频格式，raw 合成音频格式为PCM
                    "sample_rate": 24000,  # 合成音频采样率，	16000, 8000, 24000
                    "channels": 1,  # 音频声道数
                    "bit_depth": 16,  # 合成音频位深 ：16, 8
                    "frame_size": 0
                }
            }
        }

    # 生成url
    def create_url(self):
        url = 'wss://cbm01.cn-huabei-1.xf-yun.com/v1/private/mcd9m97e6'
        # 生成RFC1123格式的时间戳
        now = datetime.now()
        date = format_date_time(mktime(now.timetuple()))

        # 拼接字符串
        signature_origin = f"host: cbm01.cn-huabei-1.xf-yun.com\ndate: {date}\nGET /v1/private/mcd9m97e6 HTTP/1.1"
        # 进行hmac-sha256进行加密
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
        # 将请求的鉴权参数组合为字典
        v = {
            "authorization": authorization,
            "date": date,
            "host": "cbm01.cn-huabei-1.xf-yun.com"
        }
        # 拼接鉴权参数，生成url
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


class StreamingTTSClient:
    def __init__(
        self, 
        session_id: str = None,
        user_id: str = "default",
        on_over=None,
        APPID=settings.tts_appid_c,
        APIKey=settings.tts_apikey_c,
        APISecret=settings.tts_apisecret_c
    ):
        self.session_id = session_id or str(uuid.uuid4())
        self.user_id = user_id
        self.p = None
        self.stream = None
        self.wsParam = Ws_Param(APPID, APIKey, APISecret)
        self.ws = None
        self.on_over = on_over
        self.stopped = False
        self.text_queue = queue.Queue()  # 文本队列
        self.seq = 0  # 序列号
        self.is_first_frame = True  # 是否是第一帧
        self.session_started = False  # 会话是否已开始
        
        # 创建缓存目录
        self.cache_dir = "tts_log2"
        if not os.path.exists(self.cache_dir):
            os.makedirs(self.cache_dir)
        self.audio_data = bytearray()  # 存储音频数据用于缓存
        
        # 控制命令检查线程
        self.control_thread = None
        self.control_running = False

    def validate_and_process_text(self, text):
        """验证和预处理TTS输入文本"""
        if not text or not isinstance(text, str):
            raise ValueError("输入文本必须是非空字符串")
        
        # 清理文本，移除多余空白字符
        cleaned_text = text.strip()
        
        # 检查文本长度
        if len(cleaned_text) < 1:
            raise ValueError("文本长度过短，不能为空")
        
        # 替换特殊字符，确保兼容性
        cleaned_text = cleaned_text.replace('\n', ' ').replace('\r', ' ')
        
        return cleaned_text

    def add_text(self, text):
        """添加要合成的文本到队列"""
        if not self.stopped:
            try:
                # 验证和预处理文本
                processed_text = self.validate_and_process_text(text)
                self.text_queue.put(processed_text)
                print(f"[{datetime.now()}]添加文本到队列: {processed_text}")
                    
            except ValueError as e:
                print(f"[{datetime.now()}]文本验证失败: {e}")
                # 使用备用文本
                fallback_text = "文本内容无效，已跳过"
                self.text_queue.put(fallback_text)
                print(f"[{datetime.now()}]使用备用文本: {fallback_text}")
            except Exception as e:
                print(f"[{datetime.now()}]处理文本时出错: {e}")

    def finish_input(self):
        """标记输入结束"""
        if not self.stopped:
            self.text_queue.put(None)  # None 表示输入结束
            print(f"[{datetime.now()}]标记输入结束")

    def on_message(self, ws, message):
        try:
            if self.stopped:
                ws.close()
                return

            message = json.loads(message)
            code = message["header"]["code"]
            sid = message["header"]["sid"]

            if code != 0:
                errMsg = message["message"]
                print("sid:%s call error:%s code is:%s" % (sid, errMsg, code))
            else:
                if "payload" in message and "audio" in message["payload"]:
                    audio = base64.b64decode(message["payload"]["audio"]["audio"])
                    status = message["payload"]["audio"]["status"]
                    
                    # 简化音频数据日志，只显示状态
                    if status == 0:  # 第一块音频
                        print(f"[{datetime.now()}]开始接收音频数据")
                    elif status == 2:  # 最后一块音频
                        print(f"[{datetime.now()}]音频数据接收完成")

                    # 实时播放
                    if not self.stopped and self.stream and len(audio) > 0:
                        try:
                            # 检查音频流状态
                            if not self.stream.is_active():
                                print(f"[{datetime.now()}]音频流未激活，重新启动")
                                self.stream.start_stream()
                            
                            self.stream.write(audio)
                            # 简化播放日志，不显示具体字节数
                        except Exception as e:
                            print(f"[{datetime.now()}]tts 音频播放异常: {e}")
                            
                            # 尝试恢复音频流
                            try:
                                if self.stream and not self.stream.is_active():
                                    print(f"[{datetime.now()}]音频流恢复中...")
                                    self.stream.start_stream()
                                    # 重试播放
                                    self.stream.write(audio)
                                    print(f"[{datetime.now()}]音频流恢复成功")
                            except Exception as retry_e:
                                print(f"[{datetime.now()}]音频流恢复失败: {retry_e}")

                    if status == 2:  # 音频传输结束
                        print(f"[{datetime.now()}]音频传输完成")
                        
                        # 等待音频播放完成
                        if self.stream and not self.stopped:
                            try:
                                self.stream.stop_stream()
                            except Exception as e:
                                print(f"[{datetime.now()}]停止音频流异常: {e}")
                        
                        print(f"[{datetime.now()}]TTS播放完成")
                        ws.close()

        except Exception as e:
            print(f"[{datetime.now()}]tts receive msg, but parse exception:", e)
            import traceback
            traceback.print_exc()

    def on_error(self, ws, error):
        print(f"[{datetime.now()}]tts WebSocket错误: {error}")

    def on_close(self, ws, close_status_code, close_msg):
        print(f"[{datetime.now()}]tts WebSocket连接已关闭")
        
        # 停止控制命令检查
        self._stop_control_check()
        
        if self.stream:
            try:
                self.stream.stop_stream()
                self.stream.close()
                self.stream = None
            except Exception as e:
                print(f"[{datetime.now()}]tts 关闭音频流异常：", e)
        if self.p:
            self.p.terminate()
        
        # 调用结束回调
        if self.on_over and not self.stopped:
            self.on_over()

    def on_open(self, ws):
        """WebSocket连接打开后开始处理文本队列"""
        print(f"[{datetime.now()}]WebSocket连接已打开，开始处理文本队列")
        
        # 立即设置连接状态
        self.session_started = True
        print(f"[{datetime.now()}]WebSocket连接状态设置为已建立")
        
        def process_text_queue():
            try:
                while not self.stopped:
                    try:
                        # 从队列获取文本，设置超时避免无限等待
                        text = self.text_queue.get(timeout=1.0)
                        
                        if text is None:  # 输入结束标记
                            print(f"[{datetime.now()}]收到输入结束标记，发送结束帧")
                            # 发送结束帧，使用特殊标记而不是空字符串
                            if self.session_started:
                                end_frame = {
                                    "header": {**self.wsParam.CommonArgs, "status": STATUS_LAST_FRAME},
                                    "parameter": {},  # 结束帧不需要参数
                                    "payload": {
                                        "text": {
                                            "encoding": "utf8",
                                            "compress": "raw",
                                            "format": "plain",
                                            "status": STATUS_LAST_FRAME,
                                            "seq": self.seq,
                                            "text": base64.b64encode("END".encode('utf-8')).decode()  # 使用"END"标记而不是空字符串
                                        }
                                    }
                                }
                                ws.send(json.dumps(end_frame))
                                self.seq += 1
                                print(f"[{datetime.now()}]结束帧发送成功，使用END标记")
                            break
                        
                        # 确定帧状态
                        if self.is_first_frame:
                            frame_status = STATUS_FIRST_FRAME
                            header_status = STATUS_FIRST_FRAME
                            self.is_first_frame = False
                            print(f"[{datetime.now()}]发送第一帧文本: {text}")
                        else:
                            frame_status = STATUS_CONTINUE_FRAME
                            header_status = STATUS_CONTINUE_FRAME
                            print(f"[{datetime.now()}]发送中间帧文本: {text}")

                        # 构建并发送数据帧
                        data_frame = {
                            "header": {**self.wsParam.CommonArgs, "status": header_status},
                            "parameter": self.wsParam.BusinessArgs if self.seq == 0 else {},  # 只在第一帧发送参数
                            "payload": self.wsParam.create_data_frame(text, frame_status, self.seq)
                        }
                        
                        ws.send(json.dumps(data_frame))
                        self.seq += 1
                        
                        # 短暂延迟，避免发送过快
                        time.sleep(0.1)
                        
                    except queue.Empty:
                        continue  # 队列为空，继续等待
                    except Exception as e:
                        print(f"[{datetime.now()}]处理文本队列时出错: {e}")
                        break
                        
            except Exception as e:
                print(f"[{datetime.now()}]文本处理线程异常: {e}")

        # 启动文本处理线程
        threading.Thread(target=process_text_queue, daemon=True).start()
        
        # 启动控制命令检查
        self._start_control_check()

    def start(self):
        """启动流式TTS客户端"""
        if self.stopped:
            return
            
        print(f"[{datetime.now()}]启动流式TTS客户端")
        
        # 验证TTS配置
        if not self.wsParam.APPID or not self.wsParam.APIKey or not self.wsParam.APISecret:
            error_msg = "TTS配置不完整，请检查环境变量"
            print(f"[{datetime.now()}]错误: {error_msg}")
            return
        
        print(f"[{datetime.now()}]TTS配置验证通过")
        
        # 测试网络连接
        import socket
        try:
            test_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            test_socket.settimeout(5)
            test_socket.connect(("cbm01.cn-huabei-1.xf-yun.com", 443))
            test_socket.close()
            print(f"[{datetime.now()}]网络连接测试通过")
        except Exception as e:
            error_msg = f"网络连接测试失败: {e}"
            print(f"[{datetime.now()}]错误: {error_msg}")
            return
        
        # 初始化音频播放
        try:
            self.p = pyaudio.PyAudio()
            self.stream = self.p.open(
                format=pyaudio.paInt16,
                channels=1,
                rate=24000,
                output=True,
                frames_per_buffer=512
            )
            print(f"[{datetime.now()}]音频流初始化成功")
        except Exception as e:
            error_msg = f"音频流初始化失败: {e}"
            print(f"[{datetime.now()}]错误: {error_msg}")
            return

        # 创建WebSocket连接
        websocket.enableTrace(False)  # 关闭详细跟踪
        wsUrl = self.wsParam.create_url()
        # 只保留关键连接信息，不显示敏感数据
        print(f"[{datetime.now()}]WebSocket连接已创建")
        
        self.ws = websocket.WebSocketApp(
            wsUrl, 
            on_message=self.on_message, 
            on_error=self.on_error,
            on_close=self.on_close,
            on_open=self.on_open
        )
        
        # 启动控制命令检查
        self._start_control_check()
        
        # 在后台线程中启动WebSocket连接，避免阻塞
        ws_thread = threading.Thread(target=self._run_websocket, daemon=True)
        ws_thread.start()
        
        # 等待连接建立
        timeout = 15  # 增加到15秒超时
        start_time = time.time()
        print(f"[{datetime.now()}]等待WebSocket连接建立...")
        
        while not self.session_started and time.time() - start_time < timeout:
            time.sleep(0.1)
            if (int(time.time() - start_time) % 2) == 0:  # 每2秒打印一次状态
                print(f"[{datetime.now()}]等待连接中... 已等待 {int(time.time() - start_time)} 秒")
        
        if not self.session_started:
            print(f"[{datetime.now()}]WebSocket连接超时 ({timeout}秒)")
            self.stop()
            return
        
        print(f"[{datetime.now()}]WebSocket连接建立成功，耗时 {time.time() - start_time:.2f} 秒")
    
    def _run_websocket(self):
        """在后台线程中运行WebSocket"""
        try:
            # 添加更多连接选项，使用标准保活机制
            self.ws.run_forever(
                sslopt={"cert_reqs": ssl.CERT_NONE},
                ping_interval=30,  # 30秒发送一次ping
                ping_timeout=10,   # ping超时10秒
                ping_payload="TTS_KEEPALIVE"  # 自定义ping内容
            )
        except Exception as e:
            print(f"[{datetime.now()}]WebSocket运行异常: {e}")
            self.stop()

    def stop(self):
        """停止流式TTS客户端"""
        self.stopped = True
        
        if self.ws:
            self.ws.close()
            print(f"[{datetime.now()}]tts stopped")
    
    def _start_control_check(self):
        """启动控制命令检查线程"""
        if self.control_running:
            return
            
        self.control_running = True
        self.control_thread = threading.Thread(target=self._check_control_commands, daemon=True)
        self.control_thread.start()
    
    def _stop_control_check(self):
        """停止控制命令检查线程"""
        self.control_running = False
        if self.control_thread:
            self.control_thread.join(timeout=1.0)
    
    def _check_control_commands(self):
        """检查控制命令"""
        retry_count = 0
        max_retries = 3
        
        while self.control_running and not self.stopped:
            try:
                # 这里原本是检查控制命令的逻辑，现在简化为空循环
                time.sleep(0.1)  # 100ms检查一次
                retry_count = 0  # 重置重试计数
                
            except Exception as e:
                retry_count += 1
                if retry_count >= max_retries:
                    print(f"[{datetime.now()}]控制命令检查失败，停止检查")
                    break
                
                # 静默重试，减少日志输出
                time.sleep(1.0)
    
    def create_session(self, text: str, user_id: str = "default") -> str:
        """创建TTS会话"""
        try:
            print(f"[{datetime.now()}]创建TTS会话: {self.session_id}")
            return self.session_id
            
        except Exception as e:
            print(f"[{datetime.now()}]创建TTS会话失败: {e}")
            raise


def nextText():
    print("TTS播放完成")


# 使用示例
if __name__ == "__main__":
    def test_streaming():
        # 创建流式TTS客户端
        tts_client = StreamingTTSClient(on_over=nextText)
        
        # 创建会话
        try:
            tts_client.create_session("测试TTS播放", "test_user")
        except Exception as e:
            print(f"创建会话失败: {e}")
            return
        
        # 在单独线程中启动客户端
        tts_thread = threading.Thread(target=tts_client.start, daemon=True)
        tts_thread.start()
        
        # 等待连接建立
        time.sleep(1)
        
        # 模拟流式输入文本
        texts = [
            "你好，",
            "我是智能助手，",
            "今天天气不错，",
            "适合出门散步。",
            "有什么我可以帮助你的吗？"
        ]
        
        for text in texts:
            tts_client.add_text(text)
            time.sleep(1)  # 模拟逐步输入
        
        # 标记输入结束
        tts_client.finish_input()
        
        # 等待播放完成
        tts_thread.join(timeout=30)
        
        print("流式TTS测试完成")

    # 运行测试
    test_streaming()
