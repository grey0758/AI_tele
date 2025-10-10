# pylint: disable=too-many-lines
"""实时服务"""
import json
import queue
import random
import signal
import sys
import threading
import time
import uuid
import wave
from datetime import datetime
from dataclasses import dataclass
from typing import Optional, Dict, Any
import asyncio
import gzip
import pyaudio
import websockets
from app.core.event_bus import ProductionEventBus
from app.services.phone_service import OnMessageType
from app.core.logger import get_logger
from app.services.base_service import BaseService
from app.services.redis_service import RedisService
from app.models.events import Event, EventType
from app.models.call_record import DialogEntry


logger = get_logger(__name__)

# 协议常量
PROTOCOL_VERSION = 0b0001
DEFAULT_HEADER_SIZE = 0b0001

PROTOCOL_VERSION_BITS = 4
HEADER_BITS = 4
MESSAGE_TYPE_BITS = 4
MESSAGE_TYPE_SPECIFIC_FLAGS_BITS = 4
MESSAGE_SERIALIZATION_BITS = 4
MESSAGE_COMPRESSION_BITS = 4
RESERVED_BITS = 8

# Message Type:
CLIENT_FULL_REQUEST = 0b0001
CLIENT_AUDIO_ONLY_REQUEST = 0b0010

SERVER_FULL_RESPONSE = 0b1001
SERVER_ACK = 0b1011
SERVER_ERROR_RESPONSE = 0b1111

# Message Type Specific Flags
NO_SEQUENCE = 0b0000
POS_SEQUENCE = 0b0001
NEG_SEQUENCE = 0b0010
NEG_SEQUENCE_1 = 0b0011

MSG_WITH_EVENT = 0b0100

# Message Serialization
NO_SERIALIZATION = 0b0000
JSON = 0b0001
THRIFT = 0b0011
CUSTOM_TYPE = 0b1111

# Message Compression
NO_COMPRESSION = 0b0000
GZIP = 0b0001
CUSTOM_COMPRESSION = 0b1111

# 配置信息
CHARACTER_MANIFEST = """
[ROLE]

你是"广州大麦的 AI 外呼智能体月月。使命：通过简洁电话对话确认潜在客户是否需要"联合运营"服务，并判断是否转人工同事跟进。

[GOALS]

识别意向：兴趣 / 拒绝 / 模糊
对兴趣或模糊（未拒绝）→ 输出同事回拨结束语
不感兴趣或负面 → 输出不再打扰结束语
回答限定 FAQ 后必须再次询问需求，不直接结束

[STYLE]

语气始终保持礼貌、专业、亲和、耐心；避免咄咄逼人或过度推销。
表达简洁清晰，不冗长，不制造沟通压力。
回答要有同理心，尊重客户的语气与态度。
根据客户的语言风格和情绪状态，个性化调整回复方式。
所有结束语必须包含"再见"。

[PRIMARY_OPENING]

您好，我是广州大麦的 AI 外呼智能体月月，我们正在寻找联合运营合作伙伴，您目前有考虑联合运营的需求吗？

[FAQ ANSWERS]

### **核心异议类 (高频)**

Q：联合运营为什么还要先收费？/ 听起来像代运营。
A：很好的问题。这笔费用是双方共同启动项目的投入，确保我们能投入最好的资源。我们更像“事业合伙人”，而不是简单的“代运营”。您看是否需要我们同事结合您的项目，详细讲解一下合作模式？

Q：费用太高了 / 资金紧张怎么办？
A：我们理解创业不易。针对不同情况，我们也有更灵活的“陪跑”模式，可以先用小成本跑起来。具体方案需要同事和您沟通，我先确认您是否需要同事联系您介绍一下？

Q：有案例吗？/ 怎么保证效果？/ 不成功怎么办？
A：案例有很多，但因有保密协议，不方便透露。而且每个老板的情况都不同，关键是为您定制方案。为保证合作质量，我们的合同支持随时中止并按比例退款，风险是共担的。您需要我们同事联系您吗？

Q：我这个行业特殊（B2B/技术/本地服务），你们这套适用吗？
A：我们合作的很多老板都这么问。其实越是特殊的行业，越需要量身定制策略。我们的核心就是做“商业模式重构”，而不是套用模板。这部分正好需要我们同事和您深入聊聊，您看方便吗？

### **客户投入类**

Q：我需要做什么？/ 我很忙，没时间。
A：您主要负责出镜，每天可能就花10-15分钟，我们来负责策略和脚本，不会占用您太多时间。您看需要我们同事和您具体沟通一下分工吗？

Q：不想出镜怎么办？/ 我没信心。
A：这个您放心，我们追求的是“专业”不是“颜值”，您的行业经验是别人演不出来的。而且我们会有专门的同事指导您，帮您找到最自然的状态。您需要我们同事联系您吗？

### **基础信息类**

Q：你们是做什么的？/ 联合运营是什么？
A：我们是帮老板们重构线上商业模式，做业绩增长的。简单说，我们出团队和策略，您出产品和行业经验，像合伙人一样深度合作。您目前有这方面的需求吗？

Q：你们和别的代运营有什么不一样？
A：我们是共同投入、共同分成，是拍档关系，不只是乙方。我们更关心您生意的本质问题，而不只是发发视频。您这块需要我们同事进一步联系吗？

Q：你们在几楼？
A：14楼。您现在是否有考虑联合运营的需求？

Q：你们公司地址是什么？
A：广东省广州市天河区临江大道天德广场T1栋14楼1403。您这块是否需要我们同事联系？

[CLASSIFICATION RULES]

拒绝关键词或负面情绪（不需要 / 没兴趣 / 别打了 / 不考虑 / 很烦 / 骗子 等）→ 直接拒绝结束语。
兴趣 / 模糊但未拒绝（需要 / 可以了解 / 发资料 / 再说 / 有兴趣 / 先了解一下）→ 兴趣处理。
客户提问 FAQ → 按 FAQ 回答后再次询问需求。

[REPLIES]

REJECT_END：那我标记一下后续就不打扰您了，再见
INTEREST_END：好的，那我让我们的同事打您的电话了解一下情况，您到时候留意一下广州的号码，我让助理稍后联系您，再见
SHORT_PROBE（对方忙）：我简短说：我们承担运营团队成本，按结果分成。您需要同事联系吗？
SECOND_PROBE（首次回答含糊）：您看要不要先让同事加您发个简要模式说明，再决定要不要深入？

[EDGE CASES]

若连续两次客户无明确回答但未拒绝 → 用 INTEREST_END
任何复杂策略/合同/分成比例/退款细节问题：回应"这部分细节需要同事结合您项目情况再讲，我先确认您是否需要同事联系？"
未识别文本或听不清：我再重复一下：我们是广州大麦，正在寻找联合运营合作伙伴，您需要吗？

[DO NOT DO]

不夸大承诺，不谈成功率数字，不给具体分润比例，不争论，不多问敏感信息。

[ALGORITHM OUTLINE]

1. 发开场
2. 识别：是否为 FAQ 提问？→ 若是：答复后再次询问需求
3. 若拒绝/负面 → REJECT_END
4. 若兴趣/模糊 → 若已确认一次 → INTEREST_END；若尚未确认 → SECOND_PROBE
5. SECOND_PROBE 后仍模糊 → INTEREST_END
6. 所有结束输出必须含"再见"

"""

WS_CONNECT_CONFIG = {
    "base_url": "wss://openspeech.bytedance.com/api/v3/realtime/dialogue",
    "headers": {
        "X-Api-App-ID": "7053540602",
        "X-Api-Access-Key": "Z9q4zthzIu5w4RfuFEJbUZCRM8Z_gJBW",
        "X-Api-Resource-Id": "volc.speech.dialog",
        "X-Api-App-Key": "PlgvMymc7f3tQnJ6",
        "X-Api-Connect-Id": str(uuid.uuid4()),
    }
}

START_SESSION_REQ_1 = {
    "asr": {
        "extra": {
            "end_smooth_window_ms": 2000,
            "enable_custom_vad": True,
        },
    },
    "tts": {
        "speaker": "S_58MO6EIG1",
        "audio_config": {
            "channel": 1,
            "format": "pcm",
            "sample_rate": 24000
        },
    },
    "dialog": {
        "bot_name": "月月",
        "system_role": "你使用活泼灵动的女声，性格开朗，热爱生活。",
        "character_manifest": CHARACTER_MANIFEST,
        "location": {
          "city": "北京",
        },
        "extra": {
            "strict_audit": False,
            "audit_response": "支持客户自定义安全审核回复话术。",
            "recv_timeout": 10,
            "input_mod": "audio",
            "model": "SC"
        }
    }
}

START_SESSION_REQ = {
    "asr": {
        "extra": {
            "end_smooth_window_ms": 1000,
            "enable_custom_vad": True,
        },
    },
    "tts": {
        "speaker": "S_58MO6EIG1",
        "audio_config": {
            "channel": 1,
            "format": "pcm",
            "sample_rate": 24000
        },
    },
    "dialog": {
        "bot_name": "月月",
        "system_role": "你使用活泼灵动的女声，性格开朗，热爱生活。",
        "character_manifest": CHARACTER_MANIFEST,
        "location": {
          "city": "北京",
        },
        "extra": {
            "strict_audit": False,
            "audit_response": "支持客户自定义安全审核回复话术。",
            "recv_timeout": 10,
            "input_mod": "audio",
            "model": "SC"
        }
    }
}

INPUT_AUDIO_CONFIG = {
    "chunk": 1600,
    "format": "pcm",
    "channels": 1,
    "sample_rate": 16000,
    "bit_size": pyaudio.paInt16
}

OUTPUT_AUDIO_CONFIG = {
    "chunk": 1600,
    "format": "pcm",
    "channels": 1,
    "sample_rate": 24000,
    "bit_size": pyaudio.paFloat32
}


@dataclass
class AudioConfig:
    """音频配置数据类"""
    format: str
    bit_size: int
    channels: int
    sample_rate: int
    chunk: int


class AudioDeviceManager:
    """音频设备管理类，处理音频输入输出"""

    def __init__(self, input_config: AudioConfig, output_config: AudioConfig):
        self.input_config = input_config
        self.output_config = output_config
        self.pyaudio = pyaudio.PyAudio()
        self.input_stream: Optional[pyaudio.Stream] = None
        self.output_stream: Optional[pyaudio.Stream] = None

    def open_input_stream(self) -> pyaudio.Stream:
        """打开音频输入流"""
        self.input_stream = self.pyaudio.open(
            format=self.input_config.bit_size,
            channels=self.input_config.channels,
            rate=self.input_config.sample_rate,
            input=True,
            frames_per_buffer=self.input_config.chunk,
            stream_callback=None,
            start=False
        )
        return self.input_stream

    def open_output_stream(self) -> pyaudio.Stream:
        """打开音频输出流"""
        self.output_stream = self.pyaudio.open(
            format=self.output_config.bit_size,
            channels=self.output_config.channels,
            rate=self.output_config.sample_rate,
            output=True,
            frames_per_buffer=self.output_config.chunk
        )
        return self.output_stream

    def cleanup(self) -> None:
        """清理音频设备资源"""
        for stream in [self.input_stream, self.output_stream]:
            if stream:
                stream.stop_stream()
                stream.close()
        self.pyaudio.terminate()


class RealtimeDialogClient:
    """实时对话客户端"""
    def __init__(self, config: Dict[str, Any], session_id: str, output_audio_format: str = "pcm",
                 mod: str = "audio", recv_timeout: int = 10) -> None:
        self.config = config
        self.logid = ""
        self.session_id = session_id
        self.output_audio_format = output_audio_format
        self.mod = mod
        self.recv_timeout = recv_timeout
        self.ws = None

    async def connect(self) -> None:
        """建立WebSocket连接"""
        logger.info("连接WebSocket服务器...")
        self.ws = await websockets.connect(
            self.config['base_url'],
            additional_headers=self.config['headers'],
            ping_interval=None
        )
        # 获取logid
        logger.debug("WebSocket对象类型: %s", type(self.ws))
        logger.debug("WebSocket对象属性: %s", [attr for attr in dir(self.ws) if not attr.startswith('_')])

        self.logid = ""

        # 方式2: response.headers
        if not self.logid and hasattr(self.ws, 'response'):
            logger.debug("找到response属性")
            if hasattr(self.ws.response, 'headers'):
                logger.debug("response.headers内容: %s", self.ws.response.headers)
                self.logid = self.ws.response.headers.get("X-Tt-Logid", "")
                logger.debug("从response.headers获取到的logid: %s", self.logid)

        logger.info("WebSocket连接成功，logid: %s", self.logid)

        # StartConnection request
        start_connection_request = bytearray(self.generate_header())
        start_connection_request.extend(int(1).to_bytes(4, 'big'))
        payload_bytes = str.encode("{}")
        payload_bytes = gzip.compress(payload_bytes)
        start_connection_request.extend((len(payload_bytes)).to_bytes(4, 'big'))
        start_connection_request.extend(payload_bytes)
        await self.ws.send(start_connection_request)
        await self.ws.recv()
        logger.info("StartConnection 成功")

        # 扩大这个参数，可以在一段时间内保持静默，主要用于text模式，参数范围[10,120]
        START_SESSION_REQ["dialog"]["extra"]["recv_timeout"] = self.recv_timeout
        # 这个参数，在text或者audio_file模式，可以在一段时间内保持静默
        START_SESSION_REQ["dialog"]["extra"]["input_mod"] = self.mod
        # StartSession request
        if self.output_audio_format == "pcm_s16le":
            START_SESSION_REQ["tts"]["audio_config"]["format"] = "pcm_s16le"
        request_params = START_SESSION_REQ

        # 打印StartSession请求的所有参数
        logger.debug("发送StartSession请求，事件ID: 100")
        logger.debug("StartSession请求参数:")
        logger.debug("ASR配置: %s", json.dumps(request_params.get("asr", {}), ensure_ascii=False, indent=2))
        logger.debug("TTS配置: %s", json.dumps(request_params.get("tts", {}), ensure_ascii=False, indent=2))
        logger.debug("Dialog配置: %s", json.dumps(request_params.get("dialog", {}), ensure_ascii=False, indent=2))

        payload_bytes = str.encode(json.dumps(request_params))
        payload_bytes = gzip.compress(payload_bytes)
        start_session_request = bytearray(self.generate_header())
        start_session_request.extend(int(100).to_bytes(4, 'big'))
        start_session_request.extend((len(self.session_id)).to_bytes(4, 'big'))
        start_session_request.extend(str.encode(self.session_id))
        start_session_request.extend((len(payload_bytes)).to_bytes(4, 'big'))
        start_session_request.extend(payload_bytes)
        await self.ws.send(start_session_request)
        response = await self.ws.recv()
        
        # 解析并打印StartSession响应
        response_data = self.parse_response(response)
        logger.info("StartSession响应: %s", json.dumps(response_data, ensure_ascii=False, indent=2))
        logger.info("StartSession 成功，开始对话")

    async def say_hello(self) -> None:
        """发送Hello消息"""
        payload = {
            "content": "你好老板，我是广州大麦的月月，我们在寻找联合运营的合作伙伴，共同投入共同分成的方式，问您目前有考虑联合运营的需求吗？ ",
        }
        hello_request = bytearray(self.generate_header())
        hello_request.extend(int(300).to_bytes(4, 'big'))
        payload_bytes = str.encode(json.dumps(payload))
        payload_bytes = gzip.compress(payload_bytes)
        hello_request.extend((len(self.session_id)).to_bytes(4, 'big'))
        hello_request.extend(str.encode(self.session_id))
        hello_request.extend((len(payload_bytes)).to_bytes(4, 'big'))
        hello_request.extend(payload_bytes)
        await self.ws.send(hello_request)

    async def chat_text_query(self, content: str) -> None:
        """发送Chat Text Query消息"""
        payload = {
            "content": content,
        }
        chat_text_query_request = bytearray(self.generate_header())
        chat_text_query_request.extend(int(501).to_bytes(4, 'big'))
        payload_bytes = str.encode(json.dumps(payload))
        payload_bytes = gzip.compress(payload_bytes)
        chat_text_query_request.extend((len(self.session_id)).to_bytes(4, 'big'))
        chat_text_query_request.extend(str.encode(self.session_id))
        chat_text_query_request.extend((len(payload_bytes)).to_bytes(4, 'big'))
        chat_text_query_request.extend(payload_bytes)
        await self.ws.send(chat_text_query_request)

    async def chat_tts_text(self, is_user_querying: bool, start: bool, end: bool, content: str) -> None:
        """发送Chat TTS Text消息"""
        if is_user_querying:
            return
        payload = {
            "start": start,
            "end": end,
            "content": content,
        }
        logger.debug("发送ChatTTSText请求: %s", payload)
        payload_bytes = str.encode(json.dumps(payload))
        payload_bytes = gzip.compress(payload_bytes)

        chat_tts_text_request = bytearray(self.generate_header())
        chat_tts_text_request.extend(int(500).to_bytes(4, 'big'))
        chat_tts_text_request.extend((len(self.session_id)).to_bytes(4, 'big'))
        chat_tts_text_request.extend(str.encode(self.session_id))
        chat_tts_text_request.extend((len(payload_bytes)).to_bytes(4, 'big'))
        chat_tts_text_request.extend(payload_bytes)
        await self.ws.send(chat_tts_text_request)

    async def chat_rag_text(self, is_user_querying: bool, external_rag: str) -> None:
        """发送Chat TTS Text消息"""
        if is_user_querying:
            return
        payload = {
            "external_rag": external_rag,
        }
        logger.debug("发送ChatRAGText请求: %s", payload)
        payload_bytes = str.encode(json.dumps(payload))
        payload_bytes = gzip.compress(payload_bytes)

        chat_rag_text_request = bytearray(self.generate_header())
        chat_rag_text_request.extend(int(502).to_bytes(4, 'big'))
        chat_rag_text_request.extend((len(self.session_id)).to_bytes(4, 'big'))
        chat_rag_text_request.extend(str.encode(self.session_id))
        chat_rag_text_request.extend((len(payload_bytes)).to_bytes(4, 'big'))
        chat_rag_text_request.extend(payload_bytes)
        await self.ws.send(chat_rag_text_request)

    async def task_request(self, audio: bytes) -> None:
        """发送任务请求"""
        task_request = bytearray(
            self.generate_header(message_type=CLIENT_AUDIO_ONLY_REQUEST,
                                     serial_method=NO_SERIALIZATION))
        task_request.extend(int(200).to_bytes(4, 'big'))
        task_request.extend((len(self.session_id)).to_bytes(4, 'big'))
        task_request.extend(str.encode(self.session_id))
        payload_bytes = gzip.compress(audio)
        task_request.extend((len(payload_bytes)).to_bytes(4, 'big'))
        task_request.extend(payload_bytes)
        await self.ws.send(task_request)

    async def receive_server_response(self) -> Dict[str, Any]:
        """接收服务器响应"""
        try:
            response = await self.ws.recv()
            data = self.parse_response(response)
            return data
        except Exception as e: # pylint: disable=broad-except
            raise Exception(f"Failed to receive message: {e}") from e # pylint: disable=broad-exception-raised

    async def finish_session(self):
        """结束会话"""
        finish_session_request = bytearray(self.generate_header())
        finish_session_request.extend(int(102).to_bytes(4, 'big'))
        payload_bytes = str.encode("{}")
        payload_bytes = gzip.compress(payload_bytes)
        finish_session_request.extend((len(self.session_id)).to_bytes(4, 'big'))
        finish_session_request.extend(str.encode(self.session_id))
        finish_session_request.extend((len(payload_bytes)).to_bytes(4, 'big'))
        finish_session_request.extend(payload_bytes)
        await self.ws.send(finish_session_request)

    async def finish_connection(self):
        """结束连接"""
        finish_connection_request = bytearray(self.generate_header())
        finish_connection_request.extend(int(2).to_bytes(4, 'big'))
        payload_bytes = str.encode("{}")
        payload_bytes = gzip.compress(payload_bytes)
        finish_connection_request.extend((len(payload_bytes)).to_bytes(4, 'big'))
        finish_connection_request.extend(payload_bytes)
        await self.ws.send(finish_connection_request)
        response = await self.ws.recv()
        logger.info("FinishConnection response: %s", self.parse_response(response))

    async def close(self) -> None:
        """关闭WebSocket连接"""
        if self.ws:
            logger.debug("关闭WebSocket连接")
            await self.ws.close()

    def generate_header(
            self,
            version=PROTOCOL_VERSION,
            message_type=CLIENT_FULL_REQUEST,
            message_type_specific_flags=MSG_WITH_EVENT,
            serial_method=JSON,
            compression_type=GZIP,
            reserved_data=0x00,
            extension_header=bytes()
    ):
        """
        protocol_version(4 bits), header_size(4 bits),
        message_type(4 bits), message_type_specific_flags(4 bits)
        serialization_method(4 bits) message_compression(4 bits)
        reserved （8bits) 保留字段
        header_extensions 扩展头(大小等于 8 * 4 * (header_size - 1) )
        """
        header = bytearray()
        header_size = int(len(extension_header) / 4) + 1
        header.append((version << 4) | header_size)
        header.append((message_type << 4) | message_type_specific_flags)
        header.append((serial_method << 4) | compression_type)
        header.append(reserved_data)
        header.extend(extension_header)
        return header

    def parse_response(self, res):
        """
        - header
            - (4bytes)header
            - (4bits)version(v1) + (4bits)header_size
            - (4bits)messageType + (4bits)messageTypeFlags
                -- 0001	CompleteClient | -- 0001 hasSequence
                -- 0010	audioonly      | -- 0010 isTailPacket
                                           | -- 0100 hasEvent
            - (4bits)payloadFormat + (4bits)compression
            - (8bits) reserve
        - payload
            - [optional 4 bytes] event
            - [optional] session ID
              -- (4 bytes)session ID len
              -- session ID data
            - (4 bytes)data len
            - data
        """
        if isinstance(res, str):
            return {}
        if len(res) < 4:
            logger.warning("收到过短的响应数据: %s", len(res))
            return {}
        try:
            protocol_version = res[0] >> 4 # pylint: disable=unused-variable # noqa: F841
            header_size = res[0] & 0x0f
            message_type = res[1] >> 4
            message_type_specific_flags = res[1] & 0x0f
            serialization_method = res[2] >> 4
            message_compression = res[2] & 0x0f
            reserved = res[3] # pylint: disable=unused-variable  # noqa: F841
            header_extensions = res[4:header_size * 4] # pylint: disable=unused-variable # noqa: F841
            payload = res[header_size * 4:]
            result = {}
            payload_msg = None
            payload_size = 0
            start = 0
        except (IndexError, ValueError) as e:
            logger.error("解析响应头失败: %s, 数据长度: %s", e, len(res))
            return {}
        if message_type == SERVER_FULL_RESPONSE or message_type == SERVER_ACK:
            result['message_type'] = 'SERVER_FULL_RESPONSE'
            if message_type == SERVER_ACK:
                result['message_type'] = 'SERVER_ACK'
            if message_type_specific_flags & NEG_SEQUENCE > 0:
                result['seq'] = int.from_bytes(payload[:4], "big", signed=False)
                start += 4
            if message_type_specific_flags & MSG_WITH_EVENT > 0:
                result['event'] = int.from_bytes(payload[:4], "big", signed=False)
                start += 4
            payload = payload[start:]
            session_id_size = int.from_bytes(payload[:4], "big", signed=True)
            session_id = payload[4:session_id_size+4]
            result['session_id'] = str(session_id)
            payload = payload[4 + session_id_size:]
            payload_size = int.from_bytes(payload[:4], "big", signed=False)
            payload_msg = payload[4:]
        elif message_type == SERVER_ERROR_RESPONSE:
            result['message_type'] = 'SERVER_ERROR'
            code = int.from_bytes(payload[:4], "big", signed=False)
            result['code'] = code
            payload_size = int.from_bytes(payload[4:8], "big", signed=False)
            payload_msg = payload[8:]
        if payload_msg is None:
            return result
        if message_compression == GZIP:
            payload_msg = gzip.decompress(payload_msg)
        if serialization_method == JSON:
            payload_msg = json.loads(str(payload_msg, "utf-8"))
        elif serialization_method != NO_SERIALIZATION:
            payload_msg = str(payload_msg, "utf-8")
        result['payload_msg'] = payload_msg
        result['payload_size'] = payload_size
        return result


class DialogSession:
    """对话会话管理类"""
    is_audio_file_input: bool
    mod: str

    def __init__(self, ws_config: Dict[str, Any], output_audio_format: str = "pcm", audio_file_path: str = "",
                 mod: str = "audio", recv_timeout: int = 10, realtime_service: Optional['RealtimeService'] = None):
        self.audio_file_path = audio_file_path
        self.recv_timeout = recv_timeout
        self.is_audio_file_input = self.audio_file_path != ""
        if self.is_audio_file_input:
            mod = 'audio_file'
        else:
            self.say_hello_over_event = asyncio.Event()
        self.mod = mod

        self.session_id = str(uuid.uuid4())
        self.realtime_service = realtime_service
        self.client = RealtimeDialogClient(config=ws_config, session_id=self.session_id,
                                           output_audio_format=output_audio_format, mod=mod, recv_timeout=recv_timeout)
        if output_audio_format == "pcm_s16le":
            OUTPUT_AUDIO_CONFIG["format"] = "pcm_s16le"
            OUTPUT_AUDIO_CONFIG["bit_size"] = pyaudio.paInt16

        self.is_running = True
        self.is_session_finished = False
        self.is_user_querying = False
        self.is_sending_chat_tts_text = False
        self.is_playing_audio = False
        self.audio_buffer = b''
        self.audio_buffer_lock = threading.Lock()
        self.chat_response_buffer = ''
        self.chat_response_lock = threading.Lock()

        try:
            signal.signal(signal.SIGINT, self._keyboard_signal)
        except (ValueError, OSError):
            # Windows 上可能不支持 SIGINT，使用其他方式处理
            pass
        self.audio_queue: queue.Queue = queue.Queue()
        if not self.is_audio_file_input:
            self.audio_device = AudioDeviceManager(
                AudioConfig(**INPUT_AUDIO_CONFIG),
                AudioConfig(**OUTPUT_AUDIO_CONFIG)
            )
            # 初始化音频队列和输出流
            self.output_stream = self.audio_device.open_output_stream()
            # 启动播放线程
            self.is_recording = True
            self.is_playing = True
            self.player_thread = threading.Thread(target=self._audio_player_thread)
            self.player_thread.daemon = True
            self.player_thread.start()

            # 预先初始化麦克风流，减少后续阻塞
            self.input_stream = None
            # 异步预初始化麦克风
            asyncio.create_task(self._pre_init_microphone())

    async def _pre_init_microphone(self):
        """预初始化麦克风"""
        try:
            loop = asyncio.get_event_loop()
            self.input_stream = await loop.run_in_executor(None, self.audio_device.open_input_stream)
        except Exception as e: # pylint: disable=broad-except
            logger.error("预初始化麦克风失败: %s", e)

    def _audio_player_thread(self):
        """音频播放线程"""
        while self.is_playing:
            try:
                # 从队列获取音频数据，使用更短的超时时间
                audio_data = self.audio_queue.get(timeout=0.1)
                if audio_data is not None:
                    self.output_stream.write(audio_data)
            except queue.Empty:
                # 队列为空时短暂等待
                time.sleep(0.01)
            except Exception as e: # pylint: disable=broad-except
                logger.error("音频播放错误: %s", e)
                time.sleep(0.01)

    async def _add_dialog_entry(self, speaker: str, content: str) -> None:
        """添加对话记录"""
        logger.debug("Adding dialog entry: %s", content)
        logger.debug("realtime_service: %s, call_id: %s", self.realtime_service, self.realtime_service.call_id if self.realtime_service else None)
        if self.realtime_service and content.strip():
            try:
                dialog_entry = DialogEntry(
                    speaker=speaker,
                    content=content,
                    timestamp=datetime.now()
                )
                logger.debug("Emitting REDIS_ADD_DIALOG_RECORD event for call_id: %s", self.realtime_service.call_id)
                await self.realtime_service.emit_event(EventType.REDIS_ADD_DIALOG_RECORD, {"call_id": self.realtime_service.call_id, "dialog_entry": dialog_entry})
            except Exception as e: # pylint: disable=broad-except
                logger.error("Failed to add dialog entry: %s", e)

    def _stop_recording(self) -> None:
        """停止录音流，防止用户再次回复"""
        try:
            logger.info("正在关闭麦克风录音流...")
            self.is_recording = False

            # 停止输入流
            if hasattr(self, 'input_stream') and self.input_stream:
                try:
                    self.input_stream.stop_stream()
                    logger.info("麦克风录音流已停止")
                except Exception as e: # pylint: disable=broad-except
                    logger.error("停止麦克风录音流时出错: %s", e)

        except Exception as e: # pylint: disable=broad-except
            logger.error("关闭录音流时发生错误: %s", e)

    def handle_server_response(self, response: Dict[str, Any]) -> None:
        """处理服务器响应"""
        if response == {}:
            return
        if not response.get('message_type'):
            logger.warning("收到无效响应，缺少message_type字段: %s", response)
            return
        if response['message_type'] == 'SERVER_ACK' and isinstance(response.get('payload_msg'), bytes):
            # print(f"\n接收到音频数据: {len(response['payload_msg'])} 字节")
            if self.is_sending_chat_tts_text:
                return
            audio_data = response['payload_msg']
            if not self.is_audio_file_input:
                self.audio_queue.put(audio_data)
            with self.audio_buffer_lock:
                self.audio_buffer += audio_data
        elif response['message_type'] == 'SERVER_FULL_RESPONSE':
            event = response.get('event')
            payload_msg = response.get('payload_msg', {})

            # 只记录重要的事件，减少日志噪音
            if event in [450, 350, 359, 152, 153]:
                logger.info("服务器响应: event=%s, session_id=%s", event, response.get('session_id'))
            elif event == 150:  # SessionStarted - 会话启动成功
                logger.info("会话启动成功，事件ID: %s", event)
                logger.info("SessionStarted响应: %s", json.dumps(payload_msg, ensure_ascii=False, indent=2))
            elif event == 154:  # UsageResponse - 用量信息
                logger.info("收到用量信息，事件ID: %s", event)
                logger.info("UsageResponse: %s", json.dumps(payload_msg, ensure_ascii=False, indent=2))
            elif event == 451:  # ASR 结果
                # 只记录最终结果，不记录中间结果
                if payload_msg.get('results') and not payload_msg['results'][0].get('is_interim', True):
                    text = payload_msg['results'][0]['text']
                    logger.info("ASR最终结果: %s", text)
                    # 添加用户对话记录
                    asyncio.create_task(self._add_dialog_entry("user", text))
            elif event == 550:  # ChatResponse - 模型回复的文本内容
                content = payload_msg.get('content', '')
                if content:
                    with self.chat_response_lock:
                        self.chat_response_buffer += content
            elif event == 559:  # ChatEnded - 模型回复文本结束事件
                with self.chat_response_lock:
                    if self.chat_response_buffer:
                        logger.info("大模型回复最终结果: %s", self.chat_response_buffer)
                        # 添加agent对话记录
                        asyncio.create_task(self._add_dialog_entry("agent", self.chat_response_buffer))
                        if "再见" in self.chat_response_buffer:
                            logger.info("检测到agent回复包含'再见'，关闭麦克风录音流并挂断")
                            # 关闭麦克风录音流，防止用户再次回复
                            self._stop_recording()
                            asyncio.create_task(self.realtime_service.emit_event(EventType.PHONE_SERVICE_TERMINATECALL))

                        self.chat_response_buffer = ''
            elif event == 350:  # TTSSentenceStart - 合成音频起始事件
                tts_type = payload_msg.get('tts_type', '')
                text = payload_msg.get('text', '')
                if text:
                    logger.info("TTS开始合成 [%s]: %s", tts_type, text)
            elif event == 359:  # TTSEnded - 模型一轮音频合成结束
                logger.info("TTS合成结束")
            elif event == 459:  # ASREnded - 用户说话结束
                logger.info("用户说话结束")
            elif event == 152:  # SessionFinished - 会话已结束
                # 会话结束时检查是否有未完成的回复
                with self.chat_response_lock:
                    if self.chat_response_buffer:
                        logger.info("大模型回复最终结果(会话结束): %s", self.chat_response_buffer)
                        # 添加agent对话记录
                        asyncio.create_task(self._add_dialog_entry("agent", self.chat_response_buffer))
                        self.chat_response_buffer = ''
                logger.info("会话已结束")
            elif event == 153:  # SessionFailed - 会话失败
                # 会话失败时也检查是否有未完成的回复
                with self.chat_response_lock:
                    if self.chat_response_buffer:
                        logger.info("大模型回复最终结果(会话失败): %s", self.chat_response_buffer)
                        # 添加agent对话记录
                        asyncio.create_task(self._add_dialog_entry("agent", self.chat_response_buffer))
                        self.chat_response_buffer = ''
                error = payload_msg.get('error', '')
                logger.error("会话失败: %s", error)

            if event == 450:
                logger.info("用户开始说话，清空音频缓存")
                # 如果用户打断，显示已收集的回复内容
                with self.chat_response_lock:
                    if self.chat_response_buffer:
                        logger.info("大模型回复最终结果(被打断): %s", self.chat_response_buffer)
                        # 添加agent对话记录
                        asyncio.create_task(self._add_dialog_entry("agent", self.chat_response_buffer))
                        self.chat_response_buffer = ''
                # 非阻塞方式清空队列
                try:
                    while True:
                        self.audio_queue.get_nowait()
                except queue.Empty: # pylint: disable=bare-except
                    pass
                self.is_user_querying = True

            if event == 350 and self.is_sending_chat_tts_text and payload_msg.get("tts_type") in ["chat_tts_text", "external_rag"]:
                # 非阻塞方式清空队列
                try:
                    while True:
                        self.audio_queue.get_nowait()
                except queue.Empty:
                    pass
                self.is_sending_chat_tts_text = False

            if event == 459:
                self.is_user_querying = False
                if random.randint(0, 100000) % 100 == 0:
                    self.is_sending_chat_tts_text = True
                    asyncio.create_task(self.trigger_chat_tts_text())
                    asyncio.create_task(self.trigger_chat_rag_text())
        elif response['message_type'] == 'SERVER_ERROR':
            logger.error("服务器错误: %s", response['payload_msg'])
            raise Exception("服务器错误") # pylint: disable=broad-exception-raised

    async def trigger_chat_tts_text(self):
        """概率触发发送ChatTTSText请求"""
        logger.debug("触发ChatTTSText事件")
        await self.client.chat_tts_text(
            is_user_querying=self.is_user_querying,
            start=True,
            end=False,
            content="这是查询到外部数据之前的安抚话术。",
        )
        await self.client.chat_tts_text(
            is_user_querying=self.is_user_querying,
            start=False,
            end=True,
            content="",
        )

    async def trigger_chat_rag_text(self):
        """触发ChatRAGText事件"""
        await asyncio.sleep(5) # 模拟查询外部RAG的耗时，这里为了不影响GTA安抚话术的播报，直接sleep 5秒
        logger.debug("触发ChatRAGText事件")
        await self.client.chat_rag_text(self.is_user_querying, external_rag='[{"title":"北京天气","content":"今天北京整体以晴到多云为主，特别是午后至傍晚时段需注意突发降雨。\n💨 风况与湿度\n风力较弱，一般为 2–3 级南风或西南风\n白天湿度较高，早晚略凉爽"}]')

    def _keyboard_signal(self, _sig, _frame):
        """键盘信号处理"""
        logger.info("receive keyboard Ctrl+C")
        self.stop()
        sys.exit(0)

    def stop(self):
        """停止会话"""
        self.is_recording = False
        self.is_playing = False
        self.is_running = False

    async def receive_loop(self):
        """接收服务器响应"""
        try:
            while True:
                response = await self.client.receive_server_response()
                self.handle_server_response(response)
                if 'event' in response and (response['event'] == 152 or response['event'] == 153):
                    logger.info("会话结束事件: %s", response['event'])
                    self.is_session_finished = True
                    break
                if 'event' in response and response['event'] == 359:
                    if self.is_audio_file_input:
                        logger.info("TTS播放结束")
                        self.is_session_finished = True
                        break
                    else:
                        if not self.say_hello_over_event.is_set():
                            logger.info("开场白播放结束")
                            self.say_hello_over_event.set()
                        if self.mod == "text":
                            logger.info("请输入内容：")

        except asyncio.CancelledError:
            logger.debug("接收任务已取消")
        except Exception as e: # pylint: disable=broad-except
            logger.error("接收消息错误: %s", e)
        finally:
            self.stop()
            self.is_session_finished = True

    async def process_audio_file(self) -> None:
        """处理音频文件"""
        await self.process_audio_file_input(self.audio_file_path)

    async def process_text_input(self) -> None:
        """处理文本输入"""
        await self.client.say_hello()
        await self.say_hello_over_event.wait()

        # 确保连接最终关闭
        try:
            # 启动输入监听线程
            input_queue: queue.Queue = queue.Queue()
            input_thread = threading.Thread(target=self.input_listener, args=(input_queue,), daemon=True)
            input_thread.start()
            # 主循环：处理输入和上下文结束
            while self.is_running:
                try:
                    # 检查是否有输入（非阻塞）
                    input_str = input_queue.get_nowait()
                    if input_str is None:
                        # 输入流关闭
                        logger.info("Input channel closed")
                        break
                    if input_str:
                        # 发送输入内容
                        await self.client.chat_text_query(input_str)
                except queue.Empty:
                    # 无输入时短暂休眠
                    await asyncio.sleep(0.1)
                except Exception as e: # pylint: disable=broad-except
                    logger.error("Main loop error: %s", e)
                    break
        finally:
            logger.debug("退出文本输入模式")

    def input_listener(self, input_queue: queue.Queue) -> None:
        """在单独线程中监听标准输入"""
        logger.debug("开始监听用户输入")
        try:
            while True:
                # 读取标准输入（阻塞操作）
                line = sys.stdin.readline()
                if not line:
                    # 输入流关闭
                    input_queue.put(None)
                    break
                input_str = line.strip()
                input_queue.put(input_str)
        except Exception as e: # pylint: disable=broad-except
            logger.error("Input listener error: %s", e)
            input_queue.put(None)

    async def process_audio_file_input(self, audio_file_path: str) -> None:
        """处理音频文件输入"""
        with wave.open(audio_file_path, 'rb') as wf:
            chunk_size = INPUT_AUDIO_CONFIG["chunk"]
            framerate = wf.getframerate()  # 采样率（如16000Hz）
            # 时长 = chunkSize（帧数） ÷ 采样率（帧/秒）
            sleep_seconds = chunk_size / framerate
            logger.info("处理音频文件: %s", audio_file_path)

            # 分块读取并发送音频数据
            while True:
                audio_data = wf.readframes(chunk_size)
                if not audio_data:
                    break  # 文件读取完毕

                await self.client.task_request(audio_data)
                # sleep与chunk对应的音频时长一致，模拟实时输入
                await asyncio.sleep(sleep_seconds)

            logger.info("音频文件处理完成")

    async def process_silence_audio(self) -> None:
        """发送静音音频"""
        silence_data = b'\x00' * 320
        await self.client.task_request(silence_data)

    async def process_microphone_input(self) -> None:
        """处理麦克风输入"""
        await self.client.say_hello()
        await self.say_hello_over_event.wait()

        # 等待麦克风预初始化完成
        while self.input_stream is None:
            await asyncio.sleep(0.01)

        # 启动流
        self.input_stream.start_stream()
        logger.info("麦克风已就绪，请开始说话")

        while self.is_recording:
            try:
                # 使用更小的chunk和更短的超时时间
                audio_data = self.input_stream.read(INPUT_AUDIO_CONFIG["chunk"], exception_on_overflow=False)
                await self.client.task_request(audio_data)
                # 减少等待时间，提高响应性
                await asyncio.sleep(0.005)
            except Exception as e: # pylint: disable=broad-except
                logger.error("读取麦克风数据出错: %s", e)
                await asyncio.sleep(0.01)

    async def start(self) -> None:
        """启动对话会话"""
        try:
            await self.client.connect()

            if self.mod == "text":
                text_task = asyncio.create_task(self.process_text_input())
                receive_task = asyncio.create_task(self.receive_loop())

                try:
                    while self.is_running:
                        await asyncio.sleep(0.1)
                except KeyboardInterrupt:
                    logger.info("收到键盘中断信号，正在退出...")
                    self.stop()
                    text_task.cancel()
                    receive_task.cancel()
            else:
                if self.is_audio_file_input:
                    audio_task = asyncio.create_task(self.process_audio_file())
                    receive_task = asyncio.create_task(self.receive_loop())

                    try:
                        await receive_task
                    except KeyboardInterrupt:
                        logger.info("收到键盘中断信号，正在退出...")
                        self.stop()
                        audio_task.cancel()
                        receive_task.cancel()
                else:
                    mic_task = asyncio.create_task(self.process_microphone_input())
                    receive_task = asyncio.create_task(self.receive_loop())

                    try:
                        while self.is_running:
                            await asyncio.sleep(0.1)
                    except KeyboardInterrupt:
                        logger.info("收到键盘中断信号，正在退出...")
                        self.stop()
                        mic_task.cancel()
                        receive_task.cancel()

            await self.client.finish_session()
            while not self.is_session_finished:
                await asyncio.sleep(0.1)
            await self.client.finish_connection()
            await asyncio.sleep(0.1)
            await self.client.close()
            logger.info("对话完成，logid: %s, 模式: %s", self.client.logid, self.mod)
        except KeyboardInterrupt:
            logger.info("收到键盘中断信号，正在退出...")
            self.stop()
        except Exception as e: # pylint: disable=broad-except
            logger.error("会话错误: %s", e)
        finally:
            if not self.is_audio_file_input:
                self.audio_device.cleanup()





class RealtimeService(BaseService):
    """实时服务"""

    def __init__(
        self,
        event_bus: Optional[ProductionEventBus] = None,
        redis_service: Optional[RedisService] = None,
    ):
        super().__init__(event_bus=event_bus, service_name="RealtimeService")
        self.redis_service = redis_service
        self.ws_config = WS_CONNECT_CONFIG
        self.is_running = False
        self.current_session: str | None = None
        self.call_id: str | None = None

    async def initialize(self) -> bool:
        """初始化实时服务"""
        try:
            logger.info("RealtimeService initialized successfully")
            return True
        except Exception as e:  # pylint: disable=broad-except
            logger.error("RealtimeService initialization failed: %s", e)
            return False

    async def register_event_listeners(self):
        """注册事件监听器"""
        await self._register_listener(EventType.PHONE_SERVICE_ONANSWER, self.handle_realtime_start)
        await self._register_listener(EventType.PHONE_SERVICE_ONHANGUP, self.handle_realtime_stop)

    async def main(
        self,
        audio_format: str = "pcm",
        audio: str = "",
        mod: str = "audio",
        recv_timeout: int = 10,
    ) -> None:
        """启动实时对话会话"""
        try:
            session = DialogSession(
                ws_config=self.ws_config,
                output_audio_format=audio_format,
                audio_file_path=audio,
                mod=mod,
                recv_timeout=recv_timeout,
                realtime_service=self,
            )
            await session.start()
            self.stats["total_processed"] += 1
        except Exception as e:  # pylint: disable=broad-except
            logger.error("Error in realtime dialog: %s", e)
            self.stats["total_failed"] += 1
            raise

    async def start_realtime_dialog(self, **kwargs) -> None:
        """启动实时对话的便捷方法"""
        await self.main(**kwargs)

    async def handle_realtime_start(self, event: Event = None) -> bool:
        """处理实时服务启动事件"""
        try:
            if self.is_running:
                logger.warning("RealtimeService is already running")
                return False

            on_message : OnMessageType = event.data

            self.call_id = on_message.uuid

            audio_format = "pcm"
            audio = ""
            mod = "audio"
            recv_timeout = 10

            logger.info(
                "Starting realtime service with default params: format=%s, audio=%s, mod=%s",
                audio_format,
                audio,
                mod,
            )

            # 创建会话并启动实时对话
            self.current_session = DialogSession(
                ws_config=self.ws_config,
                output_audio_format=audio_format,
                audio_file_path=audio,
                mod=mod,
                recv_timeout=recv_timeout,
                realtime_service=self,
            )

            asyncio.create_task(self.current_session.start())

            self.is_running = True
            self.stats["total_processed"] += 1

            return True

        except Exception as e:  # pylint: disable=broad-except
            logger.error("Error starting realtime service: %s", e)
            self.stats["total_failed"] += 1
            return False

    async def handle_realtime_stop(self, _: Event = None) -> bool:
        """处理实时服务停止事件"""
        try:
            if not self.is_running:
                logger.warning("RealtimeService is not running")
                return False

            logger.info("Stopping realtime service")

            # 实际停止当前会话
            if self.current_session:
                try:
                    # 调用会话的停止方法
                    self.current_session.stop()
                    logger.info("Dialog session stopped successfully")
                except Exception as e: # pylint: disable=broad-except
                    logger.error("Error stopping dialog session: %s", e)
                finally:
                    self.current_session = None

            self.is_running = False

            return True

        except Exception as e:  # pylint: disable=broad-except
            logger.error("Error stopping realtime service: %s", e)
            self.stats["total_failed"] += 1
            return False

    async def health_check(self) -> bool:
        """健康检查"""
        base_health = await super().health_check()
        return {
            **base_health,
            "is_running": self.is_running,
            "has_redis_service": self.redis_service is not None,
            "ws_config_available": self.ws_config is not None,
        }
