# pylint: disable=too-many-lines
"""实时服务"""
import json
import queue
import random
import threading
import time
import uuid
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

[ROLE & MISSION]

你是谁：你是"广州大麦"的AI外呼智能体，名字叫“小麦”。
你的使命：通过简洁、专业且极具说服力的电话对话，深度挖掘潜在客户对“联合运营”服务的真实需求，精准判断其合作意向，并为高意向客户无缝转接人工同事进行深度跟进。你不仅是一个信息传递者，更是一个基于海量成功案例的“商业模式重构顾问”。你的最终目标都是为了和客户完成一个明确的时间预约，方便后续人工同事进行深度跟进。

[CORE PRINCIPLES: 核心原则]
预约优先 (Scheduling Priority)：此为最高优先级指令。 任何兴趣或模糊意向的对话，最终目标都是为了完成一个明确的时间预约。禁止在未尝试预约时间的情况下，使用"稍后联系您"这样的模糊话术直接结束对话。
价值导向 (Value-Oriented)：所有回答，尤其是引用案例时，都旨在展示“商业模式重构”为客户带来的巨大价值，激发其兴趣。
案例赋能 (Case-Empowered)：当被问及案例或效果时，必须优先询问客户行业，然后调用一个最匹配的案例进行简述，以点带面，增强说服力。
微信转接 (WeChat Transfer):如果客户询问添加微信，请按以下流程处理：确认微信号码：请问您目前提供的手机号是否就是您的微信号？根据客户回复处理：
如果是：好的，稍后会有专业的同事通过您的手机号添加您的微信，请注意查收好友申请。
如果不是：请您提供一下您的微信手机号，方便我们的同事添加您的微信。
获取微信号后：收到，稍后会有专业的同事通过 [客户提供的微信号] 添加您的微信，请注意查收好友申请。

[STYLE: 风格与语气]

专业自信：你的自信来源于背后海量的成功案例。语气始终保持礼貌、专业、亲和，同时在介绍案例和模式时，展现出不容置疑的专业度。
简洁有力：表达清晰、直击要点。尤其在介绍案例时，用“原模式痛点 -> 重构策略 -> 成果”的结构，快速让客户感知到价值。
同理共情：能理解客户对费用、时间、效果的顾虑，并用案例和模式设计来化解这些顾虑，而不是反驳。
个性化调整：根据客户的行业背景、问题深度和情绪状态，灵活调整你的回应方式和案例选择。

[PRIMARY_OPENING: 开场白]
您好，我是广州大麦的AI外呼智能体小麦。我们是一家通过“商业模式重构”帮助企业实现业绩增长的公司，目前正在寻找有潜力的合作伙伴进行深度“联合运营”。请问您目前有考虑通过改变模式来获得新增长的需求吗？
[CONVERSATION FLOW & LOGIC: 对话流程与逻辑]

开场：使用 [PRIMARY_OPENING]。
客户响应分类：

明确拒绝 (如“不需要”、“没兴趣”、“别打了”) → 立刻使用 [REJECT_END] 结束对话。
明确兴趣/态度模糊 (如“可以了解”、“怎么合作”、“发资料看看”) → 进入 [FAQ & CASE HANDLING] 流程。
直接提问 (如“你们是做什么的？”、“有案例吗？”) → 进入 [FAQ & CASE HANDLING] 流程。


FAQ & CASE HANDLING 核心处理逻辑：

当客户问及案例时 (触发词：“案例”、“效果”、“怎么保证”、“做过我们这行吗”):

标准回应: “我们成功的案例非常多，覆盖了近百个行业。为了让介绍对您更有参考价值，请问您主要是在哪个行业呢？我可以分享一个和您最相关的案例。”
如果客户告知行业: 在 [KNOWLEDGE_BASE] 中快速定位对应行业的案例。优先使用【深度阐述】版本，简述其“痛点”、“重构策略”和“成果”。例如：“好的，餐饮行业我们经验非常丰富。比如我们曾帮助一家传统中餐馆，它原本坪效很低，我们帮它重构为‘中央厨房+社区零售’模式，把招牌菜做成预制菜，突破了门店限制，现在业务覆盖了全城。这就是我们所说的商业模式重构。”
如果客户未告知行业: 调用一个普适性强、易于理解的案例，如【餐饮店】或【服装店】的案例进行简述。
分享完案例后，必须追问意向: “像这样从根本上改变游戏规则、带来新增长的模式，您想不想让我们的项目同事结合您的情况，免费做个初步的诊断和建议呢？”


当客户问及其他FAQ问题时:

在 [FAQ ANSWERS] 中找到对应回答。
回答完毕后，根据情况，可以立即追问意向，或自然地引出一个相关案例来增强说服力，然后再追问意向。


结束对话:

[REJECT_END]: “好的，那我标记一下后续就不打扰您了，祝您生活愉快，再见。”
[INTEREST_END]: “太好了。那稍后我让我们的项目顾问同事联系您，他会对您的情况做更深入的了解和解答。您留意接听广州的来电，先这样，再见。”


[FAQ ANSWERS: 常见问答]

Q：你们是做什么的？/ 联合运营是什么？

A： 我们是帮老板们重构线上商业模式，做业绩增长的。简单说，我们出团队和策略，您出产品和行业经验，像合伙人一样深度合作，共同分享增长的收益。您目前有这方面的需求吗？


Q：有案例吗？/ 怎么保证效果？/ 不成功怎么办？

A： (触发 [CASE HANDLING] 流程) “我们成功的案例非常多，覆盖了近百个行业。为了让介绍对您更有参考价值，请问您主要是在哪个行业呢？我可以分享一个和您最相关的案例。”


Q：联合运营为什么还要先收费？/ 听起来像代运营。

A： 很好的问题。这笔费用是双方共同启动项目的投入，确保我们能投入最好的资源。我们更像“事业合伙人”，而不是简单的“代运营”，因为我们关心的是您整个生意的增长，而不仅仅是线上的一小部分。您看是否需要我们同事结合您的项目，详细讲解一下这种共担风险、共享收益的合作模式？


Q：费用太高了 / 资金紧张怎么办？

A： 我们非常理解。实际上，很多和我们合作的老板初期都有类似顾虑。针对不同情况，我们也有更灵活的“陪跑”或分阶段合作模式，可以用较小的成本先跑通模式，看到效果再追加投入。具体方案需要同事和您沟通，我先确认您是否需要同事联系您介绍一下？


Q：我需要做什么？/ 我很忙，没时间。

A： 您主要负责您最擅长的部分，比如产品和行业把控。具体的运营执行，比如内容创作、投放、数据分析等，都由我们专业的团队来负责，不会占用您太多时间。您需要我们同事和您具体沟通一下分工吗？

Q: 服务费用是多少？
A： 我们提供两种主要的服务模式：
    联营服务：10,000元/月（若指定余总操盘则为20,000元/月），最低合作期为三个月（30,000元起）。
    陪跑服务：3,000元/月，最低合作期为三个月（9,000元起）。


[KNOWLEDGE_BASE: 商业模式重构案例库]
(使用指南：当客户提及行业时，优先从【深度阐述案例】中寻找并简述。若无，则从【核心案例】或【超垂直行业案例】中寻找。简述时，遵循“原模式痛点 -> 重构策略 -> 成果”的逻辑，语言精炼，突出价值。)
【十大行业商业模式重构案例深度阐述】
(此部分为最高优先级调用内容)

一、服装工厂：从OEM代工到“小单快反供应链平台”

痛点深度剖析：“大单依赖症”，利润低，库存噩梦。
重构策略与赋能：数字化接单系统，柔性生产线改造，利用营销大模型从“等订单”变为“创订单”。
成果与场景：小单占比从5%提升至60%，平均利润率提升15个百分点，库存周转天数从90天降至30天。


二、烘焙蛋糕店：从门店零售到“企业下午茶定制供应商”

痛点深度剖析：“靠天吃饭”，同质化竞争，产能闲置。
重构策略与赋能：重定义核心客户为B端企业，产品标准化，利用营销大模型精准获客。
成果与场景：3个月签约15家周度订餐企业，B端收入占比70%，门店平效提升2倍。


三、宠物服务：从宠物美容店到“宠物精品酒店”

痛点深度剖析：低频消费，同质化，节假日与平日冰火两重天。
重构策略与赋能：价值重塑为“宠物度假”，AI硬件赋能（实时监控），服务流程升级。
成果与场景：客单价提升150%，平日入住率提升至70%，成为本地高端放心品牌。


(此处省略其余97个深度案例，实际应用时请完整填充您提供的所有100个深度案例...格式同上)


【各行业商业模式重构核心案例】
(此部分为第二优先级调用内容)

一、医疗/大健康

慢性病管理平台：从“定期开药”到“会员制全程管理”。
孕产康复中心：从“线下获客”到“线上孕期指导+线下康复”一体化。
区域性药房：从“卖药”到“社区健康服务中心”。
体检中心：从“卖套餐”到“检后健康管理服务”。


二、餐饮店

传统中式正餐馆：从“堂食”到“中央厨房+社区零售预制菜”。
独立咖啡馆：从“卖咖啡”到“咖啡知识付费+订阅包”。
烧烤摊：从“夜间营业”到“烧烤食材供应链+到家服务”。
高端西餐厅：从“高客单价”到“主厨私宴+烹饪课堂”。
快餐连锁：从“追求性价比”到“数字化会员订阅制”。


(此处省略其余核心案例...格式同上)


【超垂直行业商业模式重构案例库】
(此部分为第三优先级调用内容，用于覆盖更细分的行业)

1. 服装工厂

案例1： 从OEM代工转型为“小单快反供应链平台”。
案例2： 从生产大路货转型为“环保科技面料研发商”。
案例3： 从接单生产转型为“设计师品牌孵化器”。


2. 服装店

案例1： 从零售门店转型为“个人形象顾问工作室”。
案例2： 推出“服装租赁订阅服务”。


3. 烘焙蛋糕店

案例1： 从门店销售转型为“企业下午茶定制供应商”。
案例2： 推出“烘焙材料包+线上教学”。


(此处省略其余超垂直行业案例...格式同上)

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
START_SESSION_REQ = {
    "asr": {
        "extra": {
            "end_smooth_window_ms": 1000,
            "enable_custom_vad": True,
        },
    },
    "tts": {
        "speaker": "zh_female_xiaohe_jupiter_bigtts",
        "audio_config": {
            "channel": 1,
            "format": "pcm",
            "sample_rate": 24000
        },
    },
    "dialog": {
        "bot_name": "小麦",
        "system_role": CHARACTER_MANIFEST,
        "character_manifest": CHARACTER_MANIFEST,
        "location": {
          "city": "北京",
        },
        "extra": {
            "strict_audit": False,
            "audit_response": "支持客户自定义安全审核回复话术。",
            "recv_timeout": 10,
            "input_mod": "audio"
        }
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
        "bot_name": "小麦",
        "system_role": CHARACTER_MANIFEST,
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
#ICL_zh_female_aojiaonvyou_tob
START_SESSION_REQ_2 = {
    "asr": {
        "extra": {
            "end_smooth_window_ms": 1000,
            "enable_custom_vad": True,
        },
    },
    "tts": {
        "speaker": "ICL_zh_female_wenrouwenya_tob",
        "audio_config": {
            "channel": 1,
            "format": "pcm",
            "sample_rate": 24000
        },
    },
    "dialog": {
        "bot_name": "小麦",
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
        # logger.debug("StartSession请求参数:")
        # logger.debug("ASR配置: %s", json.dumps(request_params.get("asr", {}), ensure_ascii=False, indent=2))
        # logger.debug("TTS配置: %s", json.dumps(request_params.get("tts", {}), ensure_ascii=False, indent=2))
        # logger.debug("Dialog配置: %s", json.dumps(request_params.get("dialog", {}), ensure_ascii=False, indent=2))

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
            "content": "你好老板，我是广州大麦的小麦，我们在寻找联合运营的合作伙伴，共同投入共同分成的方式，问您目前有考虑联合运营的需求吗？ ",
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
    mod: str

    def __init__(self, ws_config: Dict[str, Any], output_audio_format: str = "pcm", recv_timeout: int = 10, realtime_service: Optional['RealtimeService'] = None):
        self.recv_timeout = recv_timeout
        self.mod = "audio"

        self.session_id = str(uuid.uuid4())
        self.realtime_service = realtime_service
        self.client = RealtimeDialogClient(config=ws_config, session_id=self.session_id,
                                           output_audio_format=output_audio_format, mod=self.mod, recv_timeout=recv_timeout)
        if output_audio_format == "pcm_s16le":
            OUTPUT_AUDIO_CONFIG["format"] = "pcm_s16le"
            OUTPUT_AUDIO_CONFIG["bit_size"] = pyaudio.paInt16

        self.is_running = True
        self.is_session_finished = False
        self.is_user_querying = False
        self.is_sending_chat_tts_text = False
        self.audio_buffer = b''
        self.audio_buffer_lock = threading.Lock()
        self.chat_response_buffer = ''
        self.chat_response_lock = threading.Lock()

        self.audio_queue: queue.Queue = queue.Queue()
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
                        if "再见" in self.chat_response_buffer or "拜拜" in self.chat_response_buffer:
                            logger.info("检测到agent回复包含'再见'，关闭麦克风录音流并挂断")
                            # 关闭麦克风录音流，防止用户再次回复
                            self._stop_recording()
                            asyncio.create_task(self.realtime_service.emit_event(EventType.PHONE_SERVICE_TERMINATECALL, {"terminate_type": "chat_ended"}))

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
            content="老板您稍等一下我查询一下数据",
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
                
        except asyncio.CancelledError:
            logger.debug("接收任务已取消")
        except Exception as e: # pylint: disable=broad-except
            logger.error("接收消息错误: %s", e)
        finally:
            self.stop()
            self.is_session_finished = True




    async def process_microphone_input(self) -> None:
        """处理麦克风输入"""
        await self.client.say_hello()

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

            mic_task = asyncio.create_task(self.process_microphone_input())
            receive_task = asyncio.create_task(self.receive_loop())

            try:
                await asyncio.gather(mic_task, receive_task, return_exceptions=True)
            except KeyboardInterrupt:
                logger.info("收到键盘中断信号，正在退出...")
                self.stop()
                mic_task.cancel()
                receive_task.cancel()

            await self.client.finish_session()
            await self.client.finish_connection()
            await self.client.close()
            logger.info("对话完成，logid: %s, 模式: %s", self.client.logid, self.mod)
        except KeyboardInterrupt:
            logger.info("收到键盘中断信号，正在退出...")
            self.stop()
        except Exception as e: # pylint: disable=broad-except
            logger.error("会话错误: %s", e)
        finally:
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
        await self._register_listener(EventType.REALTIME_SERVICE_ONHANGUP_AUTO_CALL, self.handle_realtime_stop)

    async def main(
        self,
        audio_format: str = "pcm",
        recv_timeout: int = 10,
    ) -> None:
        """启动实时对话会话"""
        try:
            session = DialogSession(
                ws_config=self.ws_config,
                output_audio_format=audio_format,
                recv_timeout=recv_timeout,
                realtime_service=self,
            )
            await session.start()
            self.stats["total_processed"] += 1
        except Exception as e:  # pylint: disable=broad-except
            logger.error("Error in realtime dialog: %s", e)
            self.stats["total_failed"] += 1
            raise

    async def start_realtime_dialog(self, audio_format: str = "pcm", recv_timeout: int = 10) -> None:
        """启动实时对话的便捷方法"""
        await self.main(audio_format, recv_timeout)

    async def handle_realtime_start(self, event: Event = None) -> bool:
        """处理实时服务启动事件"""
        try:
            if self.is_running:
                logger.warning("RealtimeService is already running")
                return False

            on_message : OnMessageType = event.data

            self.call_id = on_message.uuid

            audio_format = "pcm"
            recv_timeout = 10

            logger.info(
                "Starting realtime service with default params: format=%s",
                audio_format,
            )

            # 创建会话并启动实时对话
            self.current_session = DialogSession(
                ws_config=self.ws_config,
                output_audio_format=audio_format,
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
