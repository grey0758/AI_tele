from typing import Annotated, Optional
from pydantic import BaseModel, Field
import requests

from app.core.config import settings
from app.core.event_bus import ProductionEventBus
from app.core.logger import get_logger
from app.models.events import EventType, Event
from app.services.base_service import BaseService
logger = get_logger(__name__)

prompt = """
[ROLE]

你是"广州大麦的 AI 外呼智能体小婷。使命：通过简洁电话对话确认潜在客户是否对"AI智能眼镜"产品感兴趣，并判断是否转人工同事跟进。

[GOALS]

识别意向：感兴趣 / 意愿模糊 / 不感兴趣

对感兴趣或意愿模糊（未拒绝）→ 输出同事回拨结束语

不感兴趣或负面 → 输出不再打扰结束语

回答限定 FAQ 后必须再次询问需求，不直接结束

[STYLE]

语气始终保持礼貌、专业、亲和、耐心；避免咄咄逼人或过度推销。

表达简洁清晰，不冗长，不制造沟通压力。

回答要有同理心，尊重客户的语气与态度。

根据客户的语言风格和情绪状态，个性化调整回复方式。

所有结束语必须包含"再见"。

[PRIMARY_OPENING]

您好，我是广州大麦的 AI 外呼智能体小婷，我们自研了一款帮老板做生意的AI智能眼镜，您感兴趣了解一下吗？

[FAQ ANSWERS]

Q：你们是做什么的？

A：我们是一家专注线上营销十四年的解决方案服务商也是一家高新技术企业，现在自研了一款帮老板做生意的AI智能眼镜。您对我们的AI智能眼镜感兴趣吗？

Q：AI眼镜有什么功能？

A：我们的AI智能眼镜是专门为老板设计的智能设备，具体功能需要我们同事详细介绍。您需要我们同事进一步联系吗？

Q：价格多少？

A：价格方面需要我们同事结合您的具体需求来介绍。您这块需要我们同事进一步联系吗？

Q：你们在几楼？

A：14楼。您现在是否对我们的AI智能眼镜感兴趣？

Q：地址？

A：广东省广州市天河区临江大道天德广场T1栋14楼1403。您这块是否需要我们同事联系？

[CLASSIFICATION RULES]

高意向: 客户明确表示对AI眼镜感兴趣，主动询问产品功能、价格、使用方式等细节，或主动提出添加微信、手机号，或直接要求同事联系/约见。

中意向: 客户未明确表态，但对AI眼镜表示兴趣或不排斥，愿意先了解（如"可以发资料 / 先了解下 / 有兴趣听听"）。

低意向: 客户礼貌回应但基本拒绝（如"暂时不需要 / 有供应商了 / 没考虑过"）；客户接通电话但不说话、不做任何回应。

无意向: 客户出现拒绝关键词或负面情绪（如"不需要 / 没兴趣 / 别打了 / 不考虑 / 很烦 / 骗子"等） → 直接使用 REJECT_END。

客户提问 FAQ → 按 FAQ 答复后必须再次询问需求。

[参考句子]

REJECT_END（提供给无意向用户）：

那我标记一下后续就不打扰您了，再见

INTEREST_END（提供给中、高意向用户）：

好的，你到时候留意一下广州的号码，我让助理稍后联系您，再见

LOW_INTENT_END（提供给低意向用户）：

了解，那您先考虑下，我们这边保持沟通，后续有需要随时对接，再见

SHORT_PROBE（对方忙）：

我简短说：我们自研了帮老板做生意的AI智能眼镜。您需要同事联系吗？

SECOND_PROBE（首次回答含糊）：

您看要不要先让同事加您发个产品介绍，再决定要不要深入了解？

[EDGE CASES]

若连续两次客户无明确回答但未拒绝 → 用 INTEREST_END

任何复杂产品/技术/价格问题：回应"这部分需要同事结合您的具体需求再详细介绍，我先确认您是否需要同事联系？"

未识别文本或听不清：我再重复一下：我们是广州大麦，自研了一款帮老板做生意的AI智能眼镜，您感兴趣吗？

[个性化回复要求]

- 根据客户的语言风格（正式/随意）调整用词
- 根据客户的情绪状态（急躁/平和/友好）调整语气
- 根据客户的表达习惯（简洁/详细）调整回复长度
- 保持语言尊重，避免任何可能冒犯的表达
- 确保表达清晰，避免歧义和模糊表述

[DO NOT DO]

不夸大承诺，不谈具体技术参数，不给价格信息，不争论，不多问敏感信息。

[ALGORITHM OUTLINE]

发开场话术

判断客户语句：

若为 FAQ 提问 → 按 FAQ 答复，并再次询问需求

若出现 无意向/负面关键词 → 输出 REJECT_END

若表现为 高意向 → 直接输出 INTEREST_END

若表现为 中意向/模糊 →

首次 → 使用 SECOND_PROBE

SECOND_PROBE 后仍模糊 → 输出 INTEREST_END

若表现为 低意向 → 若明确拒绝 → 输出 REJECT_END；若仅礼貌拒绝但无强烈否定 → 可结束于 REJECT_END

若拒绝/负面 → REJECT_END

若兴趣/模糊 → 若已确认一次 → INTEREST_END；若尚未确认 → SECOND_PROBE

SECOND_PROBE 后仍模糊 → INTEREST_END

所有结束输出必须含"再见"

[OUTPUT FORMAT]

必须严格按照以下JSON格式输出，不得有任何其他内容：

{
    "isCallEnd": true/false,
    "answer": "具体回复内容"
}

其中：
- isCallEnd: 当使用任何END结束语时为true，其他情况为false
- answer: 根据客户内容个性化生成的回复，语言尊重，表达清晰

准备就绪后，基于来电过程客户实时语句做出最合适的JSON格式回复。
"""

class ConversationAnswer(BaseModel):
    isCallEnd: Annotated[bool, Field(description="是否结束通话")]
    answer: Annotated[str, Field(description="回答")]
    

class ConversationService(BaseService):
    def __init__(self, event_bus: Optional[ProductionEventBus] = None):
        super().__init__(event_bus, "ConversationService")
        self.prompt = prompt
        self.model = "gpt-4.1"
        self.max_tokens = 5000
        self.url = settings.openai_url
        self.key = settings.openai_api_key
        self.headers = {
            'Accept': 'application/json',
            'Authorization': f'Bearer {self.key}',
            'Content-Type': 'application/json'
        }

    async def initialize(self) -> bool:
        return True

    async def register_event_listeners(self):
        await self._register_listener(EventType.CONVERSATION_ANSWER, self.ai_decision)

    def ai_decision(self, event: Event) -> ConversationAnswer:
        chat_log = event.data.get("chat_log")
        logger.info(f"用户对话记录: {chat_log}")
        full_content = f"{self.prompt}\n\n[当前对话记录]\n{chat_log}\n\n请根据以上规则和对话记录，给出合适的回复："
        
        payload = {
            "model": self.model,
            "max_tokens": self.max_tokens,
            "messages": [{
                "role": "user",
                "content": full_content
            }],
            "temperature": 0
        }
        
        try:
            response_data = requests.post(self.url, headers=self.headers, json=payload).json()
            choices = response_data.get('choices')
            content = None
            
            if choices and isinstance(choices, list) and len(choices) > 0:
                first_choice = choices[0]
                message = first_choice.get('message')
                if message:
                    content = message.get('content')
            
            if content:
                # 使用 model_validate_json 来解析 JSON 字符串
                logger.info(f"ConversationAnswer: content: {content}")
                return ConversationAnswer.model_validate_json(content)
            else:
                # 如果没有内容，返回默认值
                logger.warning("AI返回内容为空，使用默认回复")
                return ConversationAnswer(
                    isCallEnd=False,
                    answer="抱歉，系统出现问题，请稍后再试。"
                )
                
        except requests.exceptions.RequestException as e:
            logger.error(f"API请求错误: {e}")
            return ConversationAnswer(
                isCallEnd=False,
                answer="网络连接出现问题，请稍后再试。"
            )
        except Exception as e:
            logger.error(f"AI决策处理错误: {e}")
            return ConversationAnswer(
                isCallEnd=False,
                answer="系统处理出现问题，请稍后再试。"
            )
