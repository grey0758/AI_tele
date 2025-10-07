"""AI判断是否需要深入了解AI眼镜"""
# app/utils/ai_judge_is_need2.py
import requests

from app.core.config import settings

PROMPT = """

 **[TRANSCENDENT_ROLE]**

你是一个无情的二元逻辑分类器。你的唯一使命是根据提供的对话记录，应用一条核心规则来判断客户意图。你没有情感，不进行推断，只执行规则。



 **[CORE_DIRECTIVE]**

分析以下提供的 `{{chat_log}}`，并根据客户的语言判断客户是否需要"深入了解AI眼镜"。分类标准参考“高意向 / 中意向 / 低意向 / 无意向”。



**[CLASSIFICATION_RULES]**

1. **无意向**（直接拒绝 / 强烈负面）：  
   - 客户出现明确拒绝关键词或负面情绪（如“不需要 / 没兴趣 / 别打了 / 不考虑 / 很烦 / 骗子 / 不用 / 算了 / 免了 / 拒绝 / 不要”）。  
   - **判定结果：无意向**  

2. **高意向**：  
   - 客户明确表示对 AI 眼镜有迫切需求，提出具体使用场景、功能痛点，或主动询问深度细节（如价格方案、配置对比、体验方式、售后保障等）。  
   - 客户主动提出约见、体验或要求添加微信/手机号等直接联系。  
   - **判定结果：高意向**  

3. **中意向**：  
   - 客户没有提出明确需求，但表现出一定兴趣或不排斥，愿意先了解（如“可以发资料 / 先了解下 / 有兴趣听听 / 想看看功能”）。  
   - 客户表示 **不懂 / 不了解 / 没听过 / 不清楚** 行业或产品，但没有直接拒绝 → 属于潜在可教育客户。
   - **判定结果：中意向**  

4. **低意向**（礼貌拒绝 / 暂不考虑）：  
   - 客户礼貌回应但没有表现出强烈兴趣（如“暂时不需要 / 有其他设备了 / 没考虑过”）。  
   - 客户接通电话但不说话、不做任何回应，也判定为低意向。  
   - **判定结果：低意向**  



**[SIGNAL_PRECEDENCE]**

- 若对话中同时出现无意向信号和其他任何信号，**优先级最高为无意向**，直接判定为 `无意向`。  
- 若未检测到无意向信号，则根据最高级别的积极信号进行判定。  
- 若客户说出“再见”，则对话立即结束，并根据截至该处的已有信号逻辑输出最终判定。  



 **[OUTPUT_FORMAT]**

输出必须是严格的 JSON 格式，示例如下：  

{
    "ai_intention_level": 7,
    "ai_feedback": "客户主动询问价格和配置，并表示希望体验，因此判定为高意向。"
}

### 输出规则说明

1. **ai_intention_level**：  
   - 7 → 高意向（主动讨论细节 / 要求联系 / 迫切需求）  
   - 6 → 中意向（愿意先了解 / 接受资料 / 表示兴趣/ 不懂但愿意听/不了解产品但没有立刻挂断电话）  
   - 5 → 低意向（礼貌拒绝 / 暂不考虑 / 沉默不回应）  
   - 0 → 无意向（明确拒绝 / 强烈负面）  

2. **ai_feedback**：  
   - 简述判定依据，说明客户对话中哪些词汇或行为影响了意向度。  
   - 文字应简洁、直观，不包含标签或额外符号。  



 **[EXECUTION_COMMAND]**

现在，根据以下聊天记录，执行指令。  



**聊天信息:**  

{{chat_log}}

"""



MODEL = "gpt-4.1"
MAX_TOKENS = 5000
url = settings.openai_url
key = settings.openai_api_key
headers = {
    'Accept': 'application/json',
    'Authorization': f'Bearer {key}',
    'Content-Type': 'application/json'
}


def ai_decision(chat_log):
    """AI判断是否需要深入了解AI眼镜"""
    payload = {
        "model": MODEL,
        "max_tokens": 5000,
        "messages": [{
            "role": "user",
            "content": PROMPT.replace("{{chat_log}}", str(chat_log))
        }],
        "temperature": 0
    }
    response_data = requests.post(url, headers=headers, json=payload, timeout=30).json()
    # print(response_data)
    choices = response_data.get('choices')
    content = None
    if choices and isinstance(choices, list) and len(choices) > 0:
        first_choice = choices[0]
        message = first_choice.get('message')
        if message:
            content = message.get('content')
    if content and ord(content[0]) == 32:
        content = content[1:]
    if content and content[:2] == '\n\n':
        content = content[2:]
    return content


if __name__ == '__main__':
    print(ai_decision("""广州大麦-月月: 面谈：陈婉雯，13509958485，40年家族企业，在佛山，做玻璃加工，想推广建筑玻璃，BToB，直播会议全程跟的，非常认可我们，最近在做升级，先谈一下看看，后面会来公司面谈 周五回复，我到时候联系她，高概率 随时沟通，解答了疑问，高概率 他们老板想继续过来下，晚点他给我具体时间确定""").replace("\n", " ")) # pylint: disable=line-too-long
    