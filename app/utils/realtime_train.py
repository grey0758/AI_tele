"""实时训练服务"""
import base64
import json
import os
import sys
import requests
from app.core.logger import get_logger

logger = get_logger(__name__)


def encode_audio_file(file_path):
    """编码音频文件"""
    with open(file_path, 'rb') as audio_file:
        audio_data = audio_file.read()
        encoded_data = str(base64.b64encode(audio_data), "utf-8")
        audio_format = os.path.splitext(file_path)[1][1:]
        return encoded_data, audio_format


def train(appid, token, audio_path, spk_id):
    """训练语音"""
    host = "https://openspeech.bytedance.com"
    url = host + "/api/v1/mega_tts/audio/upload"
    headers = {
        "Content-Type": "application/json",
        "Authorization": "Bearer;" + token,
        "Resource-Id": "volc.megatts.voiceclone",
    }
    encoded_data, audio_format = encode_audio_file(audio_path)
    audios = [{"audio_bytes": encoded_data, "audio_format": audio_format}]
    data = {"appid": appid, "speaker_id": spk_id, "audios": audios, "source": 2,"language": 0, "model_type": 3}
    extra_params = {}
    if extra_params:
        data["extra_params"] =  json.dumps(extra_params)
    response = requests.post(url, json=data, headers=headers, timeout=300)
    logger.info("status code = %s", response.status_code)
    if response.status_code != 200:
        raise RuntimeError("train请求错误:" + response.text)
    logger.info("headers = %s", response.headers)
    logger.info("Response: %s", response.json())


def get_status(appid, token, spk_id):
    """获取语音状态"""
    host = "https://openspeech.bytedance.com"
    url = host + "/api/v1/mega_tts/status"
    headers = {
        "Content-Type": "application/json",
        "Authorization": "Bearer;" + token,
        "Resource-Id": "volc.megatts.voiceclone",
    }
    body = {"appid": appid, "speaker_id": spk_id}
    response = requests.post(url, headers=headers, json=body, timeout=300)
    logger.info("Status response: %s", response.json())


def main():
    """训练主方法"""
    appid = "7053540602"
    token = "Z9q4zthzIu5w4RfuFEJbUZCRM8Z_gJBW"
    audio_path = r"E:\xyh\9.18\215\10月10日(1).WAV"
    spk_id = "S_JGlN6EIG1"

    try:
        logger.info("开始训练语音，参数: appid=%s, audio_path=%s, spk_id=%s", appid, audio_path, spk_id)
        train(appid, token, audio_path, spk_id)
        logger.info("训练完成")
    except (RuntimeError, FileNotFoundError, ValueError, requests.exceptions.RequestException) as e:
        logger.error("训练失败: %s", e)
        sys.exit(1)


if __name__ == "__main__":
    main()
