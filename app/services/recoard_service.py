"""录音服务类，用于获取录音记录和查找录音文件路径"""
# app/services/recoard_service.py
import time
from typing import Optional, Dict, Any
from urllib.parse import unquote
import asyncio
import requests
import aiohttp
from app.core.config import settings
from app.core.event_bus import ProductionEventBus
from app.core.logger import logger
from app.models.call_record import CallRecordInfo, CallType
from app.models.events import Event, EventType
from app.schemas.base import BaseResponse
from app.schemas.recoard_service import FileUploadRequest, FileUploadResponse
from app.services.base_service import BaseService
from app.services.redis_service import RedisService


class RecordService(BaseService):
    """录音服务类，用于获取录音记录和查找录音文件路径"""

    def __init__(
        self,
        event_bus: Optional[ProductionEventBus] = None,
        redis_service: Optional[RedisService] = None,
    ):
        super().__init__(event_bus, "RecordService")
        self.redis_service = redis_service
        self.cookies: Optional[str] = None
        self.base_url = "http://127.0.0.1:9898"
        self.login_url = f"{self.base_url}/ssplogin"
        self.record_url = f"{self.base_url}/msgdispatch/record/getrecorddata"
        self.upload_server_base_url = settings.ai_tele_record_service_url + "/api/v1"

    async def initialize(self) -> bool:
        await self.login()
        return True

    async def register_event_listeners(self):
        """注册事件监听器"""
        await self._register_listener(
            EventType.RECORD_CALL_END, self.upload_record, timeout=60.0
        )

    async def login(self) -> bool:
        """登录获取cookies"""
        try:
            data = {"username": "admin", "password": "admin"}
            login_res = requests.post(url=self.login_url, json=data, timeout=30)

            if login_res.status_code == 200:
                self.cookies = f"SSPLOGIN={login_res.cookies.get('SSPLOGIN')}"
                logger.info("登录成功")
                self.stats["total_processed"] += 1
                return True
            else:
                logger.error("登录失败，状态码: %s", login_res.status_code)
                self.stats["total_failed"] += 1
                return False
        except Exception as e:  # pylint: disable=broad-except
            logger.error("登录异常: %s", str(e))
            self.stats["total_failed"] += 1
            return False

    async def get_record_data(
        self, page: int = 1, page_size: int = 10
    ) -> Optional[Dict[str, Any]]:
        """获取录音记录数据"""
        try:
            if not self.cookies:
                if not await self.login():
                    return None

            now = time.time()
            today_start = int(now - (now % 86400))
            today_end = int(today_start + 86399)

            logger.info("查询今天录音记录，时间范围: %s - %s", today_start, today_end)
            logger.info(
                "时间范围对应: %s - %s",
                time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(today_start)),
                time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(today_end)),
            )

            data = {
                "page": page,
                "pageSize": page_size,
                "timeType": 5,
                "phoneType": 0,
                "Phone": "",
                "condition": {
                    "Type": None,
                    "BeginTime": today_start,
                    "EndTime": today_end,
                    "Phone": None,
                },
                "order": "BeginTime DESC",
            }

            headers = {"Content-Type": "application/json", "cookie": self.cookies}

            response = requests.post(
                self.record_url, json=data, headers=headers, timeout=30
            )

            if response.status_code == 200:
                return response.json()
            else:
                logger.error("获取录音数据失败，状态码: %s", response.status_code)
                return None

        except Exception as e:  # pylint: disable=broad-except
            logger.error("获取录音数据异常: %s", str(e))
            return None

    async def find_record_by_call_id(self, call_id: str) -> Optional[Dict[str, Any]]:
        """通过call_id查找录音记录"""
        try:
            record_data = await self.get_record_data(page_size=10)

            if not record_data:
                logger.warning("未获取到录音数据")
                return None

            logger.info("API返回数据结构: %s", type(record_data))

            records = []
            if isinstance(record_data, dict):
                if "data" in record_data and isinstance(record_data["data"], dict):
                    records = record_data["data"].get("list", [])
                elif "list" in record_data:
                    records = record_data["list"]
                elif isinstance(record_data.get("data"), list):
                    records = record_data["data"]
            elif isinstance(record_data, list):
                records = record_data

            logger.info("解析出的记录数量: %s", len(records))

            for record in records:
                if isinstance(record, dict) and record.get("uuid") == call_id:
                    logger.info("找到call_id为 %s 的录音记录", call_id)
                    return record

            logger.warning("未找到call_id为 %s 的录音记录", call_id)
            return None

        except Exception as e:  # pylint: disable=broad-except
            logger.error("通过call_id查找录音记录异常: %s", str(e))
            return None

    async def get_record_info(self, call_id: str) -> Optional[Dict[str, Any]]:
        """获取录音详细信息

        Args:
            call_id: 通话ID

        Returns:
            录音详细信息字典
        """
        try:
            record = await self.find_record_by_call_id(call_id)

            if record:
                info = {
                    "call_id": record.get("uuid"),
                    "phone": record.get("Phone"),
                    "begin_time": record.get("BeginTime"),
                    "end_time": record.get("EndTime"),
                    "duration": record.get("TimeLen"),
                    "path": record.get("Path"),
                    "file_size": record.get("FileSize"),
                    "type": record.get("Type"),
                    "area": record.get("Area"),
                    "upload_state": record.get("UploadState"),
                    "raw_record": record,
                }
                return info

            return None

        except Exception as e:  # pylint: disable=broad-except
            logger.info("获取录音详细信息异常: %s", str(e))
            return None

    async def health_check(self) -> Dict[str, Any]:
        """录音服务健康检查"""
        base_health = await super().health_check()

        record_health = {
            "has_cookies": self.cookies is not None,
            "base_url": self.base_url,
            "upload_server_url": self.upload_server_base_url,
            "login_status": "已登录" if self.cookies else "未登录",
        }

        base_health.update(record_health)
        return base_health

    async def upload_record(self, event: Event) -> Optional[CallRecordInfo | None]:
        """获取录音信息并上传到服务器

        Args:
            event: 包含call_id的事件对象

        Returns:
            包含录音信息和上传结果的字典
        """
        try:
            call_id = event.data.get("call_id") if event.data else None
            if not call_id:
                logger.error("事件中缺少call_id")
                return None

            await self.emit_event(
                EventType.REDIS_BIND_DIALOG_RECORD_TO_CALL_RECORD,
                data={"call_id": call_id},
                wait_for_result=True,
                timeout=25.0,
            )

            record_info = None
            max_retries = 3
            retry_delay = 10
            await asyncio.sleep(retry_delay)
            for attempt in range(max_retries):
                logger.info(
                    "正在查询录音信息，第 %s 次尝试，call_id: %s", attempt + 1, call_id
                )
                record_info = await self.get_record_info(call_id)

                if record_info:
                    logger.info("第 %s 次查询成功找到录音记录", attempt + 1)
                    break

                if attempt < max_retries - 1:
                    logger.warning(
                        "第 %s 次查询未找到录音记录，%s秒后重试",
                        attempt + 1,
                        retry_delay,
                    )
                    await asyncio.sleep(retry_delay)
                else:
                    logger.error(
                        "查询 %s 次后仍未找到call_id为 %s 的录音记录",
                        max_retries,
                        call_id,
                    )

            if not record_info or not record_info.get("path"):
                logger.warning("录音记录中没有文件路径信息")
                return None

            record_path = record_info.get("path")
            if not record_path:
                logger.error("录音记录中没有文件路径")
                return None

            upload_result = await self.download_and_upload_record(record_path, call_id)

            if not self.redis_service:
                logger.error("Redis服务未初始化")
                return None

            call_record = await self.redis_service.get_call_record(call_id)
            if call_record:
                call_record_info = CallRecordInfo(**call_record.model_dump())
                if upload_result:
                    call_record_info.file_url = upload_result.file_url
                call_record_info.file_size = record_info.get("file_size")
                call_record_info.area = record_info.get("area")
                start_time = record_info.get("begin_time")
                if start_time is not None:
                    call_record_info.start_time = start_time
                end_time = record_info.get("end_time")
                if end_time is not None:
                    call_record_info.end_time = end_time
                duration = record_info.get("duration")
                if duration is not None:
                    call_record_info.duration = duration
                call_type = record_info.get("type")
                if call_type is not None:
                    call_record_info.call_type = CallType.get_description(call_type)
                instance = record_info.get("instance")
                if instance is not None:
                    call_record_info.instance = instance
            else:
                logger.warning("录音记录不存在: %s", call_id)
                return None
            logger.info("录音信息: %s", call_record_info)
            return call_record_info

        except Exception as e:  # pylint: disable=broad-except
            logger.error("获取录音并上传异常: %s", str(e))
            return None

    async def download_and_upload_record(
        self, record_path: str, call_id: str
    ) -> Optional[FileUploadResponse | None]:
        """下载录音文件并上传到服务器

        Args:
            record_path: 录音文件路径
            call_id: 通话ID

        Returns:
            上传结果信息
        """
        try:
            file_uuid = call_id

            decoded_path = unquote(record_path)
            logger.info("原始路径: %s", record_path)
            logger.info("解码后路径: %s", decoded_path)

            logger.info("读取本地文件: %s", decoded_path)
            try:
                with open(decoded_path, "rb") as f:
                    file_content = f.read()
                logger.info("成功读取本地文件，大小: %s 字节", len(file_content))
            except FileNotFoundError:
                logger.error("文件不存在: %s", decoded_path)
                return None
            except Exception as e:  # pylint: disable=broad-except
                logger.error("读取本地文件失败: %s", str(e))
                return None

            file_extension = ".mp3"
            if "." in decoded_path:
                file_extension = "." + decoded_path.split(".")[-1].lower()

            file_upload_request = FileUploadRequest(
                file=file_content,
                file_uuid=file_uuid,
                description=f"{call_id}{file_extension}",
            )

            logger.info("开始上传录音文件到服务器: %s", self.upload_server_base_url)

            form_data = aiohttp.FormData()
            form_data.add_field(
                "file", file_upload_request.file, content_type="audio/mpeg"
            )
            form_data.add_field("file_uuid", file_uuid)
            form_data.add_field("description", file_upload_request.description)

            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{self.upload_server_base_url}/upload", data=form_data
                ) as upload_response:
                    json_data = await upload_response.json()
                    logger.info("上传录音文件响应: %s", json_data)
                    response = BaseResponse[FileUploadResponse].model_validate(
                        json_data
                    )
                    if response.code == 0:
                        logger.info("录音文件上传成功: %s", response)
                        return response.data
                    else:
                        logger.error("上传录音文件失败，状态码: %s", response.code)
                        logger.error("错误信息: %s", response.msg)
                        return None

        except Exception as e:  # pylint: disable=broad-except
            logger.error("下载并上传录音文件异常: %s", str(e))
            return None
