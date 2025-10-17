"""AI电话服务类 - 处理实际的电话拨打逻辑"""

# app/services/aicall_service.py
from datetime import datetime
import asyncio
import re
from fastapi import HTTPException
from sqlalchemy import select, false
from app.models.events import Event, EventType
from app.schemas.aicall import CallRequest
from app.core.logger import get_logger
from app.models.call_record import CallRecord
from app.services.base_service import BaseService
from app.core.event_bus import ProductionEventBus
from app.services.redis_service import RedisService
from app.db.database import Database
from app.models.phone_call_queue import PhoneCallQueue, PhoneCallQueueCopy1
from app.core.config import settings


logger = get_logger(__name__)


class AicallService(BaseService):
    """AI电话服务类 - 处理实际的电话拨打逻辑"""

    def __init__(self, event_bus: ProductionEventBus, redis_service: RedisService, db: Database = None):
        super().__init__(event_bus=event_bus, service_name="AicallService")
        self.redis_service = redis_service
        self.db = db
        self.is_ended = True  # 是否通话结束
        self.machine_id = f"machine_{id(self)}"  # 机器标识符
        self.tts_opening = ""

    async def initialize(self) -> bool:
        """初始化"""
        return True

    async def register_event_listeners(self):
        """注册事件监听器"""
        await self._register_listener(EventType.PHONE_SERVICE_ONHANGUP_AUTO_CALL, self.auto_call_next_phone, timeout=30.0)

    async def make_call(self, call_request: CallRequest):
        """
        拨打电话服务函数 - 阻塞函数，一次只能有一个电话在拨打

        Args:
            call_request: 通话请求对象

        Returns:
            Dict: 包含通话状态和信息的字典
        """
        try:
            # 检查是否已有通话在进行
            if not self.is_ended:
                logger.info("Another call is already in progress")
                raise HTTPException(status_code=429, detail="Another call is already in progress")

            self.is_ended = False

            logger.info(
                "Starting call to %s with TTS opening...", call_request.phone_number
            )

            self.tts_opening = call_request.tts_opening

            call_record = CallRecord(
                **call_request.model_dump(),
                status="to_be_dialed",
                instance=call_request.device_index,
                start_time=datetime.now(),
                call_type="呼出",
                dialog_record=[]
            )

            await self.emit_event(EventType.PHONE_SERVICE_CALL_OUT, call_record)
            logger.info("Call initiated to %s", call_record.phone_number)

        except Exception as e:  # pylint: disable=broad-except
            logger.error("Error making call to %s: %s", call_request.phone_number, e)
            raise e

    async def reset_to_initialized_state(self, _: Event | None = None):
        """重置到初始化状态"""
        self.is_ended = True
        logger.info("Reset to initialized state completed successfully")

    def _is_valid_phone_number(self, phone_number: str) -> bool:
        """验证电话号码格式是否符合要求"""
        # 匹配11位手机号，以1开头，第二位是3-9
        pattern = r'^1[3-9]\d{9}$'
        return bool(re.match(pattern, phone_number))
    
    def _get_queue_model(self):
        """根据配置获取正确的队列表模型"""
        if settings.character_manifest_type == "ai_tele":
            return PhoneCallQueueCopy1
        else:
            return PhoneCallQueue

    async def get_next_phone_atomically(self) -> str | None:
        """
        原子性地获取下一个未拨打的电话号码
        使用数据库行锁确保分布式环境下的唯一性
        
        Returns:
            str: 电话号码，如果没有可用号码则返回None
        """
        if not self.db:
            logger.error("数据库连接未初始化")
            return None
            
        try:
            # 获取正确的表模型
            QueueModel = self._get_queue_model()
            table_name = QueueModel.__tablename__
            
            async with self.db.get_session() as session:
                # 先检查数据库中的总记录数和可用记录数
                total_stmt = select(QueueModel)
                total_result = await session.execute(total_stmt)
                total_records = total_result.scalars().all()
                
                available_stmt = select(QueueModel).where(QueueModel.is_called == false()) # pylint: disable=not-callable
                available_result = await session.execute(available_stmt)
                available_records = available_result.scalars().all()
                
                logger.info("数据库状态检查 [%s] - 总记录数: %d, 可用记录数: %d", table_name, len(total_records), len(available_records))
                
                if len(total_records) == 0:
                    logger.warning("数据库表 [%s] 中没有任何电话号码记录", table_name)
                    return None
                
                if len(available_records) == 0:
                    logger.warning("数据库表 [%s] 中所有电话号码都已被拨打", table_name)
                    return None
                
                # 使用SELECT ... FOR UPDATE锁定行，确保原子性
                stmt = (
                    select(QueueModel)
                    .where(QueueModel.is_called == false()) # pylint: disable=not-callable
                    .order_by(QueueModel.created_at.asc())
                    .limit(1)
                    .with_for_update(skip_locked=True)  # 跳过已被锁定的行
                )
                
                result = await session.execute(stmt)
                phone_record = result.scalar_one_or_none()
                
                if not phone_record:
                    logger.info("没有可用的电话号码，机器ID: %s, 表: %s", self.machine_id, table_name)
                    return None
                
                # 立即标记为已拨打，防止其他机器获取
                phone_record.is_called = True
                phone_record.updated_at = datetime.now()
                
                await session.commit()
                
                logger.info(
                    "成功获取电话号码: %s, 机器ID: %s, 记录ID: %s, 表: %s", 
                    phone_record.phone, self.machine_id, phone_record.id, table_name
                )
                
                return phone_record.phone
                
        except Exception as e:
            logger.error("获取电话号码失败，机器ID: %s, 错误: %s", self.machine_id, e)
            return None

    async def auto_call_next_phone(self, _: Event | None = None, retry_count: int = 0):
        """从数据库原子性获取下一个电话号码并自动拨打"""
        try:
            await asyncio.sleep(3)  # 等待3秒
            
            # 防止无限递归，最多重试10次
            if retry_count >= 10:
                logger.error("连续跳过无效电话号码超过10次，停止自动拨打")
                self.is_ended = True
                return
            
            # 原子性获取下一个电话号码
            phone_number = await self.get_next_phone_atomically()
            
            if not phone_number:
                logger.info("没有更多待打列表，自动拨打结束")
                return
            
            logger.info("开始自动拨打: 电话=%s, 机器ID=%s", phone_number, self.machine_id)
            
            # 验证电话号码格式
            if not self._is_valid_phone_number(phone_number):
                logger.warning("电话号码格式无效，跳过: %s (重试次数: %d)", phone_number, retry_count)
                # 递归调用获取下一个号码
                await self.auto_call_next_phone(retry_count=retry_count + 1)
                return
            
            # 创建拨打电话请求
            try:
                call_request = CallRequest(
                    phone_number=phone_number,
                    device_index=0,
                    tts_opening=self.tts_opening,
                    custom_id= None
                )
            except Exception as validation_error:
                logger.error("创建CallRequest失败，电话号码格式验证错误: %s, 错误: %s", phone_number, validation_error)
                # 递归调用获取下一个号码
                await self.auto_call_next_phone(retry_count=retry_count + 1)
                return

            self.is_ended = True

            # 发起拨打
            await self.make_call(call_request)
            
        except Exception as e:  # pylint: disable=broad-except
            logger.error("自动拨打失败: %s", e)
            # 发生错误时重置状态，允许手动拨打
            self.is_ended = True

    async def start_auto_calling(self):
        """启动自动拨打模式"""
        try:
            if not self.is_ended:
                logger.warning("已有通话在进行，无法启动自动拨打")
                return False
            
            logger.info("启动自动拨打模式（测试模式）")
            await self.auto_call_next_phone()
            return True
            
        except Exception as e:  # pylint: disable=broad-except
            logger.error("启动自动拨打失败: %s", e)
            return False
    
    async def reset_all_called_status(self) -> bool:
        """重置所有已打状态为未打状态"""
        if not self.db:
            logger.error("数据库连接未初始化")
            return False
            
        try:
            # 获取正确的表模型
            QueueModel = self._get_queue_model()
            table_name = QueueModel.__tablename__
            
            async with self.db.get_session() as session:
                # 重置所有已打状态
                from sqlalchemy import update
                stmt = update(QueueModel).values(
                    is_called=False,
                    updated_at=datetime.now()
                )
                
                result = await session.execute(stmt)
                await session.commit()
                
                affected_rows = result.rowcount
                logger.info("成功重置表 [%s] 中 %d 条记录的已打状态", table_name, affected_rows)
                
                return True
                
        except Exception as e:
            logger.error("重置已打状态失败，表: %s, 错误: %s", table_name, e)
            return False
