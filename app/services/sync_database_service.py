"""同步数据库服务 - 类似Redis的简单存储方式"""
import json
from datetime import datetime
from typing import Optional
from sqlalchemy import select, delete
from sqlalchemy.exc import IntegrityError

from app.core.logger import get_logger
from app.services.base_service import BaseService
from app.models.events import Event, EventType
from app.models.sync_call_record import SyncCallRecord
from app.db.database import Database

logger = get_logger(__name__)


class SyncDatabaseService(BaseService):
    """同步数据库服务 - 类似Redis的简单存储方式"""

    def __init__(self, event_bus, db: Database = None):
        super().__init__(event_bus=event_bus, service_name="SyncDatabaseService")
        self.db = db

    async def initialize(self) -> bool:
        """初始化"""
        return True

    async def register_event_listeners(self):
        """注册事件监听器"""
        await self._register_listener(EventType.SYNC_SAVE_CALL_RECORD, self.save_call_record, timeout=30.0)

    async def save_call_record(self, event: Event) -> bool:
        """
        保存通话记录到数据库 - 类似Redis的简单方式
        
        Args:
            event: 包含call_record的事件
            
        Returns:
            bool: 保存是否成功
        """
        try:
            call_record = event.data
            if not call_record:
                logger.error("Event data is None")
                return False

            if not self.db:
                logger.error("Database connection not available")
                return False

            # 获取call_id
            call_id = getattr(call_record, 'call_id', None)
            if not call_id:
                logger.error("Call record missing call_id")
                return False

            # 将通话记录转换为JSON字符串
            if hasattr(call_record, 'model_dump_json'):
                # Pydantic模型
                record_data = call_record.model_dump_json()
            elif hasattr(call_record, 'model_dump'):
                # Pydantic模型
                record_data = json.dumps(call_record.model_dump(), ensure_ascii=False)
            else:
                # 普通对象，尝试转换为字典
                try:
                    record_data = json.dumps(call_record.__dict__, ensure_ascii=False, default=str)
                except:
                    record_data = str(call_record)

            # 创建数据库记录
            db_record = SyncCallRecord(
                call_id=call_id,
                record_data=record_data
            )

            async with self.db.get_session() as session:
                # 先删除已存在的记录（随用随删）
                delete_stmt = delete(SyncCallRecord).where(SyncCallRecord.call_id == call_id)
                await session.execute(delete_stmt)
                
                # 插入新记录
                session.add(db_record)
                await session.commit()
                
                logger.info("Saved call record to sync database: %s", call_id)
                return True

        except IntegrityError as e:
            logger.error("Database integrity error saving call record: %s", e)
            return False
        except Exception as e:
            logger.error("Failed to save call record to sync database: %s", e)
            return False

    async def get_call_record(self, call_id: str) -> Optional[str]:
        """
        根据call_id获取通话记录数据
        
        Args:
            call_id: 通话记录ID
            
        Returns:
            str: 通话记录JSON字符串，如果不存在返回None
        """
        try:
            if not self.db:
                logger.error("Database connection not available")
                return None

            async with self.db.get_session() as session:
                stmt = select(SyncCallRecord.record_data).where(SyncCallRecord.call_id == call_id)
                result = await session.execute(stmt)
                record_data = result.scalar_one_or_none()
                
                if record_data:
                    logger.info("Retrieved call record from sync database: %s", call_id)
                    return record_data
                else:
                    logger.warning("Call record not found in sync database: %s", call_id)
                    return None

        except Exception as e:
            logger.error("Failed to get call record from sync database: %s", e)
            return None

    async def delete_call_record(self, call_id: str) -> bool:
        """
        删除通话记录
        
        Args:
            call_id: 通话记录ID
            
        Returns:
            bool: 删除是否成功
        """
        try:
            if not self.db:
                logger.error("Database connection not available")
                return False

            async with self.db.get_session() as session:
                delete_stmt = delete(SyncCallRecord).where(SyncCallRecord.call_id == call_id)
                result = await session.execute(delete_stmt)
                await session.commit()
                
                if result.rowcount > 0:
                    logger.info("Deleted call record from sync database: %s", call_id)
                    return True
                else:
                    logger.warning("Call record not found for deletion: %s", call_id)
                    return False

        except Exception as e:
            logger.error("Failed to delete call record from sync database: %s", e)
            return False

    async def cleanup_old_records(self, days: int = 7) -> int:
        """
        清理旧记录
        
        Args:
            days: 保留天数
            
        Returns:
            int: 删除的记录数
        """
        try:
            if not self.db:
                logger.error("Database connection not available")
                return 0

            from datetime import timedelta
            cutoff_date = datetime.now() - timedelta(days=days)

            async with self.db.get_session() as session:
                delete_stmt = delete(SyncCallRecord).where(SyncCallRecord.created_at < cutoff_date)
                result = await session.execute(delete_stmt)
                await session.commit()
                
                deleted_count = result.rowcount
                logger.info("Cleaned up %d old call records (older than %d days)", deleted_count, days)
                return deleted_count

        except Exception as e:
            logger.error("Failed to cleanup old call records: %s", e)
            return 0
