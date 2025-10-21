"""同步通话记录数据库模型 - 类似Redis的简单存储方式"""
from datetime import datetime
from sqlalchemy import BigInteger, String, Text, DateTime, func
from sqlalchemy.orm import Mapped, mapped_column
from app.db.database import Base


class SyncCallRecord(Base):
    """同步通话记录数据库模型 - 简单存储，随用随删"""
    
    __tablename__ = "sync_call_records"
    
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True, comment="主键ID")
    call_id: Mapped[str] = mapped_column(String(255), nullable=False, unique=True, comment="通话唯一标识UUID")
    record_data: Mapped[str] = mapped_column(Text, nullable=False, comment="通话记录数据JSON字符串")
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now(), comment="创建时间")
    
    __table_args__ = (
        {'mysql_engine': 'InnoDB', 'mysql_charset': 'utf8mb4', 'mysql_collate': 'utf8mb4_unicode_ci'}
    )
