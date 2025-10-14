from datetime import datetime
from sqlalchemy import BigInteger, String, Boolean, DateTime, Index, UniqueConstraint, func
from sqlalchemy.orm import Mapped, mapped_column
from app.db.database import Base


class PhoneCallQueue(Base):
    """电话待打表模型"""
    
    __tablename__ = "phone_call_queue"
    
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True, comment="主键ID")
    phone: Mapped[str] = mapped_column(String(20), nullable=False, comment="电话号码")
    is_called: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, comment="是否已经拨打：TRUE=已拨打，FALSE=未拨打")
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now(), comment="创建时间")  # pylint: disable=not-callable
    updated_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now(), onupdate=func.now(), comment="更新时间")  # pylint: disable=not-callable
    
    __table_args__ = (
        Index('idx_phone', 'phone'),
        Index('idx_is_called', 'is_called'),
        Index('idx_created_at', 'created_at'),
        UniqueConstraint('phone', name='uk_phone'),
        {'mysql_engine': 'InnoDB', 'mysql_charset': 'utf8mb4', 'mysql_collate': 'utf8mb4_unicode_ci'}
    )
