"""时间限制配置数据库模型"""
from datetime import datetime
from sqlalchemy import BigInteger, String, Integer, Boolean, DateTime, func
from sqlalchemy.orm import Mapped, mapped_column
from app.db.database import Base


class TimeLimitConfig(Base):
    """时间限制配置表模型"""
    
    __tablename__ = "time_limit_configs"
    
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True, comment="主键ID")
    name: Mapped[str] = mapped_column(String(100), nullable=False, comment="配置名称")
    limit_hour: Mapped[int] = mapped_column(Integer, nullable=False, default=22, comment="限制时间（小时）")
    wait_timeout: Mapped[int] = mapped_column(Integer, nullable=False, default=3600, comment="等待超时时间（秒）")
    is_enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True, comment="是否启用")
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, comment="是否为当前激活配置")
    description: Mapped[str] = mapped_column(String(500), nullable=True, comment="配置描述")
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now(), comment="创建时间")
    updated_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now(), onupdate=func.now(), comment="更新时间")
    
    __table_args__ = (
        {'mysql_engine': 'InnoDB', 'mysql_charset': 'utf8mb4', 'mysql_collate': 'utf8mb4_unicode_ci'}
    )
