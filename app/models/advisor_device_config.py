"""顾问设备配置数据库模型"""
from datetime import datetime
from sqlalchemy import BigInteger, String, SmallInteger, Text, DateTime, func
from sqlalchemy.orm import Mapped, mapped_column, relationship
from app.db.database import Base


class AdvisorDeviceConfig(Base):
    """顾问设备配置表模型"""
    
    __tablename__ = "advisor_device_config"
    
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True, comment="主键ID")
    device_id: Mapped[str] = mapped_column(String(50), nullable=False, comment="设备ID")
    devid: Mapped[str] = mapped_column(String(200), nullable=True, comment="设备ID（新字段）")
    advisor_id: Mapped[int] = mapped_column(SmallInteger, nullable=False, comment="顾问ID")
    advisor_name: Mapped[str] = mapped_column(String(50), nullable=False, comment="顾问姓名")
    goal: Mapped[int] = mapped_column(BigInteger, nullable=False, default=7200, comment="指标")
    character_manifest: Mapped[str] = mapped_column(Text, nullable=True, comment="提示词字段")
    opening_statement: Mapped[str] = mapped_column(Text, nullable=True, comment="开场白字段")
    start_phone_number: Mapped[str] = mapped_column(String(11), nullable=True, comment="开始电话号码")
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now(), comment="创建时间")  # pylint: disable=not-callable
    updated_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now(), onupdate=func.now(), comment="更新时间")  # pylint: disable=not-callable
    
    # 外键关系
    advisor = relationship("Advisor", foreign_keys=[advisor_id], back_populates="device_configs")
    
    __table_args__ = (
        {'mysql_engine': 'InnoDB', 'mysql_charset': 'utf8mb4', 'mysql_collate': 'utf8mb4_unicode_ci'},
    )
