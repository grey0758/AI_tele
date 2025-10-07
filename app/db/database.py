"""
数据库服务模块

提供数据库连接功能，包括MySQL和SSH隧道。
"""

from contextlib import asynccontextmanager
from typing import Dict, Any

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy import text

from app.core.config import settings
from app.core.logger import get_logger
from app.db import ssh_tunnel


logger = get_logger(__name__)


class Base(DeclarativeBase):
    """SQLAlchemy 声明式基类"""

class DatabaseConnectionError(Exception):
    """数据库连接错误"""

class Database:
    """数据库管理类，提供连接、会话管理和健康检查功能"""

    def __init__(self):
        self.engine = None
        self.async_session_factory = None
        self.ssh_tunnel = None
        self._initialized = False

    async def initialize(self) -> bool:
        """初始化数据库连接"""
        if self._initialized:
            return True

        try:
            # 如果需要SSH隧道
            if settings.ssh_host:
                self.ssh_tunnel = ssh_tunnel.SSHClient()
                if not await self.ssh_tunnel.connect():
                    raise DatabaseConnectionError("Failed to establish SSH tunnel")

                database_url = (
                    f"mysql+aiomysql://{settings.db_username}:"
                    f"{settings.db_password}@127.0.0.1:"
                    f"{settings.ssh_local_port}/{settings.db_name}"
                )
            else:
                database_url = (
                    f"mysql+aiomysql://{settings.db_username}:"
                    f"{settings.db_password}@{settings.db_host}:"
                    f"{settings.db_port}/{settings.db_name}"
                )

            # 创建异步引擎
            self.engine = create_async_engine(
                database_url,
                echo=False,
                pool_size=20,
                max_overflow=30,
                pool_pre_ping=True,
                pool_recycle=3600,
                connect_args={
                    "connect_timeout": 10,
                }
            )

            # 创建会话工厂
            self.async_session_factory = async_sessionmaker(
                bind=self.engine,
                class_=AsyncSession,
                expire_on_commit=False
            )

            # 测试连接
            async with self.get_session() as session:
                await session.execute(text("SELECT 1"))

            self._initialized = True
            logger.info("Database connection initialized successfully")
            return True

        except (DatabaseConnectionError, ConnectionError, TimeoutError) as e:
            logger.error("Failed to initialize database: %s", e)
            await self.close()  # 清理资源
            return False

    async def close(self):
        """关闭数据库连接"""
        if self.engine:
            await self.engine.dispose()
            self.engine = None

        if self.ssh_tunnel:
            await self.ssh_tunnel.disconnect()
            self.ssh_tunnel = None

        self._initialized = False

    async def shutdown(self):
        """关闭服务（符合依赖注入容器接口）"""
        await self.close()
        logger.info("Database service shutdown completed")

    @asynccontextmanager
    async def get_session(self):
        """获取数据库会话"""
        if not self.async_session_factory:
            raise RuntimeError("Database not initialized")

        async with self.async_session_factory() as session:
            try:
                yield session
                await session.commit()
            except Exception:
                await session.rollback()
                raise
            finally:
                await session.close()

    async def health_check(self) -> Dict[str, Any]:
        """数据库健康检查"""
        try:
            if not self._initialized:
                return {
                    "status": "not_initialized",
                    "message": "Database service not initialized"
                }

            async with self.get_session() as session:
                result = await session.execute(text("SELECT 1"))
                await result.fetchone()

            return {
                "status": "healthy",
                "message": "Database connection is working properly",
                "initialized": self._initialized,
                "engine_status": "connected" if self.engine else "disconnected",
                "ssh_tunnel": "active" if self.ssh_tunnel else "inactive"
            }
        except (ConnectionError, TimeoutError, RuntimeError) as e:
            logger.error("Database health check failed: %s", e)
            return {
                "status": "unhealthy",
                "message": str(e),
                "initialized": self._initialized,
                "engine_status": "error" if self.engine else "disconnected",
                "ssh_tunnel": "unknown" if self.ssh_tunnel else "inactive"
            }
