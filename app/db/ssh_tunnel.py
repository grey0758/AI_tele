"""
SSH 隧道服务模块

提供 SSH 隧道连接功能，用于数据库连接等场景。
"""

import asyncio
import asyncssh
from app.core.config import settings
from app.core.logger import get_logger

logger = get_logger(__name__)


class SSHClient:
    """SSH隧道客户端，用于数据库连接"""

    def __init__(self):
        # SSH配置
        self.ssh_host = settings.ssh_host
        self.ssh_port = settings.ssh_port
        self.ssh_username = settings.ssh_username
        self.ssh_password = settings.ssh_password
        self.ssh_key_path = settings.ssh_key_path

        # 隧道配置
        self.remote_host = settings.ssh_remote_host
        self.remote_port = settings.ssh_remote_port
        self.local_port = settings.ssh_local_port

        # 验证必需配置
        if not all([self.ssh_host, self.ssh_username]):
            raise ValueError("SSH host and username are required")
        if not (self.ssh_password or self.ssh_key_path):
            raise ValueError("SSH password or key path is required")

        self.connection = None
        self.tunnel = None

    async def connect(self) -> bool:
        """建立SSH连接和端口转发"""
        try:
            connect_kwargs = {
                "host": self.ssh_host,
                "port": self.ssh_port,
                "username": self.ssh_username,
                "known_hosts": None,
            }

            # 记录连接详情（不包含密码）
            logger.info("Attempting SSH connection:")
            logger.info("  Host: %s", self.ssh_host)
            logger.info("  Port: %s", self.ssh_port)
            logger.info("  Username: %s", self.ssh_username)
            logger.info("  Auth method: %s", "password" if self.ssh_password else "key")

            if self.ssh_password:
                connect_kwargs["password"] = self.ssh_password
            else:
                connect_kwargs["client_keys"] = [self.ssh_key_path]
                logger.info("  Key path: %s", self.ssh_key_path)

            # 建立SSH连接
            logger.info("Establishing SSH connection...")
            self.connection = await asyncssh.connect(**connect_kwargs)
            logger.info("SSH connection established successfully")

            # 创建端口转发隧道
            logger.info("Creating port forward tunnel...")
            self.tunnel = await self.connection.forward_local_port(
                "127.0.0.1",
                self.local_port,  # 本地绑定
                self.remote_host,
                self.remote_port,  # 远程目标
            )

            logger.info(
                "SSH tunnel established: localhost:%s -> %s:%s",
                self.local_port,
                self.remote_host,
                self.remote_port,
            )
            return True

        except asyncssh.PermissionDenied as e:
            logger.error("SSH Permission denied: %s", e)
            logger.error("Please check username and password/key")
            await self.disconnect()
            return False
        except asyncssh.ConnectionLost as e:
            logger.error("SSH Connection lost: %s", e)
            await self.disconnect()
            return False
        except asyncssh.Error as e:
            logger.error("SSH Error: %s", e)
            await self.disconnect()
            return False
        except OSError as e:
            logger.error("Network/OS Error: %s", e)
            await self.disconnect()
            return False

    async def disconnect(self):
        """断开SSH连接和隧道"""
        # 清理隧道
        if self.tunnel:
            try:
                logger.info("Closing SSH tunnel...")
                self.tunnel.close()
                await self.tunnel.wait_closed()
                logger.info("SSH tunnel closed")
            except (asyncssh.Error, OSError, asyncio.TimeoutError) as e:
                logger.error("Error closing SSH tunnel: %s", e)
            finally:
                self.tunnel = None

        # 清理连接
        if self.connection:
            try:
                logger.info("Closing SSH connection...")
                self.connection.close()
                await self.connection.wait_closed()
                logger.info("SSH connection closed")
            except (asyncssh.Error, OSError, asyncio.TimeoutError) as e:
                logger.error("Error closing SSH connection: %s", e)
            finally:
                self.connection = None


    def is_connected(self) -> bool:
        """检查连接状态"""
        return (
            self.connection is not None
            and not self.connection.is_closing()
            and self.tunnel is not None
        )

    async def __aenter__(self):
        """异步上下文管理器入口"""
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """异步上下文管理器出口"""
        await self.disconnect()
