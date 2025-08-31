import os
import asyncio
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import QueuePool
from app.core.config import settings
from app.core.logger import logger
from app.db.ssh_tunnel_paramiko import create_ssh_tunnel_paramiko_from_env, SSHTunnelParamiko

# Global SSH tunnel instance
_ssh_tunnel: SSHTunnelParamiko = None
_engine = None
_health_check_task = None


def get_database_url_with_ssh() -> str:
    """获取考虑SSH隧道的数据库URL"""
    global _ssh_tunnel
    
    # 检查是否需要SSH隧道
    if settings.ssh_host:
        try:
            if _ssh_tunnel is None:
                _ssh_tunnel = create_ssh_tunnel_paramiko_from_env()
                if _ssh_tunnel and _ssh_tunnel.start():
                    logger.info("SSH tunnel started for database connection")
                else:
                    logger.warning("Failed to start SSH tunnel, falling back to default database")
                    return settings.database_url
            
            # 使用本地端口连接，添加连接参数
            db_username = settings.db_username or "root"
            db_password = settings.db_password or ""
            db_name = settings.db_name or "ai_tele"
            
            # 添加MySQL连接参数来解决超时问题
            connection_params = (
                "?charset=utf8mb4"
                "&autocommit=true"
                "&connect_timeout=60"
                "&read_timeout=60"
                "&write_timeout=60"
            )
            
            return f"mysql://{db_username}:{db_password}@localhost:{_ssh_tunnel.local_port}/{db_name}{connection_params}"
        except Exception as e:
            logger.warning(f"SSH tunnel failed: {e}, falling back to default database")
            return settings.database_url
    
    return settings.database_url


def create_database_engine():
    """创建优化的数据库引擎"""
    global _engine
    
    if _engine is None:
        database_url = get_database_url_with_ssh()
        
        _engine = create_engine(
            database_url,
            # 连接池配置
            poolclass=QueuePool,
            pool_size=10,                    # 连接池大小
            max_overflow=20,                 # 最大溢出连接数
            pool_timeout=30,                 # 获取连接超时时间
            pool_recycle=3600,              # 连接回收时间（1小时）
            pool_pre_ping=True,             # 连接前检查连接有效性
            
            # MySQL特定配置
            connect_args={
                "connect_timeout": 60,       # 连接超时
                "read_timeout": 60,          # 读取超时
                "write_timeout": 60,         # 写入超时
                "charset": "utf8mb4",        # 字符集
                "autocommit": True,          # 自动提交
            },
            
            # 调试配置
            echo=settings.debug,
            echo_pool=settings.debug,
        )
        
        logger.info("Database engine created with optimized settings")
    
    return _engine


# 创建数据库引擎
engine = create_database_engine()

SessionLocal = sessionmaker(
    autocommit=False, 
    autoflush=False, 
    bind=engine,
    expire_on_commit=False  # 避免会话过期问题
)


def get_db():
    """获取数据库会话"""
    db = SessionLocal()
    try:
        yield db
    except Exception as e:
        logger.error(f"Database session error: {e}")
        db.rollback()
        raise
    finally:
        db.close()


async def check_database_health():
    """检查数据库连接健康状态"""
    try:
        with engine.connect() as connection:
            result = connection.execute(text("SELECT 1"))
            result.fetchone()
        logger.debug("Database health check passed")
        return True
    except Exception as e:
        logger.error(f"Database health check failed: {e}")
        return False


async def database_health_monitor():
    """数据库健康监控任务"""
    while True:
        try:
            await check_database_health()
            await asyncio.sleep(300)  # 每5分钟检查一次
        except Exception as e:
            logger.error(f"Health monitor error: {e}")
            await asyncio.sleep(60)  # 出错后1分钟后重试


def start_database_health_monitor():
    """启动数据库健康监控"""
    global _health_check_task
    if _health_check_task is None:
        _health_check_task = asyncio.create_task(database_health_monitor())
        logger.info("Database health monitor started")


def stop_database_health_monitor():
    """停止数据库健康监控"""
    global _health_check_task
    if _health_check_task:
        _health_check_task.cancel()
        _health_check_task = None
        logger.info("Database health monitor stopped")


def recreate_engine():
    """重新创建数据库引擎（在连接问题时使用）"""
    global _engine
    if _engine:
        _engine.dispose()
        _engine = None
    
    _engine = create_database_engine()
    
    # 重新绑定SessionLocal
    SessionLocal.configure(bind=_engine)
    logger.info("Database engine recreated")


def close_ssh_tunnel():
    """关闭SSH隧道和清理资源"""
    global _ssh_tunnel, _engine
    
    # 停止健康监控
    stop_database_health_monitor()
    
    # 关闭数据库引擎
    if _engine:
        _engine.dispose()
        _engine = None
        logger.info("Database engine disposed")
    
    # 关闭SSH隧道
    if _ssh_tunnel:
        _ssh_tunnel.stop()
        _ssh_tunnel = None
        logger.info("SSH tunnel closed")


# 数据库会话上下文管理器
class DatabaseSession:
    """数据库会话上下文管理器"""
    
    def __init__(self):
        self.db = None
    
    def __enter__(self):
        self.db = SessionLocal()
        return self.db
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            self.db.rollback()
            logger.error(f"Database transaction rolled back: {exc_val}")
        else:
            self.db.commit()
        self.db.close()


# 异步数据库操作装饰器
def with_db_retry(max_retries=3, delay=1):
    """数据库操作重试装饰器"""
    def decorator(func):
        async def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    if attempt == max_retries - 1:
                        logger.error(f"Database operation failed after {max_retries} attempts: {e}")
                        raise
                    
                    logger.warning(f"Database operation failed (attempt {attempt + 1}), retrying: {e}")
                    await asyncio.sleep(delay * (attempt + 1))
            
        return wrapper
    return decorator
