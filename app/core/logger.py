"""日志配置"""
# app/core/logger.py
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
from rich.logging import RichHandler
from rich.console import Console
from app.core.config import settings

# Create logs directory if it doesn't exist
Path("logs").mkdir(exist_ok=True)

def setup_logging():
    """设置全局日志配置"""
    root_logger = logging.getLogger()
    # 清理已存在的处理器，避免重复添加
    if root_logger.hasHandlers():
        root_logger.handlers.clear()
    root_logger.setLevel(getattr(logging, settings.log_level))

    # 关闭websockets库的调试日志
    logging.getLogger('websockets').setLevel(logging.WARNING)

    # Rich 控制台处理器
    console_handler = RichHandler(
        console=Console(stderr=True),
        show_time=True,
        show_path=True,
        markup=True,
        rich_tracebacks=True
    )
    console_handler.setLevel(getattr(logging, settings.log_level))

    # 轮转文件处理器，避免单文件变大导致频繁变更
    file_handler = RotatingFileHandler(
        "logs/app.log",
        maxBytes=10 * 1024 * 1024,  # 10MB
        backupCount=5,
        encoding='utf-8'
    )
    file_handler.setLevel(logging.DEBUG)
    file_formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    file_handler.setFormatter(file_formatter)

    root_logger.addHandler(console_handler)
    root_logger.addHandler(file_handler)

# 初始化全局日志配置
setup_logging()

def get_logger(name: str = None) -> logging.Logger:
    """
    获取logger实例
    
    Args:
        name: logger名称，如果为None则返回根logger
        
    Returns:
        logging.Logger: 配置好的logger实例
    """
    if name is None:
        return logging.getLogger("ai_tele")

    # 获取指定名称的logger
    logger_instance = logging.getLogger(name)

    # 确保logger级别设置正确
    logger_instance.setLevel(getattr(logging, settings.log_level))

    return logger_instance

# 主应用logger（向后兼容）
logger = get_logger("ai_tele")
