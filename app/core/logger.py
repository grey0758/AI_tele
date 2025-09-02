# -*- coding: utf-8 -*-
import logging
import sys
from pathlib import Path
from app.core.config import settings

# Create logs directory if it doesn't exist
Path("logs").mkdir(exist_ok=True)

# 全局日志配置
def setup_logging():
    """设置全局日志配置"""
    # 获取根logger
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, settings.log_level.upper()))
    
    # 创建格式化器
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    
    # 控制台处理器
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    
    # 文件处理器
    file_handler = logging.FileHandler("logs/app.log", encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    
    # 添加处理器到根logger
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
    logger = logging.getLogger(name)
    
    # 确保logger级别设置正确
    logger.setLevel(getattr(logging, settings.log_level.upper()))
    
    return logger

# 主应用logger（向后兼容）
logger = get_logger("ai_tele")
