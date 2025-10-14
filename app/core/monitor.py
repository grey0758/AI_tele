"""异步监控配置"""
import asyncio
import aiomonitor
from app.core.logger import get_logger

logger = get_logger(__name__)


def setup_monitor(loop: asyncio.AbstractEventLoop, host: str = "localhost", port: int = 50101):
    """
    设置 aiomonitor 监控
    
    Args:
        loop: 事件循环
        host: 监控服务主机
        port: 监控服务端口
    """
    try:
        # 启动 aiomonitor
        monitor = aiomonitor.start_monitor(
            loop=loop,
            host=host,
            port=port,
            console_enabled=True,
            locals_dump_enabled=True,
            trace_enabled=True,
            trace_filter=None,  # 可以设置过滤条件
        )
        
        logger.info(f"✅ aiomonitor 已启动 - http://{host}:{port}")
        logger.info("监控功能:")
        logger.info("  - 实时任务监控")
        logger.info("  - 阻塞检测")
        logger.info("  - 内存使用情况")
        logger.info("  - 事件循环状态")
        logger.info("  - 远程调试控制台")
        
        return monitor
        
    except Exception as e:
        logger.error(f"❌ aiomonitor 启动失败: {e}")
        return None


def get_monitor_commands():
    """获取监控命令帮助"""
    return """
aiomonitor 常用命令:
1. 查看所有任务: tasks
2. 查看阻塞任务: blocked
3. 查看内存使用: memory
4. 查看事件循环状态: loop
5. 查看任务堆栈: stack <task_id>
6. 取消任务: cancel <task_id>
7. 查看帮助: help

Web界面访问: http://localhost:50101
"""
