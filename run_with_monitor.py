#!/usr/bin/env python3
"""
带监控的启动脚本
使用 aiomonitor 监控异步代码的阻塞情况
"""
import os
import uvicorn
from app.core.config import settings

def main():
    """启动带监控的应用"""
    # 设置环境变量启用监控
    os.environ["ENABLE_MONITOR"] = "true"
    
    print("🔍 启动带监控的应用...")
    print("监控功能:")
    print("  - 实时任务监控")
    print("  - 阻塞检测")
    print("  - 内存使用情况")
    print("  - 事件循环状态")
    print("  - Web界面: http://localhost:50101")
    print("  - 控制台命令: telnet localhost 50101")
    print()
    
    # 启动应用
    uvicorn.run(
        "main:app",
        host=settings.host,
        port=settings.port,
        reload=False,  # 监控模式下禁用reload
        log_level="info"
    )

if __name__ == "__main__":
    main()
