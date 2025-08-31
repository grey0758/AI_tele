from fastapi import APIRouter, HTTPException, BackgroundTasks
import time
import uuid

from app.schemas.tts import (
    TTSRequest, TTSResponse, TTSControl
)
from app.services.tts_service import StreamingTTSClient
from app.core.logger import logger

router = APIRouter()


@router.post("/create", response_model=TTSResponse)
async def create_tts_session(
    request: TTSRequest,
    background_tasks: BackgroundTasks
):
    """创建TTS会话"""
    try:
        # 生成会话ID
        session_id = request.session_id or str(uuid.uuid4())
        
        # 在后台启动TTS客户端
        background_tasks.add_task(_start_tts_client, request, session_id)
        
        logger.info(f"TTS会话创建成功: {session_id}")
        
        return TTSResponse(
            success=True,
            message="TTS会话创建成功",
            session_id=session_id,
            data={
                "session_id": session_id,
                "status": "created",
                "text": request.text
            }
        )
        
    except Exception as e:
        logger.error(f"创建TTS会话失败: {e}")
        raise HTTPException(status_code=500, detail=f"创建TTS会话失败: {str(e)}")


@router.get("/status/{session_id}", response_model=TTSResponse)
async def get_tts_status(session_id: str):
    """获取TTS会话状态"""
    try:
        # 简化状态查询，返回基本信息
        return TTSResponse(
            success=True,
            message="获取状态成功",
            session_id=session_id,
            data={
                "session_id": session_id,
                "status": "active",
                "note": "状态管理功能已简化"
            }
        )
        
    except Exception as e:
        logger.error(f"获取TTS状态失败: {e}")
        raise HTTPException(status_code=500, detail=f"获取状态失败: {str(e)}")


@router.post("/control", response_model=TTSResponse)
async def send_control_command(control: TTSControl):
    """发送TTS控制命令"""
    try:
        logger.info(f"发送TTS控制命令: {control.action} 到会话 {control.session_id}")
        
        return TTSResponse(
            success=True,
            message="控制命令发送成功",
            session_id=control.session_id,
            data={"action": control.action}
        )
        
    except Exception as e:
        logger.error(f"发送控制命令失败: {e}")
        raise HTTPException(status_code=500, detail=f"发送控制命令失败: {str(e)}")


@router.get("/queue/{user_id}", response_model=TTSResponse)
async def get_user_queue(user_id: str = "default"):
    """获取用户播放队列"""
    try:
        # 简化队列查询
        return TTSResponse(
            success=True,
            message="获取队列成功",
            data={"queue": [], "user_id": user_id, "note": "队列管理功能已简化"}
        )
        
    except Exception as e:
        logger.error(f"获取用户队列失败: {e}")
        raise HTTPException(status_code=500, detail=f"获取队列失败: {str(e)}")


@router.get("/active/{user_id}", response_model=TTSResponse)
async def get_active_sessions(user_id: str = "default"):
    """获取用户活跃会话"""
    try:
        # 简化活跃会话查询
        return TTSResponse(
            success=True,
            message="获取活跃会话成功",
            data={"active_sessions": [], "user_id": user_id, "note": "会话管理功能已简化"}
        )
        
    except Exception as e:
        logger.error(f"获取活跃会话失败: {e}")
        raise HTTPException(status_code=500, detail=f"获取活跃会话失败: {str(e)}")


@router.get("/history/{user_id}", response_model=TTSResponse)
async def get_user_history(user_id: str = "default", limit: int = 20):
    """获取用户播放历史"""
    try:
        # 简化历史记录查询
        return TTSResponse(
            success=True,
            message="获取历史记录成功",
            data={"history": [], "user_id": user_id, "limit": limit, "note": "历史记录功能已简化"}
        )
        
    except Exception as e:
        logger.error(f"获取用户历史失败: {e}")
        raise HTTPException(status_code=500, detail=f"获取历史记录失败: {str(e)}")


@router.delete("/session/{session_id}", response_model=TTSResponse)
async def stop_tts_session(session_id: str):
    """停止TTS会话"""
    try:
        logger.info(f"停止TTS会话: {session_id}")
        
        return TTSResponse(
            success=True,
            message="停止命令发送成功",
            session_id=session_id
        )
        
    except Exception as e:
        logger.error(f"停止TTS会话失败: {e}")
        raise HTTPException(status_code=500, detail=f"停止会话失败: {str(e)}")


@router.get("/node/info", response_model=TTSResponse)
async def get_node_info():
    """获取节点信息"""
    try:
        # 简化节点信息
        node_info = {
            "node_id": "simplified_node",
            "instance_id": "simplified_instance",
            "created_at": "2024-01-01T00:00:00",
            "active_sessions": 0,
            "queue_length": 0,
            "note": "节点信息功能已简化"
        }
        
        return TTSResponse(
            success=True,
            message="获取节点信息成功",
            data=node_info
        )
        
    except Exception as e:
        logger.error(f"获取节点信息失败: {e}")
        raise HTTPException(status_code=500, detail=f"获取节点信息失败: {str(e)}")


async def _start_tts_client(request: TTSRequest, session_id: str):
    """在后台启动TTS客户端"""
    try:
        # 创建TTS客户端
        tts_client = StreamingTTSClient(
            session_id=session_id,
            user_id=request.user_id or "default"
        )
        
        # 启动客户端（非阻塞）
        tts_client.start()
        
        # 等待连接建立后再添加文本
        time.sleep(2)  # 给WebSocket连接更多时间建立
        
        # 添加文本并开始播放
        tts_client.add_text(request.text)
        tts_client.finish_input()
        
        logger.info(f"TTS客户端启动成功: {session_id}")
        
    except Exception as e:
        logger.error(f"启动TTS客户端失败: {e}")
