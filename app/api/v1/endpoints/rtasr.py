from fastapi import APIRouter, HTTPException, WebSocket, WebSocketDisconnect, Depends
from fastapi.responses import JSONResponse
from typing import Optional, Dict, Any
import json
import threading
import time
from datetime import datetime

from app.services.rtasr_service import get_rtasr_service, RtasrClient
from app.schemas.rtasr import RTASRRequest, RTASRResponse, RTASRStatus
from app.utils.response import success_response, error_response

router = APIRouter()


@router.post("/create", response_model=RTASRResponse)
async def create_rtasr_session(request: RTASRRequest):
    """创建实时语音识别会话"""
    try:
        rtasr_service = get_rtasr_service()
        
        
        # 创建客户端
        client = rtasr_service.create_client(
            session_id=request.session_id,
            user_id=request.user_id,
            debounce_delay=request.debounce_delay
        )
        
        return success_response(
            data={
                "session_id": client.session_id,
                "user_id": client.user_id,
                "status": "created"
            },
            message="RTASR会话创建成功"
        )
        
    except Exception as e:
        return error_response(f"创建RTASR会话失败: {str(e)}")


@router.get("/status/{session_id}")
async def get_rtasr_status(session_id: str):
    """获取RTASR会话状态"""
    try:
        rtasr_service = get_rtasr_service()
        client = rtasr_service.get_client(session_id)
        
        if not client:
            raise HTTPException(status_code=404, detail="会话不存在")
        
        status = client.get_status()
        return success_response(data=status, message="获取状态成功")
        
    except Exception as e:
        return error_response(f"获取状态失败: {str(e)}")


@router.get("/sessions")
async def get_all_rtasr_sessions():
    """获取所有RTASR会话"""
    try:
        rtasr_service = get_rtasr_service()
        sessions = rtasr_service.get_all_sessions()
        
        return success_response(
            data={"sessions": sessions},
            message="获取所有会话成功"
        )
        
    except Exception as e:
        return error_response(f"获取会话列表失败: {str(e)}")


@router.post("/start-audio/{session_id}")
async def start_rtasr_audio(session_id: str):
    """开始发送麦克风音频"""
    try:
        rtasr_service = get_rtasr_service()
        client = rtasr_service.get_client(session_id)
        
        if not client:
            raise HTTPException(status_code=404, detail="会话不存在")
        
        client.start_sending_audio()
        
        return success_response(
            data={"session_id": session_id},
            message="开始发送麦克风音频"
        )
        
    except Exception as e:
        return error_response(f"开始发送音频失败: {str(e)}")


@router.post("/stop-audio/{session_id}")
async def stop_rtasr_audio(session_id: str):
    """停止发送麦克风音频"""
    try:
        rtasr_service = get_rtasr_service()
        client = rtasr_service.get_client(session_id)
        
        if not client:
            raise HTTPException(status_code=404, detail="会话不存在")
        
        
        client.stop_sending_audio()
        
        return success_response(
            data={"session_id": session_id},
            message="停止发送麦克风音频"
        )
        
    except Exception as e:
        return error_response(f"停止发送音频失败: {str(e)}")


@router.post("/send-file/{session_id}")
async def send_rtasr_file(session_id: str, file_path: str):
    """发送音频文件进行识别"""
    try:
        rtasr_service = get_rtasr_service()
        client = rtasr_service.get_client(session_id)
        
        if not client:
            raise HTTPException(status_code=404, detail="会话不存在")
        
        # 在后台线程中发送文件
        def send_file_thread():
            client.send_file(file_path)
        
        threading.Thread(target=send_file_thread, daemon=True).start()
        
        return success_response(
            data={"session_id": session_id, "file_path": file_path},
            message="开始发送音频文件"
        )
        
    except Exception as e:
        return error_response(f"发送音频文件失败: {str(e)}")


@router.delete("/close/{session_id}")
async def close_rtasr_session(session_id: str):
    """关闭RTASR会话"""
    try:
        rtasr_service = get_rtasr_service()
        client = rtasr_service.get_client(session_id)
        
        if not client:
            raise HTTPException(status_code=404, detail="会话不存在")
        
        client.close()
        rtasr_service.remove_client(session_id)
        
        return success_response(
            data={"session_id": session_id},
            message="RTASR会话已关闭"
        )
        
    except Exception as e:
        return error_response(f"关闭会话失败: {str(e)}")


@router.get("/result/{session_id}")
async def get_rtasr_result(session_id: str):
    """获取RTASR识别结果（最新的一次识别）"""
    try:
        rtasr_service = get_rtasr_service()
        client = rtasr_service.get_client(session_id)
        
        if not client:
            raise HTTPException(status_code=404, detail="会话不存在")
        
        # 获取状态信息
        status = client.get_status()
        
        # 检查连接状态
        if not client.running or not client.ws or not client.ws.connected:
            return error_response(
                "连接已断开，请重新启动会话",
                data={
                    "session_id": session_id,
                    "status": status,
                    "suggestion": "请调用 /rtasr/start-audio/{session_id} 重新开始录音"
                }
            )
        
        # 获取最新的识别结果
        result = {
            "session_id": session_id,
            "last_text": client.last_text,
            "buffer_text": client.buffer_text,
            "status": status,
            "is_connected": client.running and client.ws and client.ws.connected,
            "is_sending_audio": client.is_sending_audio,
            "debug_info": {
                "connected": client.running,
                "ws_connected": client.ws.connected if client.ws else False,
                "running": client.running,
                "has_websocket": client.ws is not None
            },
            "timestamp": datetime.now().isoformat()
        }
        
        print(f"[{datetime.now()}]RTASR获取结果 - 会话ID: {session_id}")
        print(f"[{datetime.now()}]RTASR当前文本: '{client.last_text}'")
        print(f"[{datetime.now()}]RTASR缓冲区: '{client.buffer_text}'")
        print(f"[{datetime.now()}]RTASR连接状态: {result['is_connected']}")
        print(f"[{datetime.now()}]RTASR发送音频: {result['is_sending_audio']}")
        
        return success_response(
            data=result,
            message="获取识别结果成功"
        )
        
    except Exception as e:
        return error_response(f"获取识别结果失败: {str(e)}")


@router.get("/debug/{session_id}")
async def debug_rtasr_session(session_id: str):
    """调试RTASR会话状态"""
    try:
        rtasr_service = get_rtasr_service()
        client = rtasr_service.get_client(session_id)
        
        if not client:
            raise HTTPException(status_code=404, detail="会话不存在")
        
        debug_info = {
            "session_id": session_id,
            "user_id": client.user_id,
            "app_id": client.app_id,
            "api_key": "***" if client.api_key else None,
            "connected": client.running,
            "running": client.running,
            "is_sending_audio": client.is_sending_audio,
            "last_text": client.last_text,
            "buffer_text": client.buffer_text,
            "debounce_delay": client.debounce_delay,
            "has_websocket": client.ws is not None,
            "websocket_connected": client.ws.connected if client.ws else False,
            "status": client.get_status(),
            "timestamp": datetime.now().isoformat()
        }
        
        print(f"[{datetime.now()}]RTASR调试信息: {json.dumps(debug_info, ensure_ascii=False, indent=2)}")
        
        return success_response(
            data=debug_info,
            message="调试信息获取成功"
        )
        
    except Exception as e:
        return error_response(f"获取调试信息失败: {str(e)}")


@router.websocket("/ws/{session_id}")
async def rtasr_websocket(websocket: WebSocket, session_id: str):
    """WebSocket连接用于实时语音识别"""
    await websocket.accept()
    
    try:
        rtasr_service = get_rtasr_service()
        client = rtasr_service.get_client(session_id)
        
        if not client:
            await websocket.send_text(json.dumps({
                "error": "会话不存在"
            }))
            return
        
        # 设置回调函数
        def on_text_received(text_data):
            """文本识别回调"""
            try:
                message = {
                    "type": "text_received",
                    "data": text_data,
                    "timestamp": datetime.now().isoformat()
                }
                print(f"[{datetime.now()}]RTASR WebSocket发送文本识别结果: {text_data}")
                # 使用asyncio在主线程中发送
                import asyncio
                try:
                    loop = asyncio.get_event_loop()
                    if loop.is_running():
                        asyncio.create_task(websocket.send_text(json.dumps(message)))
                    else:
                        loop.run_until_complete(websocket.send_text(json.dumps(message)))
                except Exception as e:
                    print(f"发送文本识别结果失败: {e}")
            except Exception as e:
                print(f"发送文本识别结果失败: {e}")
        
        def on_sentence_received(sentence):
            """句子识别回调"""
            try:
                message = {
                    "type": "sentence_received",
                    "data": sentence,
                    "timestamp": datetime.now().isoformat()
                }
                print(f"[{datetime.now()}]RTASR WebSocket发送句子识别结果: {sentence}")
                # 使用asyncio在主线程中发送
                import asyncio
                try:
                    loop = asyncio.get_event_loop()
                    if loop.is_running():
                        asyncio.create_task(websocket.send_text(json.dumps(message)))
                    else:
                        loop.run_until_complete(websocket.send_text(json.dumps(message)))
                except Exception as e:
                    print(f"发送句子识别结果失败: {e}")
            except Exception as e:
                print(f"发送句子识别结果失败: {e}")
        
        # 设置回调
        client.on_text_received = on_text_received
        client.on_sentence_received = on_sentence_received
        
        # 发送连接成功消息
        await websocket.send_text(json.dumps({
            "type": "connected",
            "session_id": session_id,
            "timestamp": datetime.now().isoformat()
        }))
        
        # 音频发送线程已经在客户端启动时自动启动，无需手动启动
        print(f"[{datetime.now()}]RTASR WebSocket连接已建立，音频线程已就绪")
        
        # 监听WebSocket消息
        while True:
            try:
                data = await websocket.receive_text()
                message = json.loads(data)
                
                action = message.get("action")
                
                if action == "start_audio":
                    client.start_sending_audio()
                    await websocket.send_text(json.dumps({
                        "type": "audio_started",
                        "timestamp": datetime.now().isoformat()
                    }))
                    
                elif action == "stop_audio":
                    client.stop_sending_audio()
                    await websocket.send_text(json.dumps({
                        "type": "audio_stopped",
                        "timestamp": datetime.now().isoformat()
                    }))
                    
                elif action == "close":
                    client.close()
                    break
                    
            except WebSocketDisconnect:
                print(f"WebSocket连接断开: {session_id}")
                break
            except Exception as e:
                print(f"处理WebSocket消息失败: {e}")
                await websocket.send_text(json.dumps({
                    "type": "error",
                    "error": str(e),
                    "timestamp": datetime.now().isoformat()
                }))
                
    except Exception as e:
        print(f"WebSocket处理异常: {e}")
        try:
            await websocket.send_text(json.dumps({
                "type": "error",
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }))
        except:
            pass
    finally:
        # 清理资源
        try:
            client = rtasr_service.get_client(session_id)
            if client:
                client.close()
                rtasr_service.remove_client(session_id)
        except:
            pass


@router.post("/cleanup")
async def cleanup_expired_sessions():
    """清理过期的会话"""
    try:
        rtasr_service = get_rtasr_service()
        rtasr_service.cleanup_expired_sessions()
        
        return success_response(
            data={"cleaned": True},
            message="过期会话清理完成"
        )
        
    except Exception as e:
        return error_response(f"清理会话失败: {str(e)}")
