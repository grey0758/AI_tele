"""API路由"""
from fastapi import APIRouter
from app.api.v1.endpoints.aicall import router as aicall_router
from app.api.v1.endpoints.time_limit import router as time_limit_router

api_router = APIRouter()

api_router.include_router(aicall_router, prefix="/aicall", tags=["AI Call"])
api_router.include_router(time_limit_router, prefix="/time-limit", tags=["Time Limit"])
