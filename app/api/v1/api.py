from fastapi import APIRouter
from app.api.v1.endpoints.aicall import router as aicall_router

api_router = APIRouter()

api_router.include_router(aicall_router, prefix="/aicall", tags=["AI Call"])




