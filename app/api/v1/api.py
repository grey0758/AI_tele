from fastapi import APIRouter
# from app.api.v1.endpoints.phone import router as phone_router
# from app.api.v1.endpoints.tts import router as tts_router
# from app.api.v1.endpoints.rtasr import router as rtasr_router
from app.api.v1.endpoints.celery import router as celery_router
from app.api.v1.endpoints.event_monitor import router as event_monitor_router

api_router = APIRouter()

# Include phone control routes
# api_router.include_router(phone_router, prefix="/phone", tags=["Phone"])

# # Include TTS routes
# api_router.include_router(tts_router, prefix="/tts", tags=["TTS"])

# # Include RTASR routes
# api_router.include_router(rtasr_router, prefix="/rtasr", tags=["RTASR"])

# Include Celery management routes
api_router.include_router(celery_router, prefix="/celery", tags=["Celery"])

# Include Event Monitor routes
api_router.include_router(event_monitor_router, prefix="/event-monitor", tags=["Event Monitor"])





