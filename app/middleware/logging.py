import time
from fastapi import Request
from app.core.logger import logger


async def logging_middleware(request: Request, call_next):
    """Log request and response information"""
    start_time = time.time()
    
    # Log request
    logger.info("Request: %s %s", request.method, request.url)
    
    # Process request
    response = await call_next(request)
    
    # Calculate processing time
    process_time = time.time() - start_time
    
    # Log response
    logger.info("Response: %s - %.4fs", response.status_code, process_time)
    
    return response
