from typing import Any, Dict, Optional
from fastapi.responses import JSONResponse
from fastapi import status


def success_response(
    data: Any = None,
    message: str = "Success",
    status_code: int = status.HTTP_200_OK
) -> JSONResponse:
    """Create a standardized success response"""
    response_data = {
        "success": True,
        "message": message,
        "data": data
    }
    return JSONResponse(content=response_data, status_code=status_code)


def error_response(
    message: str = "Error occurred",
    status_code: int = status.HTTP_400_BAD_REQUEST,
    details: Optional[Dict[str, Any]] = None
) -> JSONResponse:
    """Create a standardized error response"""
    response_data = {
        "success": False,
        "message": message,
        "error": {
            "code": status_code,
            "details": details or {}
        }
    }
    return JSONResponse(content=response_data, status_code=status_code)
