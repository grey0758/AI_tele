# app/core/dependencies.py
from typing import Generator
from fastapi import Depends
from app.core.container import container
from app.services.phone_service import PhoneService


def get_phone_service() -> PhoneService:
    """获取 PhoneService 实例的依赖"""
    return container.phone_service()


def get_redis_client():
    """获取 Redis 客户端的依赖"""
    return container.redis_client()


def get_celery_app():
    """获取 Celery 应用的依赖"""
    return container.celery_app()
