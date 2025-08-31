"""
Database initialization module
"""
from app.core.logger import logger
from app.db.base import Base
from app.db.session import engine


def init_db():
    """Initialize database tables"""
    try:
        # Create all tables
        Base.metadata.create_all(bind=engine)
        logger.info("Database tables created successfully")
    except Exception as e:
        logger.error(f"Failed to create database tables: {e}")
        raise
