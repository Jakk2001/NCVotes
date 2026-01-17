"""
Centralized database connection management.
Provides a single source of truth for database connections.
"""
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
import logging
from config.settings import get_db_url

logger = logging.getLogger(__name__)

_engine = None

def get_engine() -> Engine: 
    """
    Get or create a singleton SQLAlchemy engine.
    
    Returns:
        Engine: SQLAlchemy database engine
    """
    global _engine
    if _engine is None:
        try:
            _engine = create_engine(
                get_db_url(),
                pool_pre_ping=True,  # Verify connections before using
                echo=False,
            )
            logger.info("Database engine created successfully")
        except SQLAlchemyError as e:
            logger.error(f"Failed to create database engine: {e}")
            raise
    return _engine

def test_connection() -> bool:
    """
    Test database connection.
    
    Returns:
        bool: True if connection successful, False otherwise
    """
    try:
        engine = get_engine()
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.info("Database connection test successful")
        return True
    except SQLAlchemyError as e:
        logger.error(f"Database connection test failed: {e}")
        return False

def close_engine():
    """Close the database engine and dispose of connections."""
    global _engine
    if _engine is not None:
        _engine.dispose()
        _engine = None
        logger.info("Database engine closed")