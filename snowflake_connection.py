#!/usr/bin/env python3
"""
Centralized Snowflake connection manager using key pair authentication.
Provides singleton connection handling and query utilities.
"""

import snowflake.connector
import pandas as pd
from dotenv import load_dotenv
import os
from cryptography.hazmat.primitives import serialization
from functools import lru_cache
import logging
from contextlib import contextmanager
from typing import Optional, Dict, Any

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SnowflakeConnectionManager:
    """Singleton manager for Snowflake connections with connection pooling."""

    _instance = None
    _connection = None
    _config = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return

        load_dotenv()
        self._initialized = True
        self._setup_config()

    def _setup_config(self):
        """Load configuration once at initialization."""
        private_key_path = os.getenv('SNOWFLAKE_PRIVATE_KEY_PATH')

        if not private_key_path:
            raise ValueError("SNOWFLAKE_PRIVATE_KEY_PATH not found in .env file")

        # Load private key
        with open(private_key_path, "rb") as key_file:
            p_key = serialization.load_pem_private_key(
                key_file.read(),
                password=os.getenv('SNOWFLAKE_PRIVATE_KEY_PASSWORD', '').encode() if os.getenv(
                    'SNOWFLAKE_PRIVATE_KEY_PASSWORD') else None,
            )

        private_key_bytes = p_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        )

        # Store configuration
        self._config = {
            'account': os.getenv('SNOWFLAKE_ACCOUNT', 'JZJIKIA-GDA24737'),
            'user': os.getenv('SNOWFLAKE_USER', 'travis@twinbrain.ai'),
            'private_key': private_key_bytes,
            'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH'),
            'database': os.getenv('SNOWFLAKE_DATABASE', 'SIL__TB_OTT_TEST'),
            'schema': os.getenv('SNOWFLAKE_SCHEMA', 'SC_TWINBRAINAI'),
            'role': os.getenv('SNOWFLAKE_ROLE'),  # Optional
            'session_parameters': {
                'QUERY_TAG': 'sports_insights_automation',
                'USE_CACHED_RESULT': True
            }
        }

        # Remove None values
        self._config = {k: v for k, v in self._config.items() if v is not None}

    @property
    def connection(self):
        """Get or create a connection."""
        if self._connection is None or self._connection.is_closed():
            logger.info("Creating new Snowflake connection...")
            self._connection = snowflake.connector.connect(**self._config)
            logger.info("Connected to Snowflake successfully")
        return self._connection

    def close(self):
        """Close the connection if it exists."""
        if self._connection and not self._connection.is_closed():
            self._connection.close()
            logger.info("Closed Snowflake connection")
            self._connection = None

    @contextmanager
    def get_connection(self):
        """Context manager for connection handling."""
        try:
            yield self.connection
        except Exception as e:
            logger.error(f"Connection error: {e}")
            raise
        finally:
            # Don't close - keep connection alive for reuse
            pass

    def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
        """Execute a query and return results as DataFrame."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            try:
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)

                # Fetch column names
                columns = [desc[0] for desc in cursor.description]

                # Fetch data
                data = cursor.fetchall()

                # Create DataFrame
                df = pd.DataFrame(data, columns=columns)
                logger.info(f"Query executed successfully, returned {len(df)} rows")
                return df

            finally:
                cursor.close()

    def execute_many(self, query: str, data: list) -> None:
        """Execute a query with multiple parameter sets."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.executemany(query, data)
                conn.commit()
                logger.info(f"Executed batch query with {len(data)} parameter sets")
            finally:
                cursor.close()


# Convenience functions for backward compatibility
_manager = None


def get_connection():
    """Get a Snowflake connection using the singleton manager."""
    global _manager
    if _manager is None:
        _manager = SnowflakeConnectionManager()
    return _manager.connection


def query_to_dataframe(query: str, params: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
    """Execute a query and return results as DataFrame."""
    global _manager
    if _manager is None:
        _manager = SnowflakeConnectionManager()
    return _manager.execute_query(query, params)


def close_connection():
    """Close the global connection."""
    global _manager
    if _manager:
        _manager.close()


# Decorator for automatic connection handling
def with_snowflake_connection(func):
    """Decorator that provides a connection to the decorated function."""

    def wrapper(*args, **kwargs):
        manager = SnowflakeConnectionManager()
        # Inject connection as first argument if function expects it
        import inspect
        sig = inspect.signature(func)
        if 'connection' in sig.parameters or 'conn' in sig.parameters:
            return func(manager.connection, *args, **kwargs)
        else:
            return func(*args, **kwargs)

    return wrapper