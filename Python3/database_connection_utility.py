"""
Database connection and utility module for accessing SQL Server database.
This module provides reusable database connection functionality for other scripts.
"""

from typing import List, Tuple, Dict, Any, Optional
import pyodbc
from contextlib import contextmanager
import logging
import os

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_database_config() -> Dict[str, str]:
    """Load database configuration from external file or environment variables."""
    try:
        # Try to import from config file first
        from db_config import DATABASE_CONFIG
        return DATABASE_CONFIG
    except ImportError:
        # Fallback to environment variables
        logger.warning("db_config.py not found, using environment variables")
        return {
            'driver': os.getenv('DB_DRIVER', 'ODBC Driver 18 for SQL Server'),
            'server': os.getenv('DB_SERVER', 'localhost'),
            'database': os.getenv('DB_DATABASE', 'EC3Database_Analysis'),
            'uid': os.getenv('DB_UID', 'sa'),
            'pwd': os.getenv('DB_PASSWORD', ''),
            'trust_server_certificate': os.getenv('DB_TRUST_CERT', 'yes')
        }

class DatabaseConfig:
    """Database configuration class."""
    
    def __init__(self, 
                 driver: Optional[str] = None,
                 server: Optional[str] = None,
                 database: Optional[str] = None,
                 uid: Optional[str] = None,
                 pwd: Optional[str] = None,
                 trust_server_certificate: Optional[str] = None):
        
        # Load configuration from external source
        config = load_database_config()
        
        # Use provided parameters or fall back to loaded config
        self.driver = driver or config.get('driver', 'ODBC Driver 18 for SQL Server')
        self.server = server or config.get('server', 'localhost')
        self.database = database or config.get('database', 'EC3Database_Analysis')
        self.uid = uid or config.get('uid', 'sa')
        self.pwd = pwd or config.get('pwd', '')
        self.trust_server_certificate = trust_server_certificate or config.get('trust_server_certificate', 'yes')
    
    def get_connection_string(self) -> str:
        """Generate connection string from configuration."""
        return (
            f'DRIVER={{{self.driver}}};'
            f'SERVER={self.server};'
            f'DATABASE={self.database};'
            f'UID={self.uid};'
            f'PWD={self.pwd};'
            f'TrustServerCertificate={self.trust_server_certificate}'
        )

class DatabaseManager:
    """Database manager class for handling database operations."""
    
    def __init__(self, config: Optional[DatabaseConfig] = None):
        """Initialize with database configuration."""
        self.config = config or DatabaseConfig()
    
    @contextmanager
    def get_connection(self):
        """Context manager for database connections."""
        connection = None
        try:
            connection_string = self.config.get_connection_string()
            connection = pyodbc.connect(connection_string)
            logger.info("Database connection established successfully")
            yield connection
        except pyodbc.Error as e:
            logger.error(f"Database connection error: {e}")
            raise
        finally:
            if connection:
                connection.close()
                logger.info("Database connection closed")
    
    def execute_query(self, query: str, params: Optional[Tuple] = None) -> List[Tuple]:
        """Execute a SELECT query and return results."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            return cursor.fetchall()
    
    def execute_non_query(self, query: str, params: Optional[Tuple] = None) -> int:
        """Execute INSERT, UPDATE, DELETE queries and return the affected rows count."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            conn.commit()
            return cursor.rowcount
    
    def execute_scalar(self, query: str, params: Optional[Tuple] = None) -> Any:
        """Execute query and return single value."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            result = cursor.fetchone()
            return result[0] if result else None
    
    def get_table_info(self, table_name: str, schema_name: str = 'dbo') -> List[Dict[str, Any]]:
        """Get column information for a specific table."""
        query = """
        SELECT 
            COLUMN_NAME,
            DATA_TYPE,
            IS_NULLABLE,
            COLUMN_DEFAULT,
            CHARACTER_MAXIMUM_LENGTH
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
        ORDER BY ORDINAL_POSITION
        """
        
        rows = self.execute_query(query, (schema_name, table_name))
        return [
            {
                'column_name': row[0],
                'data_type': row[1],
                'is_nullable': row[2],
                'column_default': row[3],
                'max_length': row[4]
            }
            for row in rows
        ]
    
    def test_connection(self) -> bool:
        """Test database connection."""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT 1")
                result = cursor.fetchone()
                return result[0] == 1
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False

    def get_non_empty_schemas(self) -> List[str]:
        """Get a list of schemas that contain at least one object (table, view, etc.)."""
        query = """
        SELECT DISTINCT s.name AS SchemaName
        FROM sys.schemas s
        INNER JOIN sys.objects o ON s.schema_id = o.schema_id
        WHERE o.type IN ('U', 'V', 'P', 'FN', 'TF', 'IF', 'TT')
        AND s.name NOT IN ('sys', 'INFORMATION_SCHEMA')
        ORDER BY s.name
        """
        
        try:
            rows = self.execute_query(query)
            return [row[0] for row in rows]
        except Exception as e:
            logger.error(f"Error getting non-empty schemas: {e}")
            return []

# Convenience functions for backward compatibility
def create_db_connection():
    """Legacy function - creates database connection context manager."""
    db_manager = DatabaseManager()
    return db_manager.get_connection()

def get_default_db_manager() -> DatabaseManager:
    """Get default database manager instance."""
    return DatabaseManager()

# Example usage and testing
if __name__ == "__main__":
    # Example usage
    db_manager = DatabaseManager()
    
    # Test connection
    if db_manager.test_connection():
        print("✅ Database connection successful!")
        
        # Fetch non-empty schemas
        try:
            non_empty_schemas = db_manager.get_non_empty_schemas()
            print(f"Non-empty schemas: {non_empty_schemas}")
            
        except Exception as e:
            print(f"❌ Query execution failed: {e}")
    else:
        print("❌ Database connection failed!")
