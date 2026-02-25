import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class DatabaseConfig:
    """Database configuration management"""
    
    # SQL Server Configuration
    SQL_SERVER_DRIVER = os.getenv('SQL_SERVER_DRIVER', 'ODBC Driver 17 for SQL Server')
    SQL_SERVER_SERVER = os.getenv('SQL_SERVER_SERVER')
    SQL_SERVER_DATABASE = os.getenv('SQL_SERVER_DATABASE')
    SQL_SERVER_USERNAME = os.getenv('SQL_SERVER_USERNAME')
    SQL_SERVER_PASSWORD = os.getenv('SQL_SERVER_PASSWORD')
    SQL_SERVER_TRUSTED_CONNECTION = os.getenv('SQL_SERVER_TRUSTED_CONNECTION', 'no').lower() == 'yes'
    
    # PostgreSQL Configuration
    POSTGRES_HOST = os.getenv('POSTGRES_HOST')
    POSTGRES_PORT = int(os.getenv('POSTGRES_PORT', 5432))
    POSTGRES_DATABASE = os.getenv('POSTGRES_DATABASE')
    POSTGRES_USERNAME = os.getenv('POSTGRES_USERNAME')
    POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')
    
    # ETL Configuration
    BATCH_SIZE = int(os.getenv('BATCH_SIZE', 1000))
    MAX_RETRIES = int(os.getenv('MAX_RETRIES', 3))
    CHUNK_SIZE = int(os.getenv('CHUNK_SIZE', 10000))  # For large queries
    QUERY_TIMEOUT = int(os.getenv('QUERY_TIMEOUT', 300))  # 5 minutes default
    
    @classmethod
    def get_sql_server_connection_string(cls):
        """Generate SQL Server connection string"""
        timeout = cls.QUERY_TIMEOUT
        if cls.SQL_SERVER_TRUSTED_CONNECTION:
            return f"DRIVER={{{cls.SQL_SERVER_DRIVER}}};SERVER={cls.SQL_SERVER_SERVER};DATABASE={cls.SQL_SERVER_DATABASE};Trusted_Connection=yes;Connection Timeout={timeout};Command Timeout={timeout};"
        else:
            return f"DRIVER={{{cls.SQL_SERVER_DRIVER}}};SERVER={cls.SQL_SERVER_SERVER};DATABASE={cls.SQL_SERVER_DATABASE};UID={cls.SQL_SERVER_USERNAME};PWD={cls.SQL_SERVER_PASSWORD};Connection Timeout={timeout};Command Timeout={timeout};"
    
    @classmethod
    def get_postgres_connection_string(cls):
        """Generate PostgreSQL connection string"""
        return f"host={cls.POSTGRES_HOST} port={cls.POSTGRES_PORT} dbname={cls.POSTGRES_DATABASE} user={cls.POSTGRES_USERNAME} password={cls.POSTGRES_PASSWORD}"