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
    SQL_SERVER_ENCRYPT = os.getenv('SQL_SERVER_ENCRYPT', 'yes').lower()
    SQL_SERVER_TRUST_SERVER_CERTIFICATE = os.getenv(
        'SQL_SERVER_TRUST_SERVER_CERTIFICATE',
        'yes'
    ).lower() == 'yes'
    
    # PostgreSQL Configuration
    POSTGRES_HOST = os.getenv('POSTGRES_HOST')
    POSTGRES_PORT = int(os.getenv('POSTGRES_PORT', 5432))
    POSTGRES_DATABASE = os.getenv('POSTGRES_DATABASE')
    POSTGRES_USERNAME = os.getenv('POSTGRES_USERNAME')
    POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')
    POSTGRES_SCHEMA = os.getenv('POSTGRES_SCHEMA', 'public')
    
    # ETL Configuration
    BATCH_SIZE = int(os.getenv('BATCH_SIZE', 1000))
    MAX_RETRIES = int(os.getenv('MAX_RETRIES', 3))
    # Log insert/upsert progress every N rows (0 = log after every batch).
    LOAD_PROGRESS_LOG_INTERVAL = int(os.getenv('LOAD_PROGRESS_LOG_INTERVAL', '10000'))
    CHUNK_SIZE = int(os.getenv('CHUNK_SIZE', 10000))  # For large queries
    QUERY_TIMEOUT = int(os.getenv('QUERY_TIMEOUT', 300))  # 5 minutes default
    
    @classmethod
    def get_sql_server_connection_string(cls):
        """Generate SQL Server connection string"""
        timeout = cls.QUERY_TIMEOUT
        connection_parts = [
            f"DRIVER={{{cls.SQL_SERVER_DRIVER}}}",
            f"SERVER={cls.SQL_SERVER_SERVER}",
            f"DATABASE={cls.SQL_SERVER_DATABASE}",
            f"Encrypt={cls.SQL_SERVER_ENCRYPT}",
            f"Connection Timeout={timeout}",
        ]

        if cls.SQL_SERVER_TRUST_SERVER_CERTIFICATE:
            connection_parts.append("TrustServerCertificate=yes")

        if cls.SQL_SERVER_TRUSTED_CONNECTION:
            connection_parts.append("Trusted_Connection=yes")
        else:
            connection_parts.append(f"UID={cls.SQL_SERVER_USERNAME}")
            connection_parts.append(f"PWD={cls.SQL_SERVER_PASSWORD}")

        return ";".join(connection_parts) + ";"
    
    @classmethod
    def get_postgres_connection_string(cls):
        """Generate PostgreSQL connection string"""
        return f"host={cls.POSTGRES_HOST} port={cls.POSTGRES_PORT} dbname={cls.POSTGRES_DATABASE} user={cls.POSTGRES_USERNAME} password={cls.POSTGRES_PASSWORD}"