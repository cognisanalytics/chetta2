import warnings

import pyodbc
import pandas as pd
from typing import List, Dict, Any, Optional
from config.database_config import DatabaseConfig
from src.utils.logger import setup_logger

class SQLServerExtractor:
    """Extract data from SQL Server database"""
    
    def __init__(self):
        self.logger = setup_logger(__name__)
        self.connection_string = DatabaseConfig.get_sql_server_connection_string()
        self.connection = None
    
    def connect(self) -> bool:
        """Establish connection to SQL Server"""
        try:
            self.connection = pyodbc.connect(self.connection_string)
            # Apply per-statement timeout at connection level.
            self.connection.timeout = DatabaseConfig.QUERY_TIMEOUT
            self.logger.info("Successfully connected to SQL Server")
            return True
        except Exception as e:
            self.logger.error(f"Failed to connect to SQL Server: {str(e)}")
            return False
    
    def disconnect(self):
        """Close SQL Server connection"""
        if self.connection:
            self.connection.close()
            self.logger.info("Disconnected from SQL Server")
    
    def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
        """Execute SELECT query and return results as DataFrame"""
        try:
            if not self.connection:
                if not self.connect():
                    raise Exception("Could not establish database connection")
            
            self.logger.info(f"Executing query: {query[:100]}...")
            
            # pandas warns on non-SQLAlchemy DBAPI connections; pyodbc is fine here.
            with warnings.catch_warnings():
                warnings.filterwarnings(
                    "ignore",
                    message="pandas only supports SQLAlchemy connectable",
                    category=UserWarning,
                )
                if params:
                    df = pd.read_sql(query, self.connection, params=params)
                else:
                    df = pd.read_sql(query, self.connection)
            
            self.logger.info(f"Query executed successfully. Retrieved {len(df)} rows")
            return df
            
        except Exception as e:
            self.logger.error(f"Error executing query: {str(e)}")
            raise
    
    def execute_batch_queries(self, queries: List[str]) -> List[pd.DataFrame]:
        """Execute multiple queries and return list of DataFrames"""
        results = []
        
        for i, query in enumerate(queries):
            try:
                self.logger.info(f"Executing query {i+1}/{len(queries)}")
                df = self.execute_query(query)
                results.append(df)
            except Exception as e:
                self.logger.error(f"Error executing query {i+1}: {str(e)}")
                raise
        
        return results
    
    def __enter__(self):
        """Context manager entry"""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.disconnect()
