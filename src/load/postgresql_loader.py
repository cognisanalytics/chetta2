import psycopg2
import pandas as pd
from typing import List, Dict, Any, Optional
from psycopg2.extras import execute_values
from config.database_config import DatabaseConfig
from src.utils.logger import setup_logger

class PostgreSQLLoader:
    """Load data into PostgreSQL database with upsert capabilities"""
    
    def __init__(self):
        self.logger = setup_logger(__name__)
        self.connection_string = DatabaseConfig.get_postgres_connection_string()
        self.connection = None
        self.cursor = None
        self.batch_size = DatabaseConfig.BATCH_SIZE
        self.max_retries = DatabaseConfig.MAX_RETRIES
    
    def connect(self) -> bool:
        """Establish connection to PostgreSQL"""
        try:
            self.connection = psycopg2.connect(self.connection_string)
            self.cursor = self.connection.cursor()
            self.logger.info("Successfully connected to PostgreSQL")
            return True
        except Exception as e:
            self.logger.error(f"Failed to connect to PostgreSQL: {str(e)}")
            return False
    
    def disconnect(self):
        """Close PostgreSQL connection"""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        self.logger.info("Disconnected from PostgreSQL")
    
    def delete_and_insert_data(self, df: pd.DataFrame, table_name: str) -> bool:
        """Delete all data from table and insert new data"""
        try:
            if not self.connection:
                if not self.connect():
                    raise Exception("Could not establish database connection")
            
            self.logger.info(f"Starting delete and insert for table {table_name} with {len(df)} rows")
            
            # Delete all data from table
            delete_query = f"DELETE FROM {table_name}"
            self.cursor.execute(delete_query)
            self.logger.info(f"Deleted all existing data from {table_name}")
            
            # Prepare data for insertion
            columns = list(df.columns)
            values = [tuple(row) for row in df.values]
            
            # Build insert query
            insert_query = f"""
                INSERT INTO {table_name} ({", ".join(columns)})
                VALUES %s
            """
            
            # Execute insert in batches
            total_rows = len(values)
            processed_rows = 0
            
            for i in range(0, total_rows, self.batch_size):
                batch_values = values[i:i + self.batch_size]
                
                try:
                    execute_values(
                        self.cursor,
                        insert_query,
                        batch_values,
                        template=None,
                        page_size=self.batch_size
                    )
                    
                    processed_rows += len(batch_values)
                    self.logger.info(f"Processed {processed_rows}/{total_rows} rows")
                    
                except Exception as e:
                    self.logger.error(f"Error processing batch {i//self.batch_size + 1}: {str(e)}")
                    raise
            
            # Commit transaction
            self.connection.commit()
            self.logger.info(f"Successfully deleted and inserted {total_rows} rows into {table_name}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error during delete and insert: {str(e)}")
            if self.connection:
                self.connection.rollback()
                self.logger.info("Transaction rolled back")
            return False

    def upsert_data(self, df: pd.DataFrame, table_name: str, 
                   conflict_columns: List[str], 
                   update_columns: Optional[List[str]] = None) -> bool:
        """Upsert data into PostgreSQL table"""
        try:
            if not self.connection:
                if not self.connect():
                    raise Exception("Could not establish database connection")
            
            self.logger.info(f"Starting upsert for table {table_name} with {len(df)} rows")
            
            # Prepare data for insertion
            columns = list(df.columns)
            values = [tuple(row) for row in df.values]
            
            # Build upsert query
            if update_columns is None:
                update_columns = [col for col in columns if col not in conflict_columns]
            
            # Create conflict target string
            conflict_target = ", ".join(conflict_columns)
            
            # Create update set string
            update_set = ", ".join([f"{col} = EXCLUDED.{col}" for col in update_columns])
            
            # Build the full upsert query
            insert_query = f"""
                INSERT INTO {table_name} ({", ".join(columns)})
                VALUES %s
                ON CONFLICT ({conflict_target})
                DO UPDATE SET {update_set}
            """
            
            # Execute upsert in batches
            total_rows = len(values)
            processed_rows = 0
            
            for i in range(0, total_rows, self.batch_size):
                batch_values = values[i:i + self.batch_size]
                
                try:
                    execute_values(
                        self.cursor,
                        insert_query,
                        batch_values,
                        template=None,
                        page_size=self.batch_size
                    )
                    
                    processed_rows += len(batch_values)
                    self.logger.info(f"Processed {processed_rows}/{total_rows} rows")
                    
                except Exception as e:
                    self.logger.error(f"Error processing batch {i//self.batch_size + 1}: {str(e)}")
                    raise
            
            # Commit transaction
            self.connection.commit()
            self.logger.info(f"Successfully upserted {total_rows} rows into {table_name}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error during upsert: {str(e)}")
            if self.connection:
                self.connection.rollback()
                self.logger.info("Transaction rolled back")
            return False
    
    def delete_and_insert_batch(self, dataframes: List[pd.DataFrame], 
                               table_names: List[str]) -> bool:
        """Delete all data and insert new data for multiple tables"""
        try:
            if len(dataframes) != len(table_names):
                raise ValueError("Number of DataFrames must match number of table names")
            
            self.logger.info(f"Starting batch delete and insert for {len(dataframes)} tables")
            
            for i, (df, table_name) in enumerate(zip(dataframes, table_names)):
                self.logger.info(f"Processing table {i+1}/{len(dataframes)}: {table_name}")
                
                if not self.delete_and_insert_data(df, table_name):
                    raise Exception(f"Failed to delete and insert data for table {table_name}")
            
            self.logger.info("Batch delete and insert completed successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Error during batch delete and insert: {str(e)}")
            if self.connection:
                self.connection.rollback()
                self.logger.info("Transaction rolled back")
            return False

    def upsert_batch(self, dataframes: List[pd.DataFrame], 
                    table_names: List[str],
                    conflict_columns_list: List[List[str]],
                    update_columns_list: Optional[List[List[str]]] = None) -> bool:
        """Upsert multiple DataFrames to multiple tables"""
        try:
            if len(dataframes) != len(table_names):
                raise ValueError("Number of DataFrames must match number of table names")
            
            if len(dataframes) != len(conflict_columns_list):
                raise ValueError("Number of DataFrames must match number of conflict columns lists")
            
            self.logger.info(f"Starting batch upsert for {len(dataframes)} tables")
            
            for i, (df, table_name, conflict_columns) in enumerate(zip(dataframes, table_names, conflict_columns_list)):
                update_columns = update_columns_list[i] if update_columns_list else None
                
                self.logger.info(f"Processing table {i+1}/{len(dataframes)}: {table_name}")
                
                if not self.upsert_data(df, table_name, conflict_columns, update_columns):
                    raise Exception(f"Failed to upsert data for table {table_name}")
            
            self.logger.info("Batch upsert completed successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Error during batch upsert: {str(e)}")
            if self.connection:
                self.connection.rollback()
                self.logger.info("Transaction rolled back")
            return False
    
    def __enter__(self):
        """Context manager entry"""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.disconnect()
