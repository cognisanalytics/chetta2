import psycopg2
import pandas as pd
from typing import List, Dict, Any, Optional
from psycopg2.extras import execute_values
from psycopg2 import sql
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
        self.load_progress_log_interval = DatabaseConfig.LOAD_PROGRESS_LOG_INTERVAL
        self.default_schema = DatabaseConfig.POSTGRES_SCHEMA

    def _split_table_name(self, table_name: str) -> tuple[str, str]:
        """Return (schema, table) from table_name or fallback to default schema."""
        cleaned = table_name.strip()
        if "." in cleaned:
            schema, table = cleaned.split(".", 1)
            return schema.strip(), table.strip()
        return self.default_schema, cleaned

    def _qualified_table_identifier(self, table_name: str) -> sql.Composed:
        schema, table = self._split_table_name(table_name)
        return sql.SQL("{}.{}").format(sql.Identifier(schema), sql.Identifier(table))

    def _table_exists(self, table_name: str) -> bool:
        schema, table = self._split_table_name(table_name)
        self.cursor.execute(
            """
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = %s AND table_name = %s
            LIMIT 1
            """,
            (schema, table),
        )
        return self.cursor.fetchone() is not None

    @staticmethod
    def _map_pd_dtype_to_pg(dtype: Any) -> str:
        """Map pandas dtypes to PostgreSQL column types."""
        if pd.api.types.is_bool_dtype(dtype):
            return "BOOLEAN"
        if pd.api.types.is_integer_dtype(dtype):
            return "BIGINT"
        if pd.api.types.is_float_dtype(dtype):
            return "DOUBLE PRECISION"
        if pd.api.types.is_datetime64_any_dtype(dtype):
            return "TIMESTAMP"
        return "TEXT"

    def _ensure_table_exists(self, df: pd.DataFrame, table_name: str) -> None:
        """Create schema/table if missing using inferred column types."""
        schema, _ = self._split_table_name(table_name)
        self.cursor.execute(
            sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema))
        )

        if self._table_exists(table_name):
            return

        if df.empty and len(df.columns) == 0:
            raise ValueError(f"Cannot create table {table_name}: DataFrame has no columns")

        column_defs = []
        for column in df.columns:
            pg_type = self._map_pd_dtype_to_pg(df[column].dtype)
            column_defs.append(
                sql.SQL("{} {}").format(sql.Identifier(str(column)), sql.SQL(pg_type))
            )

        create_query = sql.SQL("CREATE TABLE {} ({})").format(
            self._qualified_table_identifier(table_name),
            sql.SQL(", ").join(column_defs),
        )
        self.cursor.execute(create_query)
        self.logger.info(f"Created missing table {table_name}")

    def _recreate_table(self, df: pd.DataFrame, table_name: str) -> None:
        """
        Recreate destination table from source DataFrame schema.
        This keeps the pipeline simple and avoids conflicts with pre-existing
        dlt metadata columns/constraints.
        """
        schema, _ = self._split_table_name(table_name)
        self.cursor.execute(
            sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema))
        )

        drop_query = sql.SQL("DROP TABLE IF EXISTS {}").format(
            self._qualified_table_identifier(table_name)
        )
        self.cursor.execute(drop_query)

        if len(df.columns) == 0:
            raise ValueError(f"Cannot create table {table_name}: DataFrame has no columns")

        column_defs = []
        for column in df.columns:
            pg_type = self._map_pd_dtype_to_pg(df[column].dtype)
            column_defs.append(
                sql.SQL("{} {}").format(sql.Identifier(str(column)), sql.SQL(pg_type))
            )

        create_query = sql.SQL("CREATE TABLE {} ({})").format(
            self._qualified_table_identifier(table_name),
            sql.SQL(", ").join(column_defs),
        )
        self.cursor.execute(create_query)
        self.logger.info(f"Recreated table {table_name}")

    @staticmethod
    def _clean_dataframe_for_insert(df: pd.DataFrame) -> pd.DataFrame:
        """
        Replace pandas NaN/NaT with Python None for psycopg2 inserts.
        """
        if df.empty:
            return df
        # Convert to object first so None is preserved (avoids datetime columns
        # coercing None back to pandas NaT, which Postgres rejects as "NaT").
        cleaned = df.copy().astype(object)
        cleaned = cleaned.where(pd.notna(cleaned), None)
        # Defensive fallback for object/string values that may still carry NaT text.
        cleaned = cleaned.replace({"NaT": None})
        return cleaned

    def _log_load_progress_if_due(
        self, processed_rows: int, total_rows: int, last_logged_rows: int
    ) -> int:
        """Log batch progress at most every load_progress_log_interval rows (or every batch if interval is 0)."""
        interval = self.load_progress_log_interval
        if interval <= 0 or processed_rows == total_rows:
            self.logger.info("Processed %s/%s rows", processed_rows, total_rows)
            return processed_rows
        if processed_rows - last_logged_rows >= interval:
            self.logger.info("Processed %s/%s rows", processed_rows, total_rows)
            return processed_rows
        return last_logged_rows

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
            # Keep behavior deterministic and simple: always recreate table.
            self._recreate_table(df, table_name)
            
            # Prepare data for insertion
            cleaned_df = self._clean_dataframe_for_insert(df)
            columns = list(cleaned_df.columns)
            values = [tuple(row) for row in cleaned_df.values]
            
            # Build insert query
            quoted_columns = sql.SQL(", ").join([sql.Identifier(str(col)) for col in columns])
            insert_query = sql.SQL("INSERT INTO {} ({}) VALUES %s").format(
                self._qualified_table_identifier(table_name),
                quoted_columns,
            ).as_string(self.connection)
            
            # Execute insert in batches
            total_rows = len(values)
            processed_rows = 0
            last_logged_rows = 0

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
                    last_logged_rows = self._log_load_progress_if_due(
                        processed_rows, total_rows, last_logged_rows
                    )
                    
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
            last_logged_rows = 0

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
                    last_logged_rows = self._log_load_progress_if_due(
                        processed_rows, total_rows, last_logged_rows
                    )
                    
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
