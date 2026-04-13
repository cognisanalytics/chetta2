#!/usr/bin/env python3
"""
ETL Pipeline: SQL Server to PostgreSQL
Extract data from SQL Server, transform (pass-through), and load into PostgreSQL
"""

import sys
import os
import uuid
import argparse
import psycopg2
from psycopg2 import sql
from typing import List, Dict, Any, Tuple

# Add src to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from src.extract.sql_server_extractor import SQLServerExtractor
from src.transform.data_transformer import DataTransformer
from src.load.postgresql_loader import PostgreSQLLoader
from src.utils.logger import setup_logger
from config.database_config import DatabaseConfig

DEFAULT_SOURCE_DATABASE = "Chetta"
DEFAULT_SOURCE_SCHEMA = "dbo"
SOURCE_TABLES = [
    "Chetta.dbo.consArticulos",
    "Chetta.dbo.consAutSalidas",
    "Chetta.dbo.consClientes",
    "Chetta.dbo.consCotizacion",
    "Chetta.dbo.consEmpleados",
    "Chetta.dbo.consFeriados",
    "Chetta.dbo.consGrados",
    "Chetta.dbo.consHistClinicas",
    "Chetta.dbo.consItemsFac",
    "Chetta.dbo.consLegajos",
    "Chetta.dbo.consMarcas",
    "Chetta.dbo.consModelos",
    "Chetta.dbo.consOrdTrabajo",
    "Chetta.dbo.consPtoVenta",
    "Chetta.dbo.consTrabajos",
    "Chetta.dbo.consTrafico",
    "Chetta.dbo.consTurnos",
    "Chetta.dbo.consVenDetArt",
    "Chetta.dbo.consVenDetItm",
    "Chetta.dbo.consVenKit",
    "Chetta.dbo.consVentas",
]


def parse_source_table_identifier(table_identifier: str) -> Tuple[str, str, str]:
    """
    Parse SQL Server table identifier into database, schema, table.
    Supports:
    - database.schema.table (preferred)
    - schema.table
    - table
    """
    parts = [p.strip() for p in table_identifier.split(".") if p.strip()]
    if len(parts) == 3:
        return parts[0], parts[1], parts[2]
    if len(parts) == 2:
        return DEFAULT_SOURCE_DATABASE, parts[0], parts[1]
    if len(parts) == 1:
        return DEFAULT_SOURCE_DATABASE, DEFAULT_SOURCE_SCHEMA, parts[0]
    raise ValueError(f"Invalid source table identifier: {table_identifier}")


def build_select_query(table_identifier: str) -> str:
    """Build safe SQL Server SELECT * query with bracketed identifiers."""
    database, schema, table = parse_source_table_identifier(table_identifier)
    return f"SELECT * FROM [{database}].[{schema}].[{table}]"


def build_target_table_name(table_identifier: str) -> str:
    """Use source table name as lowercase PostgreSQL target table name."""
    _, _, table = parse_source_table_identifier(table_identifier)
    return table.lower()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Extract Chetta SQL Server tables and load into PostgreSQL"
    )
    parser.add_argument(
        "--extract-only",
        action="store_true",
        help="Only test extraction from SQL Server, no PostgreSQL load",
    )
    parser.add_argument(
        "--tables",
        nargs="+",
        help=(
            "Optional list of tables to process (accepts full name like "
            "Chetta.dbo.consVentas or short name like consVentas)"
        ),
    )
    return parser.parse_args()


class ETLPipeline:
    """Main ETL Pipeline orchestrator"""
    
    def __init__(self):
        self.logger = setup_logger(__name__)
        self.extractor = SQLServerExtractor()
        self.transformer = DataTransformer()
        self.loader = PostgreSQLLoader()

    def _log_dlt_load(self, load_id: str, status: int, schema_name: str = None) -> None:
        """Write ETL run status into _dlt_loads using an isolated connection."""
        connection = None
        try:
            if schema_name is None:
                schema_name = DatabaseConfig.POSTGRES_SCHEMA

            connection = psycopg2.connect(DatabaseConfig.get_postgres_connection_string())
            connection.autocommit = True

            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT 1
                    FROM information_schema.tables
                    WHERE table_schema = %s
                      AND table_name = '_dlt_loads'
                    LIMIT 1
                    """,
                    (schema_name,),
                )
                if cursor.fetchone() is None:
                    self.logger.info(
                        "Skipping _dlt_loads logging because %s._dlt_loads does not exist",
                        schema_name,
                    )
                    return

                cursor.execute(
                    sql.SQL(
                        "INSERT INTO {}._dlt_loads (load_id, schema_name, status, inserted_at) "
                        "VALUES (%s, %s, %s, NOW())"
                    ).format(sql.Identifier(schema_name)),
                    (load_id, schema_name, status),
                )
            self.logger.info(f"Logged ETL run in _dlt_loads with load_id={load_id}, status={status}")
        except Exception as e:
            self.logger.error(f"Failed to log ETL run in _dlt_loads: {str(e)}")
        finally:
            if connection:
                connection.close()
    
    def test_extract(self, queries: List[str]) -> bool:
        """
        Test SQL Server extraction only
        
        Args:
            queries: List of SQL SELECT queries to execute
        
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            self.logger.info("Starting SQL Server Extract Test")
            
            # Stage 1: Extract
            self.logger.info("=== STAGE 1: EXTRACT ===")
            with self.extractor as extractor:
                extracted_data = extractor.execute_batch_queries(queries)
            
            # Log results
            for i, df in enumerate(extracted_data):
                self.logger.info(f"Query {i+1} returned {len(df)} rows")
                self.logger.info(f"Columns ({len(df.columns)}): {list(df.columns)}")
                
                # Print column details
                self.logger.info("=" * 50)
                self.logger.info(f"TABLE {i+1} - COLUMN DETAILS:")
                self.logger.info("=" * 50)
                for j, col in enumerate(df.columns, 1):
                    self.logger.info(f"{j:2d}. {col}")
                self.logger.info("=" * 50)
            
            self.logger.info("SQL Server Extract Test completed successfully!")
            return True
                
        except Exception as e:
            self.logger.error(f"SQL Server Extract Test failed: {str(e)}")
            return False
    
    def run_etl(self, queries: List[str], 
                table_names: List[str],
                conflict_columns_list: List[List[str]],
                update_columns_list: List[List[str]] = None) -> bool:
        """
        Run complete ETL pipeline
        
        Args:
            queries: List of SQL SELECT queries to execute
            table_names: List of PostgreSQL table names for each query result
            conflict_columns_list: List of conflict columns for upsert for each table
            update_columns_list: Optional list of update columns for upsert for each table
        
        Returns:
            bool: True if successful, False otherwise
        """
        load_id = str(uuid.uuid4())
        try:
            self.logger.info("Starting ETL Pipeline")
            
            # Validate inputs
            if len(queries) != len(table_names):
                raise ValueError("Number of queries must match number of table names")
            
            if len(queries) != len(conflict_columns_list):
                raise ValueError("Number of queries must match number of conflict columns lists")
            
            # Stage 1: Extract
            self.logger.info("=== STAGE 1: EXTRACT ===")
            with self.extractor as extractor:
                extracted_data = extractor.execute_batch_queries(queries)
            
            # Stage 2: Transform
            self.logger.info("=== STAGE 2: TRANSFORM ===")
            transformed_data = self.transformer.transform_batch(extracted_data)
            
            # Stage 3: Load
            self.logger.info("=== STAGE 3: LOAD ===")
            with self.loader as loader:
                success = loader.delete_and_insert_batch(
                    transformed_data, 
                    table_names
                )
            
            if success:
                self._log_dlt_load(load_id, status=0)
                self.logger.info("ETL Pipeline completed successfully!")
                return True
            else:
                self._log_dlt_load(load_id, status=1)
                self.logger.error("ETL Pipeline failed during load stage")
                return False
                
        except Exception as e:
            self._log_dlt_load(load_id, status=1)
            self.logger.error(f"ETL Pipeline failed: {str(e)}")
            return False
    
    def run_etl_with_mapping(self, queries: List[str], 
                            table_names: List[str],
                            conflict_columns_list: List[List[str]],
                            column_mappings: List[Dict[str, str]],
                            update_columns_list: List[List[str]] = None) -> bool:
        """
        Run complete ETL pipeline with column mapping
        
        Args:
            queries: List of SQL SELECT queries to execute
            table_names: List of PostgreSQL table names for each query result
            conflict_columns_list: List of conflict columns for upsert for each table
            column_mappings: List of column mappings from SQL Server to PostgreSQL
            update_columns_list: Optional list of update columns for upsert for each table
        
        Returns:
            bool: True if successful, False otherwise
        """
        load_id = str(uuid.uuid4())
        try:
            self.logger.info("Starting ETL Pipeline with Column Mapping")
            
            # Validate inputs
            if len(queries) != len(table_names):
                raise ValueError("Number of queries must match number of table names")
            
            if len(queries) != len(conflict_columns_list):
                raise ValueError("Number of queries must match number of conflict columns lists")
            
            if len(queries) != len(column_mappings):
                raise ValueError("Number of queries must match number of column mappings")
            
            # Simple and memory-safe mode: process one table at a time.
            success = True
            with self.extractor as extractor, self.loader as loader:
                for i, (query, table_name) in enumerate(zip(queries, table_names), start=1):
                    self.logger.info("=== TABLE %s/%s: %s ===", i, len(table_names), table_name)
                    self.logger.info("=== STAGE 1: EXTRACT ===")
                    extracted_df = extractor.execute_query(query)

                    self.logger.info("=== STAGE 2: TRANSFORM ===")
                    mapping = column_mappings[i - 1] if i - 1 < len(column_mappings) else {}
                    transformed_df = self.transformer.transform_data(extracted_df, mapping)

                    self.logger.info("=== STAGE 3: LOAD ===")
                    if not loader.delete_and_insert_data(transformed_df, table_name):
                        success = False
                        break
            
            if success:
                self._log_dlt_load(load_id, status=0)
                self.logger.info("ETL Pipeline with Column Mapping completed successfully!")
                return True
            else:
                self._log_dlt_load(load_id, status=1)
                self.logger.error("ETL Pipeline failed during load stage")
                return False
                
        except Exception as e:
            self._log_dlt_load(load_id, status=1)
            self.logger.error(f"ETL Pipeline failed: {str(e)}")
            return False

def main():
    """Main function for Chetta table extraction and load."""
    logger = setup_logger("main")
    args = parse_args()

    selected_source_tables = SOURCE_TABLES
    if args.tables:
        requested = {table.lower() for table in args.tables}
        selected_source_tables = [
            table for table in SOURCE_TABLES
            if table.lower() in requested or table.split(".")[-1].lower() in requested
        ]
        missing = sorted(
            requested - {t.lower() for t in selected_source_tables} - {t.split(".")[-1].lower() for t in selected_source_tables}
        )
        if missing:
            logger.warning("Requested tables not found in SOURCE_TABLES: %s", ", ".join(missing))

    if not selected_source_tables:
        logger.error("No source tables selected. Exiting.")
        sys.exit(1)

    queries = [build_select_query(table_name) for table_name in selected_source_tables]
    table_names = [build_target_table_name(table_name) for table_name in selected_source_tables]
    conflict_columns_list = [[] for _ in selected_source_tables]
    column_mappings = [{} for _ in selected_source_tables]
    update_columns_list = [[] for _ in selected_source_tables]

    logger.info("Selected %s Chetta table(s): %s", len(selected_source_tables), ", ".join(selected_source_tables))
    logger.info("Target PostgreSQL tables: %s", ", ".join(table_names))

    etl = ETLPipeline()
    if args.extract_only:
        success = etl.test_extract(queries)
    else:
        success = etl.run_etl_with_mapping(
            queries,
            table_names,
            conflict_columns_list,
            column_mappings,
            update_columns_list,
        )
    
    if success:
        logger.info("Process completed successfully!")
        sys.exit(0)
    else:
        logger.error("Process failed!")
        sys.exit(1)

if __name__ == "__main__":
    main()
