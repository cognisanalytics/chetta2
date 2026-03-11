#!/usr/bin/env python3
"""
ETL Pipeline: SQL Server to PostgreSQL
Extract data from SQL Server, transform (pass-through), and load into PostgreSQL
"""

import sys
import os
import uuid
import psycopg2
from typing import List, Dict, Any

# Add src to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from src.extract.sql_server_extractor import SQLServerExtractor
from src.transform.data_transformer import DataTransformer
from src.load.postgresql_loader import PostgreSQLLoader
from src.utils.logger import setup_logger
from config.database_config import DatabaseConfig

class ETLPipeline:
    """Main ETL Pipeline orchestrator"""
    
    def __init__(self):
        self.logger = setup_logger(__name__)
        self.extractor = SQLServerExtractor()
        self.transformer = DataTransformer()
        self.loader = PostgreSQLLoader()

    def _log_dlt_load(self, load_id: str, status: int, schema_name: str = "oxadatabbase") -> None:
        """Write ETL run status into _dlt_loads using an isolated connection."""
        connection = None
        try:
            connection = psycopg2.connect(DatabaseConfig.get_postgres_connection_string())
            connection.autocommit = True

            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO _dlt_loads (load_id, schema_name, status, inserted_at)
                    VALUES (%s, %s, %s, NOW())
                    """,
                    (load_id, schema_name, status)
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
            
            # Stage 1: Extract
            self.logger.info("=== STAGE 1: EXTRACT ===")
            with self.extractor as extractor:
                extracted_data = extractor.execute_batch_queries(queries)
            
            # Stage 2: Transform with column mapping and boolean conversion
            self.logger.info("=== STAGE 2: TRANSFORM ===")
            transformed_data = self.transformer.transform_batch(extracted_data, column_mappings)
            
            # Stage 3: Load
            self.logger.info("=== STAGE 3: LOAD ===")
            with self.loader as loader:
                success = loader.delete_and_insert_batch(
                    transformed_data, 
                    table_names
                )
            
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
    """Main function with example usage - SQL Server Extract Test Only"""
    logger = setup_logger("main")
    
    # Example configuration - replace with your actual queries
    queries = [
        "SELECT COD_VENDED, NOMBRE_VEN, INHABILITA FROM GVA23", # Codigo vendedor, nombre vendedor, activo
        "SELECT COD_CLIENT, RAZON_SOCI, HABILITADO, LOCALIDAD FROM GVA14", # Codigo cliente, Nombre, Activo, Departamento
        "SELECT COD_ARTICU, DESCRIPCIO, CA_967_ESTADO, CA_967_FAMILIA, CA_967_LINEA FROM STA11", # Codigo producto, Nombre producto, Activo, Familia producto, Categoría producto
        """SELECT 
	GVA12.FECHA_EMIS AS [FECHA_EMISION] ,
	GVA53.T_COMP AS [TIPO_COMPROBANTE] ,
	GVA53.N_COMP AS [NRO_COMPROBANTE] ,
	GVA12.COD_VENDED AS [COD_VENDEDOR] ,
	CASE GVA12.COD_VENDED WHEN '**' THEN 'CARGA INICIAL' ELSE GVA23.NOMBRE_VEN END AS [NOMBRE_VENDEDOR] ,
	COALESCE(CLI2.COD_CLIENT,CLI3.COD_CLIENT, GVA12.COD_CLIENT) AS [COD_CLIENTE] ,
	COALESCE(CLI2.RAZON_SOCI,CLI3.RAZON_SOCI, GVA14.RAZON_SOCI) AS [RAZON_SOCIAL] ,
	GVA14.LOCALIDAD AS [LOCALIDAD] ,
	GVA53.COD_ARTICU AS [COD_ARTICULO] ,
	STA11.DESCRIPCIO AS [DESCRIPCION] ,
	SUM(CASE WHEN GVA12.T_COMP <> 'FAC' AND GVA15.TIPO_COMP = 'C' THEN (-1) ELSE (1) END * GVA53.CANTIDAD) AS [CANTIDAD] ,
	GVA53.PRECIO_PAN AS [PRECIO_UNITARIO] ,
	SUM( CASE WHEN GVA12.T_COMP <> 'FAC' AND GVA15.TIPO_COMP = 'C' THEN (-1) ELSE (1) END * CASE 'BIMONCTE' WHEN 'BIMONCTE' THEN (CASE GVA12.MON_CTE WHEN 1 THEN CASE 'NO' WHEN 'NO' THEN (GVA53.PRECIO_NET * GVA53.CANTIDAD) ELSE (GVA53.PRECIO_NET * GVA53.CANTIDAD) - ((GVA53.PRECIO_NET * GVA53.CANTIDAD) * GVA12.PORC_BONIF/100) + ((GVA53.PRECIO_NET * GVA53.CANTIDAD) * GVA12.PORC_FLE/100) + ((GVA53.PRECIO_NET * GVA53.CANTIDAD) * GVA12.PORC_REC/100) END 			 ELSE CASE 'NO' WHEN 'NO' THEN (GVA53.PRECIO_NET * GVA53.CANTIDAD) * GVA12.COTIZ ELSE ((GVA53.PRECIO_NET * GVA53.CANTIDAD) - ((GVA53.PRECIO_NET * GVA53.CANTIDAD) * GVA12.PORC_BONIF/100) + ((GVA53.PRECIO_NET * GVA53.CANTIDAD) * GVA12.PORC_FLE/100) + ((GVA53.PRECIO_NET * GVA53.CANTIDAD) * GVA12.PORC_REC/100)) * GVA12.COTIZ END END) WHEN 'BIORIGEN' THEN (CASE GVA12.MON_CTE WHEN 1 THEN CASE GVA12.COTIZ WHEN 0 THEN 0 ELSE CASE 'NO' WHEN 'NO' THEN (GVA53.PRECIO_NET * GVA53.CANTIDAD) / GVA12.COTIZ ELSE ((GVA53.PRECIO_NET * GVA53.CANTIDAD) - ((GVA53.PRECIO_NET * GVA53.CANTIDAD) * GVA12.PORC_BONIF/100) + ((GVA53.PRECIO_NET * GVA53.CANTIDAD) * GVA12.PORC_FLE/100) + ((GVA53.PRECIO_NET * GVA53.CANTIDAD) * GVA12.PORC_REC/100)) / GVA12.COTIZ END 				 END 				 ELSE CASE 'NO' WHEN 'NO' THEN (GVA53.PRECIO_NET * GVA53.CANTIDAD) ELSE (GVA53.PRECIO_NET * GVA53.CANTIDAD) - ((GVA53.PRECIO_NET * GVA53.CANTIDAD) * GVA12.PORC_BONIF/100) + ((GVA53.PRECIO_NET * GVA53.CANTIDAD) * GVA12.PORC_FLE/100) + ((GVA53.PRECIO_NET * GVA53.CANTIDAD)* GVA12.PORC_REC/100) END END) WHEN 'BICOTIZ' THEN (CASE GVA12.MON_CTE WHEN 1 THEN CASE 'NO' WHEN 'NO' THEN (GVA53.PRECIO_NET * GVA53.CANTIDAD) / 1 ELSE ((GVA53.PRECIO_NET * GVA53.CANTIDAD) - ((GVA53.PRECIO_NET * GVA53.CANTIDAD) * GVA12.PORC_BONIF/100) + ((GVA53.PRECIO_NET * GVA53.CANTIDAD) * GVA12.PORC_FLE/100) + ((GVA53.PRECIO_NET * GVA53.CANTIDAD) * GVA12.PORC_REC/100)) / 1 END ELSE CASE 'NO' WHEN 'NO' THEN (GVA53.PRECIO_NET * GVA53.CANTIDAD) * GVA12.COTIZ / 1 ELSE ((GVA53.PRECIO_NET * GVA53.CANTIDAD) - ((GVA53.PRECIO_NET * GVA53.CANTIDAD) * GVA12.PORC_BONIF/100) + ((GVA53.PRECIO_NET * GVA53.CANTIDAD) * GVA12.PORC_FLE/100) + ((GVA53.PRECIO_NET * GVA53.CANTIDAD) * GVA12.PORC_REC/100)) * GVA12.COTIZ / 1 END END) END ) AS [TOTAL] ,
	gva53.PORC_DTO AS [DESC_LINEA],
	gva12.PORC_BONIF as [DESC_ENCABEZ],
	ISNULL(CLASIF_CLI.IDFOLDER, '') AS [ID_FOLDER_CLIENTES] ,
	ISNULL(CLASIF_CLI.PADRE0, '') AS [CLI_CARPETA_NIVEL_1] ,
	(CASE WHEN CHARINDEX('<CA_967_FAMILIA>', CAST(STA11.CAMPOS_ADICIONALES AS NVARCHAR(MAX))) = 0 THEN '' ELSE (SUBSTRING( CAST(STA11.CAMPOS_ADICIONALES AS NVARCHAR(MAX)),CHARINDEX('<CA_967_FAMILIA>', CAST(STA11.CAMPOS_ADICIONALES AS NVARCHAR(MAX))) + LEN('<CA_967_FAMILIA>'),CHARINDEX('</CA_967_FAMILIA>', CAST(STA11.CAMPOS_ADICIONALES AS NVARCHAR(MAX))) - (CHARINDEX('<CA_967_FAMILIA>', CAST(STA11.CAMPOS_ADICIONALES AS NVARCHAR(MAX))) + LEN('<CA_967_FAMILIA>')))) END) AS [FAMILIA_ADIC] ,
	(CASE WHEN CHARINDEX('<CA_967_ESTADO>', CAST(STA11.CAMPOS_ADICIONALES AS NVARCHAR(MAX))) = 0 THEN '' ELSE (SUBSTRING( CAST(STA11.CAMPOS_ADICIONALES AS NVARCHAR(MAX)),CHARINDEX('<CA_967_ESTADO>', CAST(STA11.CAMPOS_ADICIONALES AS NVARCHAR(MAX))) + LEN('<CA_967_ESTADO>'),CHARINDEX('</CA_967_ESTADO>', CAST(STA11.CAMPOS_ADICIONALES AS NVARCHAR(MAX))) - (CHARINDEX('<CA_967_ESTADO>', CAST(STA11.CAMPOS_ADICIONALES AS NVARCHAR(MAX))) + LEN('<CA_967_ESTADO>')))) END) AS [ESTADO_ADIC] ,
	(CASE WHEN CHARINDEX('<CA_967_LINEA>', CAST(STA11.CAMPOS_ADICIONALES AS NVARCHAR(MAX))) = 0 THEN '' ELSE (SUBSTRING( CAST(STA11.CAMPOS_ADICIONALES AS NVARCHAR(MAX)),CHARINDEX('<CA_967_LINEA>', CAST(STA11.CAMPOS_ADICIONALES AS NVARCHAR(MAX))) + LEN('<CA_967_LINEA>'),CHARINDEX('</CA_967_LINEA>', CAST(STA11.CAMPOS_ADICIONALES AS NVARCHAR(MAX))) - (CHARINDEX('<CA_967_LINEA>', CAST(STA11.CAMPOS_ADICIONALES AS NVARCHAR(MAX))) + LEN('<CA_967_LINEA>')))) END) AS [LINEA_ADIC] ,
	(CASE WHEN CHARINDEX('<CA_967_GRUPO>', CAST(STA11.CAMPOS_ADICIONALES AS NVARCHAR(MAX))) = 0 THEN '' ELSE (SUBSTRING( CAST(STA11.CAMPOS_ADICIONALES AS NVARCHAR(MAX)),CHARINDEX('<CA_967_GRUPO>', CAST(STA11.CAMPOS_ADICIONALES AS NVARCHAR(MAX))) + LEN('<CA_967_GRUPO>'),CHARINDEX('</CA_967_GRUPO>', CAST(STA11.CAMPOS_ADICIONALES AS NVARCHAR(MAX))) - (CHARINDEX('<CA_967_GRUPO>', CAST(STA11.CAMPOS_ADICIONALES AS NVARCHAR(MAX))) + LEN('<CA_967_GRUPO>')))) END) AS [GRUPO_ADIC],
	GVA12.LEYENDA_1 AS [LEYENDA_1],GVA12.LEYENDA_2 AS [LEYENDA_2],GVA12.LEYENDA_3 AS [LEYENDA_3], GVA12.LEYENDA_4 AS [LEYENDA_4],
	CASE
		WHEN NOT CLI2.COD_CLIENT IS NULL OR NOT CLI3.COD_CLIENT IS NULL THEN GVA14.RAZON_SOCI
	END AS [RAZON_SOCIAL_FACTURACION],
	GVA21.LEYENDA_5 AS [LEY_GVA21],
	PED2.LEYENDA_5 as [LEY_GVA21_X_GVA55],
	GVA21.NRO_PEDIDO,
	PED2.NRO_PEDIDO AS PED_NRO
	
FROM 
GVA12 (NOLOCK) 
	INNER JOIN GVA53 (NOLOCK) ON 
		GVA53.T_COMP = GVA12.T_COMP AND GVA53.N_COMP = GVA12.N_COMP 
	INNER JOIN GVA23 (NOLOCK) ON 
		GVA12.COD_VENDED = GVA23.COD_VENDED
	LEFT JOIN GVA14 (NOLOCK) ON 
		GVA12.COD_CLIENT = GVA14.COD_CLIENT
	LEFT JOIN STA11 (NOLOCK) ON 
		GVA53.COD_ARTICU = STA11.COD_ARTICU
	LEFT JOIN GVA15 ON 
		GVA15.IDENT_COMP = GVA12.T_COMP
	LEFT JOIN (SELECT 
					N1.CODE, 
					N1.IDFOLDER, 
					F1.PADRE0, 
					F1.PADRE1, 
					F1.PADRE2, 
					F1.PADRE3, 
					F1.PADRE4, 
					F1.PADRE5, 
					F1.PADRE6, 
					F1.PADRE7, 
					F1.PADRE8, 
					F1.PADRE9, 
					F1.PADRE10, 
					F1.PADRE11 
				FROM 
					GVA14ITC N1 
						JOIN V_LI_CLASIFICADOR_GVA14FLD F1 
							ON (N1.IDFOLDER= F1.IDFOLDER_V) ) AS CLASIF_CLI ON 
		GVA12.COD_CLIENT = CLASIF_CLI.CODE
	LEFT JOIN 
		(SELECT	DISTINCT
			TCOMP_V,
			NCOMP_V,
			TALON_PED,
			NRO_PEDIDO
		FROM
			GVA107)	GVA107
		ON GVA12.T_COMP=GVA107.TCOMP_V
		AND GVA12.N_COMP=GVA107.NCOMP_V
	LEFT JOIN GVA21
		ON GVA107.TALON_PED=GVA21.TALON_PED
		AND GVA107.NRO_PEDIDO=GVA21.NRO_PEDIDO
	LEFT JOIN GVA14 CLI2
		ON GVA21.LEYENDA_5=CLI2.COD_CLIENT
	LEFT JOIN GVA55
		ON GVA12.T_COMP=GVA55.T_COMP
		AND GVA12.N_COMP=GVA55.N_COMP
	LEFT JOIN GVA21 PED2
		ON GVA55.TALON_PED=PED2.TALON_PED
		AND GVA55.NRO_PEDIDO=PED2.NRO_PEDIDO
	LEFT JOIN GVA14 CLI3
		ON PED2.LEYENDA_5=CLI3.COD_CLIENT
		
WHERE 
	(GVA53.COD_ARTICU <> 'ART. AJUSTE') AND (GVA53.COD_ARTICU <> '')
	AND 
	( (GVA12.FECHA_EMIS BETWEEN '01/01/2022' AND '31/01/2030')) 
	AND (GVA53.RENGL_PADR = 0 OR GVA53.INSUMO_KIT_SEPARADO =1)
GROUP BY 
	GVA12.FECHA_EMIS , 
	GVA53.T_COMP , 
	GVA53.N_COMP , 
	GVA12.COD_VENDED , 
	CASE GVA12.COD_VENDED WHEN '**' THEN 'CARGA INICIAL' ELSE GVA23.NOMBRE_VEN END , 
	GVA12.COD_CLIENT , 
	CASE GVA12.COD_CLIENT WHEN '000000' THEN 'OCASIONAL' ELSE GVA14.RAZON_SOCI END , 
	GVA14.COD_CLIENT,
	CLI2.COD_CLIENT,
	CLI2.RAZON_SOCI,
	CLI3.COD_CLIENT,
	CLI3.RAZON_SOCI,
	GVA14.RAZON_SOCI,
	GVA14.LOCALIDAD , 
	GVA53.COD_ARTICU , 
	STA11.DESCRIPCIO , 
	GVA53.PRECIO_PAN ,
	gva53.PORC_DTO,
	gva12.PORC_BONIF,
	ISNULL(CLASIF_CLI.IDFOLDER, '') , 
	ISNULL(CLASIF_CLI.PADRE0, '') ,
	(CASE WHEN CHARINDEX('<CA_967_FAMILIA>', CAST(STA11.CAMPOS_ADICIONALES AS NVARCHAR(MAX))) = 0 THEN '' ELSE 
(SUBSTRING( CAST(STA11.CAMPOS_ADICIONALES AS NVARCHAR(MAX)),CHARINDEX('<CA_967_FAMILIA>', CAST(STA11.CAMPOS_ADICIONALES AS NVARCHAR(MAX))) + LEN('<CA_967_FAMILIA>'),
CHARINDEX('</CA_967_FAMILIA>', CAST(STA11.CAMPOS_ADICIONALES AS NVARCHAR(MAX))) - (CHARINDEX('<CA_967_FAMILIA>', CAST(STA11.CAMPOS_ADICIONALES AS NVARCHAR(MAX))) + LEN('<CA_967_FAMILIA>')))) END) ,
(CASE WHEN CHARINDEX('<CA_967_ESTADO>', CAST(STA11.CAMPOS_ADICIONALES AS NVARCHAR(MAX))) = 0 THEN '' ELSE (SUBSTRING( CAST(STA11.CAMPOS_ADICIONALES AS NVARCHAR(MAX)),CHARINDEX('<CA_967_ESTADO>', CAST(STA11.CAMPOS_ADICIONALES AS NVARCHAR(MAX))) + LEN('<CA_967_ESTADO>'),
CHARINDEX('</CA_967_ESTADO>', CAST(STA11.CAMPOS_ADICIONALES AS NVARCHAR(MAX))) - (CHARINDEX('<CA_967_ESTADO>', CAST(STA11.CAMPOS_ADICIONALES AS NVARCHAR(MAX))) + LEN('<CA_967_ESTADO>')))) END) ,
(CASE WHEN CHARINDEX('<CA_967_LINEA>', CAST(STA11.CAMPOS_ADICIONALES AS NVARCHAR(MAX))) = 0 THEN '' ELSE (SUBSTRING( CAST(STA11.CAMPOS_ADICIONALES AS NVARCHAR(MAX)),
CHARINDEX('<CA_967_LINEA>', CAST(STA11.CAMPOS_ADICIONALES AS NVARCHAR(MAX))) + LEN('<CA_967_LINEA>'),CHARINDEX('</CA_967_LINEA>', 
CAST(STA11.CAMPOS_ADICIONALES AS NVARCHAR(MAX))) - (CHARINDEX('<CA_967_LINEA>', CAST(STA11.CAMPOS_ADICIONALES AS NVARCHAR(MAX))) + LEN('<CA_967_LINEA>')))) END) , 
(CASE WHEN CHARINDEX('<CA_967_GRUPO>', CAST(STA11.CAMPOS_ADICIONALES AS NVARCHAR(MAX))) = 0 THEN '' ELSE (SUBSTRING( CAST(STA11.CAMPOS_ADICIONALES AS NVARCHAR(MAX)),
CHARINDEX('<CA_967_GRUPO>', CAST(STA11.CAMPOS_ADICIONALES AS NVARCHAR(MAX))) + LEN('<CA_967_GRUPO>'),
CHARINDEX('</CA_967_GRUPO>', CAST(STA11.CAMPOS_ADICIONALES AS NVARCHAR(MAX))) - (CHARINDEX('<CA_967_GRUPO>', CAST(STA11.CAMPOS_ADICIONALES AS NVARCHAR(MAX))) + LEN('<CA_967_GRUPO>')))) END),
GVA12.LEYENDA_1,GVA12.LEYENDA_2,GVA12.LEYENDA_3, GVA12.LEYENDA_4,GVA12.LEYENDA_5,
gva21.leyenda_5, ped2.LEYENDA_5,GVA21.NRO_PEDIDO, PED2.NRO_PEDIDO""", # Complex sales detail query
        "SELECT * FROM SEIN_COTIZACIONES",  # Cotizaciones / Tipo de cambio
    ]
    
    # PostgreSQL table names
    table_names = [
        "Vendedores",  # GVA23 → Vendedores
        "Clientes",    # GVA14 → Clientes  
        "Producto",    # STA11 → Producto
        "Ventas_Detalle",  # Complex sales query → Ventas_Detalle
        "Cotizaciones"     # SEIN_COTIZACIONES → Cotizaciones
    ]
    
    # Primary key columns for upsert conflicts
    conflict_columns_list = [
        ["COD_VENDED"],  # Primary key for Vendedores
        ["COD_CLIENT"],  # Primary key for Clientes
        ["COD_ARTICU"],  # Primary key for Producto
        ["Id"],  # Auto-increment primary key for Ventas_Detalle
        []       # No conflict columns for Cotizaciones (delete + insert)
    ]
    
    # Column mappings from SQL Server to PostgreSQL (no mapping needed - using original column names)
    column_mappings = [
        {},  # No column mapping needed for Vendedores - column names already match
        {},  # No column mapping needed for Clientes - column names already match
        {},  # No column mapping needed for Producto - column names already match
        {},  # No column mapping needed for Ventas_Detalle - column names already match
        {}   # No column mapping needed for Cotizaciones
    ]
    
    # Columns to update on conflict (all columns except primary key)
    update_columns_list = [
        ["NOMBRE_VEN", "INHABILITA"],                           # Vendedores: update NOMBRE_VEN, INHABILITA
        ["RAZON_SOCI", "HABILITADO", "LOCALIDAD", "COD_PROVIN", "COD_VENDED", "FECHA_ALTA"],  # Clientes: update all columns except COD_CLIENT
        ["DESCRIPCIO", "CA_967_ESTADO", "CA_967_FAMILIA", "CA_967_LINEA", "CA_967_GRUPO", "FECHA_ALTA"],  # Producto: update all columns except COD_ARTICU
        ["Fecha_Emision", "Tipo_Comprobante", "Nro_Comprobante", "Cod_Vendedor", "Nombre_Vendedor", "Cod_Cliente", "Razon_Social", "Localidad", "Cod_Articulo", "Descripcion", "Cantidad", "Precio_Unitario", "Total", "Desc_Linea", "Desc_Encabez", "Id_Folder_Clientes", "Cli_Carpeta_Nivel_1", "Familia_Adic", "Estado_Adic", "Linea_Adic", "Grupo_Adic", "Leyenda_1", "Leyenda_2", "Leyenda_3", "Leyenda_4", "Razon_Social_Facturacion", "Ley_Gva21", "Ley_Gva21_X_Gva55", "Nro_Pedido", "Ped_Nro"],  # Ventas_Detalle: update all columns except Id
        []  # No update columns for Cotizaciones (delete + insert)
    ]
    
    # Choose which method to run:
    # Option 1: Test extraction only
    etl = ETLPipeline()
    success = etl.test_extract(queries)
    
    # Option 2: Run full ETL pipeline with column mapping (uncomment to use)
    success = etl.run_etl_with_mapping(queries, table_names, conflict_columns_list, column_mappings, update_columns_list)
    
    if success:
        logger.info("Process completed successfully!")
        sys.exit(0)
    else:
        logger.error("Process failed!")
        sys.exit(1)

if __name__ == "__main__":
    main()
