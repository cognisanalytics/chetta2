import pandas as pd
import numpy as np
from typing import List, Dict, Any
from src.utils.logger import setup_logger

class DataTransformer:
    """Transform data between extract and load stages"""
    
    def __init__(self):
        self.logger = setup_logger(__name__)
    
    def transform_data(self, data: pd.DataFrame, column_mapping: Dict[str, str] = None) -> pd.DataFrame:
        """Transform data with column mapping only"""
        self.logger.info(f"Transforming {len(data)} rows")
        
        # Apply column mapping if provided
        if column_mapping:
            data = data.rename(columns=column_mapping)
            self.logger.info(f"Applied column mapping: {column_mapping}")
        
        return data
    
    def transform_batch(self, dataframes: List[pd.DataFrame], column_mappings: List[Dict[str, str]] = None) -> List[pd.DataFrame]:
        """Transform multiple DataFrames with optional column mappings"""
        transformed_dfs = []
        
        for i, df in enumerate(dataframes):
            self.logger.info(f"Transforming DataFrame {i+1}/{len(dataframes)}")
            
            # Get column mapping for this DataFrame
            column_mapping = column_mappings[i] if column_mappings and i < len(column_mappings) else None
            
            transformed_df = self.transform_data(df, column_mapping)
            transformed_dfs.append(transformed_df)
        
        return transformed_dfs
