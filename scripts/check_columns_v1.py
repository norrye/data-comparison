#!/usr/bin/env python3
"""
Column checker v1.0 - Verify actual column names and types.

Version: 1.0
Author: Expert Data Scientist
Description: Check actual column names and data types in both datasets
"""

import duckdb
from pathlib import Path
from loguru import logger
from pydantic import BaseModel, Field
from typing import List, Dict, Any
import os

class ColumnInfo(BaseModel):
    """Data validation model for column information."""
    column_name: str = Field(..., description="Name of the column")
    data_type: str = Field(..., description="Data type of the column")
    sample_values: List[Any] = Field(..., description="Sample values from the column")

def check_columns_v1() -> None:
    """
    Check actual column names and types in both datasets.
    
    Validates the structure of both parquet files to ensure correct field mappings.
    """
    logger.info("Starting column check v1.0")
    
    base_path = Path("/data/projects/data_comparison")
    file1 = base_path / "data/external/DATADIRECT_DL_202505_20250801_subset.parquet"
    file2 = base_path / "data/interim/ad_consumers_2020805_inter.parquet"
    
    # Source validation
    if not file1.exists() or not file2.exists():
        logger.error("Source files not found")
        raise FileNotFoundError("Required parquet files not found")
    
    try:
        conn = duckdb.connect()
        logger.info("DuckDB connection established")
        
        # Check DataDirect columns
        print("\n=== DATADIRECT COLUMNS ===")
        dd_columns = conn.execute(f"DESCRIBE SELECT * FROM read_parquet('{file1}') LIMIT 1").fetchall()
        
        for col_name, col_type, null, key, default, extra in dd_columns:
            # Get sample values
            sample_query = f"SELECT DISTINCT {col_name} FROM read_parquet('{file1}') WHERE {col_name} IS NOT NULL LIMIT 5"
            try:
                samples = [row[0] for row in conn.execute(sample_query).fetchall()]
            except Exception as e:
                samples = [f"Error: {str(e)}"]
            
            print(f"{col_name:20} | {col_type:15} | Samples: {samples}")
            logger.info(f"DD Column: {col_name} ({col_type}) - Samples: {samples}")
        
        # Check Ad_consumers columns
        print("\n=== AD_CONSUMERS COLUMNS ===")
        ac_columns = conn.execute(f"DESCRIBE SELECT * FROM read_parquet('{file2}') LIMIT 1").fetchall()
        
        for col_name, col_type, null, key, default, extra in ac_columns:
            # Get sample values
            sample_query = f"SELECT DISTINCT {col_name} FROM read_parquet('{file2}') WHERE {col_name} IS NOT NULL LIMIT 5"
            try:
                samples = [row[0] for row in conn.execute(sample_query).fetchall()]
            except Exception as e:
                samples = [f"Error: {str(e)}"]
            
            print(f"{col_name:20} | {col_type:15} | Samples: {samples}")
            logger.info(f"AC Column: {col_name} ({col_type}) - Samples: {samples}")
        
        # Check for potential field mappings
        print("\n=== POTENTIAL FIELD MAPPINGS ===")
        dd_col_names = [col[0] for col in dd_columns]
        ac_col_names = [col[0] for col in ac_columns]
        
        mapping_suggestions = {
            "Title": [col for col in ac_col_names if 'title' in col.lower()],
            "FirstName": [col for col in ac_col_names if 'given' in col.lower() or 'first' in col.lower()],
            "Surname": [col for col in ac_col_names if 'surname' in col.lower() or 'last' in col.lower()],
            "Gender": [col for col in ac_col_names if 'gender' in col.lower()],
            "Landline": [col for col in ac_col_names if 'landline' in col.lower()],
            "Mobile": [col for col in ac_col_names if 'mobile' in col.lower()],
            "EmailStd": [col for col in ac_col_names if 'email' in col.lower()],
            "Suburb": [col for col in ac_col_names if 'suburb' in col.lower()],
            "State": [col for col in ac_col_names if 'state' in col.lower()],
            "Postcode": [col for col in ac_col_names if 'postcode' in col.lower()]
        }
        
        for dd_col, ac_matches in mapping_suggestions.items():
            if dd_col in dd_col_names:
                print(f"{dd_col:15} -> {ac_matches}")
                logger.info(f"Mapping suggestion: {dd_col} -> {ac_matches}")
        
        conn.close()
        logger.info("Column check v1.0 completed successfully")
        
    except Exception as e:
        logger.error(f"Critical error in column check: {str(e)}")
        raise

if __name__ == "__main__":
    check_columns_v1()