#!/usr/bin/env python3
"""Value comparison v1.0 - Compare actual values between datasets."""

import polars as pl
from pathlib import Path
from loguru import logger

def compare_values():
    """Compare actual values between datasets."""
    base_path = Path("/data/projects/data_comparison")
    file1 = base_path / "data/external/DATADIRECT_DL_202505_20250801_subset.parquet"
    file2 = base_path / "data/interim/ad_consumers_2020805_inter.parquet"
    
    # Load datasets with key columns for matching
    df1 = pl.read_parquet(file1, columns=["FirstName", "Surname", "Mobile", "EmailStd", "Suburb", "State", "Postcode"])
    df2 = pl.read_parquet(file2, columns=["given_name_1", "surname", "mobile_text", "email", "suburb", "state", "postcode_text"])
    
    logger.info(f"Dataset 1 shape: {df1.shape}")
    logger.info(f"Dataset 2 shape: {df2.shape}")
    
    # Standardize column names for comparison
    df2_renamed = df2.rename({
        "given_name_1": "FirstName",
        "surname": "Surname", 
        "mobile_text": "Mobile",
        "email": "EmailStd",
        "suburb": "Suburb",
        "state": "State",
        "postcode_text": "Postcode"
    })
    
    # Find common values by joining on multiple fields
    common_records = df1.join(
        df2_renamed,
        on=["FirstName", "Surname", "State"],
        how="inner"
    )
    
    logger.info(f"Common records found: {common_records.shape[0]:,}")
    
    # Sample unique values comparison
    print("\n=== VALUE COMPARISON ANALYSIS ===")
    print(f"Dataset 1 records: {df1.shape[0]:,}")
    print(f"Dataset 2 records: {df2.shape[0]:,}")
    print(f"Common records (Name+State match): {common_records.shape[0]:,}")
    print(f"Overlap percentage: {(common_records.shape[0] / min(df1.shape[0], df2.shape[0])) * 100:.2f}%")
    
    # Compare unique values in key fields
    for col in ["State", "Suburb"]:
        df1_unique = set(df1[col].unique().to_list())
        df2_unique = set(df2_renamed[col].unique().to_list())
        common_values = df1_unique & df2_unique
        
        print(f"\n{col} comparison:")
        print(f"  Dataset 1 unique: {len(df1_unique)}")
        print(f"  Dataset 2 unique: {len(df2_unique)}")
        print(f"  Common values: {len(common_values)}")
        print(f"  Value overlap: {(len(common_values) / len(df1_unique | df2_unique)) * 100:.1f}%")
    
    # Save sample of common records
    if common_records.shape[0] > 0:
        sample_size = min(1000, common_records.shape[0])
        common_sample = common_records.sample(n=sample_size, seed=42)
        
        output_path = Path("reports/tables")
        output_path.mkdir(parents=True, exist_ok=True)
        
        common_sample.write_csv(output_path / "common_records_sample_v1.csv")
        logger.info(f"Sample of {sample_size} common records saved")

if __name__ == "__main__":
    compare_values()