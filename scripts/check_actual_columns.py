#!/usr/bin/env python3
"""Check actual columns and fix mappings."""

import polars as pl
from pathlib import Path

def check_columns():
    """Check actual columns in both datasets."""
    base_path = Path("/data/projects/data_comparison")
    file1 = base_path / "data/external/DATADIRECT_DL_202505_20250801_subset.parquet"
    file2 = base_path / "data/interim/ad_consumers_2020805_inter.parquet"
    
    df1 = pl.read_parquet(file1)
    df2 = pl.read_parquet(file2)
    
    print("DATADIRECT_DL columns:")
    for i, col in enumerate(df1.columns):
        print(f"{i+1:2d}. {col}")
    
    print(f"\nad_consumers columns:")
    for i, col in enumerate(df2.columns):
        print(f"{i+1:2d}. {col}")
    
    # Check if mapped columns exist
    dd_cols = ['Title', 'FirstName', 'Surname', 'Gender', 'DOB', 'AgeCalc', 'Landline', 'Mobile', 'EmailStd', 'StreetAddress', 'Suburb', 'State', 'Postcode']
    ac_cols = ['title', 'given_name_1', 'surname', 'gender', 'dob_yyyymmdd', 'age_band', 'landline', 'mobile', 'email', 'gnaf_address_label', 'suburb', 'state', 'postcode']
    
    print(f"\nColumn existence check:")
    for dd, ac in zip(dd_cols, ac_cols):
        dd_exists = dd in df1.columns
        ac_exists = ac in df2.columns
        print(f"{dd:15} → {ac:20} | DD:{dd_exists} AC:{ac_exists}")

if __name__ == "__main__":
    check_columns()