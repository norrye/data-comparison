#!/usr/bin/env python3
"""Analyze and map similar columns between datasets."""

import polars as pl
from pathlib import Path

def map_similar_columns():
    """Map similar columns between datasets."""
    base_path = Path("/data/projects/data_comparison")
    file1 = base_path / "data/external/DATADIRECT_DL_202505_20250801_subset.parquet"
    file2 = base_path / "data/interim/ad_consumers_2020805_inter.parquet"
    
    df1 = pl.read_parquet(file1)
    df2 = pl.read_parquet(file2)
    
    # Column mappings (best guess matches)
    mappings = {
        'Title': 'title',
        'FirstName': 'given_name_1', 
        'Surname': 'surname',
        'Gender': 'gender',
        'DOB': 'dob_yyyymmdd',
        'AgeCalc': 'age_band',
        'Landline': 'landline',
        'Mobile': 'mobile',
        'EmailStd': 'email',
        'EmailFrmted': 'email',
        'EmailHash': 'email_md5',
        'StreetAddress': 'gnaf_address_label',
        'Suburb': 'suburb',
        'State': 'state', 
        'Postcode': 'postcode',
        'Gnaf_Pid': 'address_detail_pid'
    }
    
    print("COLUMN MAPPING ANALYSIS")
    print("="*50)
    print(f"DATADIRECT_DL → ad_consumers")
    print("="*50)
    
    matched_pairs = []
    for dd_col, ac_col in mappings.items():
        if dd_col in df1.columns and ac_col in df2.columns:
            matched_pairs.append((dd_col, ac_col))
            print(f"✓ {dd_col:15} → {ac_col}")
        else:
            print(f"✗ {dd_col:15} → {ac_col} (missing)")
    
    print(f"\nMatched Pairs: {len(matched_pairs)}")
    print(f"Similarity Score: {len(matched_pairs)/len(mappings):.1%}")
    
    return matched_pairs, mappings

def compare_mapped_columns():
    """Compare datasets using mapped columns."""
    matched_pairs, _ = map_similar_columns()
    
    if len(matched_pairs) >= 5:  # Minimum threshold
        print(f"\n✅ DECISION: SIMILAR ENOUGH TO COMPARE")
        print(f"Found {len(matched_pairs)} matching column pairs")
        print("Datasets contain similar personal data with different schemas")
    else:
        print(f"\n❌ DECISION: TOO DIFFERENT TO COMPARE")
        print(f"Only {len(matched_pairs)} matching pairs found")

if __name__ == "__main__":
    compare_mapped_columns()