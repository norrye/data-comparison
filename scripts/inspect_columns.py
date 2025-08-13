#!/usr/bin/env python3
"""Inspect columns in both datasets with semantic analysis."""

import polars as pl
from pathlib import Path
from typing import Dict, List, Tuple

def semantic_field_analyzer(dd_columns: List[str], ac_columns: List[str]) -> Dict[str, str]:
    """Analyze semantic field mappings between datasets."""
    
    # Define semantic patterns for field matching
    semantic_patterns = {
        # Personal information
        'title': ['title', 'salutation', 'prefix'],
        'first_name': ['firstname', 'first_name', 'given_name', 'fname', 'forename'],
        'surname': ['surname', 'lastname', 'last_name', 'family_name', 'lname'],
        'gender': ['gender', 'sex'],
        
        # Contact information
        'email': ['email', 'email_address', 'emailstd', 'e_mail'],
        'mobile': ['mobile', 'cell', 'cellular', 'mobile_phone', 'cell_phone'],
        'landline': ['landline', 'phone', 'telephone', 'home_phone', 'land_line'],
        
        # Address information
        'suburb': ['suburb', 'city', 'locality', 'town'],
        'state': ['state', 'province', 'region'],
        'postcode': ['postcode', 'zip', 'postal_code', 'zipcode']
    }
    
    # Manual overrides for specific field mappings
    manual_overrides = {
        'mobile_text': 'Mobile',
        'landline_text': 'Landline',
        'email_sha256': 'EmailHash'
    }
    
    mappings = {}
    
    # Apply manual overrides first
    for ac_field, dd_field in manual_overrides.items():
        if ac_field in [col.lower() for col in ac_columns] and dd_field in dd_columns:
            mappings[dd_field] = ac_field
    
    # Semantic matching for remaining fields
    for dd_col in dd_columns:
        if dd_col in mappings:  # Skip if already mapped by override
            continue
            
        dd_lower = dd_col.lower()
        best_match = None
        
        # Direct name matching first
        for ac_col in ac_columns:
            if dd_lower == ac_col.lower():
                best_match = ac_col
                break
        
        # Semantic pattern matching
        if not best_match:
            for semantic_type, patterns in semantic_patterns.items():
                if any(pattern in dd_lower for pattern in patterns):
                    for ac_col in ac_columns:
                        ac_lower = ac_col.lower()
                        if any(pattern in ac_lower for pattern in patterns):
                            best_match = ac_col
                            break
                    if best_match:
                        break
        
        if best_match:
            mappings[dd_col] = best_match
    
    return mappings

def inspect_datasets():
    """Show columns in both datasets with semantic analysis."""
    base_path = Path("/data/projects/data_comparison")
    file1 = base_path / "data/external/DATADIRECT_DL_202505_20250801_subset.parquet"
    file2 = base_path / "data/interim/ad_consumers_2020805_inter_ll_text.parquet"  # Updated to use ll_text version
    
    df1 = pl.read_parquet(file1)
    df2 = pl.read_parquet(file2)
    
    print(f"Dataset 1 (DATADIRECT_DL_subset): {df1.shape}")
    print(f"Columns: {list(df1.columns)}")
    
    print(f"\nDataset 2 (ad_consumers): {df2.shape}")
    print(f"Columns: {list(df2.columns)}")
    
    # Perform semantic analysis
    print("\n=== SEMANTIC FIELD ANALYSIS ===")
    mappings = semantic_field_analyzer(df1.columns, df2.columns)
    
    if mappings:
        print("\nSemantic field mappings found:")
        for dd_field, ac_field in mappings.items():
            print(f"  {dd_field} -> {ac_field}")
        
        print(f"\nTotal mappings: {len(mappings)} out of {len(df1.columns)} DataDirect fields")
        print(f"Mapping coverage: {len(mappings)/len(df1.columns)*100:.1f}%")
        
        # Show unmapped fields
        unmapped_dd = [col for col in df1.columns if col not in mappings]
        unmapped_ac = [col for col in df2.columns if col not in mappings.values()]
        
        if unmapped_dd:
            print(f"\nUnmapped DataDirect fields ({len(unmapped_dd)}): {unmapped_dd}")
        if unmapped_ac:
            print(f"\nUnmapped Ad_consumers fields ({len(unmapped_ac)}): {unmapped_ac}")
    else:
        print("No semantic mappings found between datasets")

if __name__ == "__main__":
    inspect_datasets()