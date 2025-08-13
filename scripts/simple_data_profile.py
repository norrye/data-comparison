#!/usr/bin/env python3
"""Simple data profiling script for parquet files using pandas."""

import pandas as pd
import json
from pathlib import Path

def profile_parquet_file(file_path: str, output_dir: str = "reports/tables"):
    """Generate basic data profile for a parquet file."""
    
    # Load the data
    df = pd.read_parquet(file_path)
    
    # Generate basic profile
    profile = {
        "file_name": Path(file_path).name,
        "shape": df.shape,
        "columns": list(df.columns),
        "dtypes": df.dtypes.astype(str).to_dict(),
        "memory_usage": df.memory_usage(deep=True).sum(),
        "null_counts": df.isnull().sum().to_dict(),
        "describe": df.describe(include='all').to_dict()
    }
    
    # Save report
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    file_name = Path(file_path).stem
    report_file = output_path / f"{file_name}_profile.json"
    
    with open(report_file, 'w') as f:
        json.dump(profile, f, indent=2, default=str)
    
    print(f"Profile saved to: {report_file}")
    print(f"Shape: {profile['shape']}")
    print(f"Columns: {len(profile['columns'])}")
    print(f"Memory usage: {profile['memory_usage']:,} bytes")
    
    return profile

def main():
    """Profile both parquet files."""
    base_path = Path("/data/projects/data_comparison")
    files = [
        "data/external/DATADIRECT_202501_20250801.parquet",
        "data/external/DATADIRECT_DL_202505_20250801.parquet"
    ]
    
    for file_path in files:
        full_path = base_path / file_path
        print(f"\nProfiling: {full_path}")
        profile_parquet_file(str(full_path))

if __name__ == "__main__":
    main()