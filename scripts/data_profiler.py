#!/usr/bin/env python3
"""Data profiling script for parquet files using dataprofiler."""

import pandas as pd
from dataprofiler import Data, Profiler
import json
from pathlib import Path

def profile_parquet_file(file_path: str, output_dir: str = "reports/tables"):
    """Generate data profile for a parquet file."""
    
    # Load the data
    data = Data(file_path)
    
    # Create profile
    profile = Profiler(data)
    
    # Generate report
    report = profile.report(report_options={"output_format": "compact"})
    
    # Save report
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    file_name = Path(file_path).stem
    report_file = output_path / f"{file_name}_profile.json"
    
    with open(report_file, 'w') as f:
        json.dump(report, f, indent=2, default=str)
    
    print(f"Profile saved to: {report_file}")
    return report

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