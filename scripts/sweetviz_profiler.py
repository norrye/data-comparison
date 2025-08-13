#!/usr/bin/env python3
"""Data profiling with sweetviz for beautiful HTML reports."""

import pandas as pd
import sweetviz as sv
from pathlib import Path

def profile_with_sweetviz(file_path: str, output_dir: str = "reports/html"):
    """Generate profile using sweetviz."""
    df = pd.read_parquet(file_path)
    
    # Sample for large datasets
    if len(df) > 50000:
        df = df.sample(n=50000, random_state=42)
        print(f"Sampled 50,000 rows from {len(df):,} total rows")
    
    report = sv.analyze(df)
    
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    report_file = output_path / f"{Path(file_path).stem}_sweetviz.html"
    report.show_html(str(report_file))
    print(f"Sweetviz profile saved to: {report_file}")

def main():
    """Profile files with sweetviz."""
    base_path = Path("/data/projects/data_comparison")
    files = [
        "data/external/DATADIRECT_202501_20250801.parquet",
        "data/external/DATADIRECT_DL_202505_20250801.parquet"
    ]
    
    for file_path in files:
        full_path = base_path / file_path
        print(f"\nProfiling: {full_path}")
        profile_with_sweetviz(str(full_path))

if __name__ == "__main__":
    main()