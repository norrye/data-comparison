#!/usr/bin/env python3
"""Enhanced data profiling with multiple tools."""

import pandas as pd
from pathlib import Path

def profile_with_ydata(file_path: str, output_dir: str = "reports/html"):
    """Generate profile using ydata-profiling."""
    try:
        from ydata_profiling import ProfileReport
        
        df = pd.read_parquet(file_path)
        profile = ProfileReport(df, title=f"Profile: {Path(file_path).stem}")
        
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        report_file = output_path / f"{Path(file_path).stem}_ydata.html"
        profile.to_file(report_file)
        print(f"YData profile saved to: {report_file}")
        
    except ImportError:
        print("ydata-profiling not installed. Install with: pip install ydata-profiling")

def profile_with_sweetviz(file_path: str, output_dir: str = "reports/html"):
    """Generate profile using sweetviz."""
    try:
        import sweetviz as sv
        
        df = pd.read_parquet(file_path)
        report = sv.analyze(df)
        
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        report_file = output_path / f"{Path(file_path).stem}_sweetviz.html"
        report.show_html(str(report_file))
        print(f"Sweetviz profile saved to: {report_file}")
        
    except ImportError:
        print("sweetviz not installed. Install with: pip install sweetviz")

def main():
    """Profile files with multiple tools."""
    base_path = Path("/data/projects/data_comparison")
    files = [
        "data/external/DATADIRECT_202501_20250801.parquet",
        "data/external/DATADIRECT_DL_202505_20250801.parquet"
    ]
    
    for file_path in files:
        full_path = base_path / file_path
        print(f"\nProfiling: {full_path}")
        profile_with_ydata(str(full_path))
        profile_with_sweetviz(str(full_path))

if __name__ == "__main__":
    main()