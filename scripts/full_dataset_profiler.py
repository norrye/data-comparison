#!/usr/bin/env python3
"""Data profiling with sweetviz using full datasets."""

import pandas as pd
import sweetviz as sv
from pathlib import Path
from loguru import logger

def profile_full_dataset(file_path: str, output_dir: str = "reports/html"):
    """Generate profile using sweetviz with full dataset."""
    logger.info(f"Loading full dataset: {file_path}")
    
    df = pd.read_parquet(file_path)
    logger.info(f"Dataset shape: {df.shape}")
    
    report = sv.analyze(df)
    
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    report_file = output_path / f"{Path(file_path).stem}_full_sweetviz.html"
    report.show_html(str(report_file))
    logger.info(f"Full dataset profile saved to: {report_file}")

def main():
    """Profile files with full datasets."""
    base_path = Path("/data/projects/data_comparison")
    files = [
        "data/external/DATADIRECT_202501_20250801.parquet",
        "data/external/DATADIRECT_DL_202505_20250801.parquet"
    ]
    
    for file_path in files:
        full_path = base_path / file_path
        try:
            profile_full_dataset(str(full_path))
        except Exception as e:
            logger.error(f"Failed to profile {full_path}: {e}")

if __name__ == "__main__":
    main()