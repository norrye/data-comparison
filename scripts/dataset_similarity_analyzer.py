#!/usr/bin/env python3
"""Dataset similarity analyzer using Sweetviz compare functionality."""

import polars as pl
import sweetviz as sv
from pathlib import Path
from loguru import logger

def compare_datasets(file1: str, file2: str, sample_size: int = 200000, output_dir: str = "reports/html"):
    """Direct comparison of two datasets using Sweetviz."""
    logger.info(f"Loading datasets for comparison")
    
    # Load both datasets
    df1_pl = pl.read_parquet(file1).sample(n=sample_size, seed=42)
    df2_pl = pl.read_parquet(file2).sample(n=sample_size, seed=42)
    
    logger.info(f"Dataset 1 shape: {df1_pl.shape}")
    logger.info(f"Dataset 2 shape: {df2_pl.shape}")
    
    # Convert to pandas for sweetviz
    df1_pd = df1_pl.to_pandas()
    df2_pd = df2_pl.to_pandas()
    
    # Find common columns
    common_cols = set(df1_pd.columns) & set(df2_pd.columns)
    logger.info(f"Common columns: {len(common_cols)}")
    
    if common_cols:
        df1_common = df1_pd[list(common_cols)]
        df2_common = df2_pd[list(common_cols)]
        
        # Generate comparison report
        report = sv.compare([df1_common, "Dataset 1"], [df2_common, "Dataset 2"])
        
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        report_file = output_path / "dataset_comparison.html"
        report.show_html(str(report_file))
        logger.info(f"Comparison report saved to: {report_file}")
    else:
        logger.error("No common columns found between datasets")

def main():
    """Compare the two datasets."""
    base_path = Path("/data/projects/data_comparison")
    file1 = base_path / "data/external/DATADIRECT_202501_20250801.parquet"
    file2 = base_path / "data/external/DATADIRECT_DL_202505_20250801.parquet"
    
    compare_datasets(str(file1), str(file2))

if __name__ == "__main__":
    main()