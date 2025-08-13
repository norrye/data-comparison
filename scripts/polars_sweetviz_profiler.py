#!/usr/bin/env python3
"""Memory-efficient profiling using Polars + Sweetviz."""

import polars as pl
import sweetviz as sv
from pathlib import Path
from loguru import logger

def profile_with_polars(file_path: str, sample_size: int = 100000, output_dir: str = "reports/html"):
    """Profile using Polars for efficient loading, then convert to pandas for sweetviz."""
    logger.info(f"Loading with Polars: {file_path}")
    
    # Load with Polars (more memory efficient)
    df_pl = pl.read_parquet(file_path)
    logger.info(f"Full dataset shape: {df_pl.shape}")
    
    # Sample if dataset is large
    if df_pl.height > sample_size:
        df_pl = df_pl.sample(n=sample_size, seed=42)
        logger.info(f"Sampled to {sample_size:,} rows")
    
    # Convert to pandas for sweetviz
    df_pd = df_pl.to_pandas()
    
    # Generate report
    report = sv.analyze(df_pd)
    
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    report_file = output_path / f"{Path(file_path).stem}_polars_sweetviz.html"
    report.show_html(str(report_file))
    logger.info(f"Profile saved to: {report_file}")

def main():
    """Profile files using Polars + Sweetviz."""
    base_path = Path("/data/projects/data_comparison")
    files = [
        "data/external/DATADIRECT_202501_20250801.parquet",
        "data/external/DATADIRECT_DL_202505_20250801.parquet"
    ]
    
    for file_path in files:
        full_path = base_path / file_path
        try:
            profile_with_polars(str(full_path))
        except Exception as e:
            logger.error(f"Failed to profile {full_path}: {e}")

if __name__ == "__main__":
    main()