#!/usr/bin/env python3
"""Large sample profiling using Polars + Sweetviz."""

import polars as pl
import sweetviz as sv
from pathlib import Path
from loguru import logger

def profile_large_sample(file_path: str, sample_size: int = 500000, output_dir: str = "reports/html"):
    """Profile using larger sample size."""
    logger.info(f"Loading with Polars: {file_path}")
    
    df_pl = pl.read_parquet(file_path)
    logger.info(f"Full dataset shape: {df_pl.shape}")
    
    if df_pl.height > sample_size:
        df_pl = df_pl.sample(n=sample_size, seed=42)
        logger.info(f"Sampled to {sample_size:,} rows")
    
    df_pd = df_pl.to_pandas()
    report = sv.analyze(df_pd)
    
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    report_file = output_path / f"{Path(file_path).stem}_500k_sweetviz.html"
    report.show_html(str(report_file))
    logger.info(f"Large sample profile saved to: {report_file}")

def main():
    """Profile files with 500K sample size."""
    base_path = Path("/data/projects/data_comparison")
    files = [
        "data/external/DATADIRECT_202501_20250801.parquet",
        "data/external/DATADIRECT_DL_202505_20250801.parquet"
    ]
    
    for file_path in files:
        full_path = base_path / file_path
        profile_large_sample(str(full_path))

if __name__ == "__main__":
    main()