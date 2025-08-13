#!/usr/bin/env python3
"""Load and compare only the mapped columns."""

import polars as pl
import sweetviz as sv
from pathlib import Path
from loguru import logger

def compare_mapped_only():
    """Compare only the mapped columns."""
    base_path = Path("/data/projects/data_comparison")
    file1 = base_path / "data/external/DATADIRECT_DL_202505_20250801_subset.parquet"
    file2 = base_path / "data/interim/ad_consumers_2020805_inter.parquet"
    
    # Load full datasets
    df1 = pl.read_parquet(file1)
    df2 = pl.read_parquet(file2)
    
    logger.info(f"Dataset 1 shape: {df1.shape}")
    logger.info(f"Dataset 2 shape: {df2.shape}")
    
    # Map only these specific columns
    df1_mapped = df1.select([
        pl.col("Mobile").cast(pl.Utf8).alias("Mobile"),
        pl.col("Suburb").cast(pl.Utf8).alias("Suburb"),
        pl.col("State").cast(pl.Utf8).alias("State")
    ])
    
    df2_mapped = df2.select([
        pl.col("mobile_text").cast(pl.Utf8).alias("Mobile"),
        pl.col("suburb").cast(pl.Utf8).alias("Suburb"),
        pl.col("state").cast(pl.Utf8).alias("State")
    ])
    
    # Convert to pandas
    df1_pd = df1_mapped.to_pandas()
    df2_pd = df2_mapped.to_pandas()
    
    logger.info(f"Comparing {len(df1_pd.columns)} mapped columns")
    
    # Generate comparison
    report = sv.compare([df1_pd, "DATADIRECT_DL"], [df2_pd, "ad_consumers"])
    
    output_path = Path("reports/html")
    output_path.mkdir(parents=True, exist_ok=True)
    
    report_file = output_path / "mapped_columns_only.html"
    report.show_html(str(report_file))
    logger.info(f"Mapped columns comparison saved to: {report_file}")

if __name__ == "__main__":
    compare_mapped_only()