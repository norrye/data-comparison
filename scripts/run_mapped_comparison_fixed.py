#!/usr/bin/env python3
"""Run comparison with mapped columns - fixed data types."""

import polars as pl
import sweetviz as sv
from pathlib import Path
from loguru import logger

def compare_with_mapping(sample_size: int = 100000):
    """Compare datasets using column mapping with data type fixes."""
    base_path = Path("/data/projects/data_comparison")
    file1 = base_path / "data/external/DATADIRECT_DL_202505_20250801_subset.parquet"
    file2 = base_path / "data/interim/ad_consumers_2020805_inter.parquet"
    
    # Load and sample datasets
    df1 = pl.read_parquet(file1).sample(n=sample_size, seed=42)
    df2 = pl.read_parquet(file2).sample(n=sample_size, seed=42)
    
    logger.info(f"Dataset 1 shape: {df1.shape}")
    logger.info(f"Dataset 2 shape: {df2.shape}")
    
    # Select and clean common columns
    df1_clean = df1.select([
        pl.col("Title").cast(pl.Utf8).alias("Title"),
        pl.col("FirstName").cast(pl.Utf8).alias("FirstName"),
        pl.col("Surname").cast(pl.Utf8).alias("Surname"),
        pl.col("Gender").cast(pl.Utf8).alias("Gender"),
        pl.col("Suburb").cast(pl.Utf8).alias("Suburb"),
        pl.col("State").cast(pl.Utf8).alias("State"),
        pl.col("Postcode").cast(pl.Utf8).alias("Postcode")
    ])
    
    df2_clean = df2.select([
        pl.col("title").cast(pl.Utf8).alias("Title"),
        pl.col("given_name_1").cast(pl.Utf8).alias("FirstName"),
        pl.col("surname").cast(pl.Utf8).alias("Surname"),
        pl.col("gender").cast(pl.Utf8).alias("Gender"),
        pl.col("suburb").cast(pl.Utf8).alias("Suburb"),
        pl.col("state").cast(pl.Utf8).alias("State"),
        pl.col("postcode").cast(pl.Utf8).alias("Postcode")
    ])
    
    # Convert to pandas
    df1_pd = df1_clean.to_pandas()
    df2_pd = df2_clean.to_pandas()
    
    logger.info(f"Comparing {len(df1_pd.columns)} cleaned columns")
    
    # Generate comparison report
    report = sv.compare([df1_pd, "DATADIRECT_DL"], [df2_pd, "ad_consumers"])
    
    output_path = Path("reports/html")
    output_path.mkdir(parents=True, exist_ok=True)
    
    report_file = output_path / "mapped_dataset_comparison.html"
    report.show_html(str(report_file))
    logger.info(f"Mapped comparison report saved to: {report_file}")

if __name__ == "__main__":
    compare_with_mapping()