#!/usr/bin/env python3
"""Final corrected full dataset comparison."""

import polars as pl
import sweetviz as sv
from pathlib import Path
from loguru import logger

def final_comparison():
    """Compare full datasets with correct column mappings."""
    base_path = Path("/data/projects/data_comparison")
    file1 = base_path / "data/external/DATADIRECT_DL_202505_20250801_subset.parquet"
    file2 = base_path / "data/interim/ad_consumers_2020805_inter.parquet"
    
    # Load full datasets
    df1 = pl.read_parquet(file1)
    df2 = pl.read_parquet(file2)
    
    logger.info(f"Dataset 1 full shape: {df1.shape}")
    logger.info(f"Dataset 2 full shape: {df2.shape}")
    
    # Correct mappings based on actual column names
    df1_clean = df1.select([
        pl.col("Title").cast(pl.Utf8).alias("Title"),
        pl.col("FirstName").cast(pl.Utf8).alias("FirstName"), 
        pl.col("Surname").cast(pl.Utf8).alias("Surname"),
        pl.col("Gender").cast(pl.Utf8).alias("Gender"),
        pl.col("Landline").cast(pl.Utf8).alias("Landline"),
        pl.col("Mobile").cast(pl.Utf8).alias("Mobile"),
        pl.col("EmailStd").cast(pl.Utf8).alias("EmailStd"),
        pl.col("Suburb").cast(pl.Utf8).alias("Suburb"),
        pl.col("State").cast(pl.Utf8).alias("State"),
        pl.col("Postcode").cast(pl.Utf8).alias("Postcode")
    ])
    
    df2_clean = df2.select([
        pl.col("title").cast(pl.Utf8).alias("Title"),
        pl.col("given_name_1").cast(pl.Utf8).alias("FirstName"),
        pl.col("surname").cast(pl.Utf8).alias("Surname"), 
        pl.col("gender").cast(pl.Utf8).alias("Gender"),
        pl.col("landline").cast(pl.Utf8).alias("Landline"),
        pl.col("mobile_text").cast(pl.Utf8).alias("Mobile"),  # Using mobile_text as requested
        pl.col("email").cast(pl.Utf8).alias("EmailStd"),
        pl.col("suburb").cast(pl.Utf8).alias("Suburb"),
        pl.col("state").cast(pl.Utf8).alias("State"),
        pl.col("postcode_text").cast(pl.Utf8).alias("Postcode")  # Using postcode_text
    ])
    
    # Convert to pandas
    df1_pd = df1_clean.to_pandas()
    df2_pd = df2_clean.to_pandas()
    
    logger.info(f"Comparing {len(df1_pd.columns)} columns on full datasets")
    
    # Generate comparison report
    report = sv.compare([df1_pd, "DATADIRECT_DL"], [df2_pd, "ad_consumers"])
    
    output_path = Path("reports/html")
    output_path.mkdir(parents=True, exist_ok=True)
    
    report_file = output_path / "final_full_comparison.html"
    report.show_html(str(report_file))
    logger.info(f"Final full comparison saved to: {report_file}")

if __name__ == "__main__":
    final_comparison()