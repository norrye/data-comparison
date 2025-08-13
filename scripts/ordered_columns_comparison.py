#!/usr/bin/env python3
"""Load columns in the same order for proper Sweetviz comparison."""

import polars as pl
import sweetviz as sv
from pathlib import Path
from loguru import logger

def compare_ordered_columns():
    """Load and compare columns in the same order."""
    base_path = Path("/data/projects/data_comparison")
    file1 = base_path / "data/external/DATADIRECT_DL_202505_20250801_subset.parquet"
    file2 = base_path / "data/interim/ad_consumers_2020805_inter.parquet"
    
    # Define the exact column order
    column_order = ["Title", "FirstName", "Surname", "Gender", "Landline", "Mobile", "EmailStd", "Suburb", "State", "Postcode"]
    
    # Load ONLY needed columns from dataset 1 in specific order
    df1_ordered = pl.read_parquet(file1, columns=[
        "Title", "FirstName", "Surname", "Gender", "Landline", 
        "Mobile", "EmailStd", "Suburb", "State", "Postcode"
    ]).select([pl.col(col).cast(pl.Utf8) for col in column_order])
    
    # Load and map dataset 2 columns in the SAME order
    df2_ordered = pl.read_parquet(file2, columns=[
        "title", "given_name_1", "surname", "gender", "landline",
        "mobile_text", "email", "suburb", "state", "postcode_text"
    ]).select([
        pl.col("title").cast(pl.Utf8).alias("Title"),
        pl.col("given_name_1").cast(pl.Utf8).alias("FirstName"),
        pl.col("surname").cast(pl.Utf8).alias("Surname"),
        pl.col("gender").cast(pl.Utf8).alias("Gender"),
        pl.col("landline").cast(pl.Utf8).alias("Landline"),
        pl.col("mobile_text").cast(pl.Utf8).alias("Mobile"),
        pl.col("email").cast(pl.Utf8).alias("EmailStd"),
        pl.col("suburb").cast(pl.Utf8).alias("Suburb"),
        pl.col("state").cast(pl.Utf8).alias("State"),
        pl.col("postcode_text").cast(pl.Utf8).alias("Postcode")
    ])
    
    logger.info(f"Dataset 1 shape: {df1_ordered.shape}")
    logger.info(f"Dataset 2 shape: {df2_ordered.shape}")
    logger.info(f"Column order: {df1_ordered.columns}")
    
    # Convert to pandas
    df1_pd = df1_ordered.to_pandas()
    df2_pd = df2_ordered.to_pandas()
    
    # Generate comparison
    report = sv.compare([df1_pd, "DATADIRECT_DL"], [df2_pd, "ad_consumers"])
    
    output_path = Path("reports/html")
    output_path.mkdir(parents=True, exist_ok=True)
    
    report_file = output_path / "ordered_columns_comparison.html"
    report.show_html(str(report_file))
    logger.info(f"Ordered columns comparison saved to: {report_file}")

if __name__ == "__main__":
    compare_ordered_columns()