#!/usr/bin/env python3
"""Load only the needed columns from each dataset."""

import polars as pl
import sweetviz as sv
from pathlib import Path
from loguru import logger

def compare_only_needed():
    """Load and compare only the needed columns."""
    base_path = Path("/data/projects/data_comparison")
    file1 = base_path / "data/external/DATADIRECT_DL_202505_20250801_subset.parquet"
    file2 = base_path / "data/interim/ad_consumers_2020805_inter.parquet"
    
    # Load ONLY the columns we need from each dataset
    df1_needed = pl.read_parquet(file1, columns=[
        "Title", "FirstName", "Surname", "Gender", "Landline", 
        "Mobile", "EmailStd", "Suburb", "State", "Postcode"
    ])
    
    df2_needed = pl.read_parquet(file2, columns=[
        "title", "given_name_1", "surname", "gender", "landline",
        "mobile_text", "email", "suburb", "state", "postcode_text"
    ])
    
    logger.info(f"Dataset 1 loaded shape: {df1_needed.shape}")
    logger.info(f"Dataset 2 loaded shape: {df2_needed.shape}")
    
    # Cast to string and rename df2 to match df1
    df1_clean = df1_needed.select([pl.col(col).cast(pl.Utf8) for col in df1_needed.columns])
    
    df2_clean = df2_needed.select([
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
    
    # Convert to pandas
    df1_pd = df1_clean.to_pandas()
    df2_pd = df2_clean.to_pandas()
    
    logger.info(f"Comparing {len(df1_pd.columns)} columns")
    
    # Generate comparison
    report = sv.compare([df1_pd, "DATADIRECT_DL"], [df2_pd, "ad_consumers"])
    
    output_path = Path("reports/html")
    output_path.mkdir(parents=True, exist_ok=True)
    
    report_file = output_path / "only_needed_columns.html"
    report.show_html(str(report_file))
    logger.info(f"Only needed columns comparison saved to: {report_file}")

if __name__ == "__main__":
    compare_only_needed()