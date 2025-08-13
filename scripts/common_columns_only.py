#!/usr/bin/env python3
"""Compare only common columns that exist in both datasets."""

import polars as pl
import sweetviz as sv
from pathlib import Path
from loguru import logger

def compare_common_only():
    """Compare only columns that exist in both datasets."""
    base_path = Path("/data/projects/data_comparison")
    file1 = base_path / "data/external/DATADIRECT_DL_202505_20250801_subset.parquet"
    file2 = base_path / "data/interim/ad_consumers_2020805_inter.parquet"
    
    df1 = pl.read_parquet(file1)
    df2 = pl.read_parquet(file2)
    
    logger.info(f"Dataset 1 shape: {df1.shape}")
    logger.info(f"Dataset 2 shape: {df2.shape}")
    
    # Define mappings only for columns that exist
    mappings = [
        ("Title", "title"),
        ("FirstName", "given_name_1"),
        ("Surname", "surname"),
        ("Gender", "gender"),
        ("Mobile", "mobile_text"),
        ("Suburb", "suburb"),
        ("State", "state")
    ]
    
    # Check which mappings are valid
    valid_mappings = []
    for dd_col, ac_col in mappings:
        if dd_col in df1.columns and ac_col in df2.columns:
            valid_mappings.append((dd_col, ac_col))
            logger.info(f"✓ {dd_col} → {ac_col}")
        else:
            logger.warning(f"✗ {dd_col} → {ac_col} (missing)")
    
    if not valid_mappings:
        logger.error("No valid column mappings found")
        return
    
    # Select only valid columns
    dd_cols = [mapping[0] for mapping in valid_mappings]
    ac_cols = [mapping[1] for mapping in valid_mappings]
    
    df1_clean = df1.select([pl.col(col).cast(pl.Utf8) for col in dd_cols])
    df2_clean = df2.select([pl.col(col).cast(pl.Utf8) for col in ac_cols])
    
    # Rename df2 columns to match df1
    df2_clean = df2_clean.rename({ac_col: dd_col for dd_col, ac_col in valid_mappings})
    
    # Convert to pandas
    df1_pd = df1_clean.to_pandas()
    df2_pd = df2_clean.to_pandas()
    
    logger.info(f"Comparing {len(df1_pd.columns)} common columns")
    
    # Generate comparison
    report = sv.compare([df1_pd, "DATADIRECT_DL"], [df2_pd, "ad_consumers"])
    
    output_path = Path("reports/html")
    output_path.mkdir(parents=True, exist_ok=True)
    
    report_file = output_path / "common_columns_comparison.html"
    report.show_html(str(report_file))
    logger.info(f"Common columns comparison saved to: {report_file}")

if __name__ == "__main__":
    compare_common_only()