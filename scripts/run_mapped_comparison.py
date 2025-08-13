#!/usr/bin/env python3
"""Run comparison with mapped columns."""

import polars as pl
import sweetviz as sv
from pathlib import Path
from loguru import logger

def compare_with_mapping(sample_size: int = 200000):
    """Compare datasets using column mapping."""
    base_path = Path("/data/projects/data_comparison")
    file1 = base_path / "data/external/DATADIRECT_DL_202505_20250801_subset.parquet"
    file2 = base_path / "data/interim/ad_consumers_2020805_inter.parquet"
    
    # Load and sample datasets
    df1 = pl.read_parquet(file1).sample(n=sample_size, seed=42)
    df2 = pl.read_parquet(file2).sample(n=sample_size, seed=42)
    
    logger.info(f"Dataset 1 shape: {df1.shape}")
    logger.info(f"Dataset 2 shape: {df2.shape}")
    
    # Column mappings
    mappings = {
        'Title': 'title',
        'FirstName': 'given_name_1', 
        'Surname': 'surname',
        'Gender': 'gender',
        'Landline': 'landline',
        'Mobile': 'mobile',
        'EmailStd': 'email',
        'Suburb': 'suburb',
        'State': 'state', 
        'Postcode': 'postcode'
    }
    
    # Create mapped dataframes
    df1_mapped = df1.select([col for col in mappings.keys() if col in df1.columns])
    df2_mapped = df2.select([mappings[col] for col in mappings.keys() if col in df1.columns])
    
    # Rename df2 columns to match df1
    rename_dict = {mappings[col]: col for col in mappings.keys() if col in df1.columns}
    df2_mapped = df2_mapped.rename(rename_dict)
    
    # Convert to pandas
    df1_pd = df1_mapped.to_pandas()
    df2_pd = df2_mapped.to_pandas()
    
    logger.info(f"Comparing {len(df1_pd.columns)} mapped columns")
    
    # Generate comparison report
    report = sv.compare([df1_pd, "DATADIRECT_DL"], [df2_pd, "ad_consumers"])
    
    output_path = Path("reports/html")
    output_path.mkdir(parents=True, exist_ok=True)
    
    report_file = output_path / "mapped_dataset_comparison.html"
    report.show_html(str(report_file))
    logger.info(f"Mapped comparison report saved to: {report_file}")

if __name__ == "__main__":
    compare_with_mapping()