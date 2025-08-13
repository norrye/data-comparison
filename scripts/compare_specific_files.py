#!/usr/bin/env python3
"""Compare specific files with full datasets."""

import polars as pl
import sweetviz as sv
import json
from pathlib import Path
from loguru import logger

def compare_full_datasets(file1: str, file2: str, output_dir: str = "reports/html"):
    """Compare full datasets without sampling."""
    logger.info(f"Loading full datasets for comparison")
    
    # Load full datasets
    df1_pl = pl.read_parquet(file1)
    df2_pl = pl.read_parquet(file2)
    
    logger.info(f"Dataset 1 shape: {df1_pl.shape}")
    logger.info(f"Dataset 2 shape: {df2_pl.shape}")
    
    # Convert to pandas
    df1_pd = df1_pl.to_pandas()
    df2_pd = df2_pl.to_pandas()
    
    # Find common columns
    common_cols = set(df1_pd.columns) & set(df2_pd.columns)
    logger.info(f"Common columns: {len(common_cols)}")
    
    if common_cols:
        df1_common = df1_pd[list(common_cols)]
        df2_common = df2_pd[list(common_cols)]
        
        # Generate comparison report
        report = sv.compare([df1_common, "DATADIRECT_DL"], [df2_common, "ad_consumers"])
        
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        report_file = output_path / "datadirect_vs_adconsumers_comparison.html"
        report.show_html(str(report_file))
        logger.info(f"Comparison report saved to: {report_file}")
        
        # Calculate similarity metrics
        column_overlap = len(common_cols) / max(len(df1_pd.columns), len(df2_pd.columns))
        
        metrics = {
            "dataset1_shape": df1_pl.shape,
            "dataset2_shape": df2_pl.shape,
            "common_columns": len(common_cols),
            "unique_to_dataset1": len(set(df1_pd.columns) - set(df2_pd.columns)),
            "unique_to_dataset2": len(set(df2_pd.columns) - set(df1_pd.columns)),
            "column_overlap_ratio": column_overlap,
            "decision": "SIMILAR" if column_overlap > 0.7 else "DIFFERENT" if column_overlap < 0.3 else "PARTIALLY_SIMILAR"
        }
        
        # Print results
        print(f"\n{'='*60}")
        print(f"DATASET COMPARISON: DATADIRECT_DL vs ad_consumers")
        print(f"{'='*60}")
        print(f"Dataset 1 (DATADIRECT_DL): {metrics['dataset1_shape']}")
        print(f"Dataset 2 (ad_consumers): {metrics['dataset2_shape']}")
        print(f"Common Columns: {metrics['common_columns']}")
        print(f"Column Overlap: {metrics['column_overlap_ratio']:.2%}")
        print(f"Decision: {metrics['decision']}")
        
        return metrics
    else:
        logger.error("No common columns found between datasets")
        return None

def main():
    """Compare the specified datasets."""
    base_path = Path("/data/projects/data_comparison")
    file1 = base_path / "data/external/DATADIRECT_DL_202505_20250801_subset.parquet"
    file2 = base_path / "data/interim/ad_consumers_2020805_inter.parquet"
    
    compare_full_datasets(str(file1), str(file2))

if __name__ == "__main__":
    main()