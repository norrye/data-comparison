#!/usr/bin/env python3
"""Dataset similarity decision framework."""

import polars as pl
import json
from pathlib import Path
from loguru import logger

def analyze_similarity_metrics(file1: str, file2: str, sample_size: int = 100000):
    """Calculate quantitative similarity metrics."""
    
    # Load datasets
    df1 = pl.read_parquet(file1).sample(n=sample_size, seed=42)
    df2 = pl.read_parquet(file2).sample(n=sample_size, seed=42)
    
    # Basic structure comparison
    common_cols = set(df1.columns) & set(df2.columns)
    unique_cols_1 = set(df1.columns) - set(df2.columns)
    unique_cols_2 = set(df2.columns) - set(df1.columns)
    
    # Calculate similarity scores
    column_overlap = len(common_cols) / max(len(df1.columns), len(df2.columns))
    
    # Data type similarity for common columns
    dtype_matches = 0
    for col in common_cols:
        if df1[col].dtype == df2[col].dtype:
            dtype_matches += 1
    dtype_similarity = dtype_matches / len(common_cols) if common_cols else 0
    
    # Statistical similarity for numeric columns
    numeric_similarity = []
    for col in common_cols:
        if df1[col].dtype in [pl.Int64, pl.Float64, pl.Int32, pl.Float32]:
            try:
                stats1 = df1[col].describe()
                stats2 = df2[col].describe()
                # Compare means (normalized difference)
                mean1 = stats1.filter(pl.col("statistic") == "mean")["value"][0]
                mean2 = stats2.filter(pl.col("statistic") == "mean")["value"][0]
                if mean1 != 0 or mean2 != 0:
                    diff = abs(mean1 - mean2) / max(abs(mean1), abs(mean2), 1)
                    numeric_similarity.append(1 - min(diff, 1))
            except:
                continue
    
    avg_numeric_similarity = sum(numeric_similarity) / len(numeric_similarity) if numeric_similarity else 0
    
    # Overall similarity score
    overall_similarity = (column_overlap * 0.4 + dtype_similarity * 0.3 + avg_numeric_similarity * 0.3)
    
    return {
        "column_overlap_ratio": column_overlap,
        "dtype_similarity": dtype_similarity,
        "numeric_similarity": avg_numeric_similarity,
        "overall_similarity": overall_similarity,
        "common_columns": len(common_cols),
        "unique_to_dataset1": len(unique_cols_1),
        "unique_to_dataset2": len(unique_cols_2),
        "decision": "SIMILAR" if overall_similarity > 0.7 else "DIFFERENT" if overall_similarity < 0.3 else "PARTIALLY_SIMILAR"
    }

def generate_similarity_report(file1: str, file2: str):
    """Generate comprehensive similarity report."""
    logger.info("Analyzing dataset similarity...")
    
    metrics = analyze_similarity_metrics(file1, file2)
    
    # Save report
    output_path = Path("reports/tables")
    output_path.mkdir(parents=True, exist_ok=True)
    
    report_file = output_path / "similarity_analysis.json"
    with open(report_file, 'w') as f:
        json.dump(metrics, f, indent=2)
    
    # Print decision summary
    print(f"\n{'='*50}")
    print(f"DATASET SIMILARITY ANALYSIS")
    print(f"{'='*50}")
    print(f"Overall Similarity Score: {metrics['overall_similarity']:.2%}")
    print(f"Decision: {metrics['decision']}")
    print(f"\nDetailed Metrics:")
    print(f"- Column Overlap: {metrics['column_overlap_ratio']:.2%}")
    print(f"- Data Type Similarity: {metrics['dtype_similarity']:.2%}")
    print(f"- Numeric Value Similarity: {metrics['numeric_similarity']:.2%}")
    print(f"\nStructural Analysis:")
    print(f"- Common Columns: {metrics['common_columns']}")
    print(f"- Unique to Dataset 1: {metrics['unique_to_dataset1']}")
    print(f"- Unique to Dataset 2: {metrics['unique_to_dataset2']}")
    
    return metrics

def main():
    """Run similarity analysis."""
    base_path = Path("/data/projects/data_comparison")
    file1 = base_path / "data/external/DATADIRECT_202501_20250801.parquet"
    file2 = base_path / "data/external/DATADIRECT_DL_202505_20250801.parquet"
    
    generate_similarity_report(str(file1), str(file2))

if __name__ == "__main__":
    main()