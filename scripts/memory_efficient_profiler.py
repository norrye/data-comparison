#!/usr/bin/env python3
"""Memory-efficient data profiling script for parquet files."""

import pandas as pd
from dataprofiler import Data, Profiler, ProfilerOptions
import json
from pathlib import Path
from loguru import logger

def profile_parquet_chunked(file_path: str, chunk_size: int = 50000, output_dir: str = "reports/tables"):
    """Generate data profile for a parquet file using chunked processing."""
    
    logger.info(f"Starting profile for: {file_path}")
    
    # Read parquet metadata first
    pf = pd.read_parquet(file_path, engine='pyarrow')
    total_rows = len(pf)
    
    logger.info(f"Total rows: {total_rows:,}")
    
    # Create profiler options to disable ML labeling
    options = ProfilerOptions()
    options.set({"data_labeler.is_enabled": False})
    
    if total_rows <= chunk_size:
        # Small file, process normally
        data = Data(file_path)
        profile = Profiler(data, options=options)
        report = profile.report(report_options={"output_format": "compact"})
    else:
        # Large file, use sample
        sample_data = pf.sample(n=min(chunk_size, total_rows), random_state=42)
        temp_file = f"/tmp/sample_{Path(file_path).stem}.parquet"
        sample_data.to_parquet(temp_file)
        
        data = Data(temp_file)
        profile = Profiler(data, options=options)
        report = profile.report(report_options={"output_format": "compact"})
        
        # Clean up temp file
        Path(temp_file).unlink()
    
    # Save report
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    file_name = Path(file_path).stem
    report_file = output_path / f"{file_name}_profile.json"
    
    with open(report_file, 'w') as f:
        json.dump(report, f, indent=2, default=str)
    
    logger.info(f"Profile saved to: {report_file}")
    return report

def main():
    """Profile both parquet files with memory efficiency."""
    base_path = Path("/data/projects/data_comparison")
    files = [
        "data/external/DATADIRECT_202501_20250801.parquet",
        "data/external/DATADIRECT_DL_202505_20250801.parquet"
    ]
    
    for file_path in files:
        full_path = base_path / file_path
        logger.info(f"Processing: {full_path}")
        try:
            profile_parquet_chunked(str(full_path))
        except Exception as e:
            logger.error(f"Failed to profile {full_path}: {e}")

if __name__ == "__main__":
    main()