#!/usr/bin/env python3
"""
VSS Name Similarity Analysis Runner

Executes vector similarity search analysis for name matching between datasets.
"""

import sys
from pathlib import Path

# Add src to path
sys.path.append(str(Path(__file__).parent.parent / "src"))

from analysis.vss_name_similarity import NameSimilarityAnalyzer, VSSConfig
from loguru import logger


def main():
    """Run VSS name similarity analysis."""
    logger.info("Starting VSS Name Similarity Analysis")
    
    # Configuration
    config = VSSConfig(
        max_records=250000,
        similarity_threshold=0.90,
        batch_size=10000,
        max_results_per_name=5,
        enable_preprocessing=True,
        detailed_analysis=True
    )
    
    db_path = "/data/projects/data_comparison/data/processed/match_analysis.duckdb"
    
    # Check if database exists
    if not Path(db_path).exists():
        logger.error(f"Database not found: {db_path}")
        logger.info("Please run the data loading script first")
        return
    
    analyzer = NameSimilarityAnalyzer(config, db_path)
    
    try:
        logger.info("Generating comprehensive VSS similarity analysis...")
        report = analyzer.generate_comprehensive_fault_analysis()
        
        # Save report
        report_path = Path("/data/projects/data_comparison/reports/vss_comprehensive_fault_analysis.md")
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(report)
        
        logger.success(f"Comprehensive VSS fault analysis saved to {report_path}")
        print("\n" + "="*80)
        print("VSS COMPREHENSIVE FAULT ANALYSIS COMPLETE")
        print("100,000 Records | Threshold: 0.85 | Batch Size: 5,000")
        print("="*80)
        print(report)
        
    except Exception as e:
        logger.error(f"VSS analysis failed: {e}")
        raise
    finally:
        analyzer.close()


if __name__ == "__main__":
    main()