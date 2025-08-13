#!/usr/bin/env python3
"""
Detailed Match Report Generator - DataDirect vs AliveData Analysis

Version: 1.0
Author: Expert Data Scientist
Description: Generates comprehensive statistical report on database matching
"""

import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple

import duckdb
from loguru import logger
from pydantic import BaseModel, Field


class MatchStatistics(BaseModel):
    """Statistical analysis model for field matching."""
    field_name: str
    total_matches: int
    dd_total_records: int
    ad_total_records: int
    dd_only_records: int
    ad_only_records: int
    dd_match_rate: float
    ad_match_rate: float
    overlap_coefficient: float
    jaccard_index: float


def generate_detailed_match_report() -> None:
    """Generate comprehensive match analysis report."""
    start_time = time.time()
    logger.info("=" * 80)
    logger.info("GENERATING DETAILED MATCH REPORT - DATADIRECT vs ALIVEDATA")
    logger.info(f"Report generation started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 80)

    base_path = Path("/data/projects/data_comparison")
    db_path = base_path / "data/processed/match_analysis.duckdb"
    report_path = base_path / "reports/detailed_match_analysis_report.md"
    
    # Ensure reports directory exists
    report_path.parent.mkdir(parents=True, exist_ok=True)
    
    conn = duckdb.connect(str(db_path))
    conn.execute("SET threads = 4")
    
    # Get dataset overview
    dd_count = conn.execute("SELECT COUNT(*) FROM datadirect").fetchone()[0]
    ad_count = conn.execute("SELECT COUNT(*) FROM ad_consumers").fetchone()[0]
    
    logger.info(f"DataDirect: {dd_count:,} records, AliveData: {ad_count:,} records")
    
    # Key matching fields
    match_fields = [
        ("EmailStd", "d.email_std = a.email_std", "Email Standard"),
        ("EmailHash", "d.email_hash = a.email_hash", "Email Hash (SHA256)"),
        ("Mobile", "d.mobile = a.mobile", "Mobile Phone"),
        ("FullName", "d.full_name = a.full_name", "Full Name")
    ]
    
    statistics: List[MatchStatistics] = []
    
    # Analyze each field
    for field_name, condition, description in match_fields:
        logger.info(f"Analyzing {field_name} matches...")
        
        # Get field-specific record counts
        dd_field_count = conn.execute(f"""
            SELECT COUNT(*) FROM datadirect 
            WHERE {condition.split(' = ')[0].replace('d.', '')} IS NOT NULL
        """).fetchone()[0]
        
        ad_field_count = conn.execute(f"""
            SELECT COUNT(*) FROM ad_consumers 
            WHERE {condition.split(' = ')[1].replace('a.', '')} IS NOT NULL
        """).fetchone()[0]
        
        # Get matches
        matches = conn.execute(f"""
            SELECT COUNT(*) FROM datadirect d
            INNER JOIN ad_consumers a ON {condition}
            WHERE {condition.split(' = ')[0]} IS NOT NULL 
            AND {condition.split(' = ')[1]} IS NOT NULL
        """).fetchone()[0]
        
        # Get DataDirect-only records
        dd_field = condition.split(' = ')[0].replace('d.', '')
        dd_only = conn.execute(f"""
            SELECT COUNT(*) FROM datadirect d
            ANTI JOIN ad_consumers a ON {condition}
            WHERE d.{dd_field} IS NOT NULL
        """).fetchone()[0]
        
        # Get AliveData-only records
        ad_field = condition.split(' = ')[1].replace('a.', '')
        ad_only = conn.execute(f"""
            SELECT COUNT(*) FROM ad_consumers a
            ANTI JOIN datadirect d ON {condition}
            WHERE a.{ad_field} IS NOT NULL
        """).fetchone()[0]
        
        # Calculate statistics
        dd_match_rate = (matches / dd_field_count * 100) if dd_field_count > 0 else 0
        ad_match_rate = (matches / ad_field_count * 100) if ad_field_count > 0 else 0
        
        # Overlap coefficient: |A ∩ B| / min(|A|, |B|)
        overlap_coeff = (matches / min(dd_field_count, ad_field_count)) if min(dd_field_count, ad_field_count) > 0 else 0
        
        # Jaccard index: |A ∩ B| / |A ∪ B|
        union_size = dd_field_count + ad_field_count - matches
        jaccard_idx = (matches / union_size) if union_size > 0 else 0
        
        stats = MatchStatistics(
            field_name=field_name,
            total_matches=matches,
            dd_total_records=dd_field_count,
            ad_total_records=ad_field_count,
            dd_only_records=dd_only,
            ad_only_records=ad_only,
            dd_match_rate=dd_match_rate,
            ad_match_rate=ad_match_rate,
            overlap_coefficient=overlap_coeff,
            jaccard_index=jaccard_idx
        )
        statistics.append(stats)
        
        logger.info(f"{field_name}: {matches:,} matches, {dd_only:,} DD-only, {ad_only:,} AD-only")
    
    # Generate detailed report
    report_content = generate_report_content(statistics, dd_count, ad_count)
    
    # Write report to file
    with open(report_path, 'w') as f:
        f.write(report_content)
    
    conn.close()
    total_time = time.time() - start_time
    
    logger.info(f"Report generated successfully: {report_path}")
    logger.info(f"Total processing time: {total_time:.2f} seconds")
    
    print(f"\n✓ Detailed match report generated: {report_path}")
    print(f"Processing time: {total_time:.2f} seconds")


def generate_report_content(statistics: List[MatchStatistics], dd_total: int, ad_total: int) -> str:
    """Generate the detailed markdown report content."""
    
    report_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    content = f"""# Detailed Match Analysis Report
## DataDirect vs AliveData Database Comparison

**Report Generated:** {report_date}  
**Analysis Version:** 1.0  

---

## Executive Summary

This report provides a comprehensive analysis of record matching between the DataDirect and AliveData databases, focusing on three key areas:

1. **Matched Records**: Records that exist in both databases with identical field values
2. **DataDirect Exclusive Records**: Records that exist only in DataDirect database
3. **AliveData Exclusive Records**: Records that exist only in AliveData database

---

## Dataset Overview

| Database | Total Records | Percentage of Combined Dataset |
|----------|---------------|-------------------------------|
| **DataDirect** | {dd_total:,} | {(dd_total/(dd_total+ad_total)*100):.1f}% |
| **AliveData** | {ad_total:,} | {(ad_total/(dd_total+ad_total)*100):.1f}% |
| **Combined Total** | {dd_total + ad_total:,} | 100.0% |

---

## Field-by-Field Analysis

"""

    for stat in statistics:
        content += f"""### {stat.field_name}

**Match Statistics:**
- **Total Matches:** {stat.total_matches:,}
- **DataDirect Records with {stat.field_name}:** {stat.dd_total_records:,}
- **AliveData Records with {stat.field_name}:** {stat.ad_total_records:,}

**Exclusive Records:**
- **DataDirect Only:** {stat.dd_only_records:,} ({(stat.dd_only_records/stat.dd_total_records*100):.1f}% of DD {stat.field_name} records)
- **AliveData Only:** {stat.ad_only_records:,} ({(stat.ad_only_records/stat.ad_total_records*100):.1f}% of AD {stat.field_name} records)

**Match Rates:**
- **DataDirect Match Rate:** {stat.dd_match_rate:.2f}%
- **AliveData Match Rate:** {stat.ad_match_rate:.2f}%

**Statistical Measures:**
- **Overlap Coefficient:** {stat.overlap_coefficient:.4f}
- **Jaccard Index:** {stat.jaccard_index:.4f}

**Data Quality Assessment:**
- **Coverage in DataDirect:** {(stat.dd_total_records/dd_total*100):.1f}%
- **Coverage in AliveData:** {(stat.ad_total_records/ad_total*100):.1f}%

---

"""

    # Summary statistics
    total_unique_matches = sum(s.total_matches for s in statistics)
    avg_dd_match_rate = sum(s.dd_match_rate for s in statistics) / len(statistics)
    avg_ad_match_rate = sum(s.ad_match_rate for s in statistics) / len(statistics)
    avg_jaccard = sum(s.jaccard_index for s in statistics) / len(statistics)

    content += f"""## Summary Statistics

### Overall Matching Performance

| Metric | Value |
|--------|-------|
| **Average DataDirect Match Rate** | {avg_dd_match_rate:.2f}% |
| **Average AliveData Match Rate** | {avg_ad_match_rate:.2f}% |
| **Average Jaccard Index** | {avg_jaccard:.4f} |
| **Best Performing Field** | {max(statistics, key=lambda x: x.total_matches).field_name} |
| **Highest DD Match Rate** | {max(statistics, key=lambda x: x.dd_match_rate).field_name} ({max(s.dd_match_rate for s in statistics):.2f}%) |
| **Highest AD Match Rate** | {max(statistics, key=lambda x: x.ad_match_rate).field_name} ({max(s.ad_match_rate for s in statistics):.2f}%) |

### Field Comparison Table

| Field | Matches | DD Match Rate | AD Match Rate | Jaccard Index | DD Only | AD Only |
|-------|---------|---------------|---------------|---------------|---------|---------|
"""

    for stat in statistics:
        content += f"| {stat.field_name} | {stat.total_matches:,} | {stat.dd_match_rate:.2f}% | {stat.ad_match_rate:.2f}% | {stat.jaccard_index:.4f} | {stat.dd_only_records:,} | {stat.ad_only_records:,} |\n"

    content += f"""
---

## Key Findings

### 1. Database Overlap Analysis
- **DataDirect Database Size:** {dd_total:,} records
- **AliveData Database Size:** {ad_total:,} records  
- **Size Ratio:** AliveData is {(ad_total/dd_total):.1f}x larger than DataDirect

### 2. Field-Specific Insights
"""

    # Find best and worst performing fields
    best_field = max(statistics, key=lambda x: x.total_matches)
    worst_field = min(statistics, key=lambda x: x.total_matches)
    
    content += f"""
- **Best Matching Field:** {best_field.field_name} with {best_field.total_matches:,} matches
- **Lowest Matching Field:** {worst_field.field_name} with {worst_field.total_matches:,} matches
- **Most Complete in DataDirect:** {max(statistics, key=lambda x: x.dd_total_records/dd_total).field_name}
- **Most Complete in AliveData:** {max(statistics, key=lambda x: x.ad_total_records/ad_total).field_name}

### 3. Data Quality Observations
"""

    for stat in statistics:
        if stat.dd_match_rate > 25:
            content += f"- **{stat.field_name}:** High match rate ({stat.dd_match_rate:.1f}%) indicates good data quality\n"
        elif stat.dd_match_rate < 10:
            content += f"- **{stat.field_name}:** Low match rate ({stat.dd_match_rate:.1f}%) suggests data quality issues\n"

    content += f"""
---

## Recommendations

### Data Integration Strategy
1. **Primary Matching Fields:** Use EmailHash and Mobile as primary matching keys
2. **Secondary Matching:** Consider FullName for fuzzy matching scenarios
3. **Data Enrichment:** Leverage AliveData's larger dataset to enrich DataDirect records

### Data Quality Improvements
1. **Standardization:** Implement consistent data formatting across both databases
2. **Validation:** Add data validation rules for email and mobile number formats
3. **Deduplication:** Address potential duplicate records within each database

### Technical Implementation
1. **Indexing:** Maintain indexes on matching fields for performance
2. **Monitoring:** Set up regular matching analysis to track data quality trends
3. **Documentation:** Document field mapping and transformation rules

---

## Technical Details

**Analysis Method:** DuckDB ANTI JOIN operations  
**Processing Time:** Optimized with sorted data and field indexes  
**Data Sources:**
- DataDirect: DATADIRECT_DL_202505_20250801_subset.parquet
- AliveData: ad_consumers_2020805_inter_ll_text.parquet

**Statistical Definitions:**
- **Overlap Coefficient:** Measures similarity as |A ∩ B| / min(|A|, |B|)
- **Jaccard Index:** Measures similarity as |A ∩ B| / |A ∪ B|
- **Match Rate:** Percentage of records in dataset that have matches in other dataset

---

*Report generated by Expert Data Scientist using DuckDB analysis engine*
"""

    return content


if __name__ == "__main__":
    generate_detailed_match_report()