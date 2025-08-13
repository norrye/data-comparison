# Detailed Match Analysis Report
## DataDirect vs AliveData Database Comparison

**Report Generated:** 2025-08-07 18:32:26  
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
| **DataDirect** | 5,000,000 | 25.9% |
| **AliveData** | 14,302,046 | 74.1% |
| **Combined Total** | 19,302,046 | 100.0% |

---

## Field-by-Field Analysis

### EmailStd

**Match Statistics:**
- **Total Matches:** 1,420,213
- **DataDirect Records with EmailStd:** 4,999,602
- **AliveData Records with EmailStd:** 14,302,046

**Exclusive Records:**
- **DataDirect Only:** 3,579,840 (71.6% of DD EmailStd records)
- **AliveData Only:** 13,028,361 (91.1% of AD EmailStd records)

**Match Rates:**
- **DataDirect Match Rate:** 28.41%
- **AliveData Match Rate:** 9.93%

**Statistical Measures:**
- **Overlap Coefficient:** 0.2841
- **Jaccard Index:** 0.0794

**Data Quality Assessment:**
- **Coverage in DataDirect:** 100.0%
- **Coverage in AliveData:** 100.0%

---

### EmailHash

**Match Statistics:**
- **Total Matches:** 1,506,462
- **DataDirect Records with EmailHash:** 4,999,724
- **AliveData Records with EmailHash:** 14,302,046

**Exclusive Records:**
- **DataDirect Only:** 3,493,713 (69.9% of DD EmailHash records)
- **AliveData Only:** 12,951,960 (90.6% of AD EmailHash records)

**Match Rates:**
- **DataDirect Match Rate:** 30.13%
- **AliveData Match Rate:** 10.53%

**Statistical Measures:**
- **Overlap Coefficient:** 0.3013
- **Jaccard Index:** 0.0847

**Data Quality Assessment:**
- **Coverage in DataDirect:** 100.0%
- **Coverage in AliveData:** 100.0%

---

### Mobile

**Match Statistics:**
- **Total Matches:** 1,502,490
- **DataDirect Records with Mobile:** 2,894,811
- **AliveData Records with Mobile:** 8,399,288

**Exclusive Records:**
- **DataDirect Only:** 1,766,009 (61.0% of DD Mobile records)
- **AliveData Only:** 7,251,713 (86.3% of AD Mobile records)

**Match Rates:**
- **DataDirect Match Rate:** 51.90%
- **AliveData Match Rate:** 17.89%

**Statistical Measures:**
- **Overlap Coefficient:** 0.5190
- **Jaccard Index:** 0.1534

**Data Quality Assessment:**
- **Coverage in DataDirect:** 57.9%
- **Coverage in AliveData:** 58.7%

---

### FullName

**Match Statistics:**
- **Total Matches:** 137,055,630
- **DataDirect Records with FullName:** 5,000,000
- **AliveData Records with FullName:** 14,189,272

**Exclusive Records:**
- **DataDirect Only:** 1,587,786 (31.8% of DD FullName records)
- **AliveData Only:** 8,105,576 (57.1% of AD FullName records)

**Match Rates:**
- **DataDirect Match Rate:** 2741.11%
- **AliveData Match Rate:** 965.91%

**Statistical Measures:**
- **Overlap Coefficient:** 27.4111
- **Jaccard Index:** 0.0000

**Data Quality Assessment:**
- **Coverage in DataDirect:** 100.0%
- **Coverage in AliveData:** 99.2%

---

## Summary Statistics

### Overall Matching Performance

| Metric | Value |
|--------|-------|
| **Average DataDirect Match Rate** | 712.89% |
| **Average AliveData Match Rate** | 251.07% |
| **Average Jaccard Index** | 0.0794 |
| **Best Performing Field** | FullName |
| **Highest DD Match Rate** | FullName (2741.11%) |
| **Highest AD Match Rate** | FullName (965.91%) |

### Field Comparison Table

| Field | Matches | DD Match Rate | AD Match Rate | Jaccard Index | DD Only | AD Only |
|-------|---------|---------------|---------------|---------------|---------|---------|
| EmailStd | 1,420,213 | 28.41% | 9.93% | 0.0794 | 3,579,840 | 13,028,361 |
| EmailHash | 1,506,462 | 30.13% | 10.53% | 0.0847 | 3,493,713 | 12,951,960 |
| Mobile | 1,502,490 | 51.90% | 17.89% | 0.1534 | 1,766,009 | 7,251,713 |
| FullName | 137,055,630 | 2741.11% | 965.91% | 0.0000 | 1,587,786 | 8,105,576 |

---

## Key Findings

### 1. Database Overlap Analysis
- **DataDirect Database Size:** 5,000,000 records
- **AliveData Database Size:** 14,302,046 records  
- **Size Ratio:** AliveData is 2.9x larger than DataDirect

### 2. Field-Specific Insights

- **Best Matching Field:** FullName with 137,055,630 matches
- **Lowest Matching Field:** EmailStd with 1,420,213 matches
- **Most Complete in DataDirect:** FullName
- **Most Complete in AliveData:** EmailStd

### 3. Data Quality Observations
- **EmailStd:** High match rate (28.4%) indicates good data quality
- **EmailHash:** High match rate (30.1%) indicates good data quality
- **Mobile:** High match rate (51.9%) indicates good data quality
- **FullName:** High match rate (2741.1%) indicates good data quality

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
