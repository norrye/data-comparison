#!/usr/bin/env python3
"""Comprehensive matching analysis with direct matches, anti-joins, and concatenated fields."""

import duckdb
from pathlib import Path
from loguru import logger
import os

def comprehensive_match_analysis():
    """Perform comprehensive matching analysis between datasets."""
    base_path = Path("/data/projects/data_comparison")
    file1 = base_path / "data/external/DATADIRECT_DL_202505_20250801_subset.parquet"
    file2 = base_path / "data/interim/ad_consumers_2020805_inter.parquet"
    
    conn = duckdb.connect()
    conn.execute(f"SET threads = {max(1, os.cpu_count() // 2)}")
    
    # Get record counts
    dd_count = conn.execute(f"SELECT COUNT(*) FROM read_parquet('{file1}')").fetchone()[0]
    ac_count = conn.execute(f"SELECT COUNT(*) FROM read_parquet('{file2}')").fetchone()[0]
    
    logger.info("Starting comprehensive matching analysis")
    print("\n=== COMPREHENSIVE MATCHING ANALYSIS ===")
    print(f"DataDirect: {dd_count:,} records")
    print(f"Ad_consumers: {ac_count:,} records")
    logger.info(f"DataDirect: {dd_count:,} records, Ad_consumers: {ac_count:,} records")
    
    # Direct field matches with anti-joins
    direct_matches = [
        ("FirstName", "UPPER(TRIM(d.FirstName)) = UPPER(TRIM(a.given_name_1))", "d.FirstName IS NOT NULL AND a.given_name_1 IS NOT NULL"),
        ("LastName", "UPPER(TRIM(d.Surname)) = UPPER(TRIM(a.surname))", "d.Surname IS NOT NULL AND a.surname IS NOT NULL"),
        ("FullName", "UPPER(TRIM(d.FirstName || ' ' || d.Surname)) = UPPER(TRIM(a.given_name_1 || ' ' || a.surname))", "d.FirstName IS NOT NULL AND d.Surname IS NOT NULL AND a.given_name_1 IS NOT NULL AND a.surname IS NOT NULL"),
        ("Email", "UPPER(TRIM(d.EmailStd)) = UPPER(TRIM(a.email))", "d.EmailStd IS NOT NULL AND a.email IS NOT NULL"),
        ("Mobile", "TRIM(d.Mobile) = TRIM(a.mobile_text)", "d.Mobile IS NOT NULL AND a.mobile_text IS NOT NULL"),
        ("Landline", "TRIM(d.Landline) = TRIM(a.landline)", "d.Landline IS NOT NULL AND a.landline IS NOT NULL")
    ]
    
    logger.info("Processing direct field matches")
    print("\n=== DIRECT FIELD MATCHES ===")
    for match_name, condition, filter_cond in direct_matches:
        # Inner join count
        matches = conn.execute(f"""
            SELECT COUNT(*) FROM read_parquet('{file1}') d
            INNER JOIN read_parquet('{file2}') a ON {condition}
            WHERE {filter_cond}
        """).fetchone()[0]
        
        # Anti-join DD -> AC (records in DD but not in AC)
        dd_filter = filter_cond.split(' AND a.')[0].replace('a.', 'd.')
        dd_only = conn.execute(f"""
            SELECT COUNT(*) FROM read_parquet('{file1}') d
            WHERE NOT EXISTS (
                SELECT 1 FROM read_parquet('{file2}') a 
                WHERE {condition} AND {filter_cond}
            ) AND {dd_filter}
        """).fetchone()[0]
        
        # Anti-join AC -> DD (records in AC but not in DD)
        ac_filter = filter_cond.split(' AND d.')[0].replace('d.', 'a.')
        ac_only = conn.execute(f"""
            SELECT COUNT(*) FROM read_parquet('{file2}') a
            WHERE NOT EXISTS (
                SELECT 1 FROM read_parquet('{file1}') d 
                WHERE {condition} AND {filter_cond}
            ) AND {ac_filter}
        """).fetchone()[0]
        
        print(f"{match_name:12}: Matches={matches:,}, DD_only={dd_only:,}, AC_only={ac_only:,}")
    
    # Concatenated field combinations
    concat_matches = [
        ("Name+Suburb+Postcode", "UPPER(TRIM(d.FirstName || ' ' || d.Surname || ' ' || d.Suburb || ' ' || d.Postcode)) = UPPER(TRIM(a.given_name_1 || ' ' || a.surname || ' ' || a.suburb || ' ' || a.postcode_text))"),
        ("Name+Suburb+Postcode+Email", "UPPER(TRIM(d.FirstName || ' ' || d.Surname || ' ' || d.Suburb || ' ' || d.Postcode || ' ' || d.EmailStd)) = UPPER(TRIM(a.given_name_1 || ' ' || a.surname || ' ' || a.suburb || ' ' || a.postcode_text || ' ' || a.email))"),
        ("Name+Suburb+Postcode+Mobile", "UPPER(TRIM(d.FirstName || ' ' || d.Surname || ' ' || d.Suburb || ' ' || d.Postcode || ' ' || d.Mobile)) = UPPER(TRIM(a.given_name_1 || ' ' || a.surname || ' ' || a.suburb || ' ' || a.postcode_text || ' ' || a.mobile_text))"),
        ("Name+Suburb+Postcode+Landline", "UPPER(TRIM(d.FirstName || ' ' || d.Surname || ' ' || d.Suburb || ' ' || d.Postcode || ' ' || d.Landline)) = UPPER(TRIM(a.given_name_1 || ' ' || a.surname || ' ' || a.suburb || ' ' || a.postcode_text || ' ' || a.landline))")
    ]
    
    logger.info("Processing concatenated field matches")
    print("\n=== CONCATENATED FIELD MATCHES ===")
    for match_name, condition in concat_matches:
        # Inner join count
        matches = conn.execute(f"""
            SELECT COUNT(*) FROM read_parquet('{file1}') d
            INNER JOIN read_parquet('{file2}') a ON {condition}
        """).fetchone()[0]
        
        # Anti-join DD -> AC
        dd_only = conn.execute(f"""
            SELECT COUNT(*) FROM read_parquet('{file1}') d
            WHERE NOT EXISTS (
                SELECT 1 FROM read_parquet('{file2}') a WHERE {condition}
            )
        """).fetchone()[0]
        
        # Anti-join AC -> DD
        ac_only = conn.execute(f"""
            SELECT COUNT(*) FROM read_parquet('{file2}') a
            WHERE NOT EXISTS (
                SELECT 1 FROM read_parquet('{file1}') d WHERE {condition}
            )
        """).fetchone()[0]
        
        print(f"{match_name:25}: Matches={matches:,}, DD_only={dd_only:,}, AC_only={ac_only:,}")
    
    # Final concatenation of all fields
    logger.info("Processing all fields concatenated")
    print("\n=== ALL FIELDS CONCATENATED ===")
    all_fields_condition = """
        UPPER(TRIM(COALESCE(d.Title,'') || ' ' || COALESCE(d.FirstName,'') || ' ' || COALESCE(d.Surname,'') || ' ' || COALESCE(d.Gender,'') || ' ' || 
                   COALESCE(d.Landline,'') || ' ' || COALESCE(d.Mobile,'') || ' ' || COALESCE(d.EmailStd,'') || ' ' || 
                   COALESCE(d.Suburb,'') || ' ' || COALESCE(d.State,'') || ' ' || COALESCE(d.Postcode,''))) = 
        UPPER(TRIM(COALESCE(a.title,'') || ' ' || COALESCE(a.given_name_1,'') || ' ' || COALESCE(a.surname,'') || ' ' || COALESCE(a.gender,'') || ' ' || 
                   COALESCE(a.landline,'') || ' ' || COALESCE(a.mobile_text,'') || ' ' || COALESCE(a.email,'') || ' ' || 
                   COALESCE(a.suburb,'') || ' ' || COALESCE(a.state,'') || ' ' || COALESCE(a.postcode_text,'')))
    """
    
    # All fields match
    all_matches = conn.execute(f"""
        SELECT COUNT(*) FROM read_parquet('{file1}') d
        INNER JOIN read_parquet('{file2}') a ON {all_fields_condition}
    """).fetchone()[0]
    
    # Anti-join DD -> AC
    dd_all_only = conn.execute(f"""
        SELECT COUNT(*) FROM read_parquet('{file1}') d
        WHERE NOT EXISTS (
            SELECT 1 FROM read_parquet('{file2}') a WHERE {all_fields_condition}
        )
    """).fetchone()[0]
    
    # Anti-join AC -> DD
    ac_all_only = conn.execute(f"""
        SELECT COUNT(*) FROM read_parquet('{file2}') a
        WHERE NOT EXISTS (
            SELECT 1 FROM read_parquet('{file1}') d WHERE {all_fields_condition}
        )
    """).fetchone()[0]
    
    print(f"All Fields      : Matches={all_matches:,}, DD_only={dd_all_only:,}, AC_only={ac_all_only:,}")
    
    # Summary statistics
    logger.info("Generating summary statistics")
    print("\n=== SUMMARY STATISTICS ===")
    print(f"Total DD records: {dd_count:,}")
    print(f"Total AC records: {ac_count:,}")
    print(f"Perfect matches (all fields): {all_matches:,} ({(all_matches/min(dd_count, ac_count))*100:.2f}%)")
    print(f"DD unique records: {dd_all_only:,} ({(dd_all_only/dd_count)*100:.2f}%)")
    print(f"AC unique records: {ac_all_only:,} ({(ac_all_only/ac_count)*100:.2f}%)")
    
    conn.close()
    logger.info("Analysis complete.")

if __name__ == "__main__":
    comprehensive_match_analysis()