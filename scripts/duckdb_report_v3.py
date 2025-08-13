#!/usr/bin/env python3
"""DuckDB report v3.0 - Query parquet files directly with anti-joins."""

import duckdb
from pathlib import Path
from loguru import logger
import os

def generate_parquet_report():
    """Generate report querying parquet files directly."""
    base_path = Path("/data/projects/data_comparison")
    file1 = base_path / "data/external/DATADIRECT_DL_202505_20250801_subset.parquet"
    file2 = base_path / "data/interim/ad_consumers_2020805_inter.parquet"
    
    conn = duckdb.connect()
    conn.execute(f"SET threads = {max(1, os.cpu_count() // 2)}")
    
    # Get record counts
    dd_count = conn.execute(f"SELECT COUNT(*) FROM read_parquet('{file1}')").fetchone()[0]
    ac_count = conn.execute(f"SELECT COUNT(*) FROM read_parquet('{file2}')").fetchone()[0]
    
    print("\n=== PARQUET DIRECT QUERY REPORT ===")
    print(f"DataDirect: {dd_count:,} records")
    print(f"Ad_consumers: {ac_count:,} records")
    
    # Direct field matches
    print("\n=== DIRECT FIELD MATCHES ===")
    
    # Name matches
    name_matches = conn.execute(f"""
        SELECT COUNT(*) FROM read_parquet('{file1}') d
        INNER JOIN read_parquet('{file2}') a 
        ON UPPER(d.FirstName) = UPPER(a.given_name_1) 
        AND UPPER(d.Surname) = UPPER(a.surname)
        WHERE d.FirstName IS NOT NULL AND a.given_name_1 IS NOT NULL
    """).fetchone()[0]
    
    # State matches
    state_matches = conn.execute(f"""
        SELECT COUNT(*) FROM read_parquet('{file1}') d
        INNER JOIN read_parquet('{file2}') a ON UPPER(d.State) = UPPER(a.state)
        WHERE d.State IS NOT NULL AND a.state IS NOT NULL
    """).fetchone()[0]
    
    # Mobile matches
    mobile_matches = conn.execute(f"""
        SELECT COUNT(*) FROM read_parquet('{file1}') d
        INNER JOIN read_parquet('{file2}') a ON d.Mobile = a.mobile_text
        WHERE d.Mobile IS NOT NULL AND a.mobile_text IS NOT NULL
    """).fetchone()[0]
    
    # Email matches
    email_matches = conn.execute(f"""
        SELECT COUNT(*) FROM read_parquet('{file1}') d
        INNER JOIN read_parquet('{file2}') a ON UPPER(d.EmailStd) = UPPER(a.email)
        WHERE d.EmailStd IS NOT NULL AND a.email IS NOT NULL
    """).fetchone()[0]
    
    print(f"Name matches    : {name_matches:,}")
    print(f"State matches   : {state_matches:,}")
    print(f"Mobile matches  : {mobile_matches:,}")
    print(f"Email matches   : {email_matches:,}")
    
    # Combined matches
    print("\n=== COMBINED MATCHES ===")
    name_state_matches = conn.execute(f"""
        SELECT COUNT(*) FROM read_parquet('{file1}') d
        INNER JOIN read_parquet('{file2}') a 
        ON UPPER(d.FirstName) = UPPER(a.given_name_1) 
        AND UPPER(d.Surname) = UPPER(a.surname)
        AND UPPER(d.State) = UPPER(a.state)
    """).fetchone()[0]
    
    print(f"Name+State      : {name_state_matches:,}")
    
    # Anti-joins
    print("\n=== ANTI-JOIN ANALYSIS ===")
    
    # Records in DD but not in AC
    dd_only = conn.execute(f"""
        SELECT COUNT(*) FROM read_parquet('{file1}') d
        ANTI JOIN read_parquet('{file2}') a 
        ON UPPER(d.FirstName) = UPPER(a.given_name_1) 
        AND UPPER(d.Surname) = UPPER(a.surname)
        AND UPPER(d.State) = UPPER(a.state)
    """).fetchone()[0]
    
    # Records in AC but not in DD
    ac_only = conn.execute(f"""
        SELECT COUNT(*) FROM read_parquet('{file2}') a
        ANTI JOIN read_parquet('{file1}') d 
        ON UPPER(a.given_name_1) = UPPER(d.FirstName) 
        AND UPPER(a.surname) = UPPER(d.Surname)
        AND UPPER(a.state) = UPPER(d.State)
    """).fetchone()[0]
    
    print(f"Only in DataDirect: {dd_only:,} ({(dd_only/dd_count)*100:.1f}%)")
    print(f"Only in Ad_consumers: {ac_only:,} ({(ac_only/ac_count)*100:.1f}%)")
    
    # Save sample of matches
    output_path = Path("reports/tables")
    output_path.mkdir(parents=True, exist_ok=True)
    
    conn.execute(f"""
        COPY (
            SELECT d.FirstName, d.Surname, d.State, d.Mobile, d.EmailStd
            FROM read_parquet('{file1}') d
            INNER JOIN read_parquet('{file2}') a 
            ON UPPER(d.FirstName) = UPPER(a.given_name_1) 
            AND UPPER(d.Surname) = UPPER(a.surname)
            AND UPPER(d.State) = UPPER(a.state)
            LIMIT 1000
        ) TO '{output_path}/parquet_matches_v3.csv' (HEADER, DELIMITER ',')
    """)
    
    logger.info("Sample matches saved to parquet_matches_v3.csv")
    conn.close()

if __name__ == "__main__":
    generate_parquet_report()