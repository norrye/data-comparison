#!/usr/bin/env python3
"""DuckDB analysis v4.0 - Simple analysis without pairwise combinations."""

import duckdb
from pathlib import Path
from loguru import logger
import os

def analyze_simple_matches():
    """Simple field matching analysis with 50% CPU usage."""
    base_path = Path("/data/projects/data_comparison")
    file1 = base_path / "data/external/DATADIRECT_DL_202505_20250801_subset.parquet"
    file2 = base_path / "data/interim/ad_consumers_2020805_inter.parquet"
    
    conn = duckdb.connect()
    
    # Set DuckDB to use 50% of available CPUs
    cpu_count = os.cpu_count()
    threads = max(1, cpu_count // 2)
    conn.execute(f"SET threads = {threads}")
    logger.info(f"Using {threads} threads (50% of {cpu_count} CPUs)")
    
    # Create tables
    conn.execute(f"""
        CREATE TABLE dd AS 
        SELECT Title, FirstName, Surname, Gender, Landline, Mobile, EmailStd as Email,
               Suburb, State, Postcode
        FROM read_parquet('{file1}')
    """)
    
    conn.execute(f"""
        CREATE TABLE ac AS 
        SELECT title as Title, given_name_1 as FirstName, surname as Surname, 
               gender as Gender, landline as Landline, mobile_text as Mobile, 
               email as Email, suburb as Suburb, state as State, postcode_text as Postcode
        FROM read_parquet('{file2}')
    """)
    
    # Get table sizes
    dd_count = conn.execute("SELECT COUNT(*) FROM dd").fetchone()[0]
    ac_count = conn.execute("SELECT COUNT(*) FROM ac").fetchone()[0]
    
    print("\n=== SIMPLE FIELD MATCHING ANALYSIS ===")
    print(f"DataDirect records: {dd_count:,}")
    print(f"Ad_consumers records: {ac_count:,}")
    
    # Key matching combinations only
    matches = [
        ("Name+State", "UPPER(d.FirstName) = UPPER(a.FirstName) AND UPPER(d.Surname) = UPPER(a.Surname) AND UPPER(d.State) = UPPER(a.State)"),
        ("Mobile", "d.Mobile = a.Mobile AND d.Mobile IS NOT NULL AND a.Mobile IS NOT NULL"),
        ("Email", "UPPER(d.Email) = UPPER(a.Email) AND d.Email IS NOT NULL AND a.Email IS NOT NULL"),
        ("Name+Suburb", "UPPER(d.FirstName) = UPPER(a.FirstName) AND UPPER(d.Surname) = UPPER(a.Surname) AND UPPER(d.Suburb) = UPPER(a.Suburb)"),
        ("Name+Postcode", "UPPER(d.FirstName) = UPPER(a.FirstName) AND UPPER(d.Surname) = UPPER(a.Surname) AND d.Postcode = a.Postcode")
    ]
    
    print("\nKey Matching Patterns:")
    for match_name, condition in matches:
        count = conn.execute(f"""
            SELECT COUNT(*) FROM dd d
            INNER JOIN ac a ON {condition}
        """).fetchone()[0]
        print(f"{match_name:15}: {count:,}")
    
    conn.close()

if __name__ == "__main__":
    analyze_simple_matches()