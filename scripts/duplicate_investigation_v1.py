#!/usr/bin/env python3
"""Duplicate investigation v1.0 - Find why we have impossible match counts."""

import duckdb
from pathlib import Path
from loguru import logger
import os

def investigate_duplicates():
    """Investigate duplicate records causing join explosion."""
    base_path = Path("/data/projects/data_comparison")
    file1 = base_path / "data/external/DATADIRECT_DL_202505_20250801_subset.parquet"
    file2 = base_path / "data/interim/ad_consumers_2020805_inter.parquet"
    
    conn = duckdb.connect()
    conn.execute(f"SET threads = {max(1, int(os.cpu_count() * 0.4))}")
    
    print("\n=== DUPLICATE INVESTIGATION ===")
    
    # Basic counts
    dd_count = conn.execute(f"SELECT COUNT(*) FROM read_parquet('{file1}')").fetchone()[0]
    ac_count = conn.execute(f"SELECT COUNT(*) FROM read_parquet('{file2}')").fetchone()[0]
    
    print(f"DataDirect total: {dd_count:,}")
    print(f"Ad_consumers total: {ac_count:,}")
    
    # Check for duplicates in DataDirect
    dd_unique_names = conn.execute(f"""
        SELECT COUNT(DISTINCT CONCAT(UPPER(FirstName), '|', UPPER(Surname), '|', UPPER(State)))
        FROM read_parquet('{file1}')
        WHERE FirstName IS NOT NULL AND Surname IS NOT NULL AND State IS NOT NULL
    """).fetchone()[0]
    
    dd_name_records = conn.execute(f"""
        SELECT COUNT(*)
        FROM read_parquet('{file1}')
        WHERE FirstName IS NOT NULL AND Surname IS NOT NULL AND State IS NOT NULL
    """).fetchone()[0]
    
    print(f"\nDataDirect Name+State analysis:")
    print(f"Records with Name+State: {dd_name_records:,}")
    print(f"Unique Name+State combinations: {dd_unique_names:,}")
    print(f"Duplicate ratio: {(dd_name_records - dd_unique_names):,} duplicates")
    
    # Check for duplicates in Ad_consumers
    ac_unique_names = conn.execute(f"""
        SELECT COUNT(DISTINCT CONCAT(UPPER(given_name_1), '|', UPPER(surname), '|', UPPER(state)))
        FROM read_parquet('{file2}')
        WHERE given_name_1 IS NOT NULL AND surname IS NOT NULL AND state IS NOT NULL
    """).fetchone()[0]
    
    ac_name_records = conn.execute(f"""
        SELECT COUNT(*)
        FROM read_parquet('{file2}')
        WHERE given_name_1 IS NOT NULL AND surname IS NOT NULL AND state IS NOT NULL
    """).fetchone()[0]
    
    print(f"\nAd_consumers Name+State analysis:")
    print(f"Records with Name+State: {ac_name_records:,}")
    print(f"Unique Name+State combinations: {ac_unique_names:,}")
    print(f"Duplicate ratio: {(ac_name_records - ac_unique_names):,} duplicates")
    
    # Find most duplicated names
    print(f"\nTop duplicated names in DataDirect:")
    dd_dups = conn.execute(f"""
        SELECT UPPER(FirstName) || ' ' || UPPER(Surname) || ' (' || UPPER(State) || ')' as name, COUNT(*) as count
        FROM read_parquet('{file1}')
        WHERE FirstName IS NOT NULL AND Surname IS NOT NULL AND State IS NOT NULL
        GROUP BY UPPER(FirstName), UPPER(Surname), UPPER(State)
        HAVING COUNT(*) > 1
        ORDER BY COUNT(*) DESC
        LIMIT 10
    """).fetchall()
    
    for name, count in dd_dups:
        print(f"  {name}: {count:,} records")
    
    print(f"\nTop duplicated names in Ad_consumers:")
    ac_dups = conn.execute(f"""
        SELECT UPPER(given_name_1) || ' ' || UPPER(surname) || ' (' || UPPER(state) || ')' as name, COUNT(*) as count
        FROM read_parquet('{file2}')
        WHERE given_name_1 IS NOT NULL AND surname IS NOT NULL AND state IS NOT NULL
        GROUP BY UPPER(given_name_1), UPPER(surname), UPPER(state)
        HAVING COUNT(*) > 1
        ORDER BY COUNT(*) DESC
        LIMIT 10
    """).fetchall()
    
    for name, count in ac_dups:
        print(f"  {name}: {count:,} records")
    
    # Calculate expected vs actual join results
    expected_max = min(dd_unique_names, ac_unique_names)
    print(f"\nJoin Analysis:")
    print(f"Maximum possible unique matches: {expected_max:,}")
    print(f"This explains the join explosion - many-to-many joins on duplicate names!")
    
    conn.close()

if __name__ == "__main__":
    investigate_duplicates()