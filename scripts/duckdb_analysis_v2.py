#!/usr/bin/env python3
"""DuckDB analysis v2.0 - Pairwise combinations for all 10 fields."""

import duckdb
from pathlib import Path
from loguru import logger
from itertools import combinations

def analyze_pairwise_matches():
    """Analyze pairwise field combinations for matches."""
    base_path = Path("/data/projects/data_comparison")
    file1 = base_path / "data/external/DATADIRECT_DL_202505_20250801_subset.parquet"
    file2 = base_path / "data/interim/ad_consumers_2020805_inter.parquet"
    
    conn = duckdb.connect()
    
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
    
    fields = ['Title', 'FirstName', 'Surname', 'Gender', 'Landline', 'Mobile', 'Email', 'Suburb', 'State', 'Postcode']
    
    print("\n=== PAIRWISE FIELD MATCHING ANALYSIS ===")
    
    # Single field matches
    print("\nSingle Field Matches:")
    for field in fields:
        count = conn.execute(f"""
            SELECT COUNT(*) FROM dd d
            INNER JOIN ac a ON UPPER(COALESCE(d.{field}, '')) = UPPER(COALESCE(a.{field}, ''))
            WHERE d.{field} IS NOT NULL AND a.{field} IS NOT NULL
        """).fetchone()[0]
        print(f"{field:12}: {count:,}")
    
    # Two field combinations
    print("\nTwo Field Combinations (Top 20):")
    two_field_results = []
    for field1, field2 in combinations(fields, 2):
        count = conn.execute(f"""
            SELECT COUNT(*) FROM dd d
            INNER JOIN ac a ON UPPER(COALESCE(d.{field1}, '')) = UPPER(COALESCE(a.{field1}, ''))
                           AND UPPER(COALESCE(d.{field2}, '')) = UPPER(COALESCE(a.{field2}, ''))
            WHERE d.{field1} IS NOT NULL AND a.{field1} IS NOT NULL
              AND d.{field2} IS NOT NULL AND a.{field2} IS NOT NULL
        """).fetchone()[0]
        two_field_results.append((f"{field1}+{field2}", count))
    
    for combo, count in sorted(two_field_results, key=lambda x: x[1], reverse=True)[:20]:
        print(f"{combo:25}: {count:,}")
    
    # Three field combinations (key ones)
    print("\nThree Field Combinations (Key ones):")
    key_combos = [
        ('FirstName', 'Surname', 'State'),
        ('FirstName', 'Surname', 'Suburb'),
        ('Email', 'FirstName', 'Surname'),
        ('Mobile', 'FirstName', 'Surname'),
        ('FirstName', 'Surname', 'Postcode')
    ]
    
    for field1, field2, field3 in key_combos:
        count = conn.execute(f"""
            SELECT COUNT(*) FROM dd d
            INNER JOIN ac a ON UPPER(COALESCE(d.{field1}, '')) = UPPER(COALESCE(a.{field1}, ''))
                           AND UPPER(COALESCE(d.{field2}, '')) = UPPER(COALESCE(a.{field2}, ''))
                           AND UPPER(COALESCE(d.{field3}, '')) = UPPER(COALESCE(a.{field3}, ''))
            WHERE d.{field1} IS NOT NULL AND a.{field1} IS NOT NULL
              AND d.{field2} IS NOT NULL AND a.{field2} IS NOT NULL
              AND d.{field3} IS NOT NULL AND a.{field3} IS NOT NULL
        """).fetchone()[0]
        print(f"{field1}+{field2}+{field3:12}: {count:,}")
    
    conn.close()

if __name__ == "__main__":
    analyze_pairwise_matches()