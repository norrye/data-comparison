#!/usr/bin/env python3
"""DuckDB report v2.0 - With indexes and anti-joins."""

import duckdb
from pathlib import Path
from loguru import logger
import os
import json

def generate_indexed_report():
    """Generate report with indexes and anti-joins."""
    base_path = Path("/data/projects/data_comparison")
    file1 = base_path / "data/external/DATADIRECT_DL_202505_20250801_subset.parquet"
    file2 = base_path / "data/interim/ad_consumers_2020805_inter.parquet"
    
    conn = duckdb.connect()
    conn.execute(f"SET threads = {max(1, os.cpu_count() // 2)}")
    
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
    
    # Create indexes on all fields
    fields = ['Title', 'FirstName', 'Surname', 'Gender', 'Landline', 'Mobile', 'Email', 'Suburb', 'State', 'Postcode']
    
    logger.info("Creating indexes...")
    for field in fields:
        conn.execute(f"CREATE INDEX idx_dd_{field.lower()} ON dd({field})")
        conn.execute(f"CREATE INDEX idx_ac_{field.lower()} ON ac({field})")
    
    # Composite indexes for common joins
    conn.execute("CREATE INDEX idx_dd_name_state ON dd(FirstName, Surname, State)")
    conn.execute("CREATE INDEX idx_ac_name_state ON ac(FirstName, Surname, State)")
    
    dd_count = conn.execute("SELECT COUNT(*) FROM dd").fetchone()[0]
    ac_count = conn.execute("SELECT COUNT(*) FROM ac").fetchone()[0]
    
    print("\n=== INDEXED MATCHING REPORT ===")
    print(f"DataDirect: {dd_count:,} records")
    print(f"Ad_consumers: {ac_count:,} records")
    
    # Direct field matches with indexes
    print("\n=== DIRECT FIELD MATCHES ===")
    for field in fields:
        match_count = conn.execute(f"""
            SELECT COUNT(*) FROM dd d
            INNER JOIN ac a ON UPPER(d.{field}) = UPPER(a.{field})
            WHERE d.{field} IS NOT NULL AND a.{field} IS NOT NULL
        """).fetchone()[0]
        print(f"{field:12}: {match_count:,}")
    
    # Regular joins
    print("\n=== JOIN ANALYSIS ===")
    name_state_matches = conn.execute("""
        SELECT COUNT(*) FROM dd d
        INNER JOIN ac a ON UPPER(d.FirstName) = UPPER(a.FirstName) 
                       AND UPPER(d.Surname) = UPPER(a.Surname)
                       AND UPPER(d.State) = UPPER(a.State)
    """).fetchone()[0]
    
    mobile_matches = conn.execute("""
        SELECT COUNT(*) FROM dd d
        INNER JOIN ac a ON d.Mobile = a.Mobile
        WHERE d.Mobile IS NOT NULL AND a.Mobile IS NOT NULL
    """).fetchone()[0]
    
    email_matches = conn.execute("""
        SELECT COUNT(*) FROM dd d
        INNER JOIN ac a ON UPPER(d.Email) = UPPER(a.Email)
        WHERE d.Email IS NOT NULL AND a.Email IS NOT NULL
    """).fetchone()[0]
    
    print(f"Name+State    : {name_state_matches:,}")
    print(f"Mobile        : {mobile_matches:,}")
    print(f"Email         : {email_matches:,}")
    
    # Anti-joins (DuckDB feature)
    print("\n=== ANTI-JOIN ANALYSIS ===")
    
    # Records in DD but not in AC (using ANTI JOIN)
    dd_only = conn.execute("""
        SELECT COUNT(*) FROM dd d
        ANTI JOIN ac a ON UPPER(d.FirstName) = UPPER(a.FirstName) 
                      AND UPPER(d.Surname) = UPPER(a.Surname)
                      AND UPPER(d.State) = UPPER(a.State)
    """).fetchone()[0]
    
    # Records in AC but not in DD (using ANTI JOIN)
    ac_only = conn.execute("""
        SELECT COUNT(*) FROM ac a
        ANTI JOIN dd d ON UPPER(a.FirstName) = UPPER(d.FirstName) 
                      AND UPPER(a.Surname) = UPPER(d.Surname)
                      AND UPPER(a.State) = UPPER(d.State)
    """).fetchone()[0]
    
    print(f"Only in DataDirect: {dd_only:,} ({(dd_only/dd_count)*100:.1f}%)")
    print(f"Only in Ad_consumers: {ac_only:,} ({(ac_only/ac_count)*100:.1f}%)")
    
    # Save samples
    output_path = Path("reports/tables")
    output_path.mkdir(parents=True, exist_ok=True)
    
    # Sample of matches
    conn.execute("""
        CREATE TABLE sample_matches AS
        SELECT d.FirstName, d.Surname, d.State, d.Mobile, d.Email
        FROM dd d
        INNER JOIN ac a ON UPPER(d.FirstName) = UPPER(a.FirstName) 
                       AND UPPER(d.Surname) = UPPER(a.Surname)
                       AND UPPER(d.State) = UPPER(a.State)
        LIMIT 1000
    """)
    
    conn.execute(f"COPY sample_matches TO '{output_path}/sample_matches_v2.csv' (HEADER, DELIMITER ',')")
    logger.info("Sample matches saved to sample_matches_v2.csv")
    
    conn.close()

if __name__ == "__main__":
    generate_indexed_report()