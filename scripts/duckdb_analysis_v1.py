#!/usr/bin/env python3
"""DuckDB analysis v1.0 - Load datasets and perform anti-joins to find matches."""

import duckdb
from pathlib import Path
from loguru import logger

def analyze_with_duckdb():
    """Load datasets into DuckDB and perform anti-join analysis."""
    base_path = Path("/data/projects/data_comparison")
    file1 = base_path / "data/external/DATADIRECT_DL_202505_20250801_subset.parquet"
    file2 = base_path / "data/interim/ad_consumers_2020805_inter.parquet"
    
    # Create DuckDB connection
    conn = duckdb.connect()
    
    logger.info("Loading datasets into DuckDB...")
    
    # Create tables from parquet files
    conn.execute(f"""
        CREATE TABLE datadirect AS 
        SELECT 
            FirstName, Surname, Gender, Mobile, EmailStd as Email,
            Suburb, State, Postcode
        FROM read_parquet('{file1}')
        WHERE FirstName IS NOT NULL AND Surname IS NOT NULL
    """)
    
    conn.execute(f"""
        CREATE TABLE ad_consumers AS 
        SELECT 
            given_name_1 as FirstName, surname as Surname, gender as Gender,
            mobile_text as Mobile, email as Email,
            suburb as Suburb, state as State, postcode_text as Postcode
        FROM read_parquet('{file2}')
        WHERE given_name_1 IS NOT NULL AND surname IS NOT NULL
    """)
    
    # Get table sizes
    dd_count = conn.execute("SELECT COUNT(*) FROM datadirect").fetchone()[0]
    ac_count = conn.execute("SELECT COUNT(*) FROM ad_consumers").fetchone()[0]
    
    logger.info(f"DataDirect records: {dd_count:,}")
    logger.info(f"Ad_consumers records: {ac_count:,}")
    
    # Exact matches on Name + State
    exact_matches = conn.execute("""
        SELECT COUNT(*) FROM datadirect d
        INNER JOIN ad_consumers a 
        ON UPPER(d.FirstName) = UPPER(a.FirstName) 
        AND UPPER(d.Surname) = UPPER(a.Surname)
        AND UPPER(d.State) = UPPER(a.State)
    """).fetchone()[0]
    
    # Mobile matches
    mobile_matches = conn.execute("""
        SELECT COUNT(*) FROM datadirect d
        INNER JOIN ad_consumers a ON d.Mobile = a.Mobile
        WHERE d.Mobile IS NOT NULL AND a.Mobile IS NOT NULL
    """).fetchone()[0]
    
    # Email matches  
    email_matches = conn.execute("""
        SELECT COUNT(*) FROM datadirect d
        INNER JOIN ad_consumers a ON UPPER(d.Email) = UPPER(a.Email)
        WHERE d.Email IS NOT NULL AND a.Email IS NOT NULL
    """).fetchone()[0]
    
    # Records only in DataDirect (anti-join)
    only_in_dd = conn.execute("""
        SELECT COUNT(*) FROM datadirect d
        WHERE NOT EXISTS (
            SELECT 1 FROM ad_consumers a 
            WHERE UPPER(d.FirstName) = UPPER(a.FirstName) 
            AND UPPER(d.Surname) = UPPER(a.Surname)
            AND UPPER(d.State) = UPPER(a.State)
        )
    """).fetchone()[0]
    
    # Records only in Ad_consumers (anti-join)
    only_in_ac = conn.execute("""
        SELECT COUNT(*) FROM ad_consumers a
        WHERE NOT EXISTS (
            SELECT 1 FROM datadirect d 
            WHERE UPPER(d.FirstName) = UPPER(a.FirstName) 
            AND UPPER(d.Surname) = UPPER(a.Surname)
            AND UPPER(d.State) = UPPER(a.State)
        )
    """).fetchone()[0]
    
    # Print results
    print("\n=== DUCKDB ANTI-JOIN ANALYSIS ===")
    print(f"DataDirect records: {dd_count:,}")
    print(f"Ad_consumers records: {ac_count:,}")
    print(f"\nMatching Analysis:")
    print(f"Exact matches (Name+State): {exact_matches:,}")
    print(f"Mobile number matches: {mobile_matches:,}")
    print(f"Email address matches: {email_matches:,}")
    print(f"\nUnique Records:")
    print(f"Only in DataDirect: {only_in_dd:,} ({(only_in_dd/dd_count)*100:.1f}%)")
    print(f"Only in Ad_consumers: {only_in_ac:,} ({(only_in_ac/ac_count)*100:.1f}%)")
    
    # Save sample of matches
    conn.execute("""
        CREATE TABLE matches AS
        SELECT d.*, a.Mobile as AC_Mobile, a.Email as AC_Email
        FROM datadirect d
        INNER JOIN ad_consumers a 
        ON UPPER(d.FirstName) = UPPER(a.FirstName) 
        AND UPPER(d.Surname) = UPPER(a.Surname)
        AND UPPER(d.State) = UPPER(a.State)
        LIMIT 1000
    """)
    
    output_path = Path("reports/tables")
    output_path.mkdir(parents=True, exist_ok=True)
    
    conn.execute(f"COPY matches TO '{output_path}/duckdb_matches_v1.csv' (HEADER, DELIMITER ',')")
    logger.info("Sample matches saved to duckdb_matches_v1.csv")
    
    conn.close()

if __name__ == "__main__":
    analyze_with_duckdb()