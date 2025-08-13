#!/usr/bin/env python3
"""DuckDB report v4.0 - Query parquet files directly with detailed logging and 40% CPU."""

import duckdb
from pathlib import Path
from loguru import logger
import os

def generate_parquet_report():
    """Generate report querying parquet files directly with detailed logging."""
    base_path = Path("/data/projects/data_comparison")
    file1 = base_path / "data/external/DATADIRECT_DL_202505_20250801_subset.parquet"
    file2 = base_path / "data/interim/ad_consumers_2020805_inter.parquet"
    
    # Setup logging
    logger.add("logs/duckdb_report_v4.log", rotation="10 MB", level="DEBUG")
    
    conn = duckdb.connect()
    
    # Set to 40% of CPUs
    cpu_count = os.cpu_count()
    threads = max(1, int(cpu_count * 0.4))
    conn.execute(f"SET threads = {threads}")
    logger.info(f"Using {threads} threads (40% of {cpu_count} CPUs)")
    
    logger.info("Starting parquet direct query analysis")
    logger.debug(f"File 1: {file1}")
    logger.debug(f"File 2: {file2}")
    
    # Get record counts
    logger.info("Getting record counts...")
    dd_count = conn.execute(f"SELECT COUNT(*) FROM read_parquet('{file1}')").fetchone()[0]
    ac_count = conn.execute(f"SELECT COUNT(*) FROM read_parquet('{file2}')").fetchone()[0]
    
    logger.info(f"DataDirect records: {dd_count:,}")
    logger.info(f"Ad_consumers records: {ac_count:,}")
    
    print("\n=== PARQUET DIRECT QUERY REPORT ===")
    print(f"DataDirect: {dd_count:,} records")
    print(f"Ad_consumers: {ac_count:,} records")
    
    # Direct field matches
    print("\n=== DIRECT FIELD MATCHES ===")
    logger.info("Starting direct field matches analysis")
    
    # Name matches
    logger.debug("Analyzing name matches...")
    name_matches = conn.execute(f"""
        SELECT COUNT(*) FROM read_parquet('{file1}') d
        INNER JOIN read_parquet('{file2}') a 
        ON UPPER(d.FirstName) = UPPER(a.given_name_1) 
        AND UPPER(d.Surname) = UPPER(a.surname)
        WHERE d.FirstName IS NOT NULL AND a.given_name_1 IS NOT NULL
    """).fetchone()[0]
    logger.info(f"Name matches: {name_matches:,}")
    
    # State matches
    logger.debug("Analyzing state matches...")
    state_matches = conn.execute(f"""
        SELECT COUNT(*) FROM read_parquet('{file1}') d
        INNER JOIN read_parquet('{file2}') a ON UPPER(d.State) = UPPER(a.state)
        WHERE d.State IS NOT NULL AND a.state IS NOT NULL
    """).fetchone()[0]
    logger.info(f"State matches: {state_matches:,}")
    
    # Mobile matches
    logger.debug("Analyzing mobile matches...")
    mobile_matches = conn.execute(f"""
        SELECT COUNT(*) FROM read_parquet('{file1}') d
        INNER JOIN read_parquet('{file2}') a ON d.Mobile = a.mobile_text
        WHERE d.Mobile IS NOT NULL AND a.mobile_text IS NOT NULL
    """).fetchone()[0]
    logger.info(f"Mobile matches: {mobile_matches:,}")
    
    # Email matches
    logger.debug("Analyzing email matches...")
    email_matches = conn.execute(f"""
        SELECT COUNT(*) FROM read_parquet('{file1}') d
        INNER JOIN read_parquet('{file2}') a ON UPPER(d.EmailStd) = UPPER(a.email)
        WHERE d.EmailStd IS NOT NULL AND a.email IS NOT NULL
    """).fetchone()[0]
    logger.info(f"Email matches: {email_matches:,}")
    
    print(f"Name matches    : {name_matches:,}")
    print(f"State matches   : {state_matches:,}")
    print(f"Mobile matches  : {mobile_matches:,}")
    print(f"Email matches   : {email_matches:,}")
    
    # Combined matches
    print("\n=== COMBINED MATCHES ===")
    logger.info("Starting combined matches analysis")
    
    logger.debug("Analyzing name+state matches...")
    name_state_matches = conn.execute(f"""
        SELECT COUNT(*) FROM read_parquet('{file1}') d
        INNER JOIN read_parquet('{file2}') a 
        ON UPPER(d.FirstName) = UPPER(a.given_name_1) 
        AND UPPER(d.Surname) = UPPER(a.surname)
        AND UPPER(d.State) = UPPER(a.state)
    """).fetchone()[0]
    logger.info(f"Name+State matches: {name_state_matches:,}")
    
    print(f"Name+State      : {name_state_matches:,}")
    
    # Anti-joins
    print("\n=== ANTI-JOIN ANALYSIS ===")
    logger.info("Starting anti-join analysis")
    
    # Records in DD but not in AC
    logger.debug("Finding records only in DataDirect...")
    dd_only = conn.execute(f"""
        SELECT COUNT(*) FROM read_parquet('{file1}') d
        ANTI JOIN read_parquet('{file2}') a 
        ON UPPER(d.FirstName) = UPPER(a.given_name_1) 
        AND UPPER(d.Surname) = UPPER(a.surname)
        AND UPPER(d.State) = UPPER(a.state)
    """).fetchone()[0]
    logger.info(f"Records only in DataDirect: {dd_only:,}")
    
    # Records in AC but not in DD
    logger.debug("Finding records only in Ad_consumers...")
    ac_only = conn.execute(f"""
        SELECT COUNT(*) FROM read_parquet('{file2}') a
        ANTI JOIN read_parquet('{file1}') d 
        ON UPPER(a.given_name_1) = UPPER(d.FirstName) 
        AND UPPER(a.surname) = UPPER(d.Surname)
        AND UPPER(a.state) = UPPER(d.State)
    """).fetchone()[0]
    logger.info(f"Records only in Ad_consumers: {ac_only:,}")
    
    print(f"Only in DataDirect: {dd_only:,} ({(dd_only/dd_count)*100:.1f}%)")
    print(f"Only in Ad_consumers: {ac_only:,} ({(ac_only/ac_count)*100:.1f}%)")
    
    # Save sample of matches
    logger.debug("Saving sample matches...")
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
        ) TO '{output_path}/parquet_matches_v4.csv' (HEADER, DELIMITER ',')
    """)
    
    logger.success("Analysis completed successfully")
    logger.info("Sample matches saved to parquet_matches_v4.csv")
    conn.close()

if __name__ == "__main__":
    generate_parquet_report()