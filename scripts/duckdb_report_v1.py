#!/usr/bin/env python3
"""DuckDB report v1.0 - Generate comprehensive matching report with joins/anti-joins."""

import duckdb
from pathlib import Path
from loguru import logger
import os
import json

def generate_matching_report():
    """Generate comprehensive matching report similar to Sweetviz."""
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
    
    fields = ['Title', 'FirstName', 'Surname', 'Gender', 'Landline', 'Mobile', 'Email', 'Suburb', 'State', 'Postcode']
    
    # Dataset overview
    dd_count = conn.execute("SELECT COUNT(*) FROM dd").fetchone()[0]
    ac_count = conn.execute("SELECT COUNT(*) FROM ac").fetchone()[0]
    
    report = {
        "dataset_overview": {
            "datadirect_records": dd_count,
            "ad_consumers_records": ac_count
        },
        "field_statistics": {},
        "direct_field_matches": {},
        "join_analysis": {},
        "anti_join_analysis": {}
    }
    
    print("\n=== COMPREHENSIVE MATCHING REPORT ===")
    print(f"DataDirect: {dd_count:,} records")
    print(f"Ad_consumers: {ac_count:,} records")
    
    # Field statistics
    print("\n=== FIELD STATISTICS ===")
    for field in fields:
        dd_stats = conn.execute(f"""
            SELECT COUNT(*) as total, COUNT({field}) as non_null, 
                   COUNT(DISTINCT {field}) as unique_vals
            FROM dd
        """).fetchone()
        
        ac_stats = conn.execute(f"""
            SELECT COUNT(*) as total, COUNT({field}) as non_null,
                   COUNT(DISTINCT {field}) as unique_vals  
            FROM ac
        """).fetchone()
        
        report["field_statistics"][field] = {
            "dd_non_null": dd_stats[1],
            "dd_unique": dd_stats[2],
            "ac_non_null": ac_stats[1], 
            "ac_unique": ac_stats[2]
        }
        
        print(f"{field:12}: DD({dd_stats[1]:,}/{dd_stats[2]:,}) AC({ac_stats[1]:,}/{ac_stats[2]:,})")
    
    # Direct field matches
    print("\n=== DIRECT FIELD MATCHES ===")
    for field in fields:
        match_count = conn.execute(f"""
            SELECT COUNT(*) FROM dd d
            INNER JOIN ac a ON UPPER(COALESCE(d.{field}, '')) = UPPER(COALESCE(a.{field}, ''))
            WHERE d.{field} IS NOT NULL AND a.{field} IS NOT NULL
        """).fetchone()[0]
        
        report["direct_field_matches"][field] = match_count
        print(f"{field:12}: {match_count:,}")
    
    # Join analysis (matching records)
    print("\n=== JOIN ANALYSIS (MATCHING RECORDS) ===")
    join_queries = {
        "Name+State": "UPPER(d.FirstName) = UPPER(a.FirstName) AND UPPER(d.Surname) = UPPER(a.Surname) AND UPPER(d.State) = UPPER(a.State)",
        "Mobile": "d.Mobile = a.Mobile AND d.Mobile IS NOT NULL AND a.Mobile IS NOT NULL",
        "Email": "UPPER(d.Email) = UPPER(a.Email) AND d.Email IS NOT NULL AND a.Email IS NOT NULL"
    }
    
    for join_name, condition in join_queries.items():
        count = conn.execute(f"""
            SELECT COUNT(*) FROM dd d INNER JOIN ac a ON {condition}
        """).fetchone()[0]
        
        report["join_analysis"][join_name] = count
        print(f"{join_name:12}: {count:,}")
    
    # Anti-join analysis (non-matching records)
    print("\n=== ANTI-JOIN ANALYSIS (NON-MATCHING RECORDS) ===")
    
    # Records in DD but not in AC
    dd_only = conn.execute("""
        SELECT COUNT(*) FROM dd d
        WHERE NOT EXISTS (
            SELECT 1 FROM ac a 
            WHERE UPPER(d.FirstName) = UPPER(a.FirstName) 
            AND UPPER(d.Surname) = UPPER(a.Surname)
            AND UPPER(d.State) = UPPER(a.State)
        )
    """).fetchone()[0]
    
    # Records in AC but not in DD
    ac_only = conn.execute("""
        SELECT COUNT(*) FROM ac a
        WHERE NOT EXISTS (
            SELECT 1 FROM dd d 
            WHERE UPPER(d.FirstName) = UPPER(a.FirstName) 
            AND UPPER(d.Surname) = UPPER(a.Surname)
            AND UPPER(d.State) = UPPER(a.State)
        )
    """).fetchone()[0]
    
    report["anti_join_analysis"] = {
        "only_in_datadirect": dd_only,
        "only_in_ad_consumers": ac_only,
        "dd_unique_percentage": (dd_only / dd_count) * 100,
        "ac_unique_percentage": (ac_only / ac_count) * 100
    }
    
    print(f"Only in DataDirect: {dd_only:,} ({(dd_only/dd_count)*100:.1f}%)")
    print(f"Only in Ad_consumers: {ac_only:,} ({(ac_only/ac_count)*100:.1f}%)")
    
    # Save report
    output_path = Path("reports/tables")
    output_path.mkdir(parents=True, exist_ok=True)
    
    with open(output_path / "duckdb_matching_report_v1.json", 'w') as f:
        json.dump(report, f, indent=2)
    
    logger.info("Comprehensive matching report saved to duckdb_matching_report_v1.json")
    conn.close()

if __name__ == "__main__":
    generate_matching_report()