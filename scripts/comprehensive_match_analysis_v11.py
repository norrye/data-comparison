#!/usr/bin/env python3
"""
Comprehensive matching analysis v10.0 - Enhanced logging and progress reporting.

Version: 10.0
Author: Expert Data Scientist
Description: Uses optimized DuckDB database with sorted data and indexes
"""

import time
from datetime import datetime
from pathlib import Path
from typing import List, Tuple

import duckdb
from loguru import logger
from pydantic import BaseModel, Field


class AntiJoinResult(BaseModel):
    """Data validation model for anti-join results."""
    match_name: str = Field(..., description="Name of the matching pattern")
    matches: int = Field(..., ge=0, description="Number of matches found")
    dd_only: int = Field(..., ge=0, description="Records only in DataDirect")
    ac_only: int = Field(..., ge=0, description="Records only in Ad_consumers")
    dd_match_rate: float = Field(..., ge=0, le=100, description="DD match rate percentage")
    ac_match_rate: float = Field(..., ge=0, le=100, description="AC match rate percentage")
    processing_time: float = Field(..., ge=0, description="Processing time in seconds")

def create_optimized_database() -> None:
    """Create optimized DuckDB database with sorted data and indexes."""
    logger.info("Creating optimized match analysis database with sorted data and indexes")
    
    base_path = Path("/data/projects/data_comparison")
    db_path = base_path / "data/processed/match_analysis.duckdb"
    file1 = base_path / "data/external/DATADIRECT_DL_202505_20250801_subset.parquet"
    file2 = base_path / "data/interim/ad_consumers_2020805_inter_ll_text.parquet"
    
    # Remove existing database
    if db_path.exists():
        db_path.unlink()
        logger.info("Removed existing database")
    
    conn = duckdb.connect(str(db_path))
    conn.execute("SET threads = 4")
    logger.info("Created database with 4 threads")
    
    # Create DataDirect table with sorting
    logger.info("Creating DataDirect table with sorting by surname, suburb, state, postcode")
    conn.execute("""
        CREATE TABLE datadirect AS
        SELECT 
            ID,
            UPPER(TRIM(Title)) as title,
            UPPER(TRIM(FirstName)) as first_name,
            UPPER(TRIM(Surname)) as surname,
            UPPER(TRIM(Gender)) as gender,
            UPPER(TRIM(EmailStd)) as email_std,
            UPPER(TRIM(EmailHash)) as email_hash,
            TRIM(Mobile) as mobile,
            TRIM(Landline) as landline,
            UPPER(TRIM(Suburb)) as suburb,
            UPPER(TRIM(State)) as state,
            CAST(Postcode AS VARCHAR) as postcode,
            -- Compound fields for indexing
            UPPER(TRIM(FirstName || ' ' || Surname)) as full_name,
            UPPER(TRIM(FirstName || ' ' || Surname || ' ' || Suburb || ' ' || CAST(Postcode AS VARCHAR))) as name_suburb_postcode,
            UPPER(TRIM(FirstName || ' ' || Surname || ' ' || Suburb || ' ' || CAST(Postcode AS VARCHAR) || ' ' || Mobile)) as name_suburb_postcode_mobile
        FROM read_parquet(?)
        WHERE Title IS NOT NULL OR FirstName IS NOT NULL OR Surname IS NOT NULL
        ORDER BY surname, suburb, state, postcode
    """, [str(file1)])
    
    dd_count = conn.execute("SELECT COUNT(*) FROM datadirect").fetchone()[0]
    logger.info(f"Loaded {dd_count:,} DataDirect records (sorted)")
    
    # Create Ad_consumers table with sorting
    logger.info("Creating Ad_consumers table with sorting by surname, suburb, state, postcode")
    conn.execute("""
        CREATE TABLE ad_consumers AS
        SELECT 
            adId,
            UPPER(TRIM(title)) as title,
            UPPER(TRIM(given_name_1)) as first_name,
            UPPER(TRIM(surname)) as surname,
            UPPER(TRIM(gender)) as gender,
            UPPER(TRIM(email)) as email_std,
            UPPER(TRIM(email_sha256)) as email_hash,
            TRIM(mobile_text) as mobile,
            TRIM(landline_text) as landline,
            UPPER(TRIM(suburb)) as suburb,
            UPPER(TRIM(state)) as state,
            TRIM(postcode_text) as postcode,
            -- Compound fields for indexing
            UPPER(TRIM(given_name_1 || ' ' || surname)) as full_name,
            UPPER(TRIM(given_name_1 || ' ' || surname || ' ' || suburb || ' ' || postcode_text)) as name_suburb_postcode,
            UPPER(TRIM(given_name_1 || ' ' || surname || ' ' || suburb || ' ' || postcode_text || ' ' || mobile_text)) as name_suburb_postcode_mobile
        FROM read_parquet(?)
        WHERE title IS NOT NULL OR given_name_1 IS NOT NULL OR surname IS NOT NULL
        ORDER BY surname, suburb, state, postcode_text
    """, [str(file2)])
    
    ac_count = conn.execute("SELECT COUNT(*) FROM ad_consumers").fetchone()[0]
    logger.info(f"Loaded {ac_count:,} Ad_consumers records (sorted)")
    
    # Create indexes on individual fields
    logger.info("Creating individual field indexes")
    individual_fields = ['title', 'first_name', 'surname', 'gender', 'email_std', 'email_hash', 
                        'mobile', 'landline', 'suburb', 'state', 'postcode']
    
    for field in individual_fields:
        conn.execute(f"CREATE INDEX idx_dd_{field} ON datadirect({field})")
        conn.execute(f"CREATE INDEX idx_ac_{field} ON ad_consumers({field})")
        logger.info(f"Created indexes for {field}")
    
    # Create multi-field compound indexes
    logger.info("Creating multi-field compound indexes")
    compound_fields = ['full_name', 'name_suburb_postcode', 'name_suburb_postcode_mobile']
    
    for field in compound_fields:
        conn.execute(f"CREATE INDEX idx_dd_{field} ON datadirect({field})")
        conn.execute(f"CREATE INDEX idx_ac_{field} ON ad_consumers({field})")
        logger.info(f"Created compound index for {field}")
    
    # Analyze tables for query optimization
    conn.execute("ANALYZE datadirect")
    conn.execute("ANALYZE ad_consumers")
    
    conn.close()
    logger.info("Database creation completed with sorted data and all indexes")

def comprehensive_anti_join_analysis_v10() -> None:
    """
    Perform comprehensive anti-join analysis using optimized database with sorted data and indexes.
    """
    start_time = time.time()
    logger.info("=" * 80)
    logger.info("STARTING COMPREHENSIVE ANTI-JOIN ANALYSIS V10.0")
    logger.info(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 80)

    base_path = Path("/data/projects/data_comparison")
    db_path = base_path / "data/processed/match_analysis.duckdb"
    
    # Create database if it doesn't exist
    if not db_path.exists():
        create_optimized_database()
    
    conn = duckdb.connect(str(db_path))
    conn.execute("SET threads = 4")
    
    # Get record counts
    dd_count = conn.execute("SELECT COUNT(*) FROM datadirect").fetchone()[0]
    ac_count = conn.execute("SELECT COUNT(*) FROM ad_consumers").fetchone()[0]
    
    logger.info(f"DataDirect: {dd_count:,} records, Ad_consumers: {ac_count:,} records")
    
    print("\n" + "=" * 80)
    print("COMPREHENSIVE ANTI-JOIN ANALYSIS V10.0")
    print("=" * 80)
    print(f"DataDirect: {dd_count:,} records")
    print(f"Ad_consumers: {ac_count:,} records")
    print(f"Total records to process: {dd_count + ac_count:,}")
    print("=" * 80)
    
    # Direct field matches using optimized tables
    direct_matches: List[Tuple[str, str, str]] = [

        ("FullName", "d.full_name = a.full_name", "d.full_name IS NOT NULL AND a.full_name IS NOT NULL"),
        ("EmailStd", "d.email_std = a.email_std", "d.email_std IS NOT NULL AND a.email_std IS NOT NULL"),
        ("EmailHash", "d.email_hash = a.email_hash", "d.email_hash IS NOT NULL AND a.email_hash IS NOT NULL"),
        ("Mobile", "d.mobile = a.mobile", "d.mobile IS NOT NULL AND a.mobile IS NOT NULL"),
        ("Landline", "d.landline = a.landline", "d.landline IS NOT NULL AND a.landline IS NOT NULL"),

    ]
    
    logger.info(f"✓ Configured {len(direct_matches)} direct field mappings")
    for i, (name, _, _) in enumerate(direct_matches, 1):
        logger.info(f"  {i:2d}. {name}")
    
    logger.info("PHASE 5: Direct field matching analysis using DuckDB native ANTI JOIN")
    print("\n=== DIRECT FIELD MATCHES WITH ANTI JOINS ===")
    
    direct_results: List[AntiJoinResult] = []
    total_direct_matches = len(direct_matches)
    
    for idx, (match_name, condition, filter_cond) in enumerate(direct_matches, 1):
        field_start_time = time.time()
        logger.info(f"Processing field {idx}/{total_direct_matches}: {match_name}")
        
        try:
            # Inner join count (matches)
            logger.info(f"  → Running INNER JOIN for {match_name}")
            inner_start = time.time()
            matches = conn.execute(f"""
                SELECT COUNT(*) FROM datadirect d
                INNER JOIN ad_consumers a ON {condition}
                WHERE {filter_cond}
            """).fetchone()[0]
            inner_time = time.time() - inner_start
            logger.info(f"  ✓ INNER JOIN completed in {inner_time:.2f}s: {matches:,} matches")
            
            # Anti-join 1: DD ANTI JOIN AC
            logger.info(f"  → Running DD ANTI JOIN AC for {match_name}")
            anti1_start = time.time()
            dd_filter = filter_cond.split(' AND a.')[0] if ' AND a.' in filter_cond else filter_cond
            dd_only = conn.execute(f"""
                SELECT COUNT(*) FROM (
                    SELECT * FROM datadirect WHERE {dd_filter}
                ) d
                ANTI JOIN ad_consumers a ON {condition}
            """).fetchone()[0]
            anti1_time = time.time() - anti1_start
            logger.info(f"  ✓ DD ANTI JOIN completed in {anti1_time:.2f}s: {dd_only:,} DD-only records")
            
            # Anti-join 2: AC ANTI JOIN DD
            logger.info(f"  → Running AC ANTI JOIN DD for {match_name}")
            anti2_start = time.time()
            ac_filter = filter_cond.split(' AND d.')[0].replace('d.', 'a.') if ' AND d.' in filter_cond else filter_cond.replace('d.', 'a.')
            ac_only = conn.execute(f"""
                SELECT COUNT(*) FROM (
                    SELECT * FROM ad_consumers WHERE {ac_filter}
                ) a
                ANTI JOIN datadirect d ON {condition}
            """).fetchone()[0]
            anti2_time = time.time() - anti2_start
            logger.info(f"  ✓ AC ANTI JOIN completed in {anti2_time:.2f}s: {ac_only:,} AC-only records")
            
            # Calculate match rates
            dd_match_rate = (matches / dd_count) * 100 if dd_count > 0 else 0
            ac_match_rate = (matches / ac_count) * 100 if ac_count > 0 else 0
            
            field_processing_time = time.time() - field_start_time
            
            result = AntiJoinResult(
                match_name=match_name,
                matches=matches,
                dd_only=dd_only,
                ac_only=ac_only,
                dd_match_rate=dd_match_rate,
                ac_match_rate=ac_match_rate,
                processing_time=field_processing_time
            )
            direct_results.append(result)
            
            print(f"{match_name:12}: Matches={matches:,}, DD_only={dd_only:,}, AC_only={ac_only:,}")
            print(f"{'':14} DD_rate={dd_match_rate:.2f}%, AC_rate={ac_match_rate:.2f}%, Time={field_processing_time:.2f}s")
            logger.info(f"{match_name} - Matches: {matches:,}, DD_only: {dd_only:,}, AC_only: {ac_only:,}")
            
        except Exception as e:
            logger.error(f"Error processing {match_name}: {str(e)}")
            continue
    
    conn.close()
    total_time = time.time() - start_time
    logger.info(f"Analysis completed in {total_time:.2f} seconds")

if __name__ == "__main__":
    comprehensive_anti_join_analysis_v10()