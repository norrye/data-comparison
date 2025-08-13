#!/usr/bin/env python3
"""
Comprehensive matching analysis v7.0 - Using persistent DuckDB database with indexes.

Version: 7.0
Author: Expert Data Scientist
Description: Creates persistent DuckDB database with optimized tables and indexes for faster matching
"""

import duckdb
from pathlib import Path
from loguru import logger
from pydantic import BaseModel, Field
from typing import List, Tuple
import os

class AntiJoinResult(BaseModel):
    """Data validation model for anti-join results."""
    match_name: str = Field(..., description="Name of the matching pattern")
    matches: int = Field(..., ge=0, description="Number of matches found")
    dd_only: int = Field(..., ge=0, description="Records only in DataDirect")
    ac_only: int = Field(..., ge=0, description="Records only in Ad_consumers")
    dd_match_rate: float = Field(..., ge=0, le=100, description="DD match rate percentage")
    ac_match_rate: float = Field(..., ge=0, le=100, description="AC match rate percentage")

def create_optimized_database() -> None:
    """Create optimized DuckDB database with only required fields and indexes."""
    logger.info("Creating optimized match analysis database")
    
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
    
    # Create DataDirect table with only required fields
    logger.info("Creating DataDirect table")
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
    """, [str(file1)])
    
    dd_count = conn.execute("SELECT COUNT(*) FROM datadirect").fetchone()[0]
    logger.info(f"Loaded {dd_count:,} DataDirect records")
    
    # Create Ad_consumers table with only required fields
    logger.info("Creating Ad_consumers table")
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
    """, [str(file2)])
    
    ac_count = conn.execute("SELECT COUNT(*) FROM ad_consumers").fetchone()[0]
    logger.info(f"Loaded {ac_count:,} Ad_consumers records")
    
    # Create indexes on individual fields
    logger.info("Creating individual field indexes")
    individual_fields = ['title', 'first_name', 'surname', 'gender', 'email_std', 'email_hash', 
                        'mobile', 'landline', 'suburb', 'state', 'postcode']
    
    for field in individual_fields:
        conn.execute(f"CREATE INDEX idx_dd_{field} ON datadirect({field})")
        conn.execute(f"CREATE INDEX idx_ac_{field} ON ad_consumers({field})")
        logger.info(f"Created indexes for {field}")
    
    # Create compound indexes
    logger.info("Creating compound indexes")
    compound_fields = ['full_name', 'name_suburb_postcode', 'name_suburb_postcode_mobile']
    
    for field in compound_fields:
        conn.execute(f"CREATE INDEX idx_dd_{field} ON datadirect({field})")
        conn.execute(f"CREATE INDEX idx_ac_{field} ON ad_consumers({field})")
        logger.info(f"Created compound index for {field}")
    
    # Analyze tables for query optimization
    conn.execute("ANALYZE datadirect")
    conn.execute("ANALYZE ad_consumers")
    
    conn.close()
    logger.info("Database creation completed with all indexes")

def comprehensive_anti_join_analysis_v7() -> None:
    """Perform comprehensive anti-join analysis using optimized database."""
    logger.info("Starting comprehensive anti-join analysis v7.0")
    
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
    
    print("\n=== COMPREHENSIVE ANTI-JOIN ANALYSIS V7.0 (OPTIMIZED DATABASE) ===")
    print(f"DataDirect: {dd_count:,} records")
    print(f"Ad_consumers: {ac_count:,} records")
    
    # Direct field matches using optimized tables
    direct_matches: List[Tuple[str, str, str]] = [
        ("Title", "d.title = a.title", "d.title IS NOT NULL AND a.title IS NOT NULL"),
        ("FirstName", "d.first_name = a.first_name", "d.first_name IS NOT NULL AND a.first_name IS NOT NULL"),
        ("Surname", "d.surname = a.surname", "d.surname IS NOT NULL AND a.surname IS NOT NULL"),
        ("FullName", "d.full_name = a.full_name", "d.full_name IS NOT NULL AND a.full_name IS NOT NULL"),
        ("Gender", "d.gender = a.gender", "d.gender IS NOT NULL AND a.gender IS NOT NULL"),
        ("EmailStd", "d.email_std = a.email_std", "d.email_std IS NOT NULL AND a.email_std IS NOT NULL"),
        ("EmailHash", "d.email_hash = a.email_hash", "d.email_hash IS NOT NULL AND a.email_hash IS NOT NULL"),
        ("Mobile", "d.mobile = a.mobile", "d.mobile IS NOT NULL AND a.mobile IS NOT NULL"),
        ("Landline", "d.landline = a.landline", "d.landline IS NOT NULL AND a.landline IS NOT NULL"),
        ("Suburb", "d.suburb = a.suburb", "d.suburb IS NOT NULL AND a.suburb IS NOT NULL"),
        ("State", "d.state = a.state", "d.state IS NOT NULL AND a.state IS NOT NULL"),
        ("Postcode", "d.postcode = a.postcode", "d.postcode IS NOT NULL AND a.postcode IS NOT NULL")
    ]
    
    logger.info("Processing direct field matches and anti-joins")
    print("\n=== DIRECT FIELD MATCHES AND ANTI-JOINS ===")
    
    direct_results: List[AntiJoinResult] = []
    
    for match_name, condition, filter_cond in direct_matches:
        try:
            # Inner join count (matches)
            matches = conn.execute(f"""
                SELECT COUNT(*) FROM datadirect d
                INNER JOIN ad_consumers a ON {condition}
                WHERE {filter_cond}
            """).fetchone()[0]
            
            # Anti-join 1: DD ANTI JOIN AC
            dd_filter = filter_cond.split(' AND a.')[0] if ' AND a.' in filter_cond else filter_cond
            dd_only = conn.execute(f"""
                SELECT COUNT(*) FROM datadirect d
                WHERE NOT EXISTS (SELECT 1 FROM ad_consumers a WHERE {condition})
                AND {dd_filter}
            """).fetchone()[0]
            
            # Anti-join 2: AC ANTI JOIN DD
            ac_filter = filter_cond.split(' AND d.')[0].replace('d.', 'a.') if ' AND d.' in filter_cond else filter_cond.replace('d.', 'a.')
            ac_only = conn.execute(f"""
                SELECT COUNT(*) FROM ad_consumers a
                WHERE NOT EXISTS (SELECT 1 FROM datadirect d WHERE {condition})
                AND {ac_filter}
            """).fetchone()[0]
            
            # Calculate match rates
            dd_match_rate = (matches / dd_count) * 100 if dd_count > 0 else 0
            ac_match_rate = (matches / ac_count) * 100 if ac_count > 0 else 0
            
            result = AntiJoinResult(
                match_name=match_name,
                matches=matches,
                dd_only=dd_only,
                ac_only=ac_only,
                dd_match_rate=dd_match_rate,
                ac_match_rate=ac_match_rate
            )
            direct_results.append(result)
            
            print(f"{match_name:12}: Matches={matches:,}, DD_only={dd_only:,}, AC_only={ac_only:,}")
            print(f"{'':14} DD_rate={dd_match_rate:.2f}%, AC_rate={ac_match_rate:.2f}%")
            logger.info(f"{match_name} - Matches: {matches:,}, DD_only: {dd_only:,}, AC_only: {ac_only:,}")
            
        except Exception as e:
            logger.error(f"Error processing {match_name}: {str(e)}")
            continue
    
    # Compound field matches using pre-computed indexes
    compound_matches: List[Tuple[str, str]] = [
        ("Name+Suburb+Postcode", "d.name_suburb_postcode = a.name_suburb_postcode"),
        ("Name+Suburb+Postcode+Mobile", "d.name_suburb_postcode_mobile = a.name_suburb_postcode_mobile")
    ]
    
    logger.info("Processing compound field matches and anti-joins")
    print("\n=== COMPOUND FIELD MATCHES AND ANTI-JOINS ===")
    
    for match_name, condition in compound_matches:
        try:
            # Inner join count (matches)
            matches = conn.execute(f"""
                SELECT COUNT(*) FROM datadirect d
                INNER JOIN ad_consumers a ON {condition}
                WHERE {condition.replace('=', 'IS NOT NULL AND').replace('a.', 'a.').replace('d.', 'd.')}
            """).fetchone()[0]
            
            # Anti-join 1: DD ANTI JOIN AC
            dd_only = conn.execute(f"""
                SELECT COUNT(*) FROM datadirect d
                WHERE NOT EXISTS (SELECT 1 FROM ad_consumers a WHERE {condition})
            """).fetchone()[0]
            
            # Anti-join 2: AC ANTI JOIN DD
            ac_only = conn.execute(f"""
                SELECT COUNT(*) FROM ad_consumers a
                WHERE NOT EXISTS (SELECT 1 FROM datadirect d WHERE {condition})
            """).fetchone()[0]
            
            # Calculate match rates
            dd_match_rate = (matches / dd_count) * 100 if dd_count > 0 else 0
            ac_match_rate = (matches / ac_count) * 100 if ac_count > 0 else 0
            
            print(f"{match_name:25}: Matches={matches:,}, DD_only={dd_only:,}, AC_only={ac_only:,}")
            print(f"{'':27} DD_rate={dd_match_rate:.2f}%, AC_rate={ac_match_rate:.2f}%")
            logger.info(f"{match_name} - Matches: {matches:,}, DD_only: {dd_only:,}, AC_only: {ac_only:,}")
            
        except Exception as e:
            logger.error(f"Error processing {match_name}: {str(e)}")
            continue
    
    conn.close()
    logger.info("Analysis completed")

if __name__ == "__main__":
    comprehensive_anti_join_analysis_v7()