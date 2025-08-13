#!/usr/bin/env python3
"""
Comprehensive matching analysis v8.0 - Export joins to parquet files then analyze.

Version: 8.0
Author: Expert Data Scientist
Description: Exports join results to parquet files, then runs statistics on them
"""

import duckdb
from pathlib import Path
from loguru import logger
from pydantic import BaseModel, Field
from typing import List, Tuple
import os

class MatchResult(BaseModel):
    """Data validation model for match results."""
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
        ORDER BY surname, suburb, state, postcode
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
        ORDER BY surname, suburb, state, postcode_text
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
    compound_fields = ['full_name', 'name_suburb_postcode', 'name_suburb_postcode_mobile', 'name_suburb_postcode_email']
    
    for field in compound_fields:
        conn.execute(f"CREATE INDEX idx_dd_{field} ON datadirect({field})")
        conn.execute(f"CREATE INDEX idx_ac_{field} ON ad_consumers({field})")
        logger.info(f"Created compound index for {field}")
    
    # Analyze tables for query optimization
    conn.execute("ANALYZE datadirect")
    conn.execute("ANALYZE ad_consumers")
    
    conn.close()
    logger.info("Database creation completed with all indexes")

def export_join_results() -> None:
    """Export all join results to parquet files."""
    logger.info("=== STARTING JOIN EXPORT PROCESS ===")
    logger.info("Exporting join results to parquet files")
    
    base_path = Path("/data/projects/data_comparison")
    db_path = base_path / "data/processed/match_analysis.duckdb"
    results_path = base_path / "data/interim/match_results"
    
    # Create results directory
    results_path.mkdir(exist_ok=True)
    
    conn = duckdb.connect(str(db_path))
    conn.execute("SET threads = 4")
    
    # Direct field matches
    direct_matches: List[Tuple[str, str, str]] = [
        ("title", "d.title = a.title", "d.title IS NOT NULL AND a.title IS NOT NULL"),
        ("first_name", "d.first_name = a.first_name", "d.first_name IS NOT NULL AND a.first_name IS NOT NULL"),
        ("surname", "d.surname = a.surname", "d.surname IS NOT NULL AND a.surname IS NOT NULL"),
        ("full_name", "d.full_name = a.full_name", "d.full_name IS NOT NULL AND a.full_name IS NOT NULL"),
        ("gender", "d.gender = a.gender", "d.gender IS NOT NULL AND a.gender IS NOT NULL"),
        ("email_std", "d.email_std = a.email_std", "d.email_std IS NOT NULL AND a.email_std IS NOT NULL"),
        ("email_hash", "d.email_hash = a.email_hash", "d.email_hash IS NOT NULL AND a.email_hash IS NOT NULL"),
        ("mobile", "d.mobile = a.mobile", "d.mobile IS NOT NULL AND a.mobile IS NOT NULL"),
        ("landline", "d.landline = a.landline", "d.landline IS NOT NULL AND a.landline IS NOT NULL"),
        ("suburb", "d.suburb = a.suburb", "d.suburb IS NOT NULL AND a.suburb IS NOT NULL"),
        ("state", "d.state = a.state", "d.state IS NOT NULL AND a.state IS NOT NULL"),
        ("postcode", "d.postcode = a.postcode", "d.postcode IS NOT NULL AND a.postcode IS NOT NULL")
    ]
    
    logger.info(f"Processing {len(direct_matches)} direct field matches:")
    for match_name, condition, filter_cond in direct_matches:
        logger.info(f"  - {match_name}: {condition} WHERE {filter_cond}")
    
    # Export inner joins (matches)
    logger.info("=== EXPORTING INNER JOIN RESULTS (MATCHES) ===")
    for match_name, condition, filter_cond in direct_matches:
        try:
            output_file = results_path / f"matches_{match_name}.parquet"
            conn.execute(f"""
                COPY (
                    SELECT d.ID as dd_id, a.adId as ac_id, '{match_name}' as match_type
                    FROM datadirect d
                    INNER JOIN ad_consumers a ON {condition}
                    WHERE {filter_cond}
                ) TO '{output_file}' (FORMAT PARQUET)
            """)
            # Get count and file size for logging
            count = conn.execute(f"SELECT COUNT(*) FROM read_parquet('{output_file}')").fetchone()[0]
            file_size_mb = output_file.stat().st_size / (1024 * 1024)
            logger.info(f"Exported {count:,} matches for {match_name} ({file_size_mb:.1f} MB)")
        except Exception as e:
            logger.error(f"Error exporting matches for {match_name}: {str(e)}")
    
    # Export anti-joins (DD only)
    logger.info("=== EXPORTING DATADIRECT ANTI-JOIN RESULTS (DD ONLY) ===")
    for match_name, condition, filter_cond in direct_matches:
        try:
            output_file = results_path / f"dd_only_{match_name}.parquet"
            dd_filter = filter_cond.split(' AND a.')[0] if ' AND a.' in filter_cond else filter_cond
            conn.execute(f"""
                COPY (
                    SELECT d.ID as dd_id, '{match_name}' as match_type
                    FROM datadirect d
                    WHERE NOT EXISTS (SELECT 1 FROM ad_consumers a WHERE {condition})
                    AND {dd_filter}
                ) TO '{output_file}' (FORMAT PARQUET)
            """)
            # Get count and file size for logging
            count = conn.execute(f"SELECT COUNT(*) FROM read_parquet('{output_file}')").fetchone()[0]
            file_size_mb = output_file.stat().st_size / (1024 * 1024)
            logger.info(f"Exported {count:,} DD-only records for {match_name} ({file_size_mb:.1f} MB)")
        except Exception as e:
            logger.error(f"Error exporting DD-only for {match_name}: {str(e)}")
    
    # Export anti-joins (AC only)
    logger.info("=== EXPORTING AD_CONSUMERS ANTI-JOIN RESULTS (AC ONLY) ===")
    for match_name, condition, filter_cond in direct_matches:
        try:
            output_file = results_path / f"ac_only_{match_name}.parquet"
            ac_filter = filter_cond.split(' AND d.')[0].replace('d.', 'a.') if ' AND d.' in filter_cond else filter_cond.replace('d.', 'a.')
            conn.execute(f"""
                COPY (
                    SELECT a.adId as ac_id, '{match_name}' as match_type
                    FROM ad_consumers a
                    WHERE NOT EXISTS (SELECT 1 FROM datadirect d WHERE {condition})
                    AND {ac_filter}
                ) TO '{output_file}' (FORMAT PARQUET)
            """)
            # Get count and file size for logging
            count = conn.execute(f"SELECT COUNT(*) FROM read_parquet('{output_file}')").fetchone()[0]
            file_size_mb = output_file.stat().st_size / (1024 * 1024)
            logger.info(f"Exported {count:,} AC-only records for {match_name} ({file_size_mb:.1f} MB)")
        except Exception as e:
            logger.error(f"Error exporting AC-only for {match_name}: {str(e)}")
    
    # Export compound matches
    compound_matches: List[Tuple[str, str]] = [
        ("name_suburb_postcode", "d.name_suburb_postcode = a.name_suburb_postcode"),
        ("name_suburb_postcode_mobile", "d.name_suburb_postcode_mobile = a.name_suburb_postcode_mobile")
    ]
    
    logger.info("=== EXPORTING COMPOUND MATCH RESULTS ===")
    logger.info(f"Processing {len(compound_matches)} compound field matches:")
    for match_name, condition in compound_matches:
        logger.info(f"  - {match_name}: {condition}")
    for match_name, condition in compound_matches:
        try:
            # Matches
            output_file = results_path / f"matches_{match_name}.parquet"
            conn.execute(f"""
                COPY (
                    SELECT d.ID as dd_id, a.adId as ac_id, '{match_name}' as match_type
                    FROM datadirect d
                    INNER JOIN ad_consumers a ON {condition}
                    WHERE d.{match_name.split('=')[0].strip()} IS NOT NULL 
                    AND a.{match_name.split('=')[0].strip().replace('d.', '')} IS NOT NULL
                ) TO '{output_file}' (FORMAT PARQUET)
            """)
            
            # DD only
            output_file = results_path / f"dd_only_{match_name}.parquet"
            conn.execute(f"""
                COPY (
                    SELECT d.ID as dd_id, '{match_name}' as match_type
                    FROM datadirect d
                    WHERE NOT EXISTS (SELECT 1 FROM ad_consumers a WHERE {condition})
                    AND d.{match_name.split('=')[0].strip()} IS NOT NULL
                ) TO '{output_file}' (FORMAT PARQUET)
            """)
            
            # AC only
            output_file = results_path / f"ac_only_{match_name}.parquet"
            conn.execute(f"""
                COPY (
                    SELECT a.adId as ac_id, '{match_name}' as match_type
                    FROM ad_consumers a
                    WHERE NOT EXISTS (SELECT 1 FROM datadirect d WHERE {condition})
                    AND a.{match_name.split('=')[0].strip().replace('d.', '')} IS NOT NULL
                ) TO '{output_file}' (FORMAT PARQUET)
            """)
            
            # Get counts for logging
            matches_count = conn.execute(f"SELECT COUNT(*) FROM read_parquet('{results_path / f'matches_{match_name}.parquet'}')").fetchone()[0]
            dd_only_count = conn.execute(f"SELECT COUNT(*) FROM read_parquet('{results_path / f'dd_only_{match_name}.parquet'}')").fetchone()[0]
            ac_only_count = conn.execute(f"SELECT COUNT(*) FROM read_parquet('{results_path / f'ac_only_{match_name}.parquet'}')").fetchone()[0]
            logger.info(f"Exported compound {match_name}: {matches_count:,} matches, {dd_only_count:,} DD-only, {ac_only_count:,} AC-only")
        except Exception as e:
            logger.error(f"Error exporting compound matches for {match_name}: {str(e)}")
    
    conn.close()
    logger.info("=== JOIN EXPORT PROCESS COMPLETED ===")

def analyze_exported_results() -> None:
    """Analyze the exported parquet files and generate statistics."""
    logger.info("Analyzing exported results")
    
    base_path = Path("/data/projects/data_comparison")
    db_path = base_path / "data/processed/match_analysis.duckdb"
    results_path = base_path / "data/interim/match_results"
    
    conn = duckdb.connect(str(db_path))
    conn.execute("SET threads = 4")
    
    # Get total counts
    dd_count = conn.execute("SELECT COUNT(*) FROM datadirect").fetchone()[0]
    ac_count = conn.execute("SELECT COUNT(*) FROM ad_consumers").fetchone()[0]
    
    print("\n=== COMPREHENSIVE MATCH ANALYSIS V8.0 (PARQUET EXPORT METHOD) ===")
    print(f"DataDirect: {dd_count:,} records")
    print(f"Ad_consumers: {ac_count:,} records")
    print("\n=== DIRECT FIELD MATCHES AND ANTI-JOINS ===")
    
    # Analyze each match type
    match_types = ["title", "first_name", "surname", "full_name", "gender", "email_std", 
                   "email_hash", "mobile", "landline", "suburb", "state", "postcode"]
    
    results: List[MatchResult] = []
    
    for match_type in match_types:
        try:
            # Count matches
            matches_file = results_path / f"matches_{match_type}.parquet"
            if matches_file.exists():
                matches = conn.execute(f"SELECT COUNT(*) FROM read_parquet('{matches_file}')").fetchone()[0]
            else:
                matches = 0
            
            # Count DD only
            dd_only_file = results_path / f"dd_only_{match_type}.parquet"
            if dd_only_file.exists():
                dd_only = conn.execute(f"SELECT COUNT(*) FROM read_parquet('{dd_only_file}')").fetchone()[0]
            else:
                dd_only = 0
            
            # Count AC only
            ac_only_file = results_path / f"ac_only_{match_type}.parquet"
            if ac_only_file.exists():
                ac_only = conn.execute(f"SELECT COUNT(*) FROM read_parquet('{ac_only_file}')").fetchone()[0]
            else:
                ac_only = 0
            
            # Calculate match rates
            dd_match_rate = (matches / dd_count) * 100 if dd_count > 0 else 0
            ac_match_rate = (matches / ac_count) * 100 if ac_count > 0 else 0
            
            result = MatchResult(
                match_name=match_type,
                matches=matches,
                dd_only=dd_only,
                ac_only=ac_only,
                dd_match_rate=dd_match_rate,
                ac_match_rate=ac_match_rate
            )
            results.append(result)
            
            print(f"{match_type:12}: Matches={matches:,}, DD_only={dd_only:,}, AC_only={ac_only:,}")
            print(f"{'':14} DD_rate={dd_match_rate:.2f}%, AC_rate={ac_match_rate:.2f}%")
            logger.info(f"{match_type} - Matches: {matches:,}, DD_only: {dd_only:,}, AC_only: {ac_only:,}")
            
        except Exception as e:
            logger.error(f"Error analyzing {match_type}: {str(e)}")
    
    # Analyze compound matches
    print("\n=== COMPOUND FIELD MATCHES ===")
    compound_types = ["name_suburb_postcode", "name_suburb_postcode_mobile"]
    
    for match_type in compound_types:
        try:
            # Count matches
            matches_file = results_path / f"matches_{match_type}.parquet"
            if matches_file.exists():
                matches = conn.execute(f"SELECT COUNT(*) FROM read_parquet('{matches_file}')").fetchone()[0]
            else:
                matches = 0
            
            # Count DD only
            dd_only_file = results_path / f"dd_only_{match_type}.parquet"
            if dd_only_file.exists():
                dd_only = conn.execute(f"SELECT COUNT(*) FROM read_parquet('{dd_only_file}')").fetchone()[0]
            else:
                dd_only = 0
            
            # Count AC only
            ac_only_file = results_path / f"ac_only_{match_type}.parquet"
            if ac_only_file.exists():
                ac_only = conn.execute(f"SELECT COUNT(*) FROM read_parquet('{ac_only_file}')").fetchone()[0]
            else:
                ac_only = 0
            
            # Calculate match rates
            dd_match_rate = (matches / dd_count) * 100 if dd_count > 0 else 0
            ac_match_rate = (matches / ac_count) * 100 if ac_count > 0 else 0
            
            print(f"{match_type:25}: Matches={matches:,}, DD_only={dd_only:,}, AC_only={ac_only:,}")
            print(f"{'':27} DD_rate={dd_match_rate:.2f}%, AC_rate={ac_match_rate:.2f}%")
            logger.info(f"{match_type} - Matches: {matches:,}, DD_only: {dd_only:,}, AC_only: {ac_only:,}")
            
        except Exception as e:
            logger.error(f"Error analyzing compound {match_type}: {str(e)}")
    
    conn.close()
    logger.info("Analysis completed")

def comprehensive_match_analysis_v8() -> None:
    """Main function to run the complete analysis."""
    base_path = Path("/data/projects/data_comparison")
    db_path = base_path / "data/processed/match_analysis.duckdb"
    
    # Create database if it doesn't exist
    if not db_path.exists():
        create_optimized_database()
    
    # Export join results to parquet files
    export_join_results()
    
    # Analyze the exported results
    analyze_exported_results()

if __name__ == "__main__":
    comprehensive_match_analysis_v8()