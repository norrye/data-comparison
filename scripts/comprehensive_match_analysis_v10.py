#!/usr/bin/env python3
"""
Comprehensive matching analysis v10.0 - Enhanced logging and progress reporting.

Version: 10.0
Author: Expert Data Scientist
Description: Uses DuckDB's native ANTI JOIN with detailed logging and progress reports
"""

# ...existing code...
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
    
    logger.info(f"✓ Configured {len(direct_matches)} direct field mappings")
    for i, (name, _, _) in enumerate(direct_matches, 1):
        logger.info(f"  {i:2d}. {name}")
    
    logger.info("PHASE 5: Direct field matching analysis")
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
                SELECT COUNT(*) FROM datadirect d
                WHERE NOT EXISTS (SELECT 1 FROM ad_consumers a WHERE {condition})
                AND {dd_filter}
            """).fetchone()[0]
            anti1_time = time.time() - anti1_start
            logger.info(f"  ✓ DD ANTI JOIN completed in {anti1_time:.2f}s: {dd_only:,} DD-only records")
            
            # Anti-join 2: AC ANTI JOIN DD
            logger.info(f"  → Running AC ANTI JOIN DD for {match_name}")
            anti2_start = time.time()
            ac_filter = filter_cond.split(' AND d.')[0].replace('d.', 'a.') if ' AND d.' in filter_cond else filter_cond.replace('d.', 'a.')
            ac_only = conn.execute(f"""
                SELECT COUNT(*) FROM ad_consumers a
                WHERE NOT EXISTS (SELECT 1 FROM datadirect d WHERE {condition})
                AND {ac_filter}
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
    comprehensive_anti_join_analysis_v10()   direct_results: List[AntiJoinResult] = []
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
                    SELECT COUNT(*) FROM datadirect d
                    WHERE NOT EXISTS (SELECT 1 FROM ad_consumers a WHERE {condition})
                    AND {dd_filter}
                """).fetchone()[0]
                anti1_time = time.time() - anti1_start
                logger.info(f"  ✓ DD ANTI JOIN completed in {anti1_time:.2f}s: {dd_only:,} DD-only records")
                
                # Anti-join 2: AC ANTI JOIN DD
                logger.info(f"  → Running AC ANTI JOIN DD for {match_name}")
                anti2_start = time.time()
                ac_filter = filter_cond.split(' AND d.')[0].replace('d.', 'a.') if ' AND d.' in filter_cond else filter_cond.replace('d.', 'a.')
                ac_only = conn.execute(f"""
                    SELECT COUNT(*) FROM ad_consumers a
                    WHERE NOT EXISTS (SELECT 1 FROM datadirect d WHERE {condition})
                    AND {ac_filter}
                """).fetchone()[0]
                anti2_time = time.time() - anti2_start
                logger.info(f"  ✓ AC ANTI JOIN completed in {anti2_time:.2f}s: {ac_only:,} AC-only records")
                
                # Calculate match rates
                dd_match_rate = (matches / dd_count) * 100 if dd_count > 0 else 0
                ac_match_rate = (matches / ac_count) * 100 if ac_count > 0 else 0
                
                field_total_time = time.time() - field_start_time
                
                result = AntiJoinResult(
                    match_name=match_name,
                    matches=matches,
                    dd_only=dd_only,
                    ac_only=ac_only,
                    dd_match_rate=dd_match_rate,
                    ac_match_rate=ac_match_rate,
                    processing_time=field_total_time
                )
                direct_results.append(result)
                
                print(f"{match_name:12}: Matches={matches:,}, DD_only={dd_only:,}, AC_only={ac_only:,}")
                print(f"{'':14} DD_rate={dd_match_rate:.2f}%, AC_rate={ac_match_rate:.2f}%, Time={field_total_time:.1f}s")
                logger.info(f"✓ {match_name} completed in {field_total_time:.2f}s - Matches: {matches:,}, DD_only: {dd_only:,}, AC_only: {ac_only:,}")
                
                # Progress update
                progress = (idx / total_direct_matches) * 100
                logger.info(f"PROGRESS: Direct fields {idx}/{total_direct_matches} ({progress:.1f}%) completed")
                
            except Exception as e:
                logger.error(f"ERROR processing {match_name}: {str(e)}")
                continue
        
        logger.info(f"✓ PHASE 5 completed: {len(direct_results)} direct field matches processed")
        
        # Concatenated field matches
        logger.info("PHASE 6: Concatenated field matching analysis")
        concat_matches: List[Tuple[str, str]] = [
            ("Name+Suburb+Postcode", "UPPER(TRIM(d.FirstName || ' ' || d.Surname || ' ' || d.Suburb || ' ' || CAST(d.Postcode AS VARCHAR))) = UPPER(TRIM(a.given_name_1 || ' ' || a.surname || ' ' || a.suburb || ' ' || a.postcode_text))"),
            ("Name+Suburb+Postcode+Email", "UPPER(TRIM(d.FirstName || ' ' || d.Surname || ' ' || d.Suburb || ' ' || CAST(d.Postcode AS VARCHAR) || ' ' || d.EmailStd)) = UPPER(TRIM(a.given_name_1 || ' ' || a.surname || ' ' || a.suburb || ' ' || a.postcode_text || ' ' || a.email))"),
            ("Name+Suburb+Postcode+Mobile", "UPPER(TRIM(d.FirstName || ' ' || d.Surname || ' ' || d.Suburb || ' ' || CAST(d.Postcode AS VARCHAR) || ' ' || d.Mobile)) = UPPER(TRIM(a.given_name_1 || ' ' || a.surname || ' ' || a.suburb || ' ' || a.postcode_text || ' ' || a.mobile_text))"),
            ("Name+Suburb+Postcode+Landline", "UPPER(TRIM(d.FirstName || ' ' || d.Surname || ' ' || d.Suburb || ' ' || CAST(d.Postcode AS VARCHAR) || ' ' || d.Landline)) = UPPER(TRIM(a.given_name_1 || ' ' || a.surname || ' ' || a.suburb || ' ' || a.postcode_text || ' ' || a.landline_text))")
        ]
        
        logger.info(f"✓ Configured {len(concat_matches)} concatenated field combinations")
        print("\n=== CONCATENATED FIELD MATCHES WITH ANTI JOINS ===")
        
        concat_results: List[AntiJoinResult] = []
        total_concat_matches = len(concat_matches)
        
        for idx, (match_name, condition) in enumerate(concat_matches, 1):
            concat_start_time = time.time()
            logger.info(f"Processing concatenated field {idx}/{total_concat_matches}: {match_name}")
            
            try:
                # Inner join count
                logger.info(f"  → Running INNER JOIN for {match_name}")
                matches = conn.execute(f"""
                    SELECT COUNT(*) FROM read_parquet('{file1}') d
                    INNER JOIN read_parquet('{file2}') a ON {condition}
                """).fetchone()[0]
                logger.info(f"  ✓ INNER JOIN completed: {matches:,} matches")
                
                # ANTI JOIN 1: DD ANTI JOIN AC
                logger.info(f"  → Running DD ANTI JOIN AC for {match_name}")
                dd_only = conn.execute(f"""
                    SELECT COUNT(*) FROM read_parquet('{file1}') d
                    ANTI JOIN read_parquet('{file2}') a ON {condition}
                """).fetchone()[0]
                logger.info(f"  ✓ DD ANTI JOIN completed: {dd_only:,} DD-only records")
                
                # ANTI JOIN 2: AC ANTI JOIN DD
                logger.info(f"  → Running AC ANTI JOIN DD for {match_name}")
                ac_only = conn.execute(f"""
                    SELECT COUNT(*) FROM read_parquet('{file2}') a
                    ANTI JOIN read_parquet('{file1}') d ON {condition}
                """).fetchone()[0]
                logger.info(f"  ✓ AC ANTI JOIN completed: {ac_only:,} AC-only records")
                
                # Calculate match rates
                dd_match_rate = (matches / dd_count) * 100 if dd_count > 0 else 0
                ac_match_rate = (matches / ac_count) * 100 if ac_count > 0 else 0
                
                concat_total_time = time.time() - concat_start_time
                
                result = AntiJoinResult(
                    match_name=match_name,
                    matches=matches,
                    dd_only=dd_only,
                    ac_only=ac_only,
                    dd_match_rate=dd_match_rate,
                    ac_match_rate=ac_match_rate,
                    processing_time=concat_total_time
                )
                concat_results.append(result)
                
                print(f"{match_name:25}: Matches={matches:,}, DD_only={dd_only:,}, AC_only={ac_only:,}")
                print(f"{'':27} DD_rate={dd_match_rate:.2f}%, AC_rate={ac_match_rate:.2f}%, Time={concat_total_time:.1f}s")
                logger.info(f"✓ {match_name} completed in {concat_total_time:.2f}s")
                
                # Progress update
                progress = (idx / total_concat_matches) * 100
                logger.info(f"PROGRESS: Concatenated fields {idx}/{total_concat_matches} ({progress:.1f}%) completed")
                
            except Exception as e:
                logger.error(f"ERROR processing {match_name}: {str(e)}")
                continue
        
        logger.info(f"✓ PHASE 6 completed: {len(concat_results)} concatenated field matches processed")
        
        # All fields concatenated
        logger.info("PHASE 7: All fields concatenated analysis")
        print("\n=== ALL FIELDS CONCATENATED WITH ANTI JOINS ===")
        
        all_fields_condition = """
            UPPER(TRIM(COALESCE(d.Title,'') || ' ' || COALESCE(d.FirstName,'') || ' ' || COALESCE(d.Surname,'') || ' ' || COALESCE(d.Gender,'') || ' ' || 
                       COALESCE(d.Landline,'') || ' ' || COALESCE(d.Mobile,'') || ' ' || COALESCE(d.EmailStd,'') || ' ' || 
                       COALESCE(d.Suburb,'') || ' ' || COALESCE(d.State,'') || ' ' || COALESCE(CAST(d.Postcode AS VARCHAR),''))) = 
            UPPER(TRIM(COALESCE(a.title,'') || ' ' || COALESCE(a.given_name_1,'') || ' ' || COALESCE(a.surname,'') || ' ' || COALESCE(a.gender,'') || ' ' || 
                       COALESCE(a.landline_text,'') || ' ' || COALESCE(a.mobile_text,'') || ' ' || COALESCE(a.email,'') || ' ' || 
                       COALESCE(a.suburb,'') || ' ' || COALESCE(a.state,'') || ' ' || COALESCE(a.postcode_text,'')))
        """
        
        try:
            all_fields_start = time.time()
            
            # All fields matches
            logger.info("  → Running INNER JOIN for all fields")
            all_matches = conn.execute(f"""
                SELECT COUNT(*) FROM read_parquet('{file1}') d
                INNER JOIN read_parquet('{file2}') a ON {all_fields_condition}
            """).fetchone()[0]
            logger.info(f"  ✓ All fields INNER JOIN completed: {all_matches:,} matches")
            
            # ANTI JOIN 1: DD ANTI JOIN AC
            logger.info("  → Running DD ANTI JOIN AC for all fields")
            dd_all_only = conn.execute(f"""
                SELECT COUNT(*) FROM read_parquet('{file1}') d
                ANTI JOIN read_parquet('{file2}') a ON {all_fields_condition}
            """).fetchone()[0]
            logger.info(f"  ✓ DD ANTI JOIN completed: {dd_all_only:,} DD-only records")
            
            # ANTI JOIN 2: AC ANTI JOIN DD
            logger.info("  → Running AC ANTI JOIN DD for all fields")
            ac_all_only = conn.execute(f"""
                SELECT COUNT(*) FROM read_parquet('{file2}') a
                ANTI JOIN read_parquet('{file1}') d ON {all_fields_condition}
            """).fetchone()[0]
            logger.info(f"  ✓ AC ANTI JOIN completed: {ac_all_only:,} AC-only records")
            
            # Calculate match rates
            dd_all_rate = (all_matches / dd_count) * 100 if dd_count > 0 else 0
            ac_all_rate = (all_matches / ac_count) * 100 if ac_count > 0 else 0
            
            all_fields_time = time.time() - all_fields_start
            
            print(f"All Fields      : Matches={all_matches:,}, DD_only={dd_all_only:,}, AC_only={ac_all_only:,}")
            print(f"{'':18} DD_rate={dd_all_rate:.2f}%, AC_rate={ac_all_rate:.2f}%, Time={all_fields_time:.1f}s")
            logger.info(f"✓ All fields analysis completed in {all_fields_time:.2f}s")
            
        except Exception as e:
            logger.error(f"ERROR processing all fields: {str(e)}")
        
        logger.info("✓ PHASE 7 completed: All fields concatenated analysis finished")
        
        # Final summary
        total_time = time.time() - start_time
        logger.info("PHASE 8: Final summary and cleanup")
        
        print("\n" + "=" * 80)
        print("FINAL SUMMARY")
        print("=" * 80)
        print(f"Total DD records: {dd_count:,}")
        print(f"Total AC records: {ac_count:,}")
        print(f"Total processing time: {total_time:.2f} seconds ({total_time/60:.1f} minutes)")
        print(f"Direct field matches processed: {len(direct_results)}")
        print(f"Concatenated field matches processed: {len(concat_results)}")
        
        # Performance summary
        if direct_results:
            avg_direct_time = sum(r.processing_time for r in direct_results) / len(direct_results)
            print(f"Average time per direct field: {avg_direct_time:.2f} seconds")
        
        if concat_results:
            avg_concat_time = sum(r.processing_time for r in concat_results) / len(concat_results)
            print(f"Average time per concatenated field: {avg_concat_time:.2f} seconds")
        
        print("=" * 80)
        
        conn.close()
        logger.info("✓ Database connection closed")
        logger.info("=" * 80)
        logger.info("COMPREHENSIVE ANTI-JOIN ANALYSIS V10.0 COMPLETED SUCCESSFULLY")
        logger.info(f"End time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"Total execution time: {total_time:.2f} seconds")
        logger.info("=" * 80)
        
    except Exception as e:
        logger.error("=" * 80)
        logger.error("CRITICAL ERROR IN ANALYSIS")
        logger.error(f"Error: {str(e)}")
        logger.error("=" * 80)
        raise

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Comprehensive Anti-Join Analysis v10")
    parser.add_argument('--file1', type=str, default="/data/projects/data_comparison/data/external/DATADIRECT_DL_202505_20250801_subset.parquet", help="Path to DataDirect parquet file")
    parser.add_argument('--file2', type=str, default="/data/projects/data_comparison/data/interim/ad_consumers_2020805_inter_ll_text.parquet", help="Path to Ad_consumers parquet file")
    parser.add_argument('--memory_limit', type=str, default="8GB", help="DuckDB memory limit")
    args = parser.parse_args()
    comprehensive_anti_join_analysis_v10(
        file1=args.file1,
        file2=args.file2,
        memory_limit=args.memory_limit
    )