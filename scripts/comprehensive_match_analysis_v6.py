#!/usr/bin/env python3
"""
Comprehensive matching analysis v6.0 - Using semantic mappings from column_mapping_analyzer.

Version: 6.0
Author: Expert Data Scientist
Description: Uses semantic field mappings from column_mapping_analyzer.py with landline_text and mobile_text
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

def comprehensive_anti_join_analysis_v6() -> None:
    """
    Perform comprehensive anti-join analysis using semantic field mappings.
    
    Uses semantic mappings: Title->title, FirstName->given_name_1, Surname->surname, 
    Gender->gender, Landline->landline_text, Mobile->mobile_text, EmailStd->email,
    EmailHash->email_sha256, Suburb->suburb, State->state, Postcode->postcode_text
    """
    logger.info("Starting comprehensive anti-join analysis v6.0")
    
    base_path = Path("/data/projects/data_comparison")
    file1 = base_path / "data/external/DATADIRECT_DL_202505_20250801_subset.parquet"
    file2 = base_path / "data/interim/ad_consumers_2020805_inter_ll_text.parquet"
    
    # Source validation
    if not file1.exists() or not file2.exists():
        logger.error(f"Source files not found: {file1.exists()=}, {file2.exists()=}")
        raise FileNotFoundError("Required parquet files not found")
    
    try:
        conn = duckdb.connect()
        conn.execute(f"SET threads = {max(1, int(os.cpu_count() * 0.4))}")
        logger.info(f"DuckDB connection established with {max(1, int(os.cpu_count() * 0.4))} threads (40% CPU)")
        
        # Get record counts
        dd_count = conn.execute(f"SELECT COUNT(*) FROM read_parquet('{file1}')").fetchone()[0]
        ac_count = conn.execute(f"SELECT COUNT(*) FROM read_parquet('{file2}')").fetchone()[0]
        
        logger.info(f"DataDirect: {dd_count:,} records, Ad_consumers: {ac_count:,} records")
        
        print("\n=== COMPREHENSIVE ANTI-JOIN ANALYSIS V6.0 ===")
        print(f"DataDirect: {dd_count:,} records")
        print(f"Ad_consumers: {ac_count:,} records")
        
        # Semantic field mappings from corrected semantic analyzer
        direct_matches: List[Tuple[str, str, str]] = [
            ("Title", "UPPER(TRIM(d.Title)) = UPPER(TRIM(a.title))", "d.Title IS NOT NULL AND a.title IS NOT NULL"),
            ("FirstName", "UPPER(TRIM(d.FirstName)) = UPPER(TRIM(a.given_name_1))", "d.FirstName IS NOT NULL AND a.given_name_1 IS NOT NULL"),
            ("Surname", "UPPER(TRIM(d.Surname)) = UPPER(TRIM(a.surname))", "d.Surname IS NOT NULL AND a.surname IS NOT NULL"),
            ("FullName", "UPPER(TRIM(d.FirstName || ' ' || d.Surname)) = UPPER(TRIM(a.given_name_1 || ' ' || a.surname))", "d.FirstName IS NOT NULL AND d.Surname IS NOT NULL AND a.given_name_1 IS NOT NULL AND a.surname IS NOT NULL"),
            ("Gender", "UPPER(TRIM(d.Gender)) = UPPER(TRIM(a.gender))", "d.Gender IS NOT NULL AND a.gender IS NOT NULL"),
            ("EmailStd", "UPPER(TRIM(d.EmailStd)) = UPPER(TRIM(a.email))", "d.EmailStd IS NOT NULL AND a.email IS NOT NULL"),
            ("EmailHash", "UPPER(TRIM(d.EmailHash)) = UPPER(TRIM(a.email_sha256))", "d.EmailHash IS NOT NULL AND a.email_sha256 IS NOT NULL"),
            ("Mobile", "TRIM(d.Mobile) = TRIM(a.mobile_text)", "d.Mobile IS NOT NULL AND a.mobile_text IS NOT NULL"),
            ("Landline", "TRIM(d.Landline) = TRIM(a.landline_text)", "d.Landline IS NOT NULL AND a.landline_text IS NOT NULL"),
            ("Suburb", "UPPER(TRIM(d.Suburb)) = UPPER(TRIM(a.suburb))", "d.Suburb IS NOT NULL AND a.suburb IS NOT NULL"),
            ("State", "UPPER(TRIM(d.State)) = UPPER(TRIM(a.state))", "d.State IS NOT NULL AND a.state IS NOT NULL"),
            ("Postcode", "CAST(d.Postcode AS VARCHAR) = TRIM(a.postcode_text)", "d.Postcode IS NOT NULL AND a.postcode_text IS NOT NULL")
        ]
        
        logger.info("Processing direct field matches and anti-joins")
        print("\n=== DIRECT FIELD MATCHES AND ANTI-JOINS ===")
        
        direct_results: List[AntiJoinResult] = []
        
        for match_name, condition, filter_cond in direct_matches:
            try:
                # Inner join count (matches)
                matches = conn.execute(f"""
                    SELECT COUNT(*) FROM read_parquet('{file1}') d
                    INNER JOIN read_parquet('{file2}') a ON {condition}
                    WHERE {filter_cond}
                """).fetchone()[0]
                
                # Anti-join 1: DD ANTI JOIN AC
                dd_filter = filter_cond.split(' AND a.')[0] if ' AND a.' in filter_cond else filter_cond
                dd_only = conn.execute(f"""
                    SELECT COUNT(*) FROM read_parquet('{file1}') d
                    ANTI JOIN read_parquet('{file2}') a ON {condition}
                    WHERE {dd_filter}
                """).fetchone()[0]
                
                # Anti-join 2: AC ANTI JOIN DD
                ac_filter = filter_cond.split(' AND d.')[0].replace('d.', 'a.') if ' AND d.' in filter_cond else filter_cond.replace('d.', 'a.')
                ac_only = conn.execute(f"""
                    SELECT COUNT(*) FROM read_parquet('{file2}') a
                    ANTI JOIN read_parquet('{file1}') d ON {condition}
                    WHERE {ac_filter}
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
        
        # Concatenated field matches
        concat_matches: List[Tuple[str, str]] = [
            ("Name+Suburb+Postcode", "UPPER(TRIM(d.FirstName || ' ' || d.Surname || ' ' || d.Suburb || ' ' || CAST(d.Postcode AS VARCHAR))) = UPPER(TRIM(a.given_name_1 || ' ' || a.surname || ' ' || a.suburb || ' ' || a.postcode_text))"),
            ("Name+Suburb+Postcode+Email", "UPPER(TRIM(d.FirstName || ' ' || d.Surname || ' ' || d.Suburb || ' ' || CAST(d.Postcode AS VARCHAR) || ' ' || d.EmailStd)) = UPPER(TRIM(a.given_name_1 || ' ' || a.surname || ' ' || a.suburb || ' ' || a.postcode_text || ' ' || a.email))"),
            ("Name+Suburb+Postcode+Mobile", "UPPER(TRIM(d.FirstName || ' ' || d.Surname || ' ' || d.Suburb || ' ' || CAST(d.Postcode AS VARCHAR) || ' ' || d.Mobile)) = UPPER(TRIM(a.given_name_1 || ' ' || a.surname || ' ' || a.suburb || ' ' || a.postcode_text || ' ' || a.mobile_text))"),
            ("Name+Suburb+Postcode+Landline", "UPPER(TRIM(d.FirstName || ' ' || d.Surname || ' ' || d.Suburb || ' ' || CAST(d.Postcode AS VARCHAR) || ' ' || d.Landline)) = UPPER(TRIM(a.given_name_1 || ' ' || a.surname || ' ' || a.suburb || ' ' || a.postcode_text || ' ' || a.landline_text))")
        ]
        
        logger.info("Processing concatenated field matches and anti-joins")
        print("\n=== CONCATENATED FIELD MATCHES AND ANTI-JOINS ===")
        
        concat_results: List[AntiJoinResult] = []
        
        for match_name, condition in concat_matches:
            try:
                # Inner join count (matches)
                matches = conn.execute(f"""
                    SELECT COUNT(*) FROM read_parquet('{file1}') d
                    INNER JOIN read_parquet('{file2}') a ON {condition}
                """).fetchone()[0]
                
                # Anti-join 1: DD ANTI JOIN AC
                dd_only = conn.execute(f"""
                    SELECT COUNT(*) FROM read_parquet('{file1}') d
                    ANTI JOIN read_parquet('{file2}') a ON {condition}
                """).fetchone()[0]
                
                # Anti-join 2: AC ANTI JOIN DD
                ac_only = conn.execute(f"""
                    SELECT COUNT(*) FROM read_parquet('{file2}') a
                    ANTI JOIN read_parquet('{file1}') d ON {condition}
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
                concat_results.append(result)
                
                print(f"{match_name:25}: Matches={matches:,}, DD_only={dd_only:,}, AC_only={ac_only:,}")
                print(f"{'':27} DD_rate={dd_match_rate:.2f}%, AC_rate={ac_match_rate:.2f}%")
                logger.info(f"{match_name} - Matches: {matches:,}, DD_only: {dd_only:,}, AC_only: {ac_only:,}")
                
            except Exception as e:
                logger.error(f"Error processing {match_name}: {str(e)}")
                continue
        
        # All fields concatenated
        logger.info("Processing all fields concatenated")
        print("\n=== ALL FIELDS CONCATENATED ===")
        
        all_fields_condition = """
            UPPER(TRIM(COALESCE(d.Title,'') || ' ' || COALESCE(d.FirstName,'') || ' ' || COALESCE(d.Surname,'') || ' ' || COALESCE(d.Gender,'') || ' ' || 
                       COALESCE(d.Landline,'') || ' ' || COALESCE(d.Mobile,'') || ' ' || COALESCE(d.EmailStd,'') || ' ' || 
                       COALESCE(d.Suburb,'') || ' ' || COALESCE(d.State,'') || ' ' || COALESCE(CAST(d.Postcode AS VARCHAR),''))) = 
            UPPER(TRIM(COALESCE(a.title,'') || ' ' || COALESCE(a.given_name_1,'') || ' ' || COALESCE(a.surname,'') || ' ' || COALESCE(a.gender,'') || ' ' || 
                       COALESCE(a.landline_text,'') || ' ' || COALESCE(a.mobile_text,'') || ' ' || COALESCE(a.email,'') || ' ' || 
                       COALESCE(a.suburb,'') || ' ' || COALESCE(a.state,'') || ' ' || COALESCE(a.postcode_text,'')))
        """
        
        try:
            # All fields matches
            all_matches = conn.execute(f"""
                SELECT COUNT(*) FROM read_parquet('{file1}') d
                INNER JOIN read_parquet('{file2}') a ON {all_fields_condition}
            """).fetchone()[0]
            
            # Anti-join 1: DD ANTI JOIN AC
            dd_all_only = conn.execute(f"""
                SELECT COUNT(*) FROM read_parquet('{file1}') d
                ANTI JOIN read_parquet('{file2}') a ON {all_fields_condition}
            """).fetchone()[0]
            
            # Anti-join 2: AC ANTI JOIN DD
            ac_all_only = conn.execute(f"""
                SELECT COUNT(*) FROM read_parquet('{file2}') a
                ANTI JOIN read_parquet('{file1}') d ON {all_fields_condition}
            """).fetchone()[0]
            
            # Calculate match rates
            dd_all_rate = (all_matches / dd_count) * 100 if dd_count > 0 else 0
            ac_all_rate = (all_matches / ac_count) * 100 if ac_count > 0 else 0
            
            print(f"All Fields      : Matches={all_matches:,}, DD_only={dd_all_only:,}, AC_only={ac_all_only:,}")
            print(f"{'':18} DD_rate={dd_all_rate:.2f}%, AC_rate={ac_all_rate:.2f}%")
            logger.info(f"All Fields - Matches: {all_matches:,}, DD_only: {dd_all_only:,}, AC_only: {ac_all_only:,}")
            
        except Exception as e:
            logger.error(f"Error processing all fields: {str(e)}")
        
        # Summary
        print("\n=== SUMMARY ===")
        print(f"Total DD records: {dd_count:,}")
        print(f"Total AC records: {ac_count:,}")
        
        conn.close()
        logger.info("Comprehensive anti-join analysis v6.0 completed successfully")
        
    except Exception as e:
        logger.error(f"Critical error in analysis: {str(e)}")
        raise

if __name__ == "__main__":
    comprehensive_anti_join_analysis_v6()