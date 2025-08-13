#!/usr/bin/env python3
"""
Comprehensive matching analysis v2.0 - Anti-joins with both tables on left side.

Version: 2.0
Author: Expert Data Scientist
Description: Comprehensive data matching analysis using anti-joins with both tables as left side for complete missing data overview
"""

import duckdb
from pathlib import Path
from loguru import logger
from pydantic import BaseModel, Field
from typing import List, Tuple, Dict, Any
import os

class AntiJoinResult(BaseModel):
    """Data validation model for anti-join results."""
    match_name: str = Field(..., description="Name of the matching pattern")
    matches: int = Field(..., ge=0, description="Number of matches found")
    dd_missing_from_ac: int = Field(..., ge=0, description="DD records missing from AC")
    ac_missing_from_dd: int = Field(..., ge=0, description="AC records missing from DD")
    dd_match_rate: float = Field(..., ge=0, le=100, description="DD match rate percentage")
    ac_match_rate: float = Field(..., ge=0, le=100, description="AC match rate percentage")

def comprehensive_anti_join_analysis_v2() -> None:
    """
    Perform comprehensive anti-join analysis with both tables on left side.
    
    Uses anti-joins to identify missing records in both directions for complete data coverage analysis.
    """
    logger.info("Starting comprehensive anti-join analysis v2.0")
    
    base_path = Path("/data/projects/data_comparison")
    file1 = base_path / "data/external/DATADIRECT_DL_202505_20250801_subset.parquet"
    file2 = base_path / "data/interim/ad_consumers_2020805_inter.parquet"
    
    # Source validation
    if not file1.exists() or not file2.exists():
        logger.error("Source files not found")
        raise FileNotFoundError("Required parquet files not found")
    
    try:
        conn = duckdb.connect()
        conn.execute(f"SET threads = {max(1, os.cpu_count() // 2)}")
        logger.info(f"DuckDB connection established")
        
        # Get record counts
        dd_count = conn.execute(f"SELECT COUNT(*) FROM read_parquet('{file1}')").fetchone()[0]
        ac_count = conn.execute(f"SELECT COUNT(*) FROM read_parquet('{file2}')").fetchone()[0]
        
        logger.info(f"DataDirect: {dd_count:,} records, Ad_consumers: {ac_count:,} records")
        
        print("\n=== COMPREHENSIVE ANTI-JOIN ANALYSIS V2.0 ===")
        print(f"DataDirect: {dd_count:,} records")
        print(f"Ad_consumers: {ac_count:,} records")
        
        # Direct field matches with anti-joins from both sides
        direct_matches: List[Tuple[str, str, str]] = [
            ("FirstName", "UPPER(TRIM(d.FirstName)) = UPPER(TRIM(a.given_name_1))", "d.FirstName IS NOT NULL AND a.given_name_1 IS NOT NULL"),
            ("LastName", "UPPER(TRIM(d.Surname)) = UPPER(TRIM(a.surname))", "d.Surname IS NOT NULL AND a.surname IS NOT NULL"),
            ("FullName", "UPPER(TRIM(d.FirstName || ' ' || d.Surname)) = UPPER(TRIM(a.given_name_1 || ' ' || a.surname))", "d.FirstName IS NOT NULL AND d.Surname IS NOT NULL AND a.given_name_1 IS NOT NULL AND a.surname IS NOT NULL"),
            ("Email", "UPPER(TRIM(d.EmailStd)) = UPPER(TRIM(a.email))", "d.EmailStd IS NOT NULL AND a.email IS NOT NULL"),
            ("Mobile", "TRIM(d.Mobile) = TRIM(a.mobile_text)", "d.Mobile IS NOT NULL AND a.mobile_text IS NOT NULL"),
            ("Landline", "TRIM(d.Landline) = TRIM(a.landline)", "d.Landline IS NOT NULL AND a.landline IS NOT NULL")
        ]
        
        logger.info("Processing direct field anti-joins")
        print("\n=== DIRECT FIELD ANTI-JOINS ===")
        
        direct_results: List[AntiJoinResult] = []
        
        for match_name, condition, filter_cond in direct_matches:
            try:
                # Inner join count (matches)
                matches = conn.execute(f"""
                    SELECT COUNT(*) FROM read_parquet('{file1}') d
                    INNER JOIN read_parquet('{file2}') a ON {condition}
                    WHERE {filter_cond}
                """).fetchone()[0]
                
                # Anti-join: DD LEFT, AC RIGHT (DD records missing from AC)
                dd_missing = conn.execute(f"""
                    SELECT COUNT(*) FROM read_parquet('{file1}') d
                    LEFT ANTI JOIN read_parquet('{file2}') a ON {condition}
                    WHERE {filter_cond.split(' AND a.')[0]}
                """).fetchone()[0]
                
                # Anti-join: AC LEFT, DD RIGHT (AC records missing from DD)  
                ac_missing = conn.execute(f"""
                    SELECT COUNT(*) FROM read_parquet('{file2}') a
                    LEFT ANTI JOIN read_parquet('{file1}') d ON {condition}
                    WHERE {filter_cond.split(' AND d.')[0].replace('d.', 'a.')}
                """).fetchone()[0]
                
                # Calculate match rates
                dd_match_rate = (matches / dd_count) * 100 if dd_count > 0 else 0
                ac_match_rate = (matches / ac_count) * 100 if ac_count > 0 else 0
                
                result = AntiJoinResult(
                    match_name=match_name,
                    matches=matches,
                    dd_missing_from_ac=dd_missing,
                    ac_missing_from_dd=ac_missing,
                    dd_match_rate=dd_match_rate,
                    ac_match_rate=ac_match_rate
                )
                direct_results.append(result)
                
                print(f"{match_name:12}: Matches={matches:,}, DD_missing={dd_missing:,}, AC_missing={ac_missing:,}")
                print(f"{'':14} DD_rate={dd_match_rate:.2f}%, AC_rate={ac_match_rate:.2f}%")
                logger.info(f"{match_name} - Matches: {matches:,}, DD_missing: {dd_missing:,}, AC_missing: {ac_missing:,}")
                
            except Exception as e:
                logger.error(f"Error processing {match_name}: {str(e)}")
                continue
        
        # Concatenated field anti-joins
        concat_matches: List[Tuple[str, str]] = [
            ("Name+Suburb+Postcode", "UPPER(TRIM(d.FirstName || ' ' || d.Surname || ' ' || d.Suburb || ' ' || d.Postcode)) = UPPER(TRIM(a.given_name_1 || ' ' || a.surname || ' ' || a.suburb || ' ' || a.postcode_text))"),
            ("Name+Suburb+Postcode+Email", "UPPER(TRIM(d.FirstName || ' ' || d.Surname || ' ' || d.Suburb || ' ' || d.Postcode || ' ' || d.EmailStd)) = UPPER(TRIM(a.given_name_1 || ' ' || a.surname || ' ' || a.suburb || ' ' || a.postcode_text || ' ' || a.email))"),
            ("Name+Suburb+Postcode+Mobile", "UPPER(TRIM(d.FirstName || ' ' || d.Surname || ' ' || d.Suburb || ' ' || d.Postcode || ' ' || d.Mobile)) = UPPER(TRIM(a.given_name_1 || ' ' || a.surname || ' ' || a.suburb || ' ' || a.postcode_text || ' ' || a.mobile_text))"),
            ("Name+Suburb+Postcode+Landline", "UPPER(TRIM(d.FirstName || ' ' || d.Surname || ' ' || d.Suburb || ' ' || d.Postcode || ' ' || d.Landline)) = UPPER(TRIM(a.given_name_1 || ' ' || a.surname || ' ' || a.suburb || ' ' || a.postcode_text || ' ' || a.landline))")
        ]
        
        logger.info("Processing concatenated field anti-joins")
        print("\n=== CONCATENATED FIELD ANTI-JOINS ===")
        
        concat_results: List[AntiJoinResult] = []
        
        for match_name, condition in concat_matches:
            try:
                # Inner join count (matches)
                matches = conn.execute(f"""
                    SELECT COUNT(*) FROM read_parquet('{file1}') d
                    INNER JOIN read_parquet('{file2}') a ON {condition}
                """).fetchone()[0]
                
                # Anti-join: DD LEFT, AC RIGHT
                dd_missing = conn.execute(f"""
                    SELECT COUNT(*) FROM read_parquet('{file1}') d
                    LEFT ANTI JOIN read_parquet('{file2}') a ON {condition}
                """).fetchone()[0]
                
                # Anti-join: AC LEFT, DD RIGHT
                ac_missing = conn.execute(f"""
                    SELECT COUNT(*) FROM read_parquet('{file2}') a
                    LEFT ANTI JOIN read_parquet('{file1}') d ON {condition}
                """).fetchone()[0]
                
                # Calculate match rates
                dd_match_rate = (matches / dd_count) * 100 if dd_count > 0 else 0
                ac_match_rate = (matches / ac_count) * 100 if ac_count > 0 else 0
                
                result = AntiJoinResult(
                    match_name=match_name,
                    matches=matches,
                    dd_missing_from_ac=dd_missing,
                    ac_missing_from_dd=ac_missing,
                    dd_match_rate=dd_match_rate,
                    ac_match_rate=ac_match_rate
                )
                concat_results.append(result)
                
                print(f"{match_name:25}: Matches={matches:,}, DD_missing={dd_missing:,}, AC_missing={ac_missing:,}")
                print(f"{'':27} DD_rate={dd_match_rate:.2f}%, AC_rate={ac_match_rate:.2f}%")
                logger.info(f"{match_name} - Matches: {matches:,}, DD_missing: {dd_missing:,}, AC_missing: {ac_missing:,}")
                
            except Exception as e:
                logger.error(f"Error processing {match_name}: {str(e)}")
                continue
        
        # All fields concatenated anti-join
        logger.info("Processing all fields concatenated anti-join")
        print("\n=== ALL FIELDS CONCATENATED ANTI-JOIN ===")
        
        all_fields_condition = """
            UPPER(TRIM(COALESCE(d.Title,'') || ' ' || COALESCE(d.FirstName,'') || ' ' || COALESCE(d.Surname,'') || ' ' || COALESCE(d.Gender,'') || ' ' || 
                       COALESCE(d.Landline,'') || ' ' || COALESCE(d.Mobile,'') || ' ' || COALESCE(d.EmailStd,'') || ' ' || 
                       COALESCE(d.Suburb,'') || ' ' || COALESCE(d.State,'') || ' ' || COALESCE(d.Postcode,''))) = 
            UPPER(TRIM(COALESCE(a.title,'') || ' ' || COALESCE(a.given_name_1,'') || ' ' || COALESCE(a.surname,'') || ' ' || COALESCE(a.gender,'') || ' ' || 
                       COALESCE(a.landline,'') || ' ' || COALESCE(a.mobile_text,'') || ' ' || COALESCE(a.email,'') || ' ' || 
                       COALESCE(a.suburb,'') || ' ' || COALESCE(a.state,'') || ' ' || COALESCE(a.postcode_text,'')))
        """
        
        try:
            # All fields matches
            all_matches = conn.execute(f"""
                SELECT COUNT(*) FROM read_parquet('{file1}') d
                INNER JOIN read_parquet('{file2}') a ON {all_fields_condition}
            """).fetchone()[0]
            
            # Anti-join: DD LEFT, AC RIGHT
            dd_all_missing = conn.execute(f"""
                SELECT COUNT(*) FROM read_parquet('{file1}') d
                LEFT ANTI JOIN read_parquet('{file2}') a ON {all_fields_condition}
            """).fetchone()[0]
            
            # Anti-join: AC LEFT, DD RIGHT
            ac_all_missing = conn.execute(f"""
                SELECT COUNT(*) FROM read_parquet('{file2}') a
                LEFT ANTI JOIN read_parquet('{file1}') d ON {all_fields_condition}
            """).fetchone()[0]
            
            # Calculate match rates
            dd_all_rate = (all_matches / dd_count) * 100 if dd_count > 0 else 0
            ac_all_rate = (all_matches / ac_count) * 100 if ac_count > 0 else 0
            
            all_fields_result = AntiJoinResult(
                match_name="All Fields",
                matches=all_matches,
                dd_missing_from_ac=dd_all_missing,
                ac_missing_from_dd=ac_all_missing,
                dd_match_rate=dd_all_rate,
                ac_match_rate=ac_all_rate
            )
            
            print(f"All Fields      : Matches={all_matches:,}, DD_missing={dd_all_missing:,}, AC_missing={ac_all_missing:,}")
            print(f"{'':18} DD_rate={dd_all_rate:.2f}%, AC_rate={ac_all_rate:.2f}%")
            logger.info(f"All Fields - Matches: {all_matches:,}, DD_missing: {dd_all_missing:,}, AC_missing: {ac_all_missing:,}")
            
        except Exception as e:
            logger.error(f"Error processing all fields: {str(e)}")
            all_fields_result = None
        
        # Comprehensive summary
        logger.info("Generating comprehensive summary")
        print("\n=== COMPREHENSIVE SUMMARY ===")
        print(f"Total DD records: {dd_count:,}")
        print(f"Total AC records: {ac_count:,}")
        
        if all_fields_result:
            print(f"Perfect matches: {all_fields_result.matches:,} ({all_fields_result.dd_match_rate:.2f}% of DD, {all_fields_result.ac_match_rate:.2f}% of AC)")
            print(f"DD records not in AC: {all_fields_result.dd_missing_from_ac:,} ({(all_fields_result.dd_missing_from_ac/dd_count)*100:.2f}%)")
            print(f"AC records not in DD: {all_fields_result.ac_missing_from_dd:,} ({(all_fields_result.ac_missing_from_dd/ac_count)*100:.2f}%)")
        
        # Data coverage analysis
        print("\n=== DATA COVERAGE ANALYSIS ===")
        total_unique_records = dd_count + ac_count - (all_fields_result.matches if all_fields_result else 0)
        overlap_rate = (all_fields_result.matches / total_unique_records * 100) if all_fields_result and total_unique_records > 0 else 0
        print(f"Total unique records across both datasets: {total_unique_records:,}")
        print(f"Data overlap rate: {overlap_rate:.2f}%")
        
        conn.close()
        logger.info("Comprehensive anti-join analysis v2.0 completed successfully")
        
    except Exception as e:
        logger.error(f"Critical error in anti-join analysis: {str(e)}")
        raise

if __name__ == "__main__":
    comprehensive_anti_join_analysis_v2()