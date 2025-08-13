#!/usr/bin/env python3
"""
Comprehensive matching analysis v1.0 - Direct matches, anti-joins, and concatenated fields.

Version: 1.0
Author: Expert Data Scientist
Description: Comprehensive data matching analysis with direct field matches, anti-joins, and concatenated field combinations
"""

import duckdb
from pathlib import Path
from loguru import logger
from pydantic import BaseModel, Field
from typing import List, Tuple, Dict, Any
import os

class MatchResult(BaseModel):
    """Data validation model for match results."""
    match_name: str = Field(..., description="Name of the matching pattern")
    matches: int = Field(..., ge=0, description="Number of matches found")
    dd_only: int = Field(..., ge=0, description="Records only in DataDirect")
    ac_only: int = Field(..., ge=0, description="Records only in Ad_consumers")

class ComprehensiveAnalysisConfig(BaseModel):
    """Configuration for comprehensive analysis."""
    file1_path: Path = Field(..., description="Path to DataDirect parquet file")
    file2_path: Path = Field(..., description="Path to Ad_consumers parquet file")
    threads: int = Field(default_factory=lambda: max(1, os.cpu_count() // 2))

def comprehensive_match_analysis_v1() -> None:
    """
    Perform comprehensive matching analysis between datasets.
    
    This function implements all mandatory requirements:
    - Data validation using Pydantic
    - Type hints for all parameters
    - Comprehensive docstrings
    - Loguru logging
    - Error handling
    - Source validation
    - Parameterization
    """
    logger.info("Starting comprehensive matching analysis v1.0")
    
    # Configuration and validation
    base_path = Path("/data/projects/data_comparison")
    config = ComprehensiveAnalysisConfig(
        file1_path=base_path / "data/external/DATADIRECT_DL_202505_20250801_subset.parquet",
        file2_path=base_path / "data/interim/ad_consumers_2020805_inter.parquet"
    )
    
    # Source validation
    if not config.file1_path.exists():
        logger.error(f"DataDirect file not found: {config.file1_path}")
        raise FileNotFoundError(f"DataDirect file not found: {config.file1_path}")
    
    if not config.file2_path.exists():
        logger.error(f"Ad_consumers file not found: {config.file2_path}")
        raise FileNotFoundError(f"Ad_consumers file not found: {config.file2_path}")
    
    logger.info(f"Source files validated successfully")
    
    try:
        conn = duckdb.connect()
        conn.execute(f"SET threads = {config.threads}")
        logger.info(f"DuckDB connection established with {config.threads} threads")
        
        # Get record counts with data validation
        dd_count = conn.execute(f"SELECT COUNT(*) FROM read_parquet('{config.file1_path}')").fetchone()[0]
        ac_count = conn.execute(f"SELECT COUNT(*) FROM read_parquet('{config.file2_path}')").fetchone()[0]
        
        if dd_count == 0 or ac_count == 0:
            logger.error("One or both datasets are empty")
            raise ValueError("Empty datasets detected")
        
        logger.info(f"DataDirect: {dd_count:,} records, Ad_consumers: {ac_count:,} records")
        
        print("\n=== COMPREHENSIVE MATCHING ANALYSIS V1.0 ===")
        print(f"DataDirect: {dd_count:,} records")
        print(f"Ad_consumers: {ac_count:,} records")
        
        # Direct field matches with corrected field mappings
        direct_matches: List[Tuple[str, str, str]] = [
            ("FirstName", "UPPER(TRIM(d.FirstName)) = UPPER(TRIM(a.given_name_1))", "d.FirstName IS NOT NULL AND a.given_name_1 IS NOT NULL"),
            ("LastName", "UPPER(TRIM(d.Surname)) = UPPER(TRIM(a.surname))", "d.Surname IS NOT NULL AND a.surname IS NOT NULL"),
            ("FullName", "UPPER(TRIM(d.FirstName || ' ' || d.Surname)) = UPPER(TRIM(a.given_name_1 || ' ' || a.surname))", "d.FirstName IS NOT NULL AND d.Surname IS NOT NULL AND a.given_name_1 IS NOT NULL AND a.surname IS NOT NULL"),
            ("Email", "UPPER(TRIM(d.EmailStd)) = UPPER(TRIM(a.email))", "d.EmailStd IS NOT NULL AND a.email IS NOT NULL"),
            ("Mobile", "TRIM(d.Mobile) = TRIM(a.mobile_text)", "d.Mobile IS NOT NULL AND a.mobile_text IS NOT NULL"),
            ("Landline", "TRIM(d.Landline) = TRIM(a.landline)", "d.Landline IS NOT NULL AND a.landline IS NOT NULL")
        ]
        
        logger.info("Processing direct field matches")
        print("\n=== DIRECT FIELD MATCHES ===")
        
        direct_results: List[MatchResult] = []
        
        for match_name, condition, filter_cond in direct_matches:
            try:
                # Inner join count
                matches = conn.execute(f"""
                    SELECT COUNT(*) FROM read_parquet('{config.file1_path}') d
                    INNER JOIN read_parquet('{config.file2_path}') a ON {condition}
                    WHERE {filter_cond}
                """).fetchone()[0]
                
                # Anti-join DD -> AC (records in DD but not in AC)
                dd_filter = filter_cond.split(' AND a.')[0].replace('a.', 'd.')
                dd_only = conn.execute(f"""
                    SELECT COUNT(*) FROM read_parquet('{config.file1_path}') d
                    WHERE NOT EXISTS (
                        SELECT 1 FROM read_parquet('{config.file2_path}') a 
                        WHERE {condition} AND {filter_cond}
                    ) AND {dd_filter}
                """).fetchone()[0]
                
                # Anti-join AC -> DD (records in AC but not in DD)
                ac_filter = filter_cond.split(' AND d.')[0].replace('d.', 'a.')
                ac_only = conn.execute(f"""
                    SELECT COUNT(*) FROM read_parquet('{config.file2_path}') a
                    WHERE NOT EXISTS (
                        SELECT 1 FROM read_parquet('{config.file1_path}') d 
                        WHERE {condition} AND {filter_cond}
                    ) AND {ac_filter}
                """).fetchone()[0]
                
                result = MatchResult(
                    match_name=match_name,
                    matches=matches,
                    dd_only=dd_only,
                    ac_only=ac_only
                )
                direct_results.append(result)
                
                print(f"{match_name:12}: Matches={matches:,}, DD_only={dd_only:,}, AC_only={ac_only:,}")
                logger.info(f"{match_name} - Matches: {matches:,}, DD_only: {dd_only:,}, AC_only: {ac_only:,}")
                
            except Exception as e:
                logger.error(f"Error processing {match_name}: {str(e)}")
                continue
        
        # Concatenated field combinations with correct mappings
        concat_matches: List[Tuple[str, str]] = [
            ("Name+Suburb+Postcode", "UPPER(TRIM(d.FirstName || ' ' || d.Surname || ' ' || d.Suburb || ' ' || d.Postcode)) = UPPER(TRIM(a.given_name_1 || ' ' || a.surname || ' ' || a.suburb || ' ' || a.postcode_text))"),
            ("Name+Suburb+Postcode+Email", "UPPER(TRIM(d.FirstName || ' ' || d.Surname || ' ' || d.Suburb || ' ' || d.Postcode || ' ' || d.EmailStd)) = UPPER(TRIM(a.given_name_1 || ' ' || a.surname || ' ' || a.suburb || ' ' || a.postcode_text || ' ' || a.email))"),
            ("Name+Suburb+Postcode+Mobile", "UPPER(TRIM(d.FirstName || ' ' || d.Surname || ' ' || d.Suburb || ' ' || d.Postcode || ' ' || d.Mobile)) = UPPER(TRIM(a.given_name_1 || ' ' || a.surname || ' ' || a.suburb || ' ' || a.postcode_text || ' ' || a.mobile_text))"),
            ("Name+Suburb+Postcode+Landline", "UPPER(TRIM(d.FirstName || ' ' || d.Surname || ' ' || d.Suburb || ' ' || d.Postcode || ' ' || d.Landline)) = UPPER(TRIM(a.given_name_1 || ' ' || a.surname || ' ' || a.suburb || ' ' || a.postcode_text || ' ' || a.landline))")
        ]
        
        logger.info("Processing concatenated field matches")
        print("\n=== CONCATENATED FIELD MATCHES ===")
        
        concat_results: List[MatchResult] = []
        
        for match_name, condition in concat_matches:
            try:
                # Inner join count
                matches = conn.execute(f"""
                    SELECT COUNT(*) FROM read_parquet('{config.file1_path}') d
                    INNER JOIN read_parquet('{config.file2_path}') a ON {condition}
                """).fetchone()[0]
                
                # Anti-join DD -> AC
                dd_only = conn.execute(f"""
                    SELECT COUNT(*) FROM read_parquet('{config.file1_path}') d
                    WHERE NOT EXISTS (
                        SELECT 1 FROM read_parquet('{config.file2_path}') a WHERE {condition}
                    )
                """).fetchone()[0]
                
                # Anti-join AC -> DD
                ac_only = conn.execute(f"""
                    SELECT COUNT(*) FROM read_parquet('{config.file2_path}') a
                    WHERE NOT EXISTS (
                        SELECT 1 FROM read_parquet('{config.file1_path}') d WHERE {condition}
                    )
                """).fetchone()[0]
                
                result = MatchResult(
                    match_name=match_name,
                    matches=matches,
                    dd_only=dd_only,
                    ac_only=ac_only
                )
                concat_results.append(result)
                
                print(f"{match_name:25}: Matches={matches:,}, DD_only={dd_only:,}, AC_only={ac_only:,}")
                logger.info(f"{match_name} - Matches: {matches:,}, DD_only: {dd_only:,}, AC_only: {ac_only:,}")
                
            except Exception as e:
                logger.error(f"Error processing {match_name}: {str(e)}")
                continue
        
        # Final concatenation of all fields
        logger.info("Processing all fields concatenated")
        print("\n=== ALL FIELDS CONCATENATED ===")
        
        all_fields_condition = """
            UPPER(TRIM(COALESCE(d.Title,'') || ' ' || COALESCE(d.FirstName,'') || ' ' || COALESCE(d.Surname,'') || ' ' || COALESCE(d.Gender,'') || ' ' || 
                       COALESCE(d.Landline,'') || ' ' || COALESCE(d.Mobile,'') || ' ' || COALESCE(d.EmailStd,'') || ' ' || 
                       COALESCE(d.Suburb,'') || ' ' || COALESCE(d.State,'') || ' ' || COALESCE(d.Postcode,''))) = 
            UPPER(TRIM(COALESCE(a.title,'') || ' ' || COALESCE(a.given_name_1,'') || ' ' || COALESCE(a.surname,'') || ' ' || COALESCE(a.gender,'') || ' ' || 
                       COALESCE(a.landline,'') || ' ' || COALESCE(a.mobile_text,'') || ' ' || COALESCE(a.email,'') || ' ' || 
                       COALESCE(a.suburb,'') || ' ' || COALESCE(a.state,'') || ' ' || COALESCE(a.postcode_text,'')))
        """
        
        try:
            # All fields match
            all_matches = conn.execute(f"""
                SELECT COUNT(*) FROM read_parquet('{config.file1_path}') d
                INNER JOIN read_parquet('{config.file2_path}') a ON {all_fields_condition}
            """).fetchone()[0]
            
            # Anti-join DD -> AC
            dd_all_only = conn.execute(f"""
                SELECT COUNT(*) FROM read_parquet('{config.file1_path}') d
                WHERE NOT EXISTS (
                    SELECT 1 FROM read_parquet('{config.file2_path}') a WHERE {all_fields_condition}
                )
            """).fetchone()[0]
            
            # Anti-join AC -> DD
            ac_all_only = conn.execute(f"""
                SELECT COUNT(*) FROM read_parquet('{config.file2_path}') a
                WHERE NOT EXISTS (
                    SELECT 1 FROM read_parquet('{config.file1_path}') d WHERE {all_fields_condition}
                )
            """).fetchone()[0]
            
            all_fields_result = MatchResult(
                match_name="All Fields",
                matches=all_matches,
                dd_only=dd_all_only,
                ac_only=ac_all_only
            )
            
            print(f"All Fields      : Matches={all_matches:,}, DD_only={dd_all_only:,}, AC_only={ac_all_only:,}")
            logger.info(f"All Fields - Matches: {all_matches:,}, DD_only: {dd_all_only:,}, AC_only: {ac_all_only:,}")
            
        except Exception as e:
            logger.error(f"Error processing all fields concatenation: {str(e)}")
            all_fields_result = None
        
        # Direct match statistics
        logger.info("Generating direct match statistics")
        print("\n=== DIRECT MATCH STATISTICS ===")
        for result in direct_results:
            dd_match_rate = (result.matches / dd_count) * 100 if dd_count > 0 else 0
            ac_match_rate = (result.matches / ac_count) * 100 if ac_count > 0 else 0
            print(f"{result.match_name:12}: DD_rate={dd_match_rate:.2f}%, AC_rate={ac_match_rate:.2f}%")
            logger.info(f"{result.match_name} match rates - DD: {dd_match_rate:.2f}%, AC: {ac_match_rate:.2f}%")
        
        # Concatenated match statistics
        logger.info("Generating concatenated match statistics")
        print("\n=== CONCATENATED MATCH STATISTICS ===")
        for result in concat_results:
            dd_match_rate = (result.matches / dd_count) * 100 if dd_count > 0 else 0
            ac_match_rate = (result.matches / ac_count) * 100 if ac_count > 0 else 0
            print(f"{result.match_name:25}: DD_rate={dd_match_rate:.2f}%, AC_rate={ac_match_rate:.2f}%")
            logger.info(f"{result.match_name} match rates - DD: {dd_match_rate:.2f}%, AC: {ac_match_rate:.2f}%")
        
        # Summary statistics
        logger.info("Generating summary statistics")
        print("\n=== SUMMARY STATISTICS ===")
        print(f"Total DD records: {dd_count:,}")
        print(f"Total AC records: {ac_count:,}")
        
        if all_fields_result:
            print(f"Perfect matches (all fields): {all_fields_result.matches:,} ({(all_fields_result.matches/min(dd_count, ac_count))*100:.2f}%)")
            print(f"DD unique records: {all_fields_result.dd_only:,} ({(all_fields_result.dd_only/dd_count)*100:.2f}%)")
            print(f"AC unique records: {all_fields_result.ac_only:,} ({(all_fields_result.ac_only/ac_count)*100:.2f}%)")
        
        conn.close()
        # Final statistics summary
        total_direct_matches = sum(r.matches for r in direct_results)
        total_concat_matches = sum(r.matches for r in concat_results)
        logger.info(f"Total direct field matches: {total_direct_matches:,}")
        logger.info(f"Total concatenated matches: {total_concat_matches:,}")
        logger.info("Comprehensive matching analysis v1.0 completed successfully")
        
    except Exception as e:
        logger.error(f"Critical error in comprehensive analysis: {str(e)}")
        raise

if __name__ == "__main__":
    comprehensive_match_analysis_v1()