#!/usr/bin/env python3

"""
Comprehensive ThirdParty vs Ad_consumers Matching Analysis (v11)

Author: Tim Loane
Created: 2025-08-01
Last Modified: 2025-08-13

Detailed Description:
------------------------------------------------------------
This script performs a comprehensive matching analysis between two large datasets: ThirdParty and Ad_consumers. It leverages DuckDB for high-performance analytics, including:

- Creation of an optimized DuckDB database with sorted tables and extensive indexing (individual and compound fields).
- Efficient loading and transformation of source parquet files, with normalization and compound key generation for robust matching.
- Automated index creation to accelerate join and anti-join queries, supporting both direct and multi-field matches.
- Direct field matching and anti-join analysis to identify records present in one dataset but not the other, with detailed progress logging and timing.
- Extraction of unique ThirdParty records based on multiple key fields (full_name, suburb, postcode, email) and export to parquet for downstream analysis.
- Modular design for easy extension to additional matching strategies or output formats.

The script is suitable for large-scale data integration, deduplication, and quality assessment tasks, and is designed for reproducibility and auditability in enterprise environments.
------------------------------------------------------------
"""

import time
from datetime import datetime
from pathlib import Path
from typing import List, Tuple

import duckdb
from loguru import logger
from pydantic import BaseModel, Field


def extract_unique_ThirdParty_records(output_path: str = None) -> None:
    """Extract records unique to ThirdParty (not present in Ad_consumers) by joining on full_name, suburb, postcode, and email. Output to parquet file."""
    base_path = Path("/data/projects/data_comparison")
    db_path = base_path / "data/processed/match_analysis.duckdb"
    if not db_path.exists():
        create_optimized_database()
    conn = duckdb.connect(str(db_path))
    logger.info(
        "Extracting unique ThirdParty records (not present in Ad_consumers) using full_name, suburb, postcode, and email"
    )
    unique_tpd_df = conn.execute(
        """
        SELECT d.* FROM ThirdParty d
        ANTI JOIN ad_consumers a ON (
            d.full_name = a.full_name
            AND d.suburb = a.suburb
            AND d.postcode = a.postcode
            AND d.email_std = a.email_std
        )
        WHERE d.full_name IS NOT NULL AND d.suburb IS NOT NULL AND d.postcode IS NOT NULL AND d.email_std IS NOT NULL
    """
    ).df()
    logger.info(f"Extracted {len(unique_tpd_df):,} unique ThirdParty records")
    output_path = base_path / "data/processed/tpd_unique_records.parquet"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    unique_tpd_df.to_parquet(str(output_path))
    logger.info(f"Saved unique ThirdParty records to {output_path}")
    conn.close()


class AntiJoinResult(BaseModel):
    """Data validation model for anti-join results."""

    match_name: str = Field(..., description="Name of the matching pattern")
    matches: int = Field(..., ge=0, description="Number of matches found")
    tpd_only: int = Field(..., ge=0, description="Records only in ThirdParty")
    ac_only: int = Field(..., ge=0, description="Records only in Ad_consumers")
    tpd_match_rate: float = Field(
        ..., ge=0, le=100, description="DD match rate percentage"
    )
    ac_match_rate: float = Field(
        ..., ge=0, le=100, description="AC match rate percentage"
    )
    processing_time: float = Field(..., ge=0,
                                   description="Processing time in seconds")


def create_optimized_database(
    source_file: str,
    alivedata_file: str,
    db_path: str,
    source_table: str = "source_data",
    alivedata_table: str = "alivedata",
    source_fields: dict = None,
    alivedata_fields: dict = None,
) -> None:
    """Create optimized DuckDB database with sorted data and indexes for any source dataset and alivedata."""
    logger.info(
        "Creating optimized match analysis database with sorted data and indexes"
    )
    db_path = Path(db_path)
    source_file = Path(source_file)
    alivedata_file = Path(alivedata_file)
    # Remove existing database
    if db_path.exists():
        db_path.unlink()
        logger.info("Removed existing database")
    conn = duckdb.connect(str(db_path))
    conn.execute("SET threads = 4")
    logger.info("Created database with 4 threads")
    # Default field mappings if not provided
    if source_fields is None:
        source_fields = {
            "id": "ID",
            "title": "Title",
            "first_name": "FirstName",
            "surname": "Surname",
            "gender": "Gender",
            "email_std": "EmailStd",
            "email_hash": "EmailHash",
            "mobile": "Mobile",
            "landline": "Landline",
            "suburb": "Suburb",
            "state": "State",
            "postcode": "Postcode",
        }
    if alivedata_fields is None:
        alivedata_fields = {
            "id": "adId",
            "title": "title",
            "first_name": "given_name_1",
            "surname": "surname",
            "gender": "gender",
            "email_std": "email",
            "email_hash": "email_sha256",
            "mobile": "mobile_text",
            "landline": "landline_text",
            "suburb": "suburb",
            "state": "state",
            "postcode": "postcode_text",
        }
    # Create source table
    logger.info(
        f"Creating {source_table} table with sorting by surname, suburb, state, postcode"
    )
    conn.execute(
        f"""
        CREATE TABLE {source_table} AS
        SELECT 
            {source_fields['id']} as id,
            UPPER(TRIM({source_fields['title']})) as title,
            UPPER(TRIM({source_fields['first_name']})) as first_name,
            UPPER(TRIM({source_fields['surname']})) as surname,
            UPPER(TRIM({source_fields['gender']})) as gender,
            UPPER(TRIM({source_fields['email_std']})) as email_std,
            UPPER(TRIM({source_fields['email_hash']})) as email_hash,
            TRIM({source_fields['mobile']}) as mobile,
            TRIM({source_fields['landline']}) as landline,
            UPPER(TRIM({source_fields['suburb']})) as suburb,
            UPPER(TRIM({source_fields['state']})) as state,
            CAST({source_fields['postcode']} AS VARCHAR) as postcode,
            UPPER(TRIM({source_fields['first_name']} || ' ' || {source_fields['surname']})) as full_name,
            UPPER(TRIM({source_fields['first_name']} || ' ' || {source_fields['surname']} || ' ' || {source_fields['suburb']} || ' ' || CAST({source_fields['postcode']} AS VARCHAR))) as name_suburb_postcode,
            UPPER(TRIM({source_fields['first_name']} || ' ' || {source_fields['surname']} || ' ' || {source_fields['suburb']} || ' ' || CAST({source_fields['postcode']} AS VARCHAR) || ' ' || {source_fields['mobile']})) as name_suburb_postcode_mobile
        FROM read_parquet(?)
        WHERE {source_fields['title']} IS NOT NULL OR {source_fields['first_name']} IS NOT NULL OR {source_fields['surname']} IS NOT NULL
        ORDER BY {source_fields['surname']}, {source_fields['suburb']}, {source_fields['state']}, {source_fields['postcode']}
    """,
        [str(source_file)],
    )
    source_count = conn.execute(
        f"SELECT COUNT(*) FROM {source_table}").fetchone()[0]
    logger.info(
        f"Loaded {source_count:,} records into {source_table} (sorted)")
    # Create alivedata table
    logger.info(
        f"Creating {alivedata_table} table with sorting by surname, suburb, state, postcode"
    )
    conn.execute(
        f"""
        CREATE TABLE {alivedata_table} AS
        SELECT 
            {alivedata_fields['id']} as id,
            UPPER(TRIM({alivedata_fields['title']})) as title,
            UPPER(TRIM({alivedata_fields['first_name']})) as first_name,
            UPPER(TRIM({alivedata_fields['surname']})) as surname,
            UPPER(TRIM({alivedata_fields['gender']})) as gender,
            UPPER(TRIM({alivedata_fields['email_std']})) as email_std,
            UPPER(TRIM({alivedata_fields['email_hash']})) as email_hash,
            TRIM({alivedata_fields['mobile']}) as mobile,
            TRIM({alivedata_fields['landline']}) as landline,
            UPPER(TRIM({alivedata_fields['suburb']})) as suburb,
            UPPER(TRIM({alivedata_fields['state']})) as state,
            TRIM({alivedata_fields['postcode']}) as postcode,
            UPPER(TRIM({alivedata_fields['first_name']} || ' ' || {alivedata_fields['surname']})) as full_name,
            UPPER(TRIM({alivedata_fields['first_name']} || ' ' || {alivedata_fields['surname']} || ' ' || {alivedata_fields['suburb']} || ' ' || {alivedata_fields['postcode']})) as name_suburb_postcode,
            UPPER(TRIM({alivedata_fields['first_name']} || ' ' || {alivedata_fields['surname']} || ' ' || {alivedata_fields['suburb']} || ' ' || {alivedata_fields['postcode']} || ' ' || {alivedata_fields['mobile']})) as name_suburb_postcode_mobile
        FROM read_parquet(?)
        WHERE {alivedata_fields['title']} IS NOT NULL OR {alivedata_fields['first_name']} IS NOT NULL OR {alivedata_fields['surname']} IS NOT NULL
        ORDER BY {alivedata_fields['surname']}, {alivedata_fields['suburb']}, {alivedata_fields['state']}, {alivedata_fields['postcode']}
    """,
        [str(alivedata_file)],
    )
    alivedata_count = conn.execute(
        f"SELECT COUNT(*) FROM {alivedata_table}"
    ).fetchone()[0]
    logger.info(
        f"Loaded {alivedata_count:,} records into {alivedata_table} (sorted)")
    # Create indexes on individual fields
    logger.info("Creating individual field indexes")
    individual_fields = [
        "title",
        "first_name",
        "surname",
        "gender",
        "email_std",
        "email_hash",
        "mobile",
        "landline",
        "suburb",
        "state",
        "postcode",
    ]
    for field in individual_fields:
        conn.execute(
            f"CREATE INDEX idx_src_{field} ON {source_table}({field})")
        conn.execute(
            f"CREATE INDEX idx_alive_{field} ON {alivedata_table}({field})")
        logger.info(f"Created indexes for {field}")
    # Create multi-field compound indexes
    logger.info("Creating multi-field compound indexes")
    compound_fields = [
        "full_name",
        "name_suburb_postcode",
        "name_suburb_postcode_mobile",
    ]
    for field in compound_fields:
        conn.execute(
            f"CREATE INDEX idx_src_{field} ON {source_table}({field})")
        conn.execute(
            f"CREATE INDEX idx_alive_{field} ON {alivedata_table}({field})")
        logger.info(f"Created compound index for {field}")
    # Analyze tables for query optimization
    conn.execute(f"ANALYZE {source_table}")
    conn.execute(f"ANALYZE {alivedata_table}")
    conn.close()
    logger.info("Database creation completed with sorted data and all indexes")


def comprehensive_anti_join_analysis(
    db_path: str,
    source_table: str = "source_data",
    alivedata_table: str = "alivedata",
    direct_matches: List[Tuple[str, str, str]] = None,
) -> None:
    """
    Perform comprehensive anti-join analysis using optimized database with sorted data and indexes.
    Parameters:
        db_path: Path to DuckDB database
        source_table: Name of source table
        alivedata_table: Name of alivedata table
        direct_matches: List of (match_name, join_condition, filter_condition)
    """
    start_time = time.time()
    logger.info("=" * 80)
    logger.info("STARTING COMPREHENSIVE ANTI-JOIN ANALYSIS")
    logger.info(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 80)
    db_path = Path(db_path)
    conn = duckdb.connect(str(db_path))
    conn.execute("SET threads = 4")
    # Get record counts
    src_count = conn.execute(
        f"SELECT COUNT(*) FROM {source_table}").fetchone()[0]
    alive_count = conn.execute(
        f"SELECT COUNT(*) FROM {alivedata_table}").fetchone()[0]
    logger.info(
        f"Source: {src_count:,} records, Alivedata: {alive_count:,} records")
    print("\n" + "=" * 80)
    print("COMPREHENSIVE ANTI-JOIN ANALYSIS")
    print("=" * 80)
    print(f"Source: {src_count:,} records")
    print(f"Alivedata: {alive_count:,} records")
    print(f"Total records to process: {src_count + alive_count:,}")
    print("=" * 80)
    # Default direct matches if not provided
    if direct_matches is None:
        direct_matches = [
            (
                "FullName",
                "d.full_name = a.full_name",
                "d.full_name IS NOT NULL AND a.full_name IS NOT NULL",
            ),
            (
                "EmailStd",
                "d.email_std = a.email_std",
                "d.email_std IS NOT NULL AND a.email_std IS NOT NULL",
            ),
            (
                "EmailHash",
                "d.email_hash = a.email_hash",
                "d.email_hash IS NOT NULL AND a.email_hash IS NOT NULL",
            ),
            (
                "Mobile",
                "d.mobile = a.mobile",
                "d.mobile IS NOT NULL AND a.mobile IS NOT NULL",
            ),
            (
                "Landline",
                "d.landline = a.landline",
                "d.landline IS NOT NULL AND a.landline IS NOT NULL",
            ),
        ]
    logger.info(f"✓ Configured {len(direct_matches)} direct field mappings")
    for i, (name, _, _) in enumerate(direct_matches, 1):
        logger.info(f"  {i:2d}. {name}")
    logger.info(
        "PHASE 5: Direct field matching analysis using DuckDB native ANTI JOIN")
    print("\n=== DIRECT FIELD MATCHES WITH ANTI JOINS ===")
    direct_results: List[AntiJoinResult] = []
    total_direct_matches = len(direct_matches)
    for idx, (match_name, condition, filter_cond) in enumerate(direct_matches, 1):
        field_start_time = time.time()
        logger.info(
            f"Processing field {idx}/{total_direct_matches}: {match_name}")
        try:
            # Inner join count (matches)
            logger.info(f"  → Running INNER JOIN for {match_name}")
            inner_start = time.time()
            matches = conn.execute(
                f"""
                SELECT COUNT(*) FROM {source_table} d
                INNER JOIN {alivedata_table} a ON {condition}
                WHERE {filter_cond}
            """
            ).fetchone()[0]
            inner_time = time.time() - inner_start
            logger.info(
                f"  ✓ INNER JOIN completed in {inner_time:.2f}s: {matches:,} matches"
            )
            # Anti-join 1: Source ANTI JOIN Alivedata
            logger.info(
                f"  → Running Source ANTI JOIN Alivedata for {match_name}")
            anti1_start = time.time()
            src_filter = (
                filter_cond.split(" AND a.")[0]
                if " AND a." in filter_cond
                else filter_cond
            )
            src_only = conn.execute(
                f"""
                SELECT COUNT(*) FROM (
                    SELECT * FROM {source_table} WHERE {src_filter}
                ) d
                ANTI JOIN {alivedata_table} a ON {condition}
            """
            ).fetchone()[0]
            anti1_time = time.time() - anti1_start
            logger.info(
                f"  ✓ Source ANTI JOIN completed in {anti1_time:.2f}s: {src_only:,} Source-only records"
            )
            # Anti-join 2: Alivedata ANTI JOIN Source
            logger.info(
                f"  → Running Alivedata ANTI JOIN Source for {match_name}")
            anti2_start = time.time()
            alive_filter = (
                filter_cond.split(" AND d.")[0].replace("d.", "a.")
                if " AND d." in filter_cond
                else filter_cond.replace("d.", "a.")
            )
            alive_only = conn.execute(
                f"""
                SELECT COUNT(*) FROM (
                    SELECT * FROM {alivedata_table} WHERE {alive_filter}
                ) a
                ANTI JOIN {source_table} d ON {condition}
            """
            ).fetchone()[0]
            anti2_time = time.time() - anti2_start
            logger.info(
                f"  ✓ Alivedata ANTI JOIN completed in {anti2_time:.2f}s: {alive_only:,} Alivedata-only records"
            )
            # Calculate match rates
            src_match_rate = (matches / src_count) * \
                100 if src_count > 0 else 0
            alive_match_rate = (matches / alive_count) * \
                100 if alive_count > 0 else 0
            field_processing_time = time.time() - field_start_time
            result = AntiJoinResult(
                match_name=match_name,
                matches=matches,
                dd_only=src_only,
                ac_only=alive_only,
                dd_match_rate=src_match_rate,
                ac_match_rate=alive_match_rate,
                processing_time=field_processing_time,
            )
            direct_results.append(result)
            print(
                f"{match_name:12}: Matches={matches:,}, Source_only={src_only:,}, Alivedata_only={alive_only:,}"
            )
            print(
                f"{'':14} Source_rate={src_match_rate:.2f}%, Alivedata_rate={alive_match_rate:.2f}%, Time={field_processing_time:.2f}s"
            )
            logger.info(
                f"{match_name} - Matches: {matches:,}, Source_only: {src_only:,}, Alivedata_only: {alive_only:,}"
            )
        except Exception as e:
            logger.error(f"Error processing {match_name}: {str(e)}")
            continue
    conn.close()
    total_time = time.time() - start_time
    logger.info(f"Analysis completed in {total_time:.2f} seconds")


def extract_unique_source_records(
    db_path: str,
    source_table: str = "source_data",
    alivedata_table: str = "alivedata",
    join_fields: List[str] = None,
    output_path: str = None,
) -> None:
    """
    Extract records unique to the source dataset (not present in alivedata) by joining on specified fields. Output to parquet file.
    Parameters:
        db_path: Path to DuckDB database
        source_table: Name of source table
        alivedata_table: Name of alivedata table
        join_fields: List of fields to join on
        output_path: Path to output parquet file
    """
    db_path = Path(db_path)
    if not db_path.exists():
        logger.error(f"Database not found: {db_path}")
        return
    conn = duckdb.connect(str(db_path))
    if join_fields is None:
        join_fields = ["full_name", "suburb", "postcode", "email_std"]
    logger.info(
        f"Extracting unique source records using join fields: {join_fields}")
    join_condition = " AND ".join([f"d.{f} = a.{f}" for f in join_fields])
    where_condition = " AND ".join([f"d.{f} IS NOT NULL" for f in join_fields])
    unique_src_df = conn.execute(
        f"""
        SELECT d.* FROM {source_table} d
        ANTI JOIN {alivedata_table} a ON ({join_condition})
        WHERE {where_condition}
    """
    ).df()
    logger.info(f"Extracted {len(unique_src_df):,} unique source records")
    if output_path is None:
        output_path = db_path.parent / "dd_unique_records.parquet"
    else:
        output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    unique_src_df.to_parquet(str(output_path))
    logger.info(f"Saved unique source records to {output_path}")
    conn.close()


if __name__ == "__main__":
    # Example usage for any dataset vs alivedata
    # 1. Create optimized database
    create_optimized_database(
        source_file="/data/projects/data_comparison/data/external/DATADIRECT_DL_202505_20250801_subset.parquet",
        alivedata_file="/data/projects/data_comparison/data/interim/ad_consumers_2020805_inter_ll_text.parquet",
        db_path="/data/projects/data_comparison/data/processed/match_analysis.duckdb",
    )
    # 2. Run main analysis
    comprehensive_anti_join_analysis(
        db_path="/data/projects/data_comparison/data/processed/match_analysis.duckdb",
        source_table="source_data",
        alivedata_table="alivedata",
    )
    # 3. Extract unique records (custom join fields and output path optional)
    extract_unique_source_records(
        db_path="/data/projects/data_comparison/data/processed/match_analysis.duckdb",
        source_table="source_data",
        alivedata_table="alivedata",
        join_fields=["full_name", "suburb", "postcode", "email_std"],
        output_path="/data/projects/data_comparison/data/processed/dd_unique_records.parquet",
    )
