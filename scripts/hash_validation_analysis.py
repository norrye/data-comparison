#!/usr/bin/env python3
"""
Hash Validation Analysis - Email and Mobile Hash Integrity Check

Version: 1.0
Author: Expert Data Scientist
Description: Analyzes hash integrity between datasets to identify incorrect hashes
"""

import hashlib
from pathlib import Path
from typing import Dict, List, Tuple

import duckdb
from loguru import logger
from pydantic import BaseModel


class HashValidationResult(BaseModel):
    """Hash validation result model."""
    field_type: str
    total_records: int
    valid_hashes: int
    invalid_hashes: int
    validation_rate: float
    sample_mismatches: List[Dict[str, str]]


def analyze_hash_integrity() -> None:
    """Analyze hash integrity between datasets."""
    logger.info("Starting hash integrity analysis")
    
    base_path = Path("/data/projects/data_comparison")
    db_path = base_path / "data/processed/match_analysis.duckdb"
    
    conn = duckdb.connect(str(db_path))
    conn.execute("SET threads = 4")
    
    print("=" * 80)
    print("HASH INTEGRITY ANALYSIS")
    print("=" * 80)
    
    # Analyze email hashes
    print("\n1. EMAIL HASH VALIDATION")
    print("-" * 40)
    
    # Get sample of email records from both datasets for validation
    email_samples = conn.execute("""
        SELECT 
            d.email_std as dd_email,
            d.email_hash as dd_hash,
            a.email_std as ad_email,
            a.email_hash as ad_hash
        FROM datadirect d
        INNER JOIN ad_consumers a ON d.email_std = a.email_std
        WHERE d.email_std IS NOT NULL 
        AND a.email_std IS NOT NULL
        AND d.email_hash IS NOT NULL 
        AND a.email_hash IS NOT NULL
    """).fetchall()
    
    email_validation = validate_email_hashes(email_samples)
    print_validation_results("Email", email_validation)
    
    # Analyze mobile hashes (if they exist)
    print("\n2. MOBILE HASH VALIDATION")
    print("-" * 40)
    
    # Check if mobile hashes exist in the data
    mobile_hash_check = conn.execute("""
        SELECT COUNT(*) FROM datadirect 
        WHERE mobile IS NOT NULL AND LENGTH(mobile) = 64
    """).fetchone()[0]
    
    if mobile_hash_check > 0:
        mobile_samples = conn.execute("""
            SELECT 
                d.mobile as dd_mobile,
                a.mobile as ad_mobile
            FROM datadirect d
            INNER JOIN ad_consumers a ON d.mobile = a.mobile
            WHERE d.mobile IS NOT NULL 
            AND a.mobile IS NOT NULL
            AND LENGTH(d.mobile) > 10
        """).fetchall()
        
        mobile_validation = validate_mobile_data(mobile_samples)
        print_validation_results("Mobile", mobile_validation)
    else:
        print("No mobile hashes detected - mobile data appears to be in plain text format")
    
    # Analyze hash consistency between datasets
    print("\n3. CROSS-DATASET HASH CONSISTENCY")
    print("-" * 40)
    
    hash_consistency = analyze_hash_consistency(conn)
    print_consistency_results(hash_consistency)
    
    # Identify potential hash algorithm differences
    print("\n4. HASH ALGORITHM ANALYSIS")
    print("-" * 40)
    
    algorithm_analysis = analyze_hash_algorithms(conn)
    print_algorithm_results(algorithm_analysis)
    
    conn.close()
    logger.info("Hash integrity analysis completed")


def validate_email_hashes(samples: List[Tuple]) -> HashValidationResult:
    """Validate email hash integrity."""
    valid_count = 0
    invalid_count = 0
    mismatches = []
    
    for dd_email, dd_hash, ad_email, ad_hash in samples:
        # Generate expected SHA256 hash
        if dd_email and dd_email.strip():
            expected_hash = hashlib.sha256(dd_email.strip().lower().encode()).hexdigest().upper()
            
            dd_valid = dd_hash and dd_hash.upper() == expected_hash
            ad_valid = ad_hash and ad_hash.upper() == expected_hash
            
            if dd_valid and ad_valid:
                valid_count += 1
            else:
                invalid_count += 1
                if len(mismatches) < 5:  # Store first 5 mismatches
                    mismatches.append({
                        "email": dd_email,
                        "expected_hash": expected_hash,
                        "dd_hash": dd_hash or "NULL",
                        "ad_hash": ad_hash or "NULL",
                        "dd_valid": str(dd_valid),
                        "ad_valid": str(ad_valid)
                    })
    
    total = valid_count + invalid_count
    validation_rate = (valid_count / total * 100) if total > 0 else 0
    
    return HashValidationResult(
        field_type="Email",
        total_records=total,
        valid_hashes=valid_count,
        invalid_hashes=invalid_count,
        validation_rate=validation_rate,
        sample_mismatches=mismatches
    )


def validate_mobile_data(samples: List[Tuple]) -> HashValidationResult:
    """Validate mobile data format."""
    hash_count = 0
    plain_count = 0
    mismatches = []
    
    for dd_mobile, ad_mobile in samples:
        # Check if mobile appears to be hashed (64 char hex) or plain text
        dd_is_hash = dd_mobile and len(dd_mobile) == 64 and all(c in '0123456789ABCDEFabcdef' for c in dd_mobile)
        ad_is_hash = ad_mobile and len(ad_mobile) == 64 and all(c in '0123456789ABCDEFabcdef' for c in ad_mobile)
        
        if dd_is_hash or ad_is_hash:
            hash_count += 1
        else:
            plain_count += 1
            
        if len(mismatches) < 5:
            mismatches.append({
                "dd_mobile": dd_mobile or "NULL",
                "ad_mobile": ad_mobile or "NULL",
                "dd_format": "HASH" if dd_is_hash else "PLAIN",
                "ad_format": "HASH" if ad_is_hash else "PLAIN"
            })
    
    total = hash_count + plain_count
    
    return HashValidationResult(
        field_type="Mobile",
        total_records=total,
        valid_hashes=plain_count,  # Plain text is "valid" for mobile
        invalid_hashes=hash_count,  # Hashes are unexpected for mobile
        validation_rate=(plain_count / total * 100) if total > 0 else 0,
        sample_mismatches=mismatches
    )


def analyze_hash_consistency(conn) -> Dict:
    """Analyze hash consistency between datasets."""
    # Check email hash consistency
    email_consistency = conn.execute("""
        SELECT 
            COUNT(*) as total_matches,
            COUNT(CASE WHEN d.email_hash = a.email_hash THEN 1 END) as hash_matches,
            COUNT(CASE WHEN d.email_hash != a.email_hash THEN 1 END) as hash_mismatches
        FROM datadirect d
        INNER JOIN ad_consumers a ON d.email_std = a.email_std
        WHERE d.email_std IS NOT NULL 
        AND a.email_std IS NOT NULL
        AND d.email_hash IS NOT NULL 
        AND a.email_hash IS NOT NULL
    """).fetchone()
    
    total, matches, mismatches = email_consistency
    consistency_rate = (matches / total * 100) if total > 0 else 0
    
    return {
        "total_email_matches": total,
        "hash_matches": matches,
        "hash_mismatches": mismatches,
        "consistency_rate": consistency_rate
    }


def analyze_hash_algorithms(conn) -> Dict:
    """Analyze hash algorithm patterns."""
    # Check hash length distribution
    hash_lengths = conn.execute("""
        SELECT 
            'DataDirect' as dataset,
            LENGTH(email_hash) as hash_length,
            COUNT(*) as count
        FROM datadirect 
        WHERE email_hash IS NOT NULL
        GROUP BY LENGTH(email_hash)
        UNION ALL
        SELECT 
            'AliveData' as dataset,
            LENGTH(email_hash) as hash_length,
            COUNT(*) as count
        FROM ad_consumers 
        WHERE email_hash IS NOT NULL
        GROUP BY LENGTH(email_hash)
        ORDER BY dataset, hash_length
    """).fetchall()
    
    # Check hash character patterns
    hash_patterns = conn.execute("""
        SELECT 
            'DataDirect' as dataset,
            CASE 
                WHEN LENGTH(email_hash) = 64 AND email_hash ~ '^[0-9A-F]+$' THEN 'SHA256_UPPER'
                WHEN LENGTH(email_hash) = 64 AND email_hash ~ '^[0-9a-f]+$' THEN 'SHA256_LOWER'
                WHEN LENGTH(email_hash) = 32 THEN 'MD5'
                ELSE 'OTHER'
            END as pattern,
            COUNT(*) as count
        FROM datadirect 
        WHERE email_hash IS NOT NULL
        GROUP BY pattern
        UNION ALL
        SELECT 
            'AliveData' as dataset,
            CASE 
                WHEN LENGTH(email_hash) = 64 AND email_hash ~ '^[0-9A-F]+$' THEN 'SHA256_UPPER'
                WHEN LENGTH(email_hash) = 64 AND email_hash ~ '^[0-9a-f]+$' THEN 'SHA256_LOWER'
                WHEN LENGTH(email_hash) = 32 THEN 'MD5'
                ELSE 'OTHER'
            END as pattern,
            COUNT(*) as count
        FROM ad_consumers 
        WHERE email_hash IS NOT NULL
        GROUP BY pattern
        ORDER BY dataset, pattern
    """).fetchall()
    
    return {
        "hash_lengths": hash_lengths,
        "hash_patterns": hash_patterns
    }


def print_validation_results(field_type: str, result: HashValidationResult) -> None:
    """Print validation results."""
    print(f"Total {field_type} Records Analyzed: {result.total_records:,}")
    print(f"Valid Hashes: {result.valid_hashes:,} ({result.validation_rate:.2f}%)")
    print(f"Invalid Hashes: {result.invalid_hashes:,}")
    
    if result.sample_mismatches:
        print(f"\nSample Mismatches:")
        for i, mismatch in enumerate(result.sample_mismatches, 1):
            print(f"  {i}. {mismatch}")


def print_consistency_results(consistency: Dict) -> None:
    """Print hash consistency results."""
    print(f"Total Email Matches: {consistency['total_email_matches']:,}")
    print(f"Hash Matches: {consistency['hash_matches']:,}")
    print(f"Hash Mismatches: {consistency['hash_mismatches']:,}")
    print(f"Consistency Rate: {consistency['consistency_rate']:.2f}%")


def print_algorithm_results(analysis: Dict) -> None:
    """Print hash algorithm analysis results."""
    print("Hash Length Distribution:")
    for dataset, length, count in analysis['hash_lengths']:
        print(f"  {dataset}: Length {length} = {count:,} records")
    
    print("\nHash Pattern Analysis:")
    for dataset, pattern, count in analysis['hash_patterns']:
        print(f"  {dataset}: {pattern} = {count:,} records")


if __name__ == "__main__":
    analyze_hash_integrity()