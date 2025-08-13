#!/usr/bin/env python3
"""
Compound Match Analysis - Full Name and Geographic Combinations

Version: 1.0
Author: Expert Data Scientist
Description: Analyzes full name and compound geographic matching patterns
"""

from pathlib import Path
import duckdb
from loguru import logger


def analyze_compound_matches() -> dict:
    """Analyze compound matching patterns."""
    logger.info("Starting compound match analysis")
    
    base_path = Path("/data/projects/data_comparison")
    db_path = base_path / "data/processed/match_analysis.duckdb"
    
    conn = duckdb.connect(str(db_path))
    conn.execute("SET threads = 4")
    
    # Get dataset counts
    dd_count = conn.execute("SELECT COUNT(*) FROM datadirect").fetchone()[0]
    ad_count = conn.execute("SELECT COUNT(*) FROM ad_consumers").fetchone()[0]
    
    results = {}
    
    # Compound matching patterns
    compound_fields = [
        ("FullName", "d.full_name = a.full_name", "Full Name"),
        ("NameSuburb", "d.first_name = a.first_name AND d.surname = a.surname AND d.suburb = a.suburb", "Full Name + Suburb"),
        ("NameSuburbPostcode", "d.first_name = a.first_name AND d.surname = a.surname AND d.suburb = a.suburb AND d.postcode = a.postcode", "Full Name + Suburb + Postcode"),
        ("NameSuburbPostcodeEmail", "d.first_name = a.first_name AND d.surname = a.surname AND d.suburb = a.suburb AND d.postcode = a.postcode AND d.email_hash = a.email_hash", "Full Name + Suburb + Postcode + EmailHash")
    ]
    
    for field_name, condition, description in compound_fields:
        logger.info(f"Analyzing {description}")
        
        # Get field coverage
        if field_name == "FullName":
            dd_total = conn.execute("SELECT COUNT(*) FROM datadirect WHERE full_name IS NOT NULL").fetchone()[0]
            ad_total = conn.execute("SELECT COUNT(*) FROM ad_consumers WHERE full_name IS NOT NULL").fetchone()[0]
            filter_cond = "d.full_name IS NOT NULL AND a.full_name IS NOT NULL"
        elif field_name == "NameSuburbPostcodeEmail":
            dd_total = conn.execute("""
                SELECT COUNT(*) FROM datadirect 
                WHERE first_name IS NOT NULL AND surname IS NOT NULL 
                AND suburb IS NOT NULL AND postcode IS NOT NULL AND email_hash IS NOT NULL
            """).fetchone()[0]
            ad_total = conn.execute("""
                SELECT COUNT(*) FROM ad_consumers 
                WHERE first_name IS NOT NULL AND surname IS NOT NULL 
                AND suburb IS NOT NULL AND postcode IS NOT NULL AND email_hash IS NOT NULL
            """).fetchone()[0]
            filter_cond = """d.first_name IS NOT NULL AND d.surname IS NOT NULL 
                           AND d.suburb IS NOT NULL AND d.postcode IS NOT NULL AND d.email_hash IS NOT NULL
                           AND a.first_name IS NOT NULL AND a.surname IS NOT NULL 
                           AND a.suburb IS NOT NULL AND a.postcode IS NOT NULL AND a.email_hash IS NOT NULL"""
        else:
            dd_total = conn.execute("""
                SELECT COUNT(*) FROM datadirect 
                WHERE first_name IS NOT NULL AND surname IS NOT NULL 
                AND suburb IS NOT NULL AND postcode IS NOT NULL
            """).fetchone()[0]
            ad_total = conn.execute("""
                SELECT COUNT(*) FROM ad_consumers 
                WHERE first_name IS NOT NULL AND surname IS NOT NULL 
                AND suburb IS NOT NULL AND postcode IS NOT NULL
            """).fetchone()[0]
            filter_cond = """d.first_name IS NOT NULL AND d.surname IS NOT NULL 
                           AND d.suburb IS NOT NULL AND d.postcode IS NOT NULL
                           AND a.first_name IS NOT NULL AND a.surname IS NOT NULL 
                           AND a.suburb IS NOT NULL AND a.postcode IS NOT NULL"""
        
        # Get matches
        matches = conn.execute(f"""
            SELECT COUNT(*) FROM datadirect d
            INNER JOIN ad_consumers a ON {condition}
            WHERE {filter_cond}
        """).fetchone()[0]
        
        # Get DD-only records
        dd_only = conn.execute(f"""
            SELECT COUNT(*) FROM datadirect d
            ANTI JOIN ad_consumers a ON {condition}
            WHERE {filter_cond.split(' AND a.')[0] if ' AND a.' in filter_cond else filter_cond}
        """).fetchone()[0]
        
        # Get AD-only records  
        ad_filter = filter_cond.replace('d.', 'a.').split(' AND a.')[0] if ' AND a.' in filter_cond else filter_cond.replace('d.', 'a.')
        ad_only = conn.execute(f"""
            SELECT COUNT(*) FROM ad_consumers a
            ANTI JOIN datadirect d ON {condition}
            WHERE {ad_filter}
        """).fetchone()[0]
        
        # Calculate rates
        dd_match_rate = (matches / dd_total * 100) if dd_total > 0 else 0
        ad_match_rate = (matches / ad_total * 100) if ad_total > 0 else 0
        jaccard = matches / (dd_total + ad_total - matches) if (dd_total + ad_total - matches) > 0 else 0
        
        results[field_name] = {
            'description': description,
            'matches': matches,
            'dd_total': dd_total,
            'ad_total': ad_total,
            'dd_only': dd_only,
            'ad_only': ad_only,
            'dd_match_rate': dd_match_rate,
            'ad_match_rate': ad_match_rate,
            'jaccard_index': jaccard,
            'dd_coverage': dd_total / dd_count * 100,
            'ad_coverage': ad_total / ad_count * 100
        }
        
        print(f"{description}: {matches:,} matches ({dd_match_rate:.2f}% DD, {ad_match_rate:.2f}% AD)")
    
    conn.close()
    logger.info("Compound match analysis completed")
    return results


if __name__ == "__main__":
    results = analyze_compound_matches()
    
    print("\n=== COMPOUND MATCHING SUMMARY ===")
    for field, data in results.items():
        print(f"\n{data['description']}:")
        print(f"  Matches: {data['matches']:,}")
        print(f"  DD Coverage: {data['dd_coverage']:.1f}%")
        print(f"  AD Coverage: {data['ad_coverage']:.1f}%")
        print(f"  Jaccard Index: {data['jaccard_index']:.4f}")