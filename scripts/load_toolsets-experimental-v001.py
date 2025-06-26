#!/usr/bin/env python3
"""
Script to load toolsets data into tb_toolsets table.
Transforms source data (e2e_group_id, building, status_level) into toolsets format.

Usage:
    python load_toolsets.py          # Interactive mode with confirmation
    python load_toolsets.py -y       # Unattended mode, auto-confirm
    python load_toolsets.py -u       # Unattended mode, minimal output
"""

import sys
import os
import argparse
from typing import List, Tuple, Optional

# Add parent directory to path to import db module
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db import Database


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Load toolsets data into tb_toolsets table",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python load_toolsets.py     # Interactive mode with confirmation
  python load_toolsets.py -y  # Auto-confirm mode
  python load_toolsets.py -u  # Unattended mode, minimal output
        """
    )
    parser.add_argument(
        '-y', '--yes', 
        action='store_true',
        help='Auto-confirm without prompting'
    )
    parser.add_argument(
        '-u', '--unattended', 
        action='store_true',
        help='Unattended mode with minimal output (only "complete" or "fail")'
    )
    return parser


def fetch_source_data(db: Database, unattended: bool = False) -> List[Tuple]:
    """
    Fetch source data and transform it for toolsets loading.
    
    Returns:
        List of tuples: (code, fab, phase, name, description)
    """
    # Your source query - modify the FROM clause and any WHERE conditions as needed
    source_query = """
        SELECT 
            e2e_group_id as code, 
            building as fab, 
            status_level as phase
        FROM your_source_table
        WHERE e2e_group_id IS NOT NULL 
          AND building IS NOT NULL 
          AND status_level IS NOT NULL
        ORDER BY e2e_group_id
    """
    
    try:
        rows = db.query(source_query)
        
        # Transform the data - add name and description if needed
        transformed_data = []
        for row in rows:
            code, fab, phase = row
            # Generate a default name based on code, or set to None
            name = f"Toolset {code}" if code else None
            # Description can be derived from fab and phase, or set to None
            description = f"Toolset for {fab} facility, phase {phase}" if fab and phase else None
            
            transformed_data.append((code, fab, phase, name, description))
        
        return transformed_data
        
    except Exception as e:
        if not unattended:
            print(f"Error fetching source data: {e}")
        raise


def clear_existing_toolsets(db: Database, unattended: bool = False) -> int:
    """
    Remove all existing toolsets from the table.
    
    Returns:
        Number of rows deleted
    """
    delete_sql = "DELETE FROM tb_toolsets"
    
    try:
        deleted_count = db.update(delete_sql)
        if not unattended:
            print(f"Cleared {deleted_count} existing toolsets")
        return deleted_count
    except Exception as e:
        if not unattended:
            print(f"Error clearing existing toolsets: {e}")
        raise


def insert_toolsets(db: Database, toolsets_data: List[Tuple], unattended: bool = False) -> int:
    """
    Insert toolsets data into tb_toolsets table.
    
    Args:
        toolsets_data: List of tuples (code, fab, phase, name, description)
        unattended: If True, suppress all output except critical errors
    
    Returns:
        Number of rows inserted
    """
    if not toolsets_data:
        if not unattended:
            print("No toolsets data to insert")
        return 0
    
    insert_sql = """
        INSERT INTO tb_toolsets (code, fab, phase, name, description, is_active)
        VALUES (?, ?, ?, ?, ?, ?)
    """
    
    inserted_count = 0
    failed_inserts = []
    
    for toolset in toolsets_data:
        code, fab, phase, name, description = toolset
        
        try:
            # Set is_active to True by default
            params = [code, fab, phase, name, description, True]
            rows_affected = db.update(insert_sql, params)
            
            if rows_affected > 0:
                inserted_count += rows_affected
                if not unattended:
                    print(f"✓ Inserted toolset: {code} ({fab}, {phase})")
            else:
                if not unattended:
                    print(f"⚠ No rows affected for toolset: {code}")
                
        except Exception as e:
            error_msg = f"Failed to insert toolset {code}: {e}"
            if not unattended:
                print(f"✗ {error_msg}")
            failed_inserts.append((code, error_msg))
    
    if failed_inserts and not unattended:
        print(f"\n{len(failed_inserts)} failed inserts:")
        for code, error in failed_inserts:
            print(f"  - {code}: {error}")
    
    return inserted_count


def validate_toolsets_data(toolsets_data: List[Tuple], unattended: bool = False) -> List[Tuple]:
    """
    Validate and clean toolsets data before insertion.
    
    Args:
        toolsets_data: Raw toolsets data
        unattended: If True, suppress warning messages
    
    Returns:
        Validated and cleaned toolsets data
    """
    valid_data = []
    invalid_count = 0
    
    for i, toolset in enumerate(toolsets_data):
        code, fab, phase, name, description = toolset
        
        # Validate required fields
        if not code or not isinstance(code, str) or len(code.strip()) == 0:
            if not unattended:
                print(f"⚠ Skipping row {i+1}: Invalid or missing code")
            invalid_count += 1
            continue
            
        if not fab or not isinstance(fab, str) or len(fab.strip()) == 0:
            if not unattended:
                print(f"⚠ Skipping row {i+1}: Invalid or missing fab for code {code}")
            invalid_count += 1
            continue
            
        if not phase or not isinstance(phase, str) or len(phase.strip()) == 0:
            if not unattended:
                print(f"⚠ Skipping row {i+1}: Invalid or missing phase for code {code}")
            invalid_count += 1
            continue
        
        # Clean and validate field lengths
        code = code.strip()[:64]  # Limit to VARCHAR(64)
        fab = fab.strip()[:10]    # Limit to VARCHAR(10)
        phase = phase.strip()[:8] # Limit to VARCHAR(8)
        
        if name:
            name = name.strip()[:128] if name.strip() else None  # VARCHAR(128)
        
        if description:
            description = description.strip()[:512] if description.strip() else None  # VARCHAR(512)
        
        valid_data.append((code, fab, phase, name, description))
    
    if invalid_count > 0 and not unattended:
        print(f"⚠ Excluded {invalid_count} invalid records")
    
    return valid_data


def load_toolsets_verification(db: Database, unattended: bool = False):
    """
    Verify the toolsets load.
    
    Args:
        db: Database connection
        unattended: If True, suppress output
    """
    verification_sql = """
        SELECT 
            COUNT(*) as total,
            COUNT(DISTINCT fab) as unique_fabs,
            COUNT(DISTINCT phase) as unique_phases
        FROM tb_toolsets 
        WHERE is_active = TRUE
    """
    
    result = db.query(verification_sql)
    if result and not unattended:
        total, unique_fabs, unique_phases = result[0]
        print(f"✓ Verification complete:")
        print(f"  Total active toolsets: {total}")
        print(f"  Unique fabs: {unique_fabs}")
        print(f"  Unique phases: {unique_phases}")


def main():
    """
    Main function to orchestrate the toolsets loading process.
    """
    # Parse command line arguments
    parser = create_parser()
    args = parser.parse_args()
    
    # If unattended mode is specified, also auto-confirm
    if args.unattended:
        args.yes = True
    
    unattended = args.unattended
    
    if not unattended:
        print("Starting toolsets loading process...")
    
    db = None
    try:
        # Initialize database connection
        if not unattended:
            print("Connecting to database...")
        db = Database()
        if not unattended:
            print("✓ Database connection established")
        
        # Fetch source data
        if not unattended:
            print("\nFetching source data...")
        raw_data = fetch_source_data(db, unattended)
        if not unattended:
            print(f"✓ Retrieved {len(raw_data)} records from source")
        
        # Validate data
        if not unattended:
            print("\nValidating data...")
        valid_data = validate_toolsets_data(raw_data, unattended)
        if not unattended:
            print(f"✓ Validated {len(valid_data)} records")
        
        if not valid_data:
            if unattended:
                print("fail")
                sys.exit(1)
            else:
                print("No valid data to process. Exiting.")
                return
        
        # Show summary of what will be loaded
        if not unattended:
            print(f"\nSummary of data to be loaded:")
            print(f"  Total toolsets: {len(valid_data)}")
            
            # Group by fab for summary
            fab_counts = {}
            for _, fab, _, _, _ in valid_data:
                fab_counts[fab] = fab_counts.get(fab, 0) + 1
            
            for fab, count in sorted(fab_counts.items()):
                print(f"  {fab}: {count} toolsets")
        
        # Confirm before proceeding (unless -y flag is used)
        if args.yes:
            if not unattended:
                print("\nAuto-confirming due to -y flag...")
        else:
            response = input("\nProceed with loading? (y/N): ").strip().lower()
            if response not in ['y', 'yes']:
                print("Loading cancelled by user.")
                return
        
        # Clear existing data
        if not unattended:
            print("\nClearing existing toolsets...")
        clear_existing_toolsets(db, unattended)
        
        # Insert new data
        if not unattended:
            print("\nInserting toolsets...")
        inserted_count = insert_toolsets(db, valid_data, unattended)
        
        if not unattended:
            print(f"\n✓ Successfully loaded {inserted_count} toolsets")
        
        # Verify the load
        if not unattended:
            print("\nVerifying load...")
        load_toolsets_verification(db, unattended)
        
        # Output final result
        if unattended:
            print("complete")
        else:
            print("\nToolsets loading process completed successfully!")
            
    except Exception as e:
        if unattended:
            print(f"fail: {e}")
        else:
            print(f"✗ Error during toolsets loading: {e}")
        sys.exit(1)
        
    finally:
        if db:
            if not unattended:
                print("\nClosing database connection...")
            db.close()
            if not unattended:
                print("✓ Database connection closed")


if __name__ == "__main__":
    main()
