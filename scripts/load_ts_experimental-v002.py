#!/usr/bin/env python3
"""
Script to load toolsets data into tb_toolsets table.
Transforms source data (e2e_group_id, building, status_level) into toolsets format.
Now includes comprehensive error logging functionality.
"""

import sys
import os
from typing import List, Tuple, Optional

# Add parent directory to path to import db module
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db import Database
from managers.batch_error_manager import BatchErrorManager, ErrorSeverity


def get_source_data(db: Database, error_manager: BatchErrorManager) -> List[Tuple]:
    """
    Fetch source data and transform it for toolsets loading.
    
    Args:
        db: Database instance
        error_manager: Error manager for logging issues
    
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
        error_msg = f"Error fetching source data: {e}"
        print(f"✗ {error_msg}")
        error_manager.log_fetch_error(error_msg, exception=e)
        raise


def clear_existing_toolsets(db: Database, error_manager: BatchErrorManager) -> int:
    """
    Remove all existing toolsets from the table.
    
    Args:
        db: Database instance
        error_manager: Error manager for logging issues
    
    Returns:
        Number of rows deleted
    """
    delete_sql = "DELETE FROM tb_toolsets"
    
    try:
        deleted_count = db.update(delete_sql)
        print(f"Cleared {deleted_count} existing toolsets")
        return deleted_count
    except Exception as e:
        error_msg = f"Error clearing existing toolsets: {e}"
        print(f"✗ {error_msg}")
        error_manager.log_deletion_error(error_msg, exception=e)
        raise


def insert_toolsets(db: Database, toolsets_data: List[Tuple], error_manager: BatchErrorManager) -> int:
    """
    Insert toolsets data into tb_toolsets table.
    
    Args:
        db: Database instance
        toolsets_data: List of tuples (code, fab, phase, name, description)
        error_manager: Error manager for logging issues
    
    Returns:
        Number of rows inserted
    """
    if not toolsets_data:
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
                print(f"✓ Inserted toolset: {code} ({fab}, {phase})")
            else:
                warning_msg = f"No rows affected for toolset: {code}"
                print(f"⚠ {warning_msg}")
                error_manager.log_error(
                    error_type="insertion",
                    error_message=warning_msg,
                    record_identifier=code,
                    record_data=toolset,
                    severity=ErrorSeverity.WARNING
                )
                
        except Exception as e:
            error_msg = f"Failed to insert toolset {code}: {e}"
            print(f"✗ {error_msg}")
            failed_inserts.append((code, error_msg))
            
            # Log the insertion error
            error_manager.log_insertion_error(
                error_message=error_msg,
                record_identifier=code,
                record_data=toolset,
                exception=e
            )
    
    if failed_inserts:
        print(f"\n{len(failed_inserts)} failed inserts:")
        for code, error in failed_inserts:
            print(f"  - {code}: {error}")
    
    return inserted_count


def validate_toolsets_data(toolsets_data: List[Tuple], error_manager: BatchErrorManager) -> List[Tuple]:
    """
    Validate and clean toolsets data before insertion.
    
    Args:
        toolsets_data: Raw toolsets data
        error_manager: Error manager for logging validation issues
    
    Returns:
        Validated and cleaned toolsets data
    """
    valid_data = []
    invalid_count = 0
    
    for i, toolset in enumerate(toolsets_data):
        code, fab, phase, name, description = toolset
        record_identifier = f"row_{i+1}"
        
        # Validate required fields
        if not code or not isinstance(code, str) or len(code.strip()) == 0:
            error_msg = f"Invalid or missing code in row {i+1}"
            print(f"⚠ Skipping {error_msg}")
            error_manager.log_validation_error(
                error_message=error_msg,
                record_identifier=record_identifier,
                record_data=toolset
            )
            invalid_count += 1
            continue
            
        if not fab or not isinstance(fab, str) or len(fab.strip()) == 0:
            error_msg = f"Invalid or missing fab for code {code} in row {i+1}"
            print(f"⚠ Skipping {error_msg}")
            error_manager.log_validation_error(
                error_message=error_msg,
                record_identifier=code,
                record_data=toolset
            )
            invalid_count += 1
            continue
            
        if not phase or not isinstance(phase, str) or len(phase.strip()) == 0:
            error_msg = f"Invalid or missing phase for code {code} in row {i+1}"
            print(f"⚠ Skipping {error_msg}")
            error_manager.log_validation_error(
                error_message=error_msg,
                record_identifier=code,
                record_data=toolset
            )
            invalid_count += 1
            continue
        
        # Clean and validate field lengths
        original_code = code
        code = code.strip()[:64]  # Limit to VARCHAR(64)
        fab = fab.strip()[:10]    # Limit to VARCHAR(10)
        phase = phase.strip()[:8] # Limit to VARCHAR(8)
        
        # Log if data was truncated
        if len(original_code.strip()) > 64:
            warning_msg = f"Code truncated from {len(original_code.strip())} to 64 characters for code {original_code}"
            error_manager.log_validation_error(
                error_message=warning_msg,
                record_identifier=code,
                record_data=toolset
            )
        
        if name:
            name = name.strip()[:128] if name.strip() else None  # VARCHAR(128)
        
        if description:
            description = description.strip()[:512] if description.strip() else None  # VARCHAR(512)
        
        valid_data.append((code, fab, phase, name, description))
    
    if invalid_count > 0:
        print(f"⚠ Excluded {invalid_count} invalid records")
    
    return valid_data


def main():
    """
    Main function to orchestrate the toolsets loading process.
    """
    print("Starting toolsets loading process...")
    
    db = None
    error_manager = None
    
    try:
        # Initialize database connection
        print("Connecting to database...")
        db = Database()
        print("✓ Database connection established")
        
        # Initialize error manager
        error_manager = BatchErrorManager(db, batch_type="toolsets")
        print(f"✓ Error manager initialized (Run ID: {error_manager.get_batch_run_id()})")
        
        # Fetch source data
        print("\nFetching source data...")
        raw_data = get_source_data(db, error_manager)
        print(f"✓ Retrieved {len(raw_data)} records from source")
        
        # Validate data
        print("\nValidating data...")
        valid_data = validate_toolsets_data(raw_data, error_manager)
        print(f"✓ Validated {len(valid_data)} records")
        
        if not valid_data:
            error_msg = "No valid data to process"
            print(f"✗ {error_msg}")
            error_manager.log_error(
                error_type="validation",
                error_message=error_msg,
                severity=ErrorSeverity.CRITICAL
            )
            return
        
        # Show summary of what will be loaded
        print(f"\nSummary of data to be loaded:")
        print(f"  Total toolsets: {len(valid_data)}")
        
        # Group by fab for summary
        fab_counts = {}
        for _, fab, _, _, _ in valid_data:
            fab_counts[fab] = fab_counts.get(fab, 0) + 1
        
        for fab, count in sorted(fab_counts.items()):
            print(f"  {fab}: {count} toolsets")
        
        # Confirm before proceeding
        response = input("\nProceed with loading? (y/N): ").strip().lower()
        if response not in ['y', 'yes']:
            print("Loading cancelled by user.")
            return
        
        # Clear existing data
        print("\nClearing existing toolsets...")
        clear_existing_toolsets(db, error_manager)
        
        # Insert new data
        print("\nInserting toolsets...")
        inserted_count = insert_toolsets(db, valid_data, error_manager)
        
        print(f"\n✓ Successfully loaded {inserted_count} toolsets")
        
        # Verify the load
        print("\nVerifying load...")
        verification_sql = """
            SELECT 
                COUNT(*) as total,
                COUNT(DISTINCT fab) as unique_fabs,
                COUNT(DISTINCT phase) as unique_phases
            FROM tb_toolsets 
            WHERE is_active = TRUE
        """
        
        try:
            result = db.query(verification_sql)
            if result:
                total, unique_fabs, unique_phases = result[0]
                print(f"✓ Verification complete:")
                print(f"  Total active toolsets: {total}")
                print(f"  Unique fabs: {unique_fabs}")
                print(f"  Unique phases: {unique_phases}")
        except Exception as e:
            error_msg = f"Error during verification: {e}"
            print(f"⚠ {error_msg}")
            error_manager.log_error(
                error_type="verification",
                error_message=error_msg,
                severity=ErrorSeverity.WARNING
            )
        
        # Print error summary
        error_manager.print_error_summary()
        
    except Exception as e:
        error_msg = f"Error during toolsets loading: {e}"
        print(f"✗ {error_msg}")
        
        if error_manager:
            error_manager.log_error(
                error_type="system",
                error_message=error_msg,
                severity=ErrorSeverity.CRITICAL
            )
            error_manager.print_error_summary()
        
        sys.exit(1)
        
    finally:
        if db:
            print("\nClosing database connection...")
            db.close()
            print("✓ Database connection closed")
    
    print("\nToolsets loading process completed!")
    
    # Show final error summary
    if error_manager:
        summary = error_manager.get_error_summary()
        if summary['TOTAL'] > 0:
            print(f"\n⚠ Process completed with {summary['TOTAL']} logged errors/warnings")
            print(f"   Run ID: {error_manager.get_batch_run_id()}")
            print("   Check tb_batch_errors table for details")
        else:
            print("\n✓ Process completed without errors")


if __name__ == "__main__":
    main()
