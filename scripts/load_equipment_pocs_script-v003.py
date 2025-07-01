#!/usr/bin/env python3
"""
Script to load equipment POCs data into tb_equipment_pocs table.
Transforms source data from nw_nodes and related tables.

Usage:
    python load_equipment_pocs.py          # Interactive mode with confirmation
    python load_equipment_pocs.py -y       # Unattended mode, auto-confirm
"""

import sys
import os
import argparse
from typing import List, Tuple, Optional, Dict

# Add parent directory to path to import db module
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db import Database


def create_parser() -> argparse.ArgumentParser:
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description="Load equipment POC data into tb_equipment_pocs table",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python load_equipment_pocs.py     # Interactive mode with confirmation
  python load_equipment_pocs.py -y  # Unattended mode, auto-confirm
        """
    )
    parser.add_argument(
        '-y', '--yes', 
        action='store_true',
        help='Auto-confirm without prompting (unattended mode)'
    )
    return parser

def fetch_equipment_mapping(db: Database) -> Dict[str, int]:
    """
    Get mapping of equipment_guid -> equipment_id for FK validation.
    
    Returns:
        Dictionary mapping equipment GUID to equipment ID
    """
    equipment_query = """
        SELECT id, guid
        FROM tb_equipments 
        WHERE is_active = TRUE
    """
    
    try:
        rows = db.query(equipment_query)
        mapping = {}
        
        for row in rows:
            equipment_id, guid = row
            mapping[guid] = equipment_id
        
        print(f"✓ Loaded {len(mapping)} active equipment mappings")
        return mapping
        
    except Exception as e:
        print(f"Error loading equipment mappings: {e}")
        raise


def fetch_source_data(db: Database) -> List[Tuple]:
    """
    Fetch source data for equipment POCs from nw_nodes and related tables.
    
    Returns:
        List of tuples: (node_id, equipment_guid, toolset, eq_poc_no, utility, 
                        nwo_type, category, connection_status)
    """
    source_query = """
        SELECT n.node_id,
               n.eq_guid as equipment_guid,
               gn.description as toolset,
               n.eq_poc_no,
               un.description as utility,
               n.net_obj_type as nwo_type,
               c.description as category,
               CASE 
                   WHEN n.eq_poc_no = at.eq_poc_no THEN 'USED'
                   ELSE 'NOT_USED'
               END as connection_status
        FROM nw_nodes n
        LEFT JOIN group_nodes gn ON n.group_node_id = gn.id
        LEFT JOIN utility_nodes un ON n.utility_node_id = un.id
        LEFT JOIN categories c ON n.category_id = c.id
        LEFT JOIN attachment_table at ON n.eq_poc_no = at.eq_poc_no
        WHERE n.node_id IS NOT NULL 
          AND n.eq_guid IS NOT NULL
          AND n.eq_poc_no IS NOT NULL
        ORDER BY n.eq_guid, n.eq_poc_no
    """
    
    try:
        rows = db.query(source_query)
        print(f"✓ Retrieved {len(rows)} equipment POC records from source")
        return rows
        
    except Exception as e:
        print(f"Error fetching equipment POC source data: {e}")
        raise


def generate_poc_code(eq_poc_no: str, utility: str, nwo_type: str) -> str:
    """
    Generate standardized POC code from raw data.
    
    Handles cases like:
    - "POC012" -> "POC012"
    - "POC007, POC007 (1), POC007 (2)" -> "POC007"
    
    Args:
        eq_poc_no: Raw equipment POC number
        utility: Utility type
        nwo_type: Network object type
    
    Returns:
        Formatted POC code (POC012, IN01, OUT01, etc.)
    """
    if not eq_poc_no:
        return "POC01"  # Default
    
    # Clean the input
    eq_poc_clean = str(eq_poc_no).strip()
    
    # Handle multi-dimensional POCs: "POC007, POC007 (1), POC007 (2)" -> "POC007"
    # Take the first element before any comma
    if ',' in eq_poc_clean:
        eq_poc_clean = eq_poc_clean.split(',')[0].strip()
    
    # Remove any parenthetical information from the first element
    # "POC007 (1)" -> "POC007"
    if '(' in eq_poc_clean:
        eq_poc_clean = eq_poc_clean.split('(')[0].strip()
    
    eq_poc_upper = eq_poc_clean.upper()
    
    # For standard POC codes like "POC012", return as-is (after validation)
    if eq_poc_upper.startswith('POC') and len(eq_poc_upper) >= 6:
        # Extract the POC part and number
        import re
        poc_match = re.match(r'(POC)(\d+)', eq_poc_upper)
        if poc_match:
            prefix, number = poc_match.groups()
            # Ensure number is at least 2 digits
            number = number.zfill(3)  # POC uses 3 digits typically
            return f"{prefix}{number}"
    
    # Handle IN/OUT cases based on nwo_type or poc name
    if nwo_type and 'IN' in nwo_type.upper():
        # Try to extract number
        import re
        number_match = re.search(r'(\d+)', eq_poc_upper)
        number = number_match.group(1).zfill(2) if number_match else "01"
        return f"IN{number}"
    elif nwo_type and 'OUT' in nwo_type.upper():
        import re
        number_match = re.search(r'(\d+)', eq_poc_upper)
        number = number_match.group(1).zfill(2) if number_match else "01"
        return f"OUT{number}"
    elif eq_poc_upper.startswith('IN'):
        import re
        number_match = re.search(r'(\d+)', eq_poc_upper)
        number = number_match.group(1).zfill(2) if number_match else "01"
        return f"IN{number}"
    elif eq_poc_upper.startswith('OUT'):
        import re
        number_match = re.search(r'(\d+)', eq_poc_upper)
        number = number_match.group(1).zfill(2) if number_match else "01"
        return f"OUT{number}"
    else:
        # Default case - try to extract number and use POC prefix
        import re
        number_match = re.search(r'(\d+)', eq_poc_upper)
        if number_match:
            number = number_match.group(1).zfill(3)
            return f"POC{number}"
        else:
            return "POC001"  # Final fallback


def determine_flow_direction(eq_poc_no: str, utility: str, nwo_type: str) -> Optional[str]:
    """
    Determine flow direction (IN/OUT) based on available data.
    
    Args:
        eq_poc_no: Raw equipment POC number
        utility: Utility type
        nwo_type: Network object type
    
    Returns:
        Flow direction ('IN', 'OUT') or None
    """
    if not eq_poc_no and not nwo_type:
        return None
    
    # Check nwo_type first
    if nwo_type:
        nwo_upper = nwo_type.upper()
        if 'IN' in nwo_upper:
            return 'IN'
        elif 'OUT' in nwo_upper:
            return 'OUT'
    
    # Check eq_poc_no
    if eq_poc_no:
        poc_upper = str(eq_poc_no).upper()
        if poc_upper.startswith('IN'):
            return 'IN'
        elif poc_upper.startswith('OUT'):
            return 'OUT'
    
    return None


def transform_poc_data(raw_data: List[Tuple], equipment_mapping: Dict[str, int]) -> List[Tuple]:
    """
    Transform raw POC data into format for tb_equipment_pocs.
    
    Args:
        raw_data: Raw data from source query
        equipment_mapping: Mapping of equipment GUID to equipment ID
    
    Returns:
        List of tuples: (equipment_id, node_id, code, eq_poc_no, is_used, utility, flow)
    """
    transformed_data = []
    missing_equipment = set()
    
    for row in raw_data:
        node_id, equipment_guid, toolset, eq_poc_no, utility, nwo_type, category, connection_status = row
        
        # Look up equipment ID
        equipment_id = equipment_mapping.get(equipment_guid)
        
        if not equipment_id:
            missing_equipment.add(equipment_guid)
            continue
        
        # Generate standardized POC code
        code = generate_poc_code(eq_poc_no, utility, nwo_type)
        
        # Determine if used
        is_used = 1 if connection_status == 'USED' else 0
        
        # Determine flow direction
        flow = determine_flow_direction(eq_poc_no, utility, nwo_type)
        
        # Clean utility - only set if POC is used
        cleaned_utility = utility.strip() if (utility and is_used) else None
        
        transformed_data.append((
            equipment_id, node_id, code, eq_poc_no, is_used, cleaned_utility, flow
        ))
    
    if missing_equipment:
        print(f"⚠ Warning: {len(missing_equipment)} POCs reference missing equipment:")
        for guid in sorted(list(missing_equipment)[:10]):  # Show first 10
            print(f"  - {guid}")
        if len(missing_equipment) > 10:
            print(f"  - ... and {len(missing_equipment) - 10} more")
        print("  These POCs will be skipped. Ensure equipments are loaded first.")
    
    return transformed_data


def clear_existing_pocs(db: Database) -> int:
    """
    Remove all existing equipment POCs from the table.
    
    Returns:
        Number of rows deleted
    """
    delete_sql = "DELETE FROM tb_equipment_pocs"
    
    try:
        deleted_count = db.update(delete_sql)
        print(f"Cleared {deleted_count} existing equipment POCs")
        return deleted_count
    except Exception as e:
        print(f"Error clearing existing equipment POCs: {e}")
        raise


def validate_poc_data(poc_data: List[Tuple]) -> List[Tuple]:
    """
    Validate and clean equipment POC data before insertion.
    
    Args:
        poc_data: Raw POC data
    
    Returns:
        Validated and cleaned POC data
    """
    valid_data = []
    invalid_count = 0
    node_id_seen = set()
    equipment_poc_combinations = set()
    
    for i, poc in enumerate(poc_data):
        equipment_id, node_id, code, eq_poc_no, is_used, utility, flow = poc
        
        # Validate required fields
        if not equipment_id:
            print(f"⚠ Skipping row {i+1}: Missing equipment_id")
            invalid_count += 1
            continue
            
        if node_id is None:
            print(f"⚠ Skipping row {i+1}: Missing node_id")
            invalid_count += 1
            continue
            
        if not code or len(code.strip()) == 0:
            print(f"⚠ Skipping row {i+1}: Missing code")
            invalid_count += 1
            continue
        
        # Check for duplicate node_id (unique constraint)
        if node_id in node_id_seen:
            print(f"⚠ Skipping row {i+1}: Duplicate node_id {node_id}")
            invalid_count += 1
            continue
        node_id_seen.add(node_id)
        
        # Check for duplicate equipment_id + code combination (unique constraint)
        combination_key = (equipment_id, code.strip())
        if combination_key in equipment_poc_combinations:
            print(f"⚠ Skipping row {i+1}: Duplicate equipment_id {equipment_id} + code {code}")
            invalid_count += 1
            continue
        equipment_poc_combinations.add(combination_key)
        
        # Clean and validate field lengths
        code = code.strip()[:8]
        
        if eq_poc_no:
            eq_poc_no = str(eq_poc_no).strip()[:128]
        
        if utility:
            utility = utility.strip()[:128] if utility.strip() else None
        
        if flow:
            flow = flow.strip()[:8] if flow.strip() else None
        
        # Ensure numeric/boolean fields are valid
        try:
            equipment_id = int(equipment_id)
            node_id = int(node_id)
            is_used = int(is_used) if is_used in [0, 1] else 0
        except (ValueError, TypeError):
            print(f"⚠ Skipping row {i+1}: Invalid numeric values")
            invalid_count += 1
            continue
        
        valid_data.append((equipment_id, node_id, code, eq_poc_no, is_used, utility, flow))
    
    if invalid_count > 0:
        print(f"⚠ Excluded {invalid_count} invalid records")
    
    return valid_data


def insert_pocs_batch(db: Database, poc_data: List[Tuple], batch_size: int = 1000) -> int:
    """
    Insert equipment POC data in batches for better performance.
    
    Args:
        poc_data: List of validated POC tuples
        batch_size: Number of records to insert per batch
    
    Returns:
        Total number of rows inserted
    """
    if not poc_data:
        print("No equipment POC data to insert")
        return 0
    
    insert_sql = """
        INSERT INTO tb_equipment_pocs (equipment_id, node_id, code, eq_poc_no, is_used, utility, flow)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """
    
    total_inserted = 0
    total_batches = (len(poc_data) + batch_size - 1) // batch_size
    
    for batch_num in range(total_batches):
        start_idx = batch_num * batch_size
        end_idx = min(start_idx + batch_size, len(poc_data))
        batch = poc_data[start_idx:end_idx]
        
        batch_inserted = 0
        failed_in_batch = []
        
        for poc in batch:
            equipment_id, node_id, code, eq_poc_no, is_used, utility, flow = poc
            
            try:
                params = [equipment_id, node_id, code, eq_poc_no, is_used, utility, flow]
                rows_affected = db.update(insert_sql, params)
                
                if rows_affected > 0:
                    batch_inserted += rows_affected
                else:
                    failed_in_batch.append((node_id, "No rows affected"))
                    
            except Exception as e:
                failed_in_batch.append((node_id, str(e)))
        
        total_inserted += batch_inserted
        print(f"✓ Batch {batch_num + 1}/{total_batches}: Inserted {batch_inserted}/{len(batch)} POCs")
        
        if failed_in_batch:
            print(f"  ⚠ {len(failed_in_batch)} failures in this batch")
            for node_id, error in failed_in_batch[:3]:  # Show first 3 errors
                print(f"    - Node {node_id}: {error}")
            if len(failed_in_batch) > 3:
                print(f"    - ... and {len(failed_in_batch) - 3} more")
    
    return total_inserted


def main():
    """
    Main function to orchestrate the equipment POC loading process.
    """
    parser = create_parser()    
    args = parser.parse_args()
    
    print("Starting equipment POC loading process...")
    
    db = None
    try:
        # Initialize database connection
        print("Connecting to database...")
        db = Database()
        print("✓ Database connection established")
        
        # Load equipment mappings first
        print("\nLoading equipment mappings...")
        equipment_mapping = fetch_equipment_mapping(db)
        
        if not equipment_mapping:
            print("✗ No active equipments found. Please load equipments first.")
            return
        
        # Fetch source data
        print("\nFetching equipment POC source data...")
        raw_data = fetch_source_data(db)
        
        if not raw_data:
            print("No source data found. Exiting.")
            return
        
        # Transform data
        print("\nTransforming data...")
        transformed_data = transform_poc_data(raw_data, equipment_mapping)
        
        # Validate data
        print("\nValidating data...")
        valid_data = validate_poc_data(transformed_data)
        print(f"✓ Validated {len(valid_data)} equipment POC records")
        
        if not valid_data:
            print("No valid data to process. Exiting.")
            return
        
        # Show summary
        print(f"\nSummary of data to be loaded:")
        print(f"  Total equipment POCs: {len(valid_data)}")
        
        # Analyze the data
        used_count = sum(1 for _, _, _, _, is_used, _, _ in valid_data if is_used)
        unused_count = len(valid_data) - used_count
        
        code_counts = {}
        utility_counts = {}
        flow_counts = {}
        
        for _, _, code, _, is_used, utility, flow in valid_data:
            # Count POC codes
            code_prefix = code[:3] if len(code) >= 3 else code
            code_counts[code_prefix] = code_counts.get(code_prefix, 0) + 1
            
            # Count utilities (only for used POCs)
            if is_used and utility:
                utility_counts[utility] = utility_counts.get(utility, 0) + 1
            
            # Count flow directions
            if flow:
                flow_counts[flow] = flow_counts.get(flow, 0) + 1
        
        print(f"  Used POCs: {used_count}")
        print(f"  Unused POCs: {unused_count}")
        print(f"  Unique equipments with POCs: {len(set(eq_id for eq_id, _, _, _, _, _, _ in valid_data))}")
        
        # Show code distribution
        print("  POC code distribution:")
        for code_type, count in sorted(code_counts.items()):
            print(f"    {code_type}**: {count}")
        
        # Show top utilities
        if utility_counts:
            top_utilities = sorted(utility_counts.items(), key=lambda x: x[1], reverse=True)[:5]
            print("  Top utilities:")
            for utility, count in top_utilities:
                print(f"    {utility}: {count}")
        
        # Show flow distribution
        if flow_counts:
            print("  Flow distribution:")
            for flow, count in sorted(flow_counts.items()):
                print(f"    {flow}: {count}")
        
        # Confirm before proceeding (unless -y flag is used)
        if args.yes:
            print("\nAuto-confirming due to -y flag...")
        else:
            response = input("\nProceed with loading? (y/N): ").strip().lower()
            if response not in ['y', 'yes']:
                print("Loading cancelled by user.")
                return
        
        # Clear existing data
        print("\nClearing existing equipment POCs...")
        clear_existing_pocs(db)
        
        # Insert new data in batches
        print("\nInserting equipment POCs...")
        inserted_count = insert_pocs_batch(db, valid_data)
        
        print(f"\n✓ Successfully loaded {inserted_count} equipment POCs")
        
        # Verify the load
        print("\nVerifying load...")
        verification_sql = """
            SELECT 
                COUNT(*) as total,
                COUNT(DISTINCT equipment_id) as unique_equipments,
                SUM(is_used) as used_pocs,
                COUNT(*) - SUM(is_used) as unused_pocs,
                COUNT(DISTINCT utility) as unique_utilities
            FROM tb_equipment_pocs
        """
        
        result = db.query(verification_sql)
        if result:
            total, unique_equipments, used_pocs, unused_pocs, unique_utilities = result[0]
            print(f"✓ Verification complete:")
            print(f"  Total POCs: {total}")
            print(f"  Unique equipments: {unique_equipments}")
            print(f"  Used POCs: {used_pocs}")
            print(f"  Unused POCs: {unused_pocs}")
            print(f"  Unique utilities: {unique_utilities}")
        
        # Additional verification - check for constraint violations
        print("\nChecking data integrity...")
        
        # Check unique node_id constraint
        duplicate_nodes_sql = """
            SELECT node_id, COUNT(*) as cnt
            FROM tb_equipment_pocs 
            GROUP BY node_id 
            HAVING COUNT(*) > 1
        """
        duplicate_nodes = db.query(duplicate_nodes_sql)
        if duplicate_nodes:
            print(f"⚠ Found {len(duplicate_nodes)} duplicate node_ids (should be 0)")
        else:
            print("✓ No duplicate node_ids found")
        
        # Check unique equipment_id + code constraint
        duplicate_codes_sql = """
            SELECT equipment_id, code, COUNT(*) as cnt
            FROM tb_equipment_pocs 
            GROUP BY equipment_id, code 
            HAVING COUNT(*) > 1
        """
        duplicate_codes = db.query(duplicate_codes_sql)
        if duplicate_codes:
            print(f"⚠ Found {len(duplicate_codes)} duplicate equipment_id+code combinations (should be 0)")
        else:
            print("✓ No duplicate equipment_id+code combinations found")
        
    except Exception as e:
        print(f"✗ Error during equipment POC loading: {e}")
        sys.exit(1)
        
    finally:
        if db:
            print("\nClosing database connection...")
            db.close()
            print("✓ Database connection closed")
    
    print("\nEquipment POC loading process completed successfully!")


if __name__ == "__main__":
    main()