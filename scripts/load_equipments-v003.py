#!/usr/bin/env python3
"""
Script to load equipments data into tb_equipments table.
Transforms source data from tb_shapes and related tables.

Usage:
    python load_equipments.py          # Interactive mode with confirmation
    python load_equipments.py -y       # Unattended mode, auto-confirm
"""

import sys
import os
import argparse
from typing import List, Tuple, Optional, Dict

# Add parent directory to path to import db module
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db import Database


def get_existing_toolsets(db: Database) -> set:
    """
    Get set of existing toolset codes for validation.
    
    Returns:
        Set of valid toolset codes
    """
    toolset_query = """
        SELECT code
        FROM tb_toolsets 
        WHERE is_active = 1
    """
    
    try:
        rows = db.query(toolset_query)
        toolset_codes = {row[0] for row in rows if row[0]}
        
        print(f"✓ Loaded {len(toolset_codes)} active toolset codes")
        return toolset_codes
        
    except Exception as e:
        print(f"Error loading toolset codes: {e}")
        raise


def get_source_data(db: Database) -> List[Tuple]:
    """
    Fetch source data for equipments from tb_shapes and related tables.
    
    Returns:
        List of tuples: (guid, node_id, fab, phase, model, toolset_desc, category, 
                        data_code, vertices, name)
    """
    source_query = """
        SELECT sh.guid,
               sh.node_id,
               bn.description as fab,
               p.description as phase,
               dmt.description as model,
               gn.code as toolset,
               c.description as category,
               dc.code as data_code,
               sh.vertices_count as vertices
        FROM tb_shapes sh
        JOIN building_nodes bn ON sh.building_node_id = bn.id
        JOIN phases p ON sh.phase_id = p.id
        JOIN data_model_types dmt ON sh.data_model_type_id = dmt.id
        JOIN group_nodes gn ON sh.group_node_id = gn.id
        JOIN categories c ON sh.category_id = c.id
        JOIN data_codes dc ON sh.data_code_id = dc.id
        WHERE sh.guid IS NOT NULL 
          AND sh.node_id IS NOT NULL
          AND bn.description IS NOT NULL
          AND p.description IS NOT NULL
          AND dmt.description IS NOT NULL
          AND gn.description IS NOT NULL
          AND c.description IS NOT NULL
          AND dc.code IS NOT NULL
        ORDER BY sh.guid
    """
    
    try:
        rows = db.query(source_query)
        print(f"✓ Retrieved {len(rows)} equipment records from source")
        return rows
        
    except Exception as e:
        print(f"Error fetching equipment source data: {e}")
        raise


def transform_equipment_data(raw_data: List[Tuple], valid_toolsets: set) -> List[Tuple]:
    """
    Transform raw equipment data into format for tb_equipments.
    
    Args:
        raw_data: Raw data from source query
        valid_toolsets: Set of valid toolset codes
    
    Returns:
        List of tuples: (toolset_code, guid, node_id, data_code, category, vertices, 
                        kind, name, description)
    """
    transformed_data = []
    missing_toolsets = set()
    
    for row in raw_data:
        guid, node_id, fab, phase, model, toolset_code, category, data_code, vertices = row
        
        # Validate toolset code exists
        if not toolset_code or toolset_code not in valid_toolsets:
            missing_toolsets.add(toolset_code or 'NULL')
            continue
        
        # Determine equipment kind based on category (customize as needed)
        kind = determine_equipment_kind(category)
        
        # Generate name and description - keep simple since most are NULL
        name = f"Equipment {guid[:8]}" if guid else "Equipment"
        description = None  # Keep NULL since most descriptions are empty
        
        transformed_data.append((
            toolset_code, guid, node_id, data_code, category, vertices,
            kind, name, description
        ))
    
    if missing_toolsets:
        print(f"⚠ Warning: {len(missing_toolsets)} records with invalid toolset codes:")
        for toolset_code in sorted(missing_toolsets):
            print(f"  - '{toolset_code}'")
        print("  These records will be skipped. Ensure toolsets are loaded first.")
    
    return transformed_data


def determine_equipment_kind(category: str) -> Optional[str]:
    """
    Determine equipment kind based on category.
    Customize this logic based on your business rules.
    
    Args:
        category: Equipment category
    
    Returns:
        Equipment kind or None
    """
    if not category:
        return None
    
    category_lower = category.lower()
    
    # Define category to kind mappings (customize as needed)
    kind_mappings = {
        'production': 'PRODUCTION',
        'processing': 'PROCESSING', 
        'supply': 'SUPPLY',
        'utility': 'UTILITY',
        'support': 'SUPPORT',
        'storage': 'STORAGE',
        'transport': 'TRANSPORT'
    }
    
    for key, kind in kind_mappings.items():
        if key in category_lower:
            return kind
    
    return 'OTHER'  # Default kind


def clear_existing_equipments(db: Database) -> int:
    """
    Remove all existing equipments from the table.
    This will cascade delete equipment POCs due to FK constraint.
    
    Returns:
        Number of rows deleted
    """
    delete_sql = "DELETE FROM tb_equipments"
    
    try:
        deleted_count = db.update(delete_sql)
        print(f"Cleared {deleted_count} existing equipments (POCs deleted via cascade)")
        return deleted_count
    except Exception as e:
        print(f"Error clearing existing equipments: {e}")
        raise


def validate_equipment_data(equipment_data: List[Tuple]) -> List[Tuple]:
    """
    Validate and clean equipment data before insertion.
    
    Args:
        equipment_data: Raw equipment data
    
    Returns:
        Validated and cleaned equipment data
    """
    valid_data = []
    invalid_count = 0
    guid_seen = set()
    node_id_seen = set()
    
    for i, equipment in enumerate(equipment_data):
        toolset_code, guid, node_id, data_code, category, vertices, kind, name, description = equipment
        
        # Validate required fields
        if not toolset_code or len(toolset_code.strip()) == 0:
            print(f"⚠ Skipping row {i+1}: Missing toolset_code")
            invalid_count += 1
            continue
            
        if not guid or len(guid.strip()) == 0:
            print(f"⚠ Skipping row {i+1}: Missing guid")
            invalid_count += 1
            continue
            
        if node_id is None:
            print(f"⚠ Skipping row {i+1}: Missing node_id for guid {guid}")
            invalid_count += 1
            continue
            
        if not name or len(name.strip()) == 0:
            print(f"⚠ Skipping row {i+1}: Missing name for guid {guid}")
            invalid_count += 1
            continue
        
        # Check for duplicates
        guid = guid.strip()
        if guid in guid_seen:
            print(f"⚠ Skipping row {i+1}: Duplicate guid {guid}")
            invalid_count += 1
            continue
        guid_seen.add(guid)
        
        if node_id in node_id_seen:
            print(f"⚠ Skipping row {i+1}: Duplicate node_id {node_id}")
            invalid_count += 1
            continue
        node_id_seen.add(node_id)
        
        # Clean and validate field lengths
        toolset_code = toolset_code.strip()[:64]
        guid = guid[:64]
        category = category.strip()[:64] if category else ''
        name = name.strip()[:128]
        
        if kind:
            kind = kind.strip()[:32]
        
        if description:
            description = description.strip()[:512] if description.strip() else None
        
        # Ensure numeric fields are valid
        try:
            data_code = int(data_code) if data_code is not None else 0
            node_id = int(node_id)
            vertices = int(vertices) if vertices is not None else 0
        except (ValueError, TypeError):
            print(f"⚠ Skipping row {i+1}: Invalid numeric values for guid {guid}")
            invalid_count += 1
            continue
        
        valid_data.append((toolset_code, guid, node_id, data_code, category, vertices, kind, name, description))
    
    if invalid_count > 0:
        print(f"⚠ Excluded {invalid_count} invalid records")
    
    return valid_data


def insert_equipments_batch(db: Database, equipment_data: List[Tuple], batch_size: int = 1000) -> int:
    """
    Insert equipment data in batches for better performance.
    
    Args:
        equipment_data: List of validated equipment tuples
        batch_size: Number of records to insert per batch
    
    Returns:
        Total number of rows inserted
    """
    if not equipment_data:
        print("No equipment data to insert")
        return 0
    
    insert_sql = """
        INSERT INTO tb_equipments (toolset, guid, node_id, data_code, category, vertices, 
                                 kind, name, description, is_active)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    
    total_inserted = 0
    total_batches = (len(equipment_data) + batch_size - 1) // batch_size
    
    for batch_num in range(total_batches):
        start_idx = batch_num * batch_size
        end_idx = min(start_idx + batch_size, len(equipment_data))
        batch = equipment_data[start_idx:end_idx]
        
        batch_inserted = 0
        failed_in_batch = []
        
        for equipment in batch:
            toolset_code, guid, node_id, data_code, category, vertices, kind, name, description = equipment
            
            try:
                params = [toolset_code, guid, node_id, data_code, category, vertices, 
                         kind, name, description, True]
                rows_affected = db.update(insert_sql, params)
                
                if rows_affected > 0:
                    batch_inserted += rows_affected
                else:
                    failed_in_batch.append((guid, "No rows affected"))
                    
            except Exception as e:
                failed_in_batch.append((guid, str(e)))
        
        total_inserted += batch_inserted
        print(f"✓ Batch {batch_num + 1}/{total_batches}: Inserted {batch_inserted}/{len(batch)} equipments")
        
        if failed_in_batch:
            print(f"  ⚠ {len(failed_in_batch)} failures in this batch")
            for guid, error in failed_in_batch[:3]:  # Show first 3 errors
                print(f"    - {guid}: {error}")
            if len(failed_in_batch) > 3:
                print(f"    - ... and {len(failed_in_batch) - 3} more")
    
    return total_inserted


def main():
    """
    Main function to orchestrate the equipment loading process.
    """
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description="Load equipment data into tb_equipments table",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python load_equipments.py     # Interactive mode with confirmation
  python load_equipments.py -y  # Unattended mode, auto-confirm
        """
    )
    parser.add_argument(
        '-y', '--yes', 
        action='store_true',
        help='Auto-confirm without prompting (unattended mode)'
    )
    
    args = parser.parse_args()
    
    print("Starting equipment loading process...")
    
    db = None
    try:
        # Initialize database connection
        print("Connecting to database...")
        db = Database()
        print("✓ Database connection established")
        
        # Load toolset codes for validation
        print("\nLoading valid toolset codes...")
        valid_toolsets = get_existing_toolsets(db)
        
        if not valid_toolsets:
            print("✗ No active toolsets found. Please load toolsets first.")
            return
        
        # Fetch source data
        print("\nFetching equipment source data...")
        raw_data = get_source_data(db)
        
        if not raw_data:
            print("No source data found. Exiting.")
            return
        
        # Transform data
        print("\nTransforming data...")
        transformed_data = transform_equipment_data(raw_data, valid_toolsets)
        
        # Validate data
        print("\nValidating data...")
        valid_data = validate_equipment_data(transformed_data)
        print(f"✓ Validated {len(valid_data)} equipment records")
        
        if not valid_data:
            print("No valid data to process. Exiting.")
            return
        
        # Show summary
        print(f"\nSummary of data to be loaded:")
        print(f"  Total equipments: {len(valid_data)}")
        
        # Group by toolset for summary
        toolset_counts = {}
        category_counts = {}
        for toolset_code, _, _, _, category, _, _, _, _ in valid_data:
            toolset_counts[toolset_code] = toolset_counts.get(toolset_code, 0) + 1
            category_counts[category] = category_counts.get(category, 0) + 1
        
        print(f"  Unique toolsets: {len(toolset_counts)}")
        print(f"  Unique categories: {len(category_counts)}")
        
        # Show top categories
        top_categories = sorted(category_counts.items(), key=lambda x: x[1], reverse=True)[:5]
        print("  Top categories:")
        for category, count in top_categories:
            print(f"    {category}: {count}")
        
        # Show toolset distribution  
        top_toolsets = sorted(toolset_counts.items(), key=lambda x: x[1], reverse=True)[:5]
        print("  Top toolsets:")
        for toolset, count in top_toolsets:
            print(f"    {toolset}: {count}")
        
        # Confirm before proceeding (unless -y flag is used)
        if args.yes:
            print("\nAuto-confirming due to -y flag...")
        else:
            response = input("\nProceed with loading? (y/N): ").strip().lower()
            if response not in ['y', 'yes']:
                print("Loading cancelled by user.")
                return
        
        # Clear existing data
        print("\nClearing existing equipments...")
        clear_existing_equipments(db)
        
        # Insert new data in batches
        print("\nInserting equipments...")
        inserted_count = insert_equipments_batch(db, valid_data)
        
        print(f"\n✓ Successfully loaded {inserted_count} equipments")
        
        # Verify the load
        print("\nVerifying load...")
        verification_sql = """
            SELECT 
                COUNT(*) as total,
                COUNT(DISTINCT toolset) as unique_toolsets,
                COUNT(DISTINCT category) as unique_categories,
                AVG(vertices) as avg_vertices
            FROM tb_equipments 
            WHERE is_active = TRUE
        """
        
        result = db.query(verification_sql)
        if result:
            total, unique_toolsets, unique_categories, avg_vertices = result[0]
            print(f"✓ Verification complete:")
            print(f"  Total active equipments: {total}")
            print(f"  Unique toolsets: {unique_toolsets}")
            print(f"  Unique categories: {unique_categories}")
            print(f"  Average vertices: {avg_vertices:.1f}" if avg_vertices else "  Average vertices: N/A")
        
    except Exception as e:
        print(f"✗ Error during equipment loading: {e}")
        sys.exit(1)
        
    finally:
        if db:
            print("\nClosing database connection...")
            db.close()
            print("✓ Database connection closed")
    
    print("\nEquipment loading process completed successfully!")


if __name__ == "__main__":
    main()
