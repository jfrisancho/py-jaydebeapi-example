#!/usr/bin/env python3
"""
Script to load equipment connections data into tb_equipment_connections table.
Transforms source data from network topology and connection tables.

Usage:
    python load_equipment_connections.py          # Interactive mode with confirmation
    python load_equipment_connections.py -y       # Unattended mode, auto-confirm
"""

import sys
import os
import argparse
from typing import List, Tuple, Optional, Dict, Set

# Add parent directory to path to import db module
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db import Database


def create_parser() -> argparse.ArgumentParser:
    """
    Parse command line arguments
    """
    parser = argparse.ArgumentParser(
        description="Load equipment connections data into tb_equipment_connections table",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python load_equipment_connections.py     # Interactive mode with confirmation
  python load_equipment_connections.py -y  # Unattended mode, auto-confirm
        """
    )
    parser.add_argument(
        '-y', '--yes', 
        action='store_true',
        help='Auto-confirm without prompting (unattended mode)'
    )
    return parser


def fetch_equipment_poc_mapping(db: Database) -> Dict[int, Tuple[int, int]]:
    """
    Get mapping of node_id -> (equipment_id, poc_id) for connection validation.
    
    Returns:
        Dictionary mapping node ID to (equipment_id, poc_id) tuple
    """
    poc_query = """
        SELECT ep.node_id, ep.equipment_id, ep.id as poc_id
        FROM tb_equipment_pocs ep
        JOIN tb_equipments e ON ep.equipment_id = e.id
        WHERE e.is_active = TRUE
          AND ep.is_active = TRUE
    """
    
    try:
        rows = db.query(poc_query)
        mapping = {}
        
        for row in rows:
            node_id, equipment_id, poc_id = row
            mapping[node_id] = (equipment_id, poc_id)
        
        print(f"✓ Loaded {len(mapping)} active equipment POC mappings")
        return mapping
        
    except Exception as e:
        print(f"Error loading equipment POC mappings: {e}")
        raise


def fetch_toolset_mapping(db: Database) -> Dict[str, str]:
    """
    Get mapping of toolset codes for reference.
    
    Returns:
        Dictionary mapping toolset code to phase info
    """
    toolset_query = """
        SELECT code, CONCAT(fab, '-', phase_no) as phase_info
        FROM tb_toolsets 
        WHERE is_active = TRUE
    """
    
    try:
        rows = db.query(toolset_query)
        mapping = {}
        
        for row in rows:
            code, phase_info = row
            mapping[code] = phase_info
        
        print(f"✓ Loaded {len(mapping)} active toolset mappings")
        return mapping
        
    except Exception as e:
        print(f"Error loading toolset mappings: {e}")
        raise


def fetch_source_data(db: Database) -> List[Tuple]:
    """
    Fetch source data for equipment connections from network topology tables.
    This assumes there are source tables with connection information.
    
    Returns:
        List of tuples: (from_node_id, to_node_id, connection_type, is_valid)
    """
    # This query structure assumes source tables exist with connection data
    # Modify the table names and columns based on your actual source schema
    source_query = """
        SELECT DISTINCT
               c.from_node_id,
               c.to_node_id,
               COALESCE(c.connection_type, 'STRAIGHT') as connection_type,
               CASE 
                   WHEN c.is_blocked = 1 OR c.status = 'INACTIVE' THEN 0
                   ELSE 1
               END as is_valid
        FROM connection_table c
        WHERE c.from_node_id IS NOT NULL 
          AND c.to_node_id IS NOT NULL
          AND c.from_node_id != c.to_node_id  -- Exclude self-connections
        
        UNION
        
        -- Alternative source: derive from attachment_table if direct connections aren't available
        SELECT DISTINCT
               at1.eq_poc_no as from_node_id,
               at2.eq_poc_no as to_node_id,
               'DERIVED' as connection_type,
               1 as is_valid
        FROM attachment_table at1
        JOIN attachment_table at2 ON at1.connection_id = at2.connection_id
        WHERE at1.eq_poc_no != at2.eq_poc_no
          AND at1.eq_poc_no IS NOT NULL
          AND at2.eq_poc_no IS NOT NULL
        
        ORDER BY from_node_id, to_node_id
    """
    
    try:
        rows = db.query(source_query)
        print(f"✓ Retrieved {len(rows)} equipment connection records from source")
        return rows
        
    except Exception as e:
        print(f"Error fetching equipment connection source data: {e}")
        print("Note: This may indicate that source connection tables don't exist yet.")
        print("You may need to create sample data or modify the source query.")
        
        # Return empty list if source tables don't exist
        # In a real scenario, you might want to generate connections based on other logic
        return []


def detect_connection_patterns(db: Database, poc_mapping: Dict[int, Tuple[int, int]]) -> List[Tuple]:
    """
    Alternative method to detect connections when direct connection tables aren't available.
    This method analyzes equipment POCs to infer likely connections.
    
    Args:
        poc_mapping: Node ID to (equipment_id, poc_id) mapping
    
    Returns:
        List of inferred connection tuples
    """
    print("Attempting to detect connection patterns from POC data...")
    
    # Get POC data with additional context
    poc_analysis_query = """
        SELECT 
            ep.node_id,
            ep.equipment_id,
            ep.id as poc_id,
            ep.utility_no,
            ep.flow,
            ep.is_used,
            e.toolset,
            e.kind,
            e.category_no
        FROM tb_equipment_pocs ep
        JOIN tb_equipments e ON ep.equipment_id = e.id
        WHERE e.is_active = TRUE 
          AND ep.is_active = TRUE
          AND ep.is_used = TRUE  -- Only consider used POCs
        ORDER BY ep.utility_no, ep.flow, e.toolset
    """
    
    try:
        rows = db.query(poc_analysis_query)
        
        connections = []
        utility_groups = {}
        
        # Group POCs by utility
        for row in rows:
            node_id, equipment_id, poc_id, utility_no, flow, is_used, toolset, kind, category_no = row
            
            if utility_no not in utility_groups:
                utility_groups[utility_no] = {'IN': [], 'OUT': [], 'UNKNOWN': []}
            
            flow_key = flow if flow in ['IN', 'OUT'] else 'UNKNOWN'
            utility_groups[utility_no][flow_key].append({
                'node_id': node_id,
                'equipment_id': equipment_id,
                'poc_id': poc_id,
                'toolset': toolset,
                'kind': kind,
                'category_no': category_no
            })
        
        # Create connections based on utility flow patterns
        for utility_no, flows in utility_groups.items():
            # Connect OUTs to INs within the same utility
            for out_poc in flows['OUT']:
                for in_poc in flows['IN']:
                    # Don't connect POCs from the same equipment
                    if out_poc['equipment_id'] != in_poc['equipment_id']:
                        connections.append((
                            out_poc['node_id'],      # from_node_id
                            in_poc['node_id'],       # to_node_id
                            'STRAIGHT',              # connection_type
                            1                        # is_valid
                        ))
            
            # Handle UNKNOWN flow POCs - connect them in sequence
            unknown_pocs = flows['UNKNOWN']
            for i in range(len(unknown_pocs) - 1):
                from_poc = unknown_pocs[i]
                to_poc = unknown_pocs[i + 1]
                
                if from_poc['equipment_id'] != to_poc['equipment_id']:
                    connections.append((
                        from_poc['node_id'],
                        to_poc['node_id'],
                        'INFERRED',
                        1
                    ))
        
        print(f"✓ Detected {len(connections)} potential connections from POC analysis")
        return connections
        
    except Exception as e:
        print(f"Error in connection pattern detection: {e}")
        return []


def determine_connection_type(from_equipment_id: int, to_equipment_id: int, 
                            from_poc_data: Dict, to_poc_data: Dict) -> str:
    """
    Determine connection type based on equipment and POC characteristics.
    
    Args:
        from_equipment_id: Source equipment ID
        to_equipment_id: Target equipment ID
        from_poc_data: Additional data about source POC
        to_poc_data: Additional data about target POC
    
    Returns:
        Connection type string
    """
    # Check if it's a loopback (same equipment)
    if from_equipment_id == to_equipment_id:
        return 'LOOPBACK'
    
    # Check for branched connections (multiple connections from same POC)
    # This would require additional analysis of the connection patterns
    
    # Default to straight connection
    return 'STRAIGHT'


def transform_connection_data(raw_data: List[Tuple], poc_mapping: Dict[int, Tuple[int, int]]) -> List[Tuple]:
    """
    Transform raw connection data into format for tb_equipment_connections.
    
    Args:
        raw_data: Raw data from source query
        poc_mapping: Mapping of node ID to (equipment_id, poc_id)
    
    Returns:
        List of tuples: (from_equipment_id, to_equipment_id, from_poc_id, to_poc_id, is_valid, connection_type)
    """
    transformed_data = []
    missing_pocs = set()
    invalid_connections = 0
    
    for row in raw_data:
        from_node_id, to_node_id, connection_type, is_valid = row
        
        # Look up equipment and POC IDs
        from_mapping = poc_mapping.get(from_node_id)
        to_mapping = poc_mapping.get(to_node_id)
        
        if not from_mapping:
            missing_pocs.add(from_node_id)
            continue
            
        if not to_mapping:
            missing_pocs.add(to_node_id)
            continue
        
        from_equipment_id, from_poc_id = from_mapping
        to_equipment_id, to_poc_id = to_mapping
        
        # Skip self-connections at equipment level (unless it's a valid loopback)
        if from_equipment_id == to_equipment_id and connection_type != 'LOOPBACK':
            invalid_connections += 1
            continue
        
        # Determine final connection type
        final_connection_type = determine_connection_type(
            from_equipment_id, to_equipment_id, {}, {}
        )
        
        # Override with source connection type if it's more specific
        if connection_type and connection_type != 'DERIVED':
            final_connection_type = connection_type
        
        transformed_data.append((
            from_equipment_id,
            to_equipment_id,
            from_poc_id,
            to_poc_id,
            is_valid,
            final_connection_type
        ))
    
    if missing_pocs:
        print(f"⚠ Warning: {len(missing_pocs)} connections reference missing POCs:")
        for node_id in sorted(list(missing_pocs)[:10]):  # Show first 10
            print(f"  - Node {node_id}")
        if len(missing_pocs) > 10:
            print(f"  - ... and {len(missing_pocs) - 10} more")
        print("  These connections will be skipped.")
    
    if invalid_connections > 0:
        print(f"⚠ Excluded {invalid_connections} invalid self-connections")
    
    return transformed_data


def clear_existing_connections(db: Database) -> int:
    """
    Remove all existing equipment connections from the table.
    
    Returns:
        Number of rows deleted
    """
    delete_sql = "DELETE FROM tb_equipment_connections"
    
    try:
        deleted_count = db.update(delete_sql)
        print(f"Cleared {deleted_count} existing equipment connections")
        return deleted_count
    except Exception as e:
        print(f"Error clearing existing equipment connections: {e}")
        raise


def validate_connection_data(connection_data: List[Tuple]) -> List[Tuple]:
    """
    Validate and clean equipment connection data before insertion.
    
    Args:
        connection_data: Raw connection data
    
    Returns:
        Validated and cleaned connection data
    """
    valid_data = []
    invalid_count = 0
    duplicate_connections = set()
    
    for i, connection in enumerate(connection_data):
        from_equipment_id, to_equipment_id, from_poc_id, to_poc_id, is_valid, connection_type = connection
        
        # Validate required fields
        if not all([from_equipment_id, to_equipment_id, from_poc_id, to_poc_id]):
            print(f"⚠ Skipping row {i+1}: Missing required IDs")
            invalid_count += 1
            continue
        
        # Check for duplicate connections
        connection_key = (from_equipment_id, to_equipment_id, from_poc_id, to_poc_id)
        if connection_key in duplicate_connections:
            print(f"⚠ Skipping row {i+1}: Duplicate connection")
            invalid_count += 1
            continue
        duplicate_connections.add(connection_key)
        
        # Validate and clean fields
        try:
            from_equipment_id = int(from_equipment_id)
            to_equipment_id = int(to_equipment_id)
            from_poc_id = int(from_poc_id)
            to_poc_id = int(to_poc_id)
            is_valid = int(is_valid) if is_valid in [0, 1] else 1
        except (ValueError, TypeError):
            print(f"⚠ Skipping row {i+1}: Invalid numeric values")
            invalid_count += 1
            continue
        
        # Clean connection type
        if connection_type:
            connection_type = connection_type.strip()[:16]
        else:
            connection_type = 'STRAIGHT'
        
        valid_data.append((
            from_equipment_id, to_equipment_id, from_poc_id, to_poc_id, is_valid, connection_type
        ))
    
    if invalid_count > 0:
        print(f"⚠ Excluded {invalid_count} invalid records")
    
    return valid_data


def insert_connections_batch(db: Database, connection_data: List[Tuple], batch_size: int = 1000) -> int:
    """
    Insert equipment connection data in batches for better performance.
    
    Args:
        connection_data: List of validated connection tuples
        batch_size: Number of records to insert per batch
    
    Returns:
        Total number of rows inserted
    """
    if not connection_data:
        print("No equipment connection data to insert")
        return 0
    
    insert_sql = """
        INSERT INTO tb_equipment_connections 
        (from_equipment_id, to_equipment_id, from_poc_id, to_poc_id, is_valid, connection_type)
        VALUES (?, ?, ?, ?, ?, ?)
    """
    
    total_inserted = 0
    total_batches = (len(connection_data) + batch_size - 1) // batch_size
    
    for batch_num in range(total_batches):
        start_idx = batch_num * batch_size
        end_idx = min(start_idx + batch_size, len(connection_data))
        batch = connection_data[start_idx:end_idx]
        
        batch_inserted = 0
        failed_in_batch = []
        
        for connection in batch:
            from_equipment_id, to_equipment_id, from_poc_id, to_poc_id, is_valid, connection_type = connection
            
            try:
                params = [from_equipment_id, to_equipment_id, from_poc_id, to_poc_id, is_valid, connection_type]
                rows_affected = db.update(insert_sql, params)
                
                if rows_affected > 0:
                    batch_inserted += rows_affected
                else:
                    failed_in_batch.append((f"{from_equipment_id}->{to_equipment_id}", "No rows affected"))
                    
            except Exception as e:
                failed_in_batch.append((f"{from_equipment_id}->{to_equipment_id}", str(e)))
        
        total_inserted += batch_inserted
        print(f"✓ Batch {batch_num + 1}/{total_batches}: Inserted {batch_inserted}/{len(batch)} connections")
        
        if failed_in_batch:
            print(f"  ⚠ {len(failed_in_batch)} failures in this batch")
            for connection_desc, error in failed_in_batch[:3]:  # Show first 3 errors
                print(f"    - {connection_desc}: {error}")
            if len(failed_in_batch) > 3:
                print(f"    - ... and {len(failed_in_batch) - 3} more")
    
    return total_inserted


def analyze_connection_topology(db: Database):
    """
    Analyze the loaded connection topology and provide insights.
    """
    print("\nAnalyzing connection topology...")
    
    # Basic statistics
    stats_query = """
        SELECT 
            COUNT(*) as total_connections,
            COUNT(DISTINCT from_equipment_id) as source_equipments,
            COUNT(DISTINCT to_equipment_id) as target_equipments,
            COUNT(DISTINCT CONCAT(from_equipment_id, '-', to_equipment_id)) as unique_equipment_pairs,
            SUM(is_valid) as valid_connections,
            COUNT(*) - SUM(is_valid) as invalid_connections
        FROM tb_equipment_connections
    """
    
    result = db.query(stats_query)
    if result:
        stats = result[0]
        print(f"  Total connections: {stats[0]}")
        print(f"  Source equipments: {stats[1]}")
        print(f"  Target equipments: {stats[2]}")
        print(f"  Unique equipment pairs: {stats[3]}")
        print(f"  Valid connections: {stats[4]}")
        print(f"  Invalid connections: {stats[5]}")
    
    # Connection type distribution
    type_query = """
        SELECT connection_type, COUNT(*) as count
        FROM tb_equipment_connections
        GROUP BY connection_type
        ORDER BY count DESC
    """
    
    type_results = db.query(type_query)
    if type_results:
        print("  Connection type distribution:")
        for connection_type, count in type_results:
            print(f"    {connection_type}: {count}")
    
    # Find potential issues
    print("\nChecking for potential topology issues...")
    
    # Isolated equipments (no connections)
    isolated_query = """
        SELECT e.id, e.guid, e.kind
        FROM tb_equipments e
        WHERE e.is_active = TRUE
          AND e.id NOT IN (
              SELECT DISTINCT from_equipment_id FROM tb_equipment_connections
              UNION
              SELECT DISTINCT to_equipment_id FROM tb_equipment_connections
          )
    """
    
    isolated_results = db.query(isolated_query)
    if isolated_results:
        print(f"  ⚠ Found {len(isolated_results)} isolated equipments (no connections)")
        if len(isolated_results) <= 5:
            for eq_id, guid, kind in isolated_results:
                print(f"    - Equipment {eq_id} ({guid}): {kind}")
    else:
        print("  ✓ No isolated equipments found")


def main():
    """
    Main function to orchestrate the equipment connections loading process.
    """
    parser = create_parser()    
    args = parser.parse_args()
    
    print("Starting equipment connections loading process...")
    
    db = None
    try:
        # Initialize database connection
        print("Connecting to database...")
        db = Database()
        print("✓ Database connection established")
        
        # Load equipment POC mappings first
        print("\nLoading equipment POC mappings...")
        poc_mapping = fetch_equipment_poc_mapping(db)
        
        if not poc_mapping:
            print("✗ No active equipment POCs found. Please load equipment POCs first.")
            return
        
        # Load toolset mappings for reference
        print("\nLoading toolset mappings...")
        toolset_mapping = fetch_toolset_mapping(db)
        
        # Fetch source data
        print("\nFetching equipment connection source data...")
        raw_data = fetch_source_data(db)
        
        # If no direct source data, try to detect patterns
        if not raw_data:
            print("No direct connection data found. Attempting pattern detection...")
            raw_data = detect_connection_patterns(db, poc_mapping)
        
        if not raw_data:
            print("No connection data found or could be inferred. Exiting.")
            return
        
        # Transform data
        print("\nTransforming data...")
        transformed_data = transform_connection_data(raw_data, poc_mapping)
        
        # Validate data
        print("\nValidating data...")
        valid_data = validate_connection_data(transformed_data)
        print(f"✓ Validated {len(valid_data)} equipment connection records")
        
        if not valid_data:
            print("No valid data to process. Exiting.")
            return
        
        # Show summary
        print(f"\nSummary of data to be loaded:")
        print(f"  Total equipment connections: {len(valid_data)}")
        
        # Analyze the data
        valid_count = sum(1 for _, _, _, _, is_valid, _ in valid_data if is_valid)
        invalid_count = len(valid_data) - valid_count
        
        connection_type_counts = {}
        equipment_pairs = set()
        
        for from_eq, to_eq, _, _, is_valid, conn_type in valid_data:
            # Count connection types
            connection_type_counts[conn_type] = connection_type_counts.get(conn_type, 0) + 1
            
            # Count unique equipment pairs
            equipment_pairs.add((from_eq, to_eq))
        
        print(f"  Valid connections: {valid_count}")
        print(f"  Invalid connections: {invalid_count}")
        print(f"  Unique equipment pairs: {len(equipment_pairs)}")
        
        # Show connection type distribution
        print("  Connection type distribution:")
        for conn_type, count in sorted(connection_type_counts.items()):
            print(f"    {conn_type}: {count}")
        
        # Confirm before proceeding (unless -y flag is used)
        if args.yes:
            print("\nAuto-confirming due to -y flag...")
        else:
            response = input("\nProceed with loading? (y/N): ").strip().lower()
            if response not in ['y', 'yes']:
                print("Loading cancelled by user.")
                return
        
        # Clear existing data
        print("\nClearing existing equipment connections...")
        clear_existing_connections(db)
        
        # Insert new data in batches
        print("\nInserting equipment connections...")
        inserted_count = insert_connections_batch(db, valid_data)
        
        print(f"\n✓ Successfully loaded {inserted_count} equipment connections")
        
        # Verify the load
        print("\nVerifying load...")
        verification_sql = """
            SELECT 
                COUNT(*) as total,
                COUNT(DISTINCT from_equipment_id) as unique_from_equipments,
                COUNT(DISTINCT to_equipment_id) as unique_to_equipments,
                SUM(is_valid) as valid_connections,
                COUNT(*) - SUM(is_valid) as invalid_connections,
                COUNT(DISTINCT connection_type) as unique_connection_types
            FROM tb_equipment_connections
        """
        
        result = db.query(verification_sql)
        if result:
            total, unique_from, unique_to, valid_conns, invalid_conns, unique_types = result[0]
            print(f"✓ Verification complete:")
            print(f"  Total connections: {total}")
            print(f"  Unique source equipments: {unique_from}")
            print(f"  Unique target equipments: {unique_to}")
            print(f"  Valid connections: {valid_conns}")
            print(f"  Invalid connections: {invalid_conns}")
            print(f"  Connection types: {unique_types}")
        
        # Analyze topology
        analyze_connection_topology(db)
        
    except Exception as e:
        print(f"✗ Error during equipment connections loading: {e}")
        sys.exit(1)
        
    finally:
        if db:
            print("\nClosing database connection...")
            db.close()
            print("✓ Database connection closed")
    
    print("\nEquipment connections loading process completed successfully!")


if __name__ == "__main__":
    main()