#!/usr/bin/env python3
"""
Script to load equipment connections data into tb_equipment_connections table.
Uses NetworkPathFinder.find_downstream_paths_dijkstra to discover connections between equipment POCs.

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
from network_path_finder import NetworkPathFinder


def create_parser() -> argparse.ArgumentParser:
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description='Load equipment connections into tb_equipment_connections table',
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
    parser.add_argument(
        '--max-equipments', 
        type=int,
        default=100,
        help='Maximum number of equipments to process (default: 100)'
    )
    return parser


def fetch_equipment_data(db: Database) -> List[Tuple]:
    """
    Fetch active equipment data with their POCs.
    
    Returns:
        List of tuples: (equipment_id, equipment_guid, toolset, node_id)
    """
    equipment_query = """
        SELECT e.id, e.guid, e.toolset, e.node_id
        FROM tb_equipments e
        WHERE e.is_active = 1
        ORDER BY e.id
    """
    
    try:
        rows = db.query(equipment_query)
        print(f'✓ Retrieved {len(rows)} active equipments')
        return rows
        
    except Exception as e:
        print(f'Error fetching equipment data: {e}')
        raise


def fetch_equipment_pocs(db: Database) -> Dict[int, List[Tuple]]:
    """
    Fetch equipment POCs grouped by equipment_id.
    
    Returns:
        Dictionary mapping equipment_id to list of POC tuples:
        (poc_id, node_id, reference, utility_no, flow, is_used)
    """
    pocs_query = """
        SELECT ep.id, ep.equipment_id, ep.node_id, ep.reference, 
               ep.utility_no, ep.flow, ep.is_used
        FROM tb_equipment_pocs ep
        WHERE ep.is_active = 1
        ORDER BY ep.equipment_id, ep.id
    """
    
    try:
        rows = db.query(pocs_query)
        print(f'✓ Retrieved {len(rows)} active equipment POCs')
        
        pocs_by_equipment = {}
        for row in rows:
            poc_id, equipment_id, node_id, reference, utility_no, flow, is_used = row
            
            if equipment_id not in pocs_by_equipment:
                pocs_by_equipment[equipment_id] = []
                
            pocs_by_equipment[equipment_id].append((
                poc_id, node_id, reference, utility_no, flow, is_used
            ))
        
        print(f'✓ Grouped POCs for {len(pocs_by_equipment)} equipments')
        return pocs_by_equipment
        
    except Exception as e:
        print(f'Error fetching equipment POCs: {e}')
        raise


def find_equipment_connections(equipment_data: List[Tuple], 
                             pocs_data: Dict[int, List[Tuple]],
                             max_equipments: int) -> List[Tuple]:
    """
    Find connections between equipment POCs using NetworkPathFinder.
    
    Args:
        equipment_data: List of equipment tuples
        pocs_data: Dictionary of POCs by equipment
        max_equipments: Maximum number of equipments to process
    
    Returns:
        List of connection tuples: (from_equipment_id, to_equipment_id, 
                                   from_poc_id, to_poc_id, is_valid, connection_type)
    """
    connections = []
    processed_count = 0
    
    print(f'Starting path finding for up to {max_equipments} equipments...')
    
    for equipment_id, equipment_guid, toolset, equipment_node_id in equipment_data:
        if processed_count >= max_equipments:
            print(f'Reached maximum equipment limit ({max_equipments})')
            break
            
        # Get POCs for this equipment
        equipment_pocs = pocs_data.get(equipment_id, [])
        if not equipment_pocs:
            print(f'  Equipment {equipment_id}: No POCs found, skipping')
            continue
        
        # Filter for used output POCs as potential starting points
        output_pocs = [poc for poc in equipment_pocs 
                      if poc[5] and poc[4] == 'OUT']  # is_used and flow == 'OUT'
        
        if not output_pocs:
            print(f'  Equipment {equipment_id}: No output POCs found, skipping')
            continue
        
        print(f'  Equipment {equipment_id} ({equipment_guid}): Processing {len(output_pocs)} output POCs')
        
        # Process each output POC
        for poc_id, node_id, reference, utility_no, flow, is_used in output_pocs:
            try:
                # Initialize NetworkPathFinder for this starting node
                path_finder = NetworkPathFinder(start_node_id=node_id)
                
                # Load network data (assuming this method exists)
                if hasattr(path_finder, 'load_network_data'):
                    path_finder.load_network_data()
                
                # Find downstream paths using Dijkstra
                paths = path_finder.find_downstream_paths_dijkstra()
                
                if not paths:
                    continue
                
                # Process each path to find equipment connections
                for path in paths:
                    endpoint_node = path.endpoint_node_id if hasattr(path, 'endpoint_node_id') else None
                    
                    if not endpoint_node:
                        continue
                    
                    # Find which equipment POC this endpoint belongs to
                    target_equipment_id, target_poc_id = find_poc_by_node_id(
                        endpoint_node, pocs_data
                    )
                    
                    if target_equipment_id and target_poc_id:
                        # Skip self-connections
                        if target_equipment_id == equipment_id:
                            continue
                        
                        # Determine connection validity and type
                        is_valid = determine_connection_validity(
                            equipment_id, target_equipment_id, 
                            poc_id, target_poc_id, path
                        )
                        
                        connection_type = determine_connection_type(path)
                        
                        connections.append((
                            equipment_id,      # from_equipment_id
                            target_equipment_id,  # to_equipment_id
                            poc_id,           # from_poc_id
                            target_poc_id,    # to_poc_id
                            is_valid,         # is_valid
                            connection_type   # connection_type
                        ))
                        
                        print(f'    Found connection: POC {poc_id} -> Equipment {target_equipment_id} POC {target_poc_id}')
                
            except Exception as e:
                print(f'    Error processing POC {poc_id}: {e}')
                continue
        
        processed_count += 1
        
        if processed_count % 10 == 0:
            print(f'  Processed {processed_count}/{min(len(equipment_data), max_equipments)} equipments')
    
    print(f'✓ Found {len(connections)} total connections')
    return connections


def find_poc_by_node_id(node_id: int, pocs_data: Dict[int, List[Tuple]]) -> Tuple[Optional[int], Optional[int]]:
    """
    Find which equipment and POC a node_id belongs to.
    
    Args:
        node_id: Network node ID to search for
        pocs_data: Dictionary of POCs by equipment
    
    Returns:
        Tuple of (equipment_id, poc_id) or (None, None) if not found
    """
    for equipment_id, pocs in pocs_data.items():
        for poc_id, poc_node_id, reference, utility_no, flow, is_used in pocs:
            if poc_node_id == node_id:
                return equipment_id, poc_id
    
    return None, None


def determine_connection_validity(from_equipment_id: int, to_equipment_id: int,
                                from_poc_id: int, to_poc_id: int, path) -> bool:
    """
    Determine if a connection is valid based on path properties.
    
    Args:
        from_equipment_id: Source equipment ID
        to_equipment_id: Target equipment ID
        from_poc_id: Source POC ID
        to_poc_id: Target POC ID
        path: Path object from Dijkstra algorithm
    
    Returns:
        True if connection is valid, False otherwise
    """
    # Basic validation - connections between different equipments are generally valid
    if from_equipment_id != to_equipment_id:
        # Additional validation based on path properties could be added here
        # For now, assume valid if path exists
        return True
    
    return False


def determine_connection_type(path) -> str:
    """
    Determine connection type based on path characteristics.
    
    Args:
        path: Path object from Dijkstra algorithm
    
    Returns:
        Connection type string (STRAIGHT, BRANCHED, etc.)
    """
    # Default to STRAIGHT connection
    # More sophisticated logic could be added based on path properties
    if hasattr(path, 'links') and len(path.links) == 1:
        return 'STRAIGHT'
    elif hasattr(path, 'links') and len(path.links) > 1:
        return 'BRANCHED'
    else:
        return 'STRAIGHT'


def clear_existing_connections(db: Database) -> int:
    """
    Remove all existing equipment connections from the table.
    
    Returns:
        Number of rows deleted
    """
    delete_sql = 'DELETE FROM tb_equipment_connections'
    
    try:
        deleted_count = db.update(delete_sql)
        print(f'Cleared {deleted_count} existing equipment connections')
        return deleted_count
    except Exception as e:
        print(f'Error clearing existing equipment connections: {e}')
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
    seen_connections = set()
    
    for i, connection in enumerate(connection_data):
        from_equipment_id, to_equipment_id, from_poc_id, to_poc_id, is_valid, connection_type = connection
        
        # Validate required fields
        if not all([from_equipment_id, to_equipment_id, from_poc_id, to_poc_id]):
            print(f'⚠ Skipping row {i+1}: Missing required fields')
            invalid_count += 1
            continue
        
        # Check for duplicate connections
        connection_key = (from_equipment_id, to_equipment_id, from_poc_id, to_poc_id)
        if connection_key in seen_connections:
            print(f'⚠ Skipping row {i+1}: Duplicate connection')
            invalid_count += 1
            continue
        seen_connections.add(connection_key)
        
        # Validate field types and ranges
        try:
            from_equipment_id = int(from_equipment_id)
            to_equipment_id = int(to_equipment_id)
            from_poc_id = int(from_poc_id)
            to_poc_id = int(to_poc_id)
            is_valid = bool(is_valid)
        except (ValueError, TypeError):
            print(f'⚠ Skipping row {i+1}: Invalid data types')
            invalid_count += 1
            continue
        
        # Clean connection_type
        if connection_type:
            connection_type = str(connection_type).strip()[:16]
        else:
            connection_type = 'STRAIGHT'
        
        valid_data.append((
            from_equipment_id, to_equipment_id, from_poc_id, 
            to_poc_id, is_valid, connection_type
        ))
    
    if invalid_count > 0:
        print(f'⚠ Excluded {invalid_count} invalid records')
    
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
        print('No equipment connection data to insert')
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
                params = [from_equipment_id, to_equipment_id, from_poc_id, 
                         to_poc_id, is_valid, connection_type]
                rows_affected = db.update(insert_sql, params)
                
                if rows_affected > 0:
                    batch_inserted += rows_affected
                else:
                    failed_in_batch.append((from_equipment_id, to_equipment_id, 'No rows affected'))
                    
            except Exception as e:
                failed_in_batch.append((from_equipment_id, to_equipment_id, str(e)))
        
        total_inserted += batch_inserted
        print(f'✓ Batch {batch_num + 1}/{total_batches}: Inserted {batch_inserted}/{len(batch)} connections')
        
        if failed_in_batch:
            print(f'  ⚠ {len(failed_in_batch)} failures in this batch')
            for from_eq, to_eq, error in failed_in_batch[:3]:  # Show first 3 errors
                print(f'    - Equipment {from_eq} -> {to_eq}: {error}')
            if len(failed_in_batch) > 3:
                print(f'    - ... and {len(failed_in_batch) - 3} more')
    
    return total_inserted


def main():
    """
    Main function to orchestrate the equipment connections loading process.
    """
    parser = create_parser()
    args = parser.parse_args()
    
    print('Starting equipment connections loading process...')
    
    db = None
    try:
        # Initialize database connection
        print('Connecting to database...')
        db = Database()
        print('✓ Database connection established')
        
        # Fetch equipment data
        print('\nFetching equipment data...')
        equipment_data = fetch_equipment_data(db)
        
        if not equipment_data:
            print('No active equipments found. Exiting.')
            return
        
        # Fetch equipment POCs
        print('\nFetching equipment POCs...')
        pocs_data = fetch_equipment_pocs(db)
        
        if not pocs_data:
            print('No active equipment POCs found. Exiting.')
            return
        
        # Find connections using NetworkPathFinder
        print('\nFinding equipment connections...')
        connection_data = find_equipment_connections(
            equipment_data, pocs_data, args.max_equipments
        )
        
        if not connection_data:
            print('No connections found. Exiting.')
            return
        
        # Validate data
        print('\nValidating connection data...')
        valid_data = validate_connection_data(connection_data)
        print(f'✓ Validated {len(valid_data)} equipment connections')
        
        if not valid_data:
            print('No valid data to process. Exiting.')
            return
        
        # Show summary
        print(f'\nSummary of data to be loaded:')
        print(f'  Total connections: {len(valid_data)}')
        
        # Analyze the data
        valid_connections = sum(1 for _, _, _, _, is_valid, _ in valid_data if is_valid)
        invalid_connections = len(valid_data) - valid_connections
        
        connection_type_counts = {}
        equipment_pairs = set()
        
        for from_eq, to_eq, _, _, is_valid, conn_type in valid_data:
            equipment_pairs.add((from_eq, to_eq))
            if conn_type:
                connection_type_counts[conn_type] = connection_type_counts.get(conn_type, 0) + 1
        
        print(f'  Valid connections: {valid_connections}')
        print(f'  Invalid connections: {invalid_connections}')
        print(f'  Unique equipment pairs: {len(equipment_pairs)}')
        
        # Show connection type distribution
        if connection_type_counts:
            print('  Connection type distribution:')
            for conn_type, count in sorted(connection_type_counts.items()):
                print(f'    {conn_type}: {count}')
        
        # Confirm before proceeding (unless -y flag is used)
        if args.yes:
            print('\nAuto-confirming due to -y flag...')
        else:
            response = input('\nProceed with loading? (y/N): ').strip().lower()
            if response not in ['y', 'yes']:
                print('Loading cancelled by user.')
                return
        
        # Clear existing data
        print('\nClearing existing equipment connections...')
        clear_existing_connections(db)
        
        # Insert new data in batches
        print('\nInserting equipment connections...')
        inserted_count = insert_connections_batch(db, valid_data)
        
        print(f'\n✓ Successfully loaded {inserted_count} equipment connections')
        
        # Verify the load
        print('\nVerifying load...')
        verification_sql = """
            SELECT 
                COUNT(*) as total,
                COUNT(DISTINCT from_equipment_id) as unique_from_equipments,
                COUNT(DISTINCT to_equipment_id) as unique_to_equipments,
                SUM(is_valid) as valid_connections,
                COUNT(*) - SUM(is_valid) as invalid_connections
            FROM tb_equipment_connections
        """
        
        result = db.query(verification_sql)
        if result:
            total, unique_from, unique_to, valid_conns, invalid_conns = result[0]
            print(f'✓ Verification complete:')
            print(f'  Total connections: {total}')
            print(f'  Unique source equipments: {unique_from}')
            print(f'  Unique target equipments: {unique_to}')
            print(f'  Valid connections: {valid_conns}')
            print(f'  Invalid connections: {invalid_conns}')
        
    except Exception as e:
        print(f'✗ Error during equipment connections loading: {e}')
        sys.exit(1)
        
    finally:
        if db:
            print('\nClosing database connection...')
            db.close()
            print('✓ Database connection closed')
    
    print('\nEquipment connections loading process completed successfully!')


if __name__ == '__main__':
    main()