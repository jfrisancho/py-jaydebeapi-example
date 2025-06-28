#!/usr/bin/env python3

"""
Script to load equipment connections data into tb_equipment_connections table.

Analyzes equipment connectivity using spatial downstream analysis from each POC.
Determines which equipment connects to which equipment through which POCs.

Usage:
    python load_equipment_connections.py          # Interactive mode with confirmation
    python load_equipment_connections.py -y       # Unattended mode, auto-confirm
"""

import sys
import os
import argparse
from typing import List, Tuple, Optional, Dict, Set
from collections import defaultdict

from db import Database

def create_connections_table(db: Database) -> bool:
    """
    Create the equipment connections table if it doesn't exist.
    
    Returns:
        True if table created/exists, False on error
    """
    create_sql = '''
        CREATE TABLE IF NOT EXISTS tb_equipment_connections (
            id INTEGER AUTO_INCREMENT PRIMARY KEY,
            
            -- Source equipment and POC
            source_equipment_id INTEGER REFERENCES tb_equipments(id) NOT NULL,
            source_poc_id INTEGER REFERENCES tb_equipment_pocs(id) NOT NULL,
            source_poc_code VARCHAR(8) NOT NULL,
            
            -- Target equipment and POC  
            target_equipment_id INTEGER REFERENCES tb_equipments(id) NOT NULL,
            target_poc_id INTEGER REFERENCES tb_equipment_pocs(id) NOT NULL,
            target_poc_code VARCHAR(8) NOT NULL,
            
            -- Connection metadata
            path_id INTEGER NOT NULL,           -- Reference to spatial path
            link_count INTEGER NOT NULL,        -- Number of links in path
            utility VARCHAR(128),               -- Utility type (N2, CDA, PW, etc.)
            flow_direction VARCHAR(8),          -- IN, OUT
            
            -- Path analysis
            has_intermediate_equipment BIT(1) NOT NULL DEFAULT 0,  -- Path passes through other equipment
            intermediate_count INTEGER DEFAULT 0,                  -- Number of intermediate equipment
            
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            
            -- Constraints
            UNIQUE KEY uk_equipment_connections (source_equipment_id, source_poc_id, target_equipment_id, target_poc_id),
            INDEX idx_connections_source_eq (source_equipment_id),
            INDEX idx_connections_target_eq (target_equipment_id),
            INDEX idx_connections_utility (utility),
            INDEX idx_connections_path (path_id)
        )
    '''
    
    try:
        db.update(create_sql)
        print('✓ Equipment connections table ready')
        return True
    except Exception as e:
        print(f'Error creating connections table: {e}')
        return False

def get_active_equipment_pocs(db: Database) -> List[Tuple]:
    """
    Get all active equipment POCs for analysis.
    
    Returns:
        List of tuples: (poc_id, equipment_id, node_id, code, utility, flow, equipment_guid)
    """
    query = '''
        SELECT p.id, p.equipment_id, p.node_id, p.code, p.utility, p.flow,
               e.guid as equipment_guid
        FROM tb_equipment_pocs p
        INNER JOIN tb_equipments e ON p.equipment_id = e.id
        WHERE e.is_active = 1 AND p.is_used = 1
        ORDER BY e.id, p.code
    '''
    
    try:
        rows = db.query(query)
        print(f'✓ Retrieved {len(rows)} active equipment POCs for analysis')
        return rows
    except Exception as e:
        print(f'Error fetching equipment POCs: {e}')
        raise

def get_equipment_logical_nodes(db: Database) -> Dict[int, int]:
    """
    Get mapping of equipment_id -> logical_node_id for equipment.
    
    Returns:
        Dictionary mapping equipment ID to its logical node ID
    """
    query = '''
        SELECT id, node_id
        FROM tb_equipments
        WHERE is_active = 1
    '''
    
    try:
        rows = db.query(query)
        mapping = {equipment_id: node_id for equipment_id, node_id in rows}
        print(f'✓ Loaded {len(mapping)} equipment logical node mappings')
        return mapping
    except Exception as e:
        print(f'Error loading equipment logical nodes: {e}')
        raise

def analyze_downstream_paths(db: Database, source_node_id: int, ignore_node_id: int = 0, 
                           utility_no: int = 0, toolset_id: int = 0, eq_poc_no: str = '', 
                           data_codes: str = '15000') -> List[int]:
    """
    Find downstream paths using the NetworkPathFinder.
    
    Args:
        source_node_id: Starting node ID
        ignore_node_id: Node to ignore (0 = none)
        utility_no: Utility number filter (0 = all)
        toolset_id: Toolset filter (0 = all)
        eq_poc_no: POC number filter ('' = all)
        data_codes: Data codes filter ('15000' for equipment)
    
    Returns:
        List of path IDs if successful, empty list if no paths found
    """
    try:
        from network_pathfinder import find_network_downstream
        
        path_ids = find_network_downstream(
            db=db,
            start_node_id=source_node_id,
            ignore_node_id=ignore_node_id,
            utility_no=utility_no,
            toolset_id=toolset_id,
            eq_poc_no=eq_poc_no,
            data_codes=data_codes
        )
        
        return path_ids
        
    except Exception as e:
        print(f'Error in downstream analysis for node {source_node_id}: {e}')
        return []

def get_path_links(db: Database, path_id: int) -> List[Tuple]:
    """
    Get the links for a given path ID.
    
    Args:
        path_id: Path ID to analyze
    
    Returns:
        List of tuples: (link_id, start_node_id, end_node_id, ...)
    """
    query = '''
        SELECT link_id, start_node_id, end_node_id
        FROM nw_path_links
        WHERE path_id = ?
        ORDER BY link_order
    '''
    
    try:
        return db.query(query, [path_id])
    except Exception as e:
        print(f'Error fetching path links for path {path_id}: {e}')
        return []

def find_equipment_in_path(db: Database, path_links: List[Tuple], 
                          source_equipment_id: int) -> List[Tuple]:
    """
    Find equipment nodes in the path links.
    
    Args:
        path_links: List of path links
        source_equipment_id: Source equipment ID to exclude
    
    Returns:
        List of tuples: (equipment_id, node_id, equipment_guid)
    """
    if not path_links:
        return []
    
    # Get all node IDs from the path
    all_nodes = set()
    for _, start_node, end_node in path_links:
        all_nodes.add(start_node)
        all_nodes.add(end_node)
    
    if not all_nodes:
        return []
    
    # Convert to list for SQL IN clause
    node_list = list(all_nodes)
    placeholders = ','.join(['?' for _ in node_list])
    
    query = f'''
        SELECT DISTINCT e.id, e.node_id, e.guid
        FROM tb_equipments e
        WHERE e.node_id IN ({placeholders})
          AND e.is_active = 1
          AND e.id != ?
    '''
    
    params = node_list + [source_equipment_id]
    
    try:
        return db.query(query, params)
    except Exception as e:
        print(f'Error finding equipment in path: {e}')
        return []

def find_target_pocs(db: Database, target_equipment_id: int, path_links: List[Tuple]) -> List[Tuple]:
    """
    Find POCs of target equipment that are part of the path.
    
    Args:
        target_equipment_id: Target equipment ID
        path_links: List of path links
    
    Returns:
        List of tuples: (poc_id, node_id, code, utility, flow)
    """
    if not path_links:
        return []
    
    # Get all node IDs from the path
    all_nodes = set()
    for _, start_node, end_node in path_links:
        all_nodes.add(start_node)
        all_nodes.add(end_node)
    
    if not all_nodes:
        return []
    
    node_list = list(all_nodes)
    placeholders = ','.join(['?' for _ in node_list])
    
    query = f'''
        SELECT p.id, p.node_id, p.code, p.utility, p.flow
        FROM tb_equipment_pocs p
        WHERE p.equipment_id = ?
          AND p.node_id IN ({placeholders})
          AND p.is_used = 1
    '''
    
    params = [target_equipment_id] + node_list
    
    try:
        return db.query(query, params)
    except Exception as e:
        print(f'Error finding target POCs: {e}')
        return []

def analyze_poc_connections(db: Database, equipment_pocs: List[Tuple], 
                          equipment_logical_nodes: Dict[int, int]) -> List[Tuple]:
    """
    Analyze connections from each equipment POC using the NetworkPathFinder.
    
    Args:
        equipment_pocs: List of equipment POCs to analyze
        equipment_logical_nodes: Mapping of equipment ID to logical node
    
    Returns:
        List of connection tuples for insertion
    """
    connections = []
    processed_count = 0
    
    print(f'Analyzing connections for {len(equipment_pocs)} POCs...')
    
    for poc_data in equipment_pocs:
        poc_id, equipment_id, node_id, code, utility, flow, equipment_guid = poc_data
        processed_count += 1
        
        if processed_count % 50 == 0:
            print(f'  Processed {processed_count}/{len(equipment_pocs)} POCs...')
        
        # Get the logical node for this equipment (to use as ignore_node_id)
        ignore_node_id = equipment_logical_nodes.get(equipment_id, 0)
        
        # Convert utility to utility_no (this might need mapping logic)
        utility_no = 0  # For now, analyze all utilities - could be enhanced
        
        # Analyze downstream from this POC
        path_ids = analyze_downstream_paths(
            db, 
            source_node_id=node_id,
            ignore_node_id=ignore_node_id,
            utility_no=utility_no,
            toolset_id=0,  # All toolsets for now
            eq_poc_no='',  # All POCs for now
            data_codes='15000'  # Equipment data code - could be enhanced to include other target types
        )
        
        if not path_ids:
            continue
        
        # Process each path found
        for path_id in path_ids:
            # Get the path links for this path
            path_links = get_path_links(db, path_id)
            if not path_links:
                continue
            
            # Find equipment in the path
            target_equipment = find_equipment_in_path(db, path_links, equipment_id)
            
            for target_eq_id, target_node_id, target_guid in target_equipment:
                # Find target POCs that are part of this path
                target_pocs = find_target_pocs(db, target_eq_id, path_links)
                
                for target_poc_id, target_poc_node_id, target_poc_code, target_utility, target_flow in target_pocs:
                    # Check for intermediate equipment
                    intermediate_equipment = [eq for eq in target_equipment if eq[0] not in [equipment_id, target_eq_id]]
                    has_intermediate = 1 if intermediate_equipment else 0
                    intermediate_count = len(intermediate_equipment)
                    
                    # Determine connection type
                    connection_type = 'STRAIGHT'
                    if has_intermediate:
                        connection_type = 'INTERMEDIATE'
                    elif len(path_links) > 3:  # Arbitrary threshold for 'BRANCHED'
                        connection_type = 'BRANCHED'
                    
                    # Create connection record using the new table structure
                    connection = (
                        equipment_id,          # from_equipment_id
                        target_eq_id,         # to_equipment_id
                        poc_id,               # from_poc_id
                        target_poc_id,        # to_poc_id
                        path_id,              # path_id
                        1,                    # is_valid (default to valid)
                        None,                 # path_length_mm (could be calculated)
                        len(path_links),      # link_count
                        len(set(link[1] for link in path_links)) + len(set(link[2] for link in path_links)),  # node_count (approximate)
                        connection_type       # connection_type
                    )
                    
                    connections.append(connection)
    
    print(f'✓ Analysis complete. Found {len(connections)} connections')
    return connections

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
        print(f'Error clearing existing connections: {e}')
        raise

def insert_connections_batch(db: Database, connections: List[Tuple], batch_size: int = 500) -> int:
    """
    Insert equipment connections in batches for better performance.
    
    Args:
        connections: List of connection tuples
        batch_size: Number of records to insert per batch
    
    Returns:
        Total number of rows inserted
    """
    if not connections:
        print('No connections to insert')
        return 0
    
    insert_sql = '''
        INSERT INTO tb_equipment_connections (
            from_equipment_id, to_equipment_id, from_poc_id, to_poc_id,
            path_id, is_valid, path_length_mm, link_count, node_count, connection_type
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    '''
    
    total_inserted = 0
    total_batches = (len(connections) + batch_size - 1) // batch_size
    
    for batch_num in range(total_batches):
        start_idx = batch_num * batch_size
        end_idx = min(start_idx + batch_size, len(connections))
        batch = connections[start_idx:end_idx]
        
        batch_inserted = 0
        failed_in_batch = []
        
        for connection in batch:
            try:
                rows_affected = db.update(insert_sql, list(connection))
                if rows_affected > 0:
                    batch_inserted += rows_affected
                else:
                    failed_in_batch.append(('No rows affected', connection[0], connection[1]))
            except Exception as e:
                failed_in_batch.append((str(e), connection[0], connection[1]))
        
        total_inserted += batch_inserted
        print(f'✓ Batch {batch_num + 1}/{total_batches}: Inserted {batch_inserted}/{len(batch)} connections')
        
        if failed_in_batch:
            print(f'  ⚠ {len(failed_in_batch)} failures in this batch')
            for error, from_eq, to_eq in failed_in_batch[:3]:
                print(f'    - Equipment {from_eq} -> {to_eq}: {error}')
            if len(failed_in_batch) > 3:
                print(f'    - ... and {len(failed_in_batch) - 3} more')
    
    return total_inserted

def verify_connections(db: Database) -> bool:
    """
    Comprehensive verification of the loaded equipment connections.
    
    Returns:
        True if all verifications pass, False otherwise
    """
    print('\nPerforming comprehensive connections verification...')
    verification_passed = True
    
    # Basic statistics
    print('\n1. Basic Statistics:')
    basic_stats_sql = '''
        SELECT 
            COUNT(*) as total_connections,
            COUNT(DISTINCT from_equipment_id) as from_equipment_count,
            COUNT(DISTINCT to_equipment_id) as to_equipment_count,
            COUNT(DISTINCT CONCAT(from_equipment_id, '-', to_equipment_id)) as unique_equipment_pairs,
            AVG(link_count) as avg_link_count,
            AVG(node_count) as avg_node_count,
            SUM(CASE WHEN is_valid = 1 THEN 1 ELSE 0 END) as valid_connections
        FROM tb_equipment_connections
    '''
    
    result = db.query(basic_stats_sql)
    if result:
        (total_connections, from_count, to_count, unique_pairs, 
         avg_links, avg_nodes, valid_connections) = result[0]
        print(f'   Total connections: {total_connections}')
        print(f'   From equipment count: {from_count}')
        print(f'   To equipment count: {to_count}')
        print(f'   Unique equipment pairs: {unique_pairs}')
        print(f'   Average link count: {avg_links:.2f}' if avg_links else '   Average link count: 0')
        print(f'   Average node count: {avg_nodes:.2f}' if avg_nodes else '   Average node count: 0')
        print(f'   Valid connections: {valid_connections}')
    
    # Connection type distribution
    print('\n2. Connection Type Distribution:')
    type_sql = '''
        SELECT 
            COALESCE(connection_type, 'NULL') as conn_type,
            COUNT(*) as connection_count,
            ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 2) as percentage
        FROM tb_equipment_connections
        GROUP BY connection_type
        ORDER BY COUNT(*) DESC
    '''
    
    type_result = db.query(type_sql)
    if type_result:
        for conn_type, count, percentage in type_result:
            print(f'   {conn_type}: {count} connections ({percentage}%)')
    
    # Equipment with most connections
    print('\n3. Equipment with Most Connections (Top 10):')
    top_equipment_sql = '''
        SELECT 
            e.guid,
            e.name,
            COUNT(*) as total_connections,
            SUM(CASE WHEN c.from_equipment_id = e.id THEN 1 ELSE 0 END) as outgoing,
            SUM(CASE WHEN c.to_equipment_id = e.id THEN 1 ELSE 0 END) as incoming
        FROM tb_equipment_connections c
        INNER JOIN tb_equipments e ON (c.from_equipment_id = e.id OR c.to_equipment_id = e.id)
        GROUP BY e.id, e.guid, e.name
        ORDER BY COUNT(*) DESC
        LIMIT 10
    '''
    
    top_result = db.query(top_equipment_sql)
    if top_result:
        for guid, name, total, outgoing, incoming in top_result:
            name_display = name[:30] + '...' if name and len(name) > 30 else name or 'N/A'
            print(f'   {guid[:12]}... ({name_display}): {total} total ({outgoing} out, {incoming} in)')
    
    # Data integrity checks
    print('\n4. Data Integrity Checks:')
    
    # Check for self-connections
    self_connections_sql = '''
        SELECT COUNT(*) 
        FROM tb_equipment_connections 
        WHERE from_equipment_id = to_equipment_id
    '''
    
    self_count = db.query(self_connections_sql)[0][0]
    if self_count > 0:
        print(f'   ⚠ Found {self_count} self-connections (equipment to itself)')
        verification_passed = False
    else:
        print('   ✓ No self-connections found')
    
    # Check for missing POCs
    missing_from_pocs_sql = '''
        SELECT COUNT(*) 
        FROM tb_equipment_connections c
        LEFT JOIN tb_equipment_pocs p ON c.from_poc_id = p.id
        WHERE p.id IS NULL
    '''
    
    missing_from = db.query(missing_from_pocs_sql)[0][0]
    if missing_from > 0:
        print(f'   ✗ Found {missing_from} connections with missing from POCs')
        verification_passed = False
    else:
        print('   ✓ All from POCs exist')
    
    missing_to_pocs_sql = '''
        SELECT COUNT(*) 
        FROM tb_equipment_connections c
        LEFT JOIN tb_equipment_pocs p ON c.to_poc_id = p.id
        WHERE p.id IS NULL
    '''
    
    missing_to = db.query(missing_to_pocs_sql)[0][0]
    if missing_to > 0:
        print(f'   ✗ Found {missing_to} connections with missing to POCs')
        verification_passed = False
    else:
        print('   ✓ All to POCs exist')
    
    # Check path references
    print('\n5. Path Reference Validation:')
    missing_paths_sql = '''
        SELECT COUNT(DISTINCT c.path_id) as unique_paths,
               COUNT(*) as total_connections
        FROM tb_equipment_connections c
        LEFT JOIN nw_paths p ON c.path_id = p.id
        WHERE p.id IS NULL
    '''
    
    path_result = db.query(missing_paths_sql)
    if path_result and path_result[0]:
        unique_paths, total_with_missing = path_result[0]
        if total_with_missing > 0:
            print(f'   ⚠ Found {total_with_missing} connections referencing missing paths')
            print(f'   ⚠ {unique_paths} unique missing path IDs')
        else:
            print('   ✓ All path references are valid')
    
    # Final verification summary
    print(f'\n{"="*60}')
    if verification_passed:
        print('✓ VERIFICATION PASSED: All critical checks completed successfully')
    else:
        print('✗ VERIFICATION FAILED: Critical issues found that need attention')
        print('\nRecommended actions:')
        print('1. Check for data inconsistencies in source tables')
        print('2. Verify POC loading completed successfully')
        print('3. Review path generation logic')
        print('4. Consider data cleanup before re-running the load')
    print(f'{"="*60}')
    
    return verification_passed

def main():
    """
    Main function to orchestrate the equipment connections loading process.
    """
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description='Load equipment connections data into tb_equipment_connections table',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  python load_equipment_connections.py     # Interactive mode with confirmation
  python load_equipment_connections.py -y  # Unattended mode, auto-confirm
        '''
    )
    parser.add_argument(
        '-y', '--yes', 
        action='store_true',
        help='Auto-confirm without prompting (unattended mode)'
    )
    
    args = parser.parse_args()
    
    print('Starting equipment connections analysis and loading process...')
    
    db = None
    try:
        # Initialize database connection
        print('Connecting to database...')
        db = Database()
        print('✓ Database connection established')
        
        # Create/verify table
        print('\nVerifying connections table...')
        if not create_connections_table(db):
            print('✗ Failed to create/verify connections table')
            return
        
        # Load equipment POCs
        print('\nLoading equipment POCs...')
        equipment_pocs = get_active_equipment_pocs(db)
        
        if not equipment_pocs:
            print('No active equipment POCs found. Please load equipment and POCs first.')
            return
        
        # Load equipment logical nodes
        print('\nLoading equipment logical nodes...')
        equipment_logical_nodes = get_equipment_logical_nodes(db)
        
        # Analyze connections
        print('\nAnalyzing equipment connections...')
        print('⚠ This may take a while for large datasets...')
        
        connections = analyze_poc_connections(db, equipment_pocs, equipment_logical_nodes)
        
        if not connections:
            print('No connections found. This might indicate:')
            print('  - No downstream paths exist')
            print('  - Spatial analysis procedure issues')
            print('  - Data filtering too restrictive')
            return
        
        # Show summary
        print(f'\nSummary of connections to be loaded:')
        print(f'  Total connections: {len(connections)}')
        
        # Analyze connections data
        unique_from_eq = len(set(conn[0] for conn in connections))
        unique_to_eq = len(set(conn[1] for conn in connections))
        unique_pairs = len(set((conn[0], conn[1]) for conn in connections))
        
        connection_type_counts = defaultdict(int)
        valid_count = sum(1 for conn in connections if conn[5] == 1)  # is_valid field
        
        for conn in connections:
            conn_type = conn[9] if conn[9] else 'NULL'  # connection_type field
            connection_type_counts[conn_type] += 1
        
        print(f'  Unique from equipment: {unique_from_eq}')
        print(f'  Unique to equipment: {unique_to_eq}')
        print(f'  Unique equipment pairs: {unique_pairs}')
        print(f'  Valid connections: {valid_count}')
        
        print('  Connection types:')
        for conn_type, count in sorted(connection_type_counts.items(), key=lambda x: x[1], reverse=True):
            print(f'    {conn_type}: {count}')
        
        # Confirm before proceeding
        if args.yes:
            print('\nAuto-confirming due to -y flag...')
        else:
            response = input('\nProceed with loading? (y/N): ').strip().lower()
            if response not in ['y', 'yes']:
                print('Loading cancelled by user.')
                return
        
        # Clear existing data
        print('\nClearing existing connections...')
        clear_existing_connections(db)
        
        # Insert new connections
        print('\nInserting equipment connections...')
        inserted_count = insert_connections_batch(db, connections)
        
        print(f'\n✓ Successfully loaded {inserted_count} equipment connections')
        
        # Verify the load
        verify_connections(db)
        
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
