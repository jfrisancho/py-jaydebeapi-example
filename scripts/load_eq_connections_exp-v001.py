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

def analyze_downstream_path(db: Database, source_node_id: int, ignore_node_id: int = 0, 
                          utility: str = '', toolset_id: int = 0, eq_poc_no: str = '', 
                          data_codes: str = '15000') -> Optional[int]:
    """
    Call the spatial downstream analysis procedure.
    
    Args:
        source_node_id: Starting node ID
        ignore_node_id: Node to ignore (0 = none)
        utility: Utility filter ('' = all)
        toolset_id: Toolset filter (0 = all)
        eq_poc_no: POC number filter ('' = all)
        data_codes: Data codes filter ('15000' for equipment)
    
    Returns:
        Path ID if successful, None if no path found
    """
    try:
        # Convert empty strings to None for the stored procedure
        proc_params = [
            source_node_id,
            ignore_node_id if ignore_node_id > 0 else 0,
            utility if utility else '',
            toolset_id if toolset_id > 0 else 0,
            eq_poc_no if eq_poc_no else '',
            data_codes if data_codes else '15000'
        ]
        
        # Call the stored procedure
        db.callproc('nw_downstream', proc_params)
        
        # Get the path ID - this might require a different approach depending on how the SP returns the ID
        # For now, assume we need to query for the most recent path
        path_query = '''
            SELECT MAX(id) as path_id 
            FROM nw_paths 
            WHERE created_at >= NOW() - INTERVAL 1 MINUTE
        '''
        
        result = db.query(path_query)
        if result and result[0][0]:
            return result[0][0]
        return None
        
    except Exception as e:
        print(f'Error in downstream analysis for node {source_node_id}: {e}')
        return None

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
    Analyze connections from each equipment POC using spatial analysis.
    
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
        
        # Analyze downstream from this POC
        # Filter by utility if the POC has one
        utility_filter = utility if utility else ''
        
        path_id = analyze_downstream_path(
            db, 
            source_node_id=node_id,
            ignore_node_id=ignore_node_id,
            utility=utility_filter,
            data_codes='15000'  # Equipment data code
        )
        
        if not path_id:
            continue
        
        # Get the path links
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
                
                # Create connection record
                connection = (
                    equipment_id,          # source_equipment_id
                    poc_id,               # source_poc_id
                    code,                 # source_poc_code
                    target_eq_id,         # target_equipment_id
                    target_poc_id,        # target_poc_id
                    target_poc_code,      # target_poc_code
                    path_id,              # path_id
                    len(path_links),      # link_count
                    utility_filter,       # utility
                    flow,                 # flow_direction
                    has_intermediate,     # has_intermediate_equipment
                    intermediate_count    # intermediate_count
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
            source_equipment_id, source_poc_id, source_poc_code,
            target_equipment_id, target_poc_id, target_poc_code,
            path_id, link_count, utility, flow_direction,
            has_intermediate_equipment, intermediate_count
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                    failed_in_batch.append(('No rows affected', connection[0], connection[3]))
            except Exception as e:
                failed_in_batch.append((str(e), connection[0], connection[3]))
        
        total_inserted += batch_inserted
        print(f'✓ Batch {batch_num + 1}/{total_batches}: Inserted {batch_inserted}/{len(batch)} connections')
        
        if failed_in_batch:
            print(f'  ⚠ {len(failed_in_batch)} failures in this batch')
            for error, src_eq, tgt_eq in failed_in_batch[:3]:
                print(f'    - Equipment {src_eq} -> {tgt_eq}: {error}')
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
            COUNT(DISTINCT source_equipment_id) as source_equipment_count,
            COUNT(DISTINCT target_equipment_id) as target_equipment_count,
            COUNT(DISTINCT CONCAT(source_equipment_id, '-', target_equipment_id)) as unique_equipment_pairs,
            AVG(link_count) as avg_link_count,
            SUM(has_intermediate_equipment) as connections_with_intermediates
        FROM tb_equipment_connections
    '''
    
    result = db.query(basic_stats_sql)
    if result:
        (total_connections, source_count, target_count, unique_pairs, 
         avg_links, with_intermediates) = result[0]
        print(f'   Total connections: {total_connections}')
        print(f'   Source equipment count: {source_count}')
        print(f'   Target equipment count: {target_count}')
        print(f'   Unique equipment pairs: {unique_pairs}')
        print(f'   Average link count: {avg_links:.2f}' if avg_links else '   Average link count: 0')
        print(f'   Connections with intermediates: {with_intermediates}')
    
    # Utility distribution
    print('\n2. Utility Distribution:')
    utility_sql = '''
        SELECT 
            COALESCE(utility, 'NULL') as utility_type,
            COUNT(*) as connection_count,
            ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 2) as percentage
        FROM tb_equipment_connections
        GROUP BY utility
        ORDER BY COUNT(*) DESC
        LIMIT 10
    '''
    
    utility_result = db.query(utility_sql)
    if utility_result:
        for utility, count, percentage in utility_result:
            print(f'   {utility}: {count} connections ({percentage}%)')
    
    # Flow direction analysis
    print('\n3. Flow Direction Analysis:')
    flow_sql = '''
        SELECT 
            COALESCE(flow_direction, 'NULL') as flow,
            COUNT(*) as connection_count
        FROM tb_equipment_connections
        GROUP BY flow_direction
        ORDER BY COUNT(*) DESC
    '''
    
    flow_result = db.query(flow_sql)
    if flow_result:
        for flow, count in flow_result:
            print(f'   {flow}: {count} connections')
    
    # Equipment with most connections
    print('\n4. Equipment with Most Connections (Top 10):')
    top_equipment_sql = '''
        SELECT 
            e.guid,
            e.name,
            COUNT(*) as total_connections,
            SUM(CASE WHEN c.source_equipment_id = e.id THEN 1 ELSE 0 END) as outgoing,
            SUM(CASE WHEN c.target_equipment_id = e.id THEN 1 ELSE 0 END) as incoming
        FROM tb_equipment_connections c
        INNER JOIN tb_equipments e ON (c.source_equipment_id = e.id OR c.target_equipment_id = e.id)
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
    print('\n5. Data Integrity Checks:')
    
    # Check for self-connections
    self_connections_sql = '''
        SELECT COUNT(*) 
        FROM tb_equipment_connections 
        WHERE source_equipment_id = target_equipment_id
    '''
    
    self_count = db.query(self_connections_sql)[0][0]
    if self_count > 0:
        print(f'   ⚠ Found {self_count} self-connections (equipment to itself)')
        verification_passed = False
    else:
        print('   ✓ No self-connections found')
    
    # Check for missing POCs
    missing_source_pocs_sql = '''
        SELECT COUNT(*) 
        FROM tb_equipment_connections c
        LEFT JOIN tb_equipment_pocs p ON c.source_poc_id = p.id
        WHERE p.id IS NULL
    '''
    
    missing_source = db.query(missing_source_pocs_sql)[0][0]
    if missing_source > 0:
        print(f'   ✗ Found {missing_source} connections with missing source POCs')
        verification_passed = False
    else:
        print('   ✓ All source POCs exist')
    
    missing_target_pocs_sql = '''
        SELECT COUNT(*) 
        FROM tb_equipment_connections c
        LEFT JOIN tb_equipment_pocs p ON c.target_poc_id = p.id
        WHERE p.id IS NULL
    '''
    
    missing_target = db.query(missing_target_pocs_sql)[0][0]
    if missing_target > 0:
        print(f'   ✗ Found {missing_target} connections with missing target POCs')
        verification_passed = False
    else:
        print('   ✓ All target POCs exist')
    
    # Final verification summary
    print(f'\n{"="*60}')
    if verification_passed:
        print('✓ VERIFICATION PASSED: All critical checks completed successfully')
    else:
        print('✗ VERIFICATION FAILED: Critical issues found that need attention')
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
        unique_sources = len(set(conn[0] for conn in connections))
        unique_targets = len(set(conn[3] for conn in connections))
        unique_pairs = len(set((conn[0], conn[3]) for conn in connections))
        
        utility_counts = defaultdict(int)
        flow_counts = defaultdict(int)
        
        for conn in connections:
            utility = conn[8] if conn[8] else 'NULL'
            flow = conn[9] if conn[9] else 'NULL'
            utility_counts[utility] += 1
            flow_counts[flow] += 1
        
        print(f'  Unique source equipment: {unique_sources}')
        print(f'  Unique target equipment: {unique_targets}')
        print(f'  Unique equipment pairs: {unique_pairs}')
        
        print('  Top utilities:')
        for utility, count in sorted(utility_counts.items(), key=lambda x: x[1], reverse=True)[:5]:
            print(f'    {utility}: {count}')
        
        print('  Flow distribution:')
        for flow, count in sorted(flow_counts.items()):
            print(f'    {flow}: {count}')
        
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
