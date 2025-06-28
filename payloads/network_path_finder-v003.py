#!/usr/bin/env python3

"""
Network Path Finder using Dijkstra algorithm for downstream analysis.

Finds all downstream paths from a given node to leaf nodes (nodes with no outgoing connections)
using the nw_links table structure with proper filtering.

This replaces the buggy ST_NW_DOWNSTREAM stored procedure with a reliable Python implementation.
"""

import heapq
from typing import Dict, List, Tuple, Set, Optional
from collections import defaultdict, deque
from dataclasses import dataclass

@dataclass
class NetworkNode:
    """Represents a node in the network with its metadata."""
    node_id: int
    data_code: int = 0
    utility_no: int = 0
    toolset_id: int = 0
    eq_poc_no: str = ''
    net_obj_type: int = 0  # 1=logical, 2=poc, 3=virtual

@dataclass
class NetworkLink:
    """Represents a link in the network."""
    link_id: int
    guid: str
    start_node_id: int
    end_node_id: int
    is_bidirected: bool
    cost: float
    net_obj_type: int  # 101=link, 201=logical-poc, 202=poc-poc, 203=poc-distance, 204=poc-virtual

@dataclass
class PathResult:
    """Represents a complete path from source to destination."""
    path_id: int
    start_node_id: int
    end_node_id: int
    total_cost: float
    links: List[Tuple[int, int, int, float, bool]]  # (seq, link_id, start_node, end_node, cost, reverse)

class NetworkPathFinder:
    """
    Efficient network path finder using Dijkstra algorithm.
    Handles complex filtering and finds all downstream paths to leaf nodes.
    """
    
    def __init__(self, db):
        self.db = db
        self.graph = defaultdict(list)  # adjacency list: node_id -> [(neighbor_id, link_id, cost, reverse)]
        self.nodes = {}  # node_id -> NetworkNode
        self.links = {}  # link_id -> NetworkLink
        self.loaded = False
    
    def load_network_data(self, utility_no: int = 0, toolset_id: int = 0, 
                         eq_poc_no: str = '', data_codes: str = '') -> None:
        """
        Load network topology from database with optional filtering.
        
        Args:
            utility_no: Filter by utility (0 = all)
            toolset_id: Filter by toolset (0 = all) 
            eq_poc_no: Filter by POC number ('' = all)
            data_codes: Filter by data codes ('15000,107' = equipment and elbows)
        """
        print('Loading network topology...')
        
        # Build node filter conditions
        node_conditions = ['1=1']  # Always true base condition
        node_params = []
        
        if utility_no > 0:
            node_conditions.append('n.utility_no = ?')
            node_params.append(utility_no)
        
        if toolset_id > 0:
            node_conditions.append('n.toolset_id = ?')
            node_params.append(toolset_id)
            
        if eq_poc_no and eq_poc_no.strip():
            node_conditions.append('n.eq_poc_no LIKE ?')
            node_params.append(f'%{eq_poc_no.strip()}%')
            
        if data_codes and data_codes.strip() and data_codes != '0':
            # Parse comma-separated data codes
            codes = [code.strip() for code in data_codes.split(',') if code.strip()]
            if codes:
                placeholders = ','.join(['?' for _ in codes])
                node_conditions.append(f'n.data_code IN ({placeholders})')
                node_params.extend(codes)
        
        node_filter = ' AND '.join(node_conditions)
        
        # Load nodes with filtering
        nodes_query = f'''
            SELECT n.node_id, n.data_code, n.utility_no, n.toolset_id, 
                   COALESCE(n.eq_poc_no, '') as eq_poc_no, n.net_obj_type
            FROM nw_nodes n
            WHERE {node_filter}
        '''
        
        try:
            node_rows = self.db.query(nodes_query, node_params)
            for row in node_rows:
                node_id, data_code, utility_no, toolset_id, eq_poc_no, net_obj_type = row
                self.nodes[node_id] = NetworkNode(
                    node_id=node_id,
                    data_code=data_code or 0,
                    utility_no=utility_no or 0,
                    toolset_id=toolset_id or 0,
                    eq_poc_no=eq_poc_no or '',
                    net_obj_type=net_obj_type or 0
                )
            
            print(f'✓ Loaded {len(self.nodes)} nodes')
        except Exception as e:
            print(f'Error loading nodes: {e}')
            raise
        
        # Load links - only between filtered nodes
        if not self.nodes:
            print('No nodes loaded, skipping links')
            return
            
        node_ids = list(self.nodes.keys())
        placeholders = ','.join(['?' for _ in node_ids])
        
        links_query = f'''
            SELECT l.id, l.guid, l.start_node_id, l.end_node_id, 
                   l.is_bidirected, l.cost, l.net_obj_type
            FROM nw_links l
            WHERE l.start_node_id IN ({placeholders})
              AND l.end_node_id IN ({placeholders})
        '''
        
        try:
            link_params = node_ids + node_ids  # For both start and end node filters
            link_rows = self.db.query(links_query, link_params)
            
            for row in link_rows:
                link_id, guid, start_node_id, end_node_id, is_bidirected, cost, net_obj_type = row
                
                link = NetworkLink(
                    link_id=link_id,
                    guid=guid or '',
                    start_node_id=start_node_id,
                    end_node_id=end_node_id,
                    is_bidirected=bool(is_bidirected),
                    cost=float(cost or 1.0),
                    net_obj_type=net_obj_type or 101
                )
                
                self.links[link_id] = link
                
                # Add forward direction
                self.graph[start_node_id].append((end_node_id, link_id, link.cost, False))
                
                # Add reverse direction if bidirectional
                if link.is_bidirected:
                    self.graph[end_node_id].append((start_node_id, link_id, link.cost, True))
            
            print(f'✓ Loaded {len(self.links)} links')
            print(f'✓ Graph has {len(self.graph)} nodes with connections')
            
        except Exception as e:
            print(f'Error loading links: {e}')
            raise
        
        self.loaded = True
    
    def find_target_nodes(self, ignore_node_id: int = 0, data_codes: str = '') -> Set[int]:
        """
        Find target nodes based on data_codes parameter or true leaf nodes.
        
        Args:
            ignore_node_id: Node to ignore in traversal
            data_codes: Target data codes ('15000,107' for equipment+elbows, '' for leaves)
            
        Returns:
            Set of target node IDs
        """
        if not self.loaded:
            raise RuntimeError('Network data not loaded. Call load_network_data() first.')
        
        target_nodes = set()
        
        if data_codes and data_codes.strip() and data_codes != '0':
            # Find nodes matching specific data codes
            target_data_codes = [int(code.strip()) for code in data_codes.split(',') if code.strip().isdigit()]
            
            if target_data_codes:
                for node_id, node in self.nodes.items():
                    if node_id == ignore_node_id:
                        continue
                    if node.data_code in target_data_codes:
                        target_nodes.add(node_id)
                
                print(f'Found {len(target_nodes)} target nodes matching data codes: {data_codes}')
                print(f'  Target data codes: {target_data_codes}')
        else:
            # Find true leaf nodes (no outgoing connections)
            for node_id in self.nodes:
                if node_id == ignore_node_id:
                    continue
                    
                # Check if node has any outgoing connections (not counting ignored node)
                has_outgoing = False
                for neighbor_id, _, _, _ in self.graph.get(node_id, []):
                    if neighbor_id != ignore_node_id:
                        has_outgoing = True
                        break
                
                if not has_outgoing:
                    target_nodes.add(node_id)
            
            print(f'Found {len(target_nodes)} true leaf nodes (no outgoing connections)')
        
        return target_nodes
    
    def find_downstream_paths(self, start_node_id: int, ignore_node_id: int = 0, 
                             data_codes: str = '') -> List[PathResult]:
        """
        Find all downstream paths from start_node_id to target nodes using Dijkstra.
        
        Args:
            start_node_id: Starting node ID
            ignore_node_id: Node ID to ignore in traversal (0 = none)
            data_codes: Target data codes ('15000,107' for equipment+elbows, '' for leaves)
            
        Returns:
            List of PathResult objects for each reachable target node
        """
        if not self.loaded:
            raise RuntimeError('Network data not loaded. Call load_network_data() first.')
        
        if start_node_id not in self.nodes:
            raise ValueError(f'Start node {start_node_id} not found in loaded network')
        
        # Find target nodes based on data_codes parameter
        target_nodes = self.find_target_nodes(ignore_node_id, data_codes)
        if not target_nodes:
            if data_codes and data_codes.strip() and data_codes != '0':
                print(f'No target nodes found matching data codes: {data_codes}')
            else:
                print('No leaf nodes found in network')
            return []
        
        # Dijkstra's algorithm with path tracking
        distances = {start_node_id: 0.0}
        previous = {}  # node_id -> (previous_node_id, link_id, reverse)
        visited = set()
        heap = [(0.0, start_node_id)]
        
        while heap:
            current_dist, current_node = heapq.heappop(heap)
            
            if current_node in visited:
                continue
                
            visited.add(current_node)
            
            # Skip ignored node
            if current_node == ignore_node_id:
                continue
            
            # Explore neighbors
            for neighbor_id, link_id, cost, reverse in self.graph.get(current_node, []):
                if neighbor_id == ignore_node_id or neighbor_id in visited:
                    continue
                
                new_dist = current_dist + cost
                
                if neighbor_id not in distances or new_dist < distances[neighbor_id]:
                    distances[neighbor_id] = new_dist
                    previous[neighbor_id] = (current_node, link_id, reverse)
                    heapq.heappush(heap, (new_dist, neighbor_id))
        
        # Build paths to reachable target nodes
        paths = []
        path_id = 1
        
        for target_node in target_nodes:
            if target_node not in distances:
                continue  # Unreachable target
            
            # Reconstruct path
            path_links = []
            current = target_node
            seq = 0
            
            while current in previous:
                prev_node, link_id, reverse = previous[current]
                seq += 1
                
                link = self.links[link_id]
                path_links.append((
                    seq, 
                    link_id, 
                    prev_node if not reverse else current,
                    current if not reverse else prev_node, 
                    link.cost, 
                    reverse
                ))
                
                current = prev_node
            
            # Reverse to get correct order (start to end)
            path_links.reverse()
            for i, (_, link_id, start_node, end_node, cost, reverse) in enumerate(path_links):
                path_links[i] = (i + 1, link_id, start_node, end_node, cost, reverse)
            
            if path_links:  # Only add if path exists
                paths.append(PathResult(
                    path_id=path_id,
                    start_node_id=start_node_id,
                    end_node_id=target_node,
                    total_cost=distances[target_node],
                    links=path_links
                ))
                path_id += 1
        
        target_type = f'nodes with data codes {data_codes}' if data_codes and data_codes.strip() and data_codes != '0' else 'leaf nodes'
        print(f'Found {len(paths)} downstream paths from node {start_node_id} to {target_type}')
        return paths
    
    def analyze_node_flags(self, paths: List[PathResult], data_codes: str = '') -> Dict[Tuple[int, int], str]:
        """
        Analyze node flags for all paths to classify nodes properly.
        
        Args:
            paths: List of all computed paths
            data_codes: Target data codes to distinguish between 'E' and 'L' flags
            
        Returns:
            Dictionary mapping (path_id, node_id) -> node_flag
        """
        node_flags = {}
        
        # Parse target data codes
        target_data_codes = set()
        if data_codes and data_codes.strip() and data_codes != '0':
            target_data_codes = {int(code.strip()) for code in data_codes.split(',') if code.strip().isdigit()}
        
        # First pass: identify convergence points across all paths
        node_incoming_count = defaultdict(int)  # node_id -> count of incoming links across all paths
        node_outgoing_count = defaultdict(int)  # node_id -> count of outgoing links across all paths
        
        for path in paths:
            nodes_in_path = set()
            for _, _, start_node_id, end_node_id, _, _ in path.links:
                nodes_in_path.add(start_node_id)
                nodes_in_path.add(end_node_id)
                node_outgoing_count[start_node_id] += 1
                node_incoming_count[end_node_id] += 1
        
        # Second pass: classify nodes for each path
        for path in paths:
            if not path.links:
                continue
                
            # Get all nodes in this path in order
            path_nodes = [path.start_node_id]
            for _, _, start_node_id, end_node_id, _, _ in path.links:
                if end_node_id not in path_nodes:
                    path_nodes.append(end_node_id)
            
            # Classify each node in the path
            for i, node_id in enumerate(path_nodes):
                flag_key = (path.path_id, node_id)
                
                if i == 0:
                    # Starting node
                    node_flags[flag_key] = 'S'
                elif i == len(path_nodes) - 1:
                    # Last node in path - determine if it's E or L
                    node = self.nodes.get(node_id)
                    
                    if target_data_codes and node and node.data_code in target_data_codes:
                        # Path ends at a specific target type (equipment, elbow, flange, etc.)
                        node_flags[flag_key] = 'E'
                    elif node_outgoing_count.get(node_id, 0) == 0:
                        # True leaf node (no outgoing connections in entire network)
                        node_flags[flag_key] = 'L'
                    else:
                        # End of this path but has other outgoing connections
                        node_flags[flag_key] = 'E'
                else:
                    # Intermediate node - check for convergence
                    if node_incoming_count.get(node_id, 0) > 1:
                        node_flags[flag_key] = 'C'  # Convergence point
                    else:
                        node_flags[flag_key] = 'I'  # Regular intermediate node
        
        return node_flags
        """
        Save computed paths to nw_paths and nw_path_links tables.
        
        Args:
            paths: List of PathResult objects to save
            algorithm: Algorithm name for the paths
            
        Returns:
            List of created path IDs in database
        """
        if not paths:
            return []
        
        created_path_ids = []
        
        try:
            for path in paths:
                # Insert into nw_paths
                paths_sql = '''
                    INSERT INTO nw_paths (algorithm, start_node_id, end_node_id, cost)
                    VALUES (?, ?, ?, ?)
                '''
                
                rows_affected = self.db.update(paths_sql, [
                    algorithm,
                    path.start_node_id,
                    path.end_node_id,
                    path.total_cost
                ])
                
                if rows_affected == 0:
                    print(f'Failed to insert path {path.path_id}')
                    continue
                
                # Get the inserted path ID
                path_id_query = 'SELECT LAST_INSERT_ID()'
                result = self.db.query(path_id_query)
                if not result or not result[0][0]:
                    print(f'Failed to get path ID for path {path.path_id}')
                    continue
                    
                db_path_id = result[0][0]
                created_path_ids.append(db_path_id)
                
                # Insert path links
                for seq, link_id, start_node_id, end_node_id, cost, reverse in path.links:
                    # Get node metadata for path links
                    start_node = self.nodes.get(start_node_id)
                    end_node = self.nodes.get(end_node_id)
                    
                    link_sql = '''
                        INSERT INTO nw_path_links (
                            path_id, seq, link_id, length, 
                            start_node_id, start_node_data_code, start_node_utility_no,
                            end_node_id, end_node_data_code, end_node_utility_no,
                            reverse, group_no, sub_group_no, end_node_flag
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    '''
                    
                    self.db.update(link_sql, [
                        db_path_id,
                        seq,
                        link_id,
                        cost,  # Using cost as length
                        start_node_id,
                        start_node.data_code if start_node else 0,
                        start_node.utility_no if start_node else 0,
                        end_node_id,
                        end_node.data_code if end_node else 0,
                        end_node.utility_no if end_node else 0,
                        1 if reverse else 0,
                        1,  # group_no - could be enhanced
                        1,  # sub_group_no - could be enhanced
                        'E' if end_node_id == path.end_node_id else 'I'  # End or Intermediate
                    ])
            
            print(f'✓ Saved {len(created_path_ids)} paths to database')
            return created_path_ids
            
        except Exception as e:
            print(f'Error saving paths to database: {e}')
            raise

def find_network_downstream(db, start_node_id: int, ignore_node_id: int = 0,
                          utility_no: int = 0, toolset_id: int = 0, 
                          eq_poc_no: str = '', data_codes: str = '') -> List[int]:
    """
    Main function to find downstream paths and save to database.
    
    Args:
        db: Database connection
        start_node_id: Starting node ID
        ignore_node_id: Node to ignore (0 = none)
        utility_no: Utility filter (0 = all)
        toolset_id: Toolset filter (0 = all)
        eq_poc_no: POC number filter ('' = all)
        data_codes: Data codes filter ('' = all)
        
    Returns:
        List of created path IDs in database
    """
    pathfinder = NetworkPathFinder(db)
    
    try:
        # Load network data with filtering
        pathfinder.load_network_data(utility_no, toolset_id, eq_poc_no, data_codes)
        
        # Find downstream paths
        paths = pathfinder.find_downstream_paths(start_node_id, ignore_node_id, data_codes)
        
        # Store data_codes for node flag analysis
        pathfinder._last_data_codes = data_codes
        
        if not paths:
            print(f'No downstream paths found from node {start_node_id}')
            return []
        
        # Save paths to database
        created_path_ids = pathfinder.save_paths_to_db(paths)
        
        return created_path_ids
        
    except Exception as e:
        print(f'Error in downstream analysis: {e}')
        raise

# Example usage and testing
if __name__ == '__main__':
    from db import Database
    
    # Test the pathfinder
    db = Database()
    
    try:
        # Example: Find downstream from equipment POC node 12345
        # Ignore the equipment logical node 12340
        # Filter for equipment connections (data_code 15000)
        path_ids = find_network_downstream(
            db,
            start_node_id=12345,
            ignore_node_id=12340,
            utility_no=0,          # All utilities
            toolset_id=0,          # All toolsets
            eq_poc_no='',          # All POCs
            data_codes='15000'     # Equipment only
        )
        
        print(f'Created {len(path_ids)} paths: {path_ids}')
        
    finally:
        db.close()
