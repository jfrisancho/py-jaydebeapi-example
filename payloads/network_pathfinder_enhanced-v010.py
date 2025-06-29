#!/usr/bin/env python3

"""
Enhanced Network Path Finder supporting both DFS and Dijkstra algorithms.

Key features:
1. DFS: Finds ALL possible downstream paths (can be exponential)
2. Dijkstra: Finds shortest paths to each reachable endpoint
3. Common endpoint classification logic for both algorithms
4. Reusable components with clear separation of concerns
"""

import heapq
import hashlib
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

@dataclass 
class CachedPathResult:
    """Cached path analysis result."""
    paths: List[PathResult]
    created_path_ids: List[int]
    timestamp: float
    parameter_hash: str

class NetworkPathFinder:
    """
    Enhanced network path finder supporting multiple algorithms.
    
    Algorithms:
    - DFS: Finds ALL possible paths (can be exponential for highly connected graphs)
    - Dijkstra: Finds shortest path to each reachable endpoint
    
    Key concepts:
    - PATH FILTERS (utility_no, toolset_id, eq_poc_no): Control which nodes can be traversed
    - TARGET CODES (data_codes): Define specific node types where paths can end
    - START NODE: Always included regardless of path filters
    
    Paths end in 3 ways:
    1. Leaf nodes (no outgoing connections in full network)
    2. Target nodes (matching data_codes parameter)  
    3. Filter boundaries (no more traversable nodes due to path filters)
    """
    
    def __init__(self, db):
        self.db = db
        self.graph = defaultdict(list)  # adjacency list: node_id -> [(neighbor_id, link_id, cost, reverse)]
        self.all_nodes = {}  # node_id -> NetworkNode (ALL nodes from DB)
        self.traversable_nodes = set()  # Set of node_ids that can be traversed (filtered)
        self.links = {}  # link_id -> NetworkLink
        self.loaded = False
        self.start_node_id = None  # Track start node for special handling
    
    def load_network_data(self, start_node_id: int, utility_no: int = 0, toolset_id: int = 0, 
                         eq_poc_no: str = '') -> None:
        """
        Load network topology from database with path filtering.
        
        PATH FILTERS control which nodes can be traversed during pathfinding:
        - utility_no: Filter by utility (0 = all)
        - toolset_id: Filter by toolset (0 = all) 
        - eq_poc_no: Filter by POC number ('' = all)
        
        The start_node_id is ALWAYS included regardless of filters.
        
        Args:
            start_node_id: Starting node (always included)
            utility_no: Path filter by utility (0 = all)
            toolset_id: Path filter by toolset (0 = all) 
            eq_poc_no: Path filter by POC number ('' = all)
        """
        print(f'Loading network topology with start_node_id={start_node_id}...')
        self.start_node_id = start_node_id
        
        # First, load ALL nodes from database
        all_nodes_query = '''
            SELECT n.node_id, n.data_code, n.utility_no, n.toolset_id, 
                   COALESCE(n.eq_poc_no, '') as eq_poc_no, n.net_obj_type
            FROM nw_nodes n
        '''
        
        try:
            node_rows = self.db.query(all_nodes_query)
            for row in node_rows:
                node_id, data_code, utility_no_val, toolset_id_val, eq_poc_no_val, net_obj_type = row
                self.all_nodes[node_id] = NetworkNode(
                    node_id=node_id,
                    data_code=data_code or 0,
                    utility_no=utility_no_val or 0,
                    toolset_id=toolset_id_val or 0,
                    eq_poc_no=eq_poc_no_val or '',
                    net_obj_type=net_obj_type or 0
                )
            
            print(f'✓ Loaded {len(self.all_nodes)} total nodes from database')
        except Exception as e:
            print(f'Error loading all nodes: {e}')
            raise
        
        # Verify start node exists
        if start_node_id not in self.all_nodes:
            raise ValueError(f'Start node {start_node_id} not found in database')
        
        # Apply path filters to determine traversable nodes
        self.traversable_nodes = set()
        
        # ALWAYS include start node regardless of filters
        self.traversable_nodes.add(start_node_id)
        start_node = self.all_nodes[start_node_id]
        print(f'✓ Start node {start_node_id} (data_code={start_node.data_code}, utility={start_node.utility_no}) always included')
        
        # Apply filters to other nodes
        filter_count = 0
        for node_id, node in self.all_nodes.items():
            if node_id == start_node_id:
                continue  # Already added
            
            # Check path filters
            include_node = True
            
            if utility_no > 0 and node.utility_no != utility_no:
                include_node = False
            
            if include_node and toolset_id > 0 and node.toolset_id != toolset_id:
                include_node = False
                
            if include_node and eq_poc_no and eq_poc_no.strip():
                if eq_poc_no.strip().lower() not in node.eq_poc_no.lower():
                    include_node = False
            
            if include_node:
                self.traversable_nodes.add(node_id)
                filter_count += 1
        
        print(f'✓ Path filters applied: {filter_count} additional nodes can be traversed')
        print(f'  - Total traversable nodes: {len(self.traversable_nodes)}')
        print(f'  - Filters: utility_no={utility_no}, toolset_id={toolset_id}, eq_poc_no="{eq_poc_no}"')
        
        # Load links between ANY nodes (not just traversable ones)
        # We need all links to understand the full network structure
        links_query = '''
            SELECT l.id, l.guid, l.start_node_id, l.end_node_id, 
                   l.is_bidirected, l.cost, l.net_obj_type
            FROM nw_links l
            WHERE l.start_node_id IN (SELECT node_id FROM nw_nodes)
              AND l.end_node_id IN (SELECT node_id FROM nw_nodes)
        '''
        
        try:
            link_rows = self.db.query(links_query)
            
            for row in link_rows:
                link_id, guid, start_node_id, end_node_id, is_bidirected, cost, net_obj_type = row
                
                # Skip links where both nodes don't exist in our loaded nodes
                if start_node_id not in self.all_nodes or end_node_id not in self.all_nodes:
                    continue
                
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
            
            print(f'✓ Loaded {len(self.links)} links (full network)')
            print(f'✓ Graph has {len(self.graph)} nodes with connections')
            
        except Exception as e:
            print(f'Error loading links: {e}')
            raise
        
        self.loaded = True
    
    def _parse_target_codes(self, target_data_codes: str) -> Set[int]:
        """Parse target data codes string into a set of integers."""
        target_codes_set = set()
        if target_data_codes and target_data_codes.strip() and target_data_codes != '0':
            for code in target_data_codes.split(','):
                code = code.strip()
                if code.isdigit():
                    target_codes_set.add(int(code))
        return target_codes_set
    
    def _is_endpoint(self, node_id: int, target_codes_set: Set[int], ignore_node_id: int) -> Tuple[bool, str, str]:
        """
        Determine if a node should be an endpoint and classify the endpoint type.
        
        Returns:
            (is_endpoint, endpoint_type, reason)
        """
        if node_id == self.start_node_id or node_id == ignore_node_id:
            return False, '', ''
        
        current_node_obj = self.all_nodes.get(node_id)
        
        # Get all neighbors and traversable neighbors
        all_neighbors = self.graph.get(node_id, [])
        traversable_neighbors = []
        
        for neighbor_id, link_id, cost, reverse in all_neighbors:
            if neighbor_id == ignore_node_id:
                continue
            if neighbor_id in self.traversable_nodes:
                traversable_neighbors.append((neighbor_id, link_id, cost, reverse))
        
        # Rule 1: LEAF NODE - No outgoing connections in full network
        if len(all_neighbors) == 0:
            return True, 'LEAF', 'No outgoing connections (true leaf)'
        
        # Rule 2: TARGET NODE - Matches target data codes
        elif target_codes_set and current_node_obj and current_node_obj.data_code in target_codes_set:
            return True, 'TARGET', f'Matches target data code {current_node_obj.data_code}'
        
        # Rule 3: FILTER BOUNDARY - No more traversable neighbors
        elif len(traversable_neighbors) == 0 and len(all_neighbors) > 0:
            return True, 'BOUNDARY', f'Filter boundary ({len(all_neighbors)} total neighbors, 0 traversable)'
        
        return False, '', ''
    
    def _get_traversable_neighbors(self, node_id: int, ignore_node_id: int, visited: Set[int] = None) -> List[Tuple[int, int, float, bool]]:
        """Get list of traversable neighbors for a node."""
        neighbors = []
        for neighbor_id, link_id, cost, reverse in self.graph.get(node_id, []):
            if neighbor_id == ignore_node_id:
                continue
            if visited and neighbor_id in visited:
                continue
            if neighbor_id in self.traversable_nodes:
                neighbors.append((neighbor_id, link_id, cost, reverse))
        return neighbors
    
    def _build_path_result(self, path_id: int, end_node_id: int, total_cost: float, 
                          path_links: List[Tuple[int, int, int, int, float, bool]]) -> PathResult:
        """Build a PathResult object from path information."""
        # Set sequence numbers and format links
        formatted_links = []
        for i, (link_id, start_node, end_node, _, cost, reverse) in enumerate(path_links):
            formatted_links.append((i + 1, link_id, start_node, end_node, cost, reverse))
        
        return PathResult(
            path_id=path_id,
            start_node_id=self.start_node_id,
            end_node_id=end_node_id,
            total_cost=total_cost,
            links=formatted_links
        )
    
    def find_downstream_paths_dfs(self, ignore_node_id: int = 0, target_data_codes: str = '') -> List[PathResult]:
        """
        Find ALL downstream paths using Depth-First Search.
        
        WARNING: This can be exponential in highly connected graphs!
        DFS explores every possible path, which can result in a very large number of paths.
        
        Args:
            ignore_node_id: Node ID to ignore in traversal (0 = none)
            target_data_codes: Target data codes for endpoints ('15000,107' etc, '' = none)
            
        Returns:
            List of PathResult objects for each valid path
        """
        if not self.loaded:
            raise RuntimeError('Network data not loaded. Call load_network_data() first.')
        
        target_codes_set = self._parse_target_codes(target_data_codes)
        
        print(f'Finding ALL downstream paths using DFS from node {self.start_node_id}:')
        print(f'  - Ignore node: {ignore_node_id}')
        print(f'  - Target data codes: {target_codes_set if target_codes_set else "None (leaf/boundary only)"}')
        print(f'  - WARNING: DFS can find exponentially many paths!')
        
        paths = []
        path_id = 1
        endpoint_stats = {'LEAF': 0, 'TARGET': 0, 'BOUNDARY': 0}
        
        def dfs(current_node: int, visited: Set[int], path_links: List, total_cost: float):
            nonlocal path_id
            
            # Check if current node is an endpoint
            is_endpoint, endpoint_type, reason = self._is_endpoint(current_node, target_codes_set, ignore_node_id)
            
            if is_endpoint:
                # Create path to this endpoint
                paths.append(self._build_path_result(path_id, current_node, total_cost, path_links.copy()))
                endpoint_stats[endpoint_type] += 1
                path_id += 1
                
                if len(paths) % 100 == 0:  # Progress indicator
                    print(f'  Found {len(paths)} paths so far...')
                
                # For DFS, we continue exploring even from endpoints (unless they're true leaves)
                if endpoint_type == 'LEAF':
                    return  # True leaves have no neighbors to explore
            
            # Explore traversable neighbors
            neighbors = self._get_traversable_neighbors(current_node, ignore_node_id, visited)
            
            for neighbor_id, link_id, cost, reverse in neighbors:
                if neighbor_id not in visited:  # Avoid cycles
                    new_visited = visited | {neighbor_id}
                    new_path_links = path_links + [(link_id, current_node, neighbor_id, 0, cost, reverse)]
                    new_total_cost = total_cost + cost
                    
                    dfs(neighbor_id, new_visited, new_path_links, new_total_cost)
        
        # Start DFS from the start node
        initial_visited = {self.start_node_id}
        dfs(self.start_node_id, initial_visited, [], 0.0)
        
        # Report results
        print(f'\n✓ DFS found {len(paths)} downstream paths from node {self.start_node_id}:')
        print(f'  - {endpoint_stats["LEAF"]} paths to leaf nodes')
        print(f'  - {endpoint_stats["TARGET"]} paths to target nodes')  
        print(f'  - {endpoint_stats["BOUNDARY"]} paths to filter boundaries')
        
        return paths
    
    def find_downstream_paths_dijkstra(self, ignore_node_id: int = 0, target_data_codes: str = '') -> List[PathResult]:
        """
        Find shortest downstream paths using Dijkstra's algorithm.
        
        This finds the shortest path to each reachable endpoint (one path per endpoint).
        
        Args:
            ignore_node_id: Node ID to ignore in traversal (0 = none)
            target_data_codes: Target data codes for endpoints ('15000,107' etc, '' = none)
            
        Returns:
            List of PathResult objects for each valid path endpoint
        """
        if not self.loaded:
            raise RuntimeError('Network data not loaded. Call load_network_data() first.')
        
        target_codes_set = self._parse_target_codes(target_data_codes)
        
        print(f'Finding shortest downstream paths using Dijkstra from node {self.start_node_id}:')
        print(f'  - Ignore node: {ignore_node_id}')
        print(f'  - Target data codes: {target_codes_set if target_codes_set else "None (leaf/boundary only)"}')
        
        # Dijkstra's algorithm with path tracking
        distances = {self.start_node_id: 0.0}
        previous = {}  # node_id -> (previous_node_id, link_id, reverse)
        visited = set()
        heap = [(0.0, self.start_node_id)]
        potential_endpoints = []  # List of (node_id, endpoint_type, reason)
        
        while heap:
            current_dist, current_node = heapq.heappop(heap)
            
            if current_node in visited:
                continue
                
            visited.add(current_node)
            
            # Skip ignored node (but don't mark as endpoint)
            if current_node == ignore_node_id:
                continue
            
            # Check if current node should be an endpoint (except start node)
            if current_node != self.start_node_id:
                is_endpoint, endpoint_type, reason = self._is_endpoint(current_node, target_codes_set, ignore_node_id)
                
                if is_endpoint:
                    potential_endpoints.append((current_node, endpoint_type, reason))
                    print(f'  Found {endpoint_type} endpoint: node {current_node} - {reason}')
            
            # Explore traversable neighbors
            neighbors = self._get_traversable_neighbors(current_node, ignore_node_id, visited)
            
            for neighbor_id, link_id, cost, reverse in neighbors:
                new_dist = current_dist + cost
                
                if neighbor_id not in distances or new_dist < distances[neighbor_id]:
                    distances[neighbor_id] = new_dist
                    previous[neighbor_id] = (current_node, link_id, reverse)
                    heapq.heappush(heap, (new_dist, neighbor_id))
        
        # Build paths to all valid endpoints
        paths = []
        path_id = 1
        endpoint_stats = {'LEAF': 0, 'TARGET': 0, 'BOUNDARY': 0}
        
        for endpoint_node, endpoint_type, reason in potential_endpoints:
            if endpoint_node not in distances:
                print(f'  Warning: Endpoint {endpoint_node} is unreachable')
                continue  # Unreachable endpoint
            
            # Reconstruct path
            path_links = []
            current = endpoint_node
            
            while current in previous:
                prev_node, link_id, reverse = previous[current]
                
                link = self.links[link_id]
                path_links.append((
                    link_id,
                    prev_node if not reverse else current,
                    current if not reverse else prev_node, 
                    0,  # placeholder seq
                    link.cost, 
                    reverse
                ))
                
                current = prev_node
            
            # Reverse to get correct order (start to end)
            path_links.reverse()
            
            if path_links:  # Only add if path exists
                paths.append(self._build_path_result(path_id, endpoint_node, distances[endpoint_node], path_links))
                path_id += 1
                endpoint_stats[endpoint_type] += 1
        
        # Report results
        print(f'\n✓ Dijkstra found {len(paths)} shortest downstream paths from node {self.start_node_id}:')
        print(f'  - {endpoint_stats["LEAF"]} paths to leaf nodes')
        print(f'  - {endpoint_stats["TARGET"]} paths to target nodes')  
        print(f'  - {endpoint_stats["BOUNDARY"]} paths to filter boundaries')
        
        return paths
    
    def find_downstream_paths(self, algorithm: str = 'dijkstra', ignore_node_id: int = 0, 
                            target_data_codes: str = '') -> List[PathResult]:
        """
        Find downstream paths using specified algorithm.
        
        Args:
            algorithm: 'dfs' for all paths, 'dijkstra' for shortest paths
            ignore_node_id: Node ID to ignore in traversal (0 = none)
            target_data_codes: Target data codes for endpoints ('15000,107' etc, '' = none)
            
        Returns:
            List of PathResult objects
        """
        algorithm = algorithm.lower()
        
        if algorithm == 'dfs':
            return self.find_downstream_paths_dfs(ignore_node_id, target_data_codes)
        elif algorithm == 'dijkstra':
            return self.find_downstream_paths_dijkstra(ignore_node_id, target_data_codes)
        else:
            raise ValueError(f"Unsupported algorithm: {algorithm}. Use 'dfs' or 'dijkstra'")
    
    def analyze_node_flags(self, paths: List[PathResult], target_data_codes: str = '') -> Dict[Tuple[int, int], str]:
        """
        Analyze node flags for all paths to classify nodes properly.
        
        Node flags:
        - S: Start node
        - E: Target endpoint (matches target_data_codes)
        - L: Leaf endpoint (no outgoing connections)
        - F: Filter boundary endpoint (has connections but not traversable)
        - C: Convergence point (multiple paths merge)
        - I: Intermediate node
        
        Args:
            paths: List of all computed paths
            target_data_codes: Target data codes to distinguish endpoint types
            
        Returns:
            Dictionary mapping (path_id, node_id) -> node_flag
        """
        node_flags = {}
        
        # Parse target data codes
        target_codes_set = self._parse_target_codes(target_data_codes)
        
        # Count how many times each node appears across all paths (for convergence detection)
        node_path_count = defaultdict(int)
        for path in paths:
            visited_in_path = set()
            visited_in_path.add(path.start_node_id)
            for _, _, start_node_id, end_node_id, _, _ in path.links:
                visited_in_path.add(end_node_id)
            
            for node_id in visited_in_path:
                node_path_count[node_id] += 1
        
        # Classify nodes for each path
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
                    # Last node in path - determine endpoint type
                    node = self.all_nodes.get(node_id)
                    
                    # Check what type of endpoint this is
                    all_neighbors = self.graph.get(node_id, [])
                    traversable_neighbors = [n for n, _, _, _ in all_neighbors if n in self.traversable_nodes]
                    
                    if len(all_neighbors) == 0:
                        node_flags[flag_key] = 'L'  # True leaf node
                    elif target_codes_set and node and node.data_code in target_codes_set:
                        node_flags[flag_key] = 'E'  # Target endpoint
                    elif len(traversable_neighbors) == 0:
                        node_flags[flag_key] = 'F'  # Filter boundary
                    else:
                        node_flags[flag_key] = 'E'  # Generic endpoint
                else:
                    # Intermediate node - check for convergence
                    if node_path_count.get(node_id, 0) > 1:
                        node_flags[flag_key] = 'C'  # Convergence point
                    else:
                        node_flags[flag_key] = 'I'  # Regular intermediate node
        
        return node_flags
    
    def save_paths_to_db(self, paths: List[PathResult], target_data_codes: str = '', 
                        algorithm: str = 'DIJKSTRA_DOWNSTREAM') -> List[int]:
        """
        Save computed paths to nw_paths and nw_path_links tables with enhanced node flags.
        
        Args:
            paths: List of PathResult objects to save
            target_data_codes: Target data codes used for classification
            algorithm: Algorithm name for the paths
            
        Returns:
            List of created path IDs in database
        """
        if not paths:
            return []
        
        # Analyze node flags across all paths
        print('Analyzing node classifications...')
        node_flags = self.analyze_node_flags(paths, target_data_codes)
        
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
                
                # Insert path links with enhanced node flags
                for seq, link_id, start_node_id, end_node_id, cost, reverse in path.links:
                    # Get node metadata for path links
                    start_node = self.all_nodes.get(start_node_id)
                    end_node = self.all_nodes.get(end_node_id)
                    
                    # Determine node flag for the end node of this link
                    end_node_flag = node_flags.get((path.path_id, end_node_id), 'I')
                    
                    # Calculate subgroup information
                    group_no = 1
                    sub_group_no = 1
                    
                    # Enhanced subgroup logic for convergence points
                    if end_node_flag == 'C':
                        # Convergence points might start new subgroups
                        sub_group_no = seq // 10 + 1  # Simple subgrouping every 10 links
                    
                    link_sql = '''
                        INSERT INTO nw_path_links (
                            path_id, seq, link_id, length, 
                            start_node_id, start_node_data_code, start_node_utility_no,
                            end_node_id, end_node_data_code, end_node_utility_no,
                            reverse, group_no, sub_group_no, node_flag
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
                        group_no,
                        sub_group_no,
                        end_node_flag
                    ])
            
            print(f'✓ Saved {len(created_path_ids)} paths to database with enhanced node classification')
            
            # Print node flag statistics
            flag_stats = defaultdict(int)
            for flag in node_flags.values():
                flag_stats[flag] += 1
            
            print('  Node classification summary:')
            flag_descriptions = {
                'S': 'Start nodes',
                'E': 'Target endpoint nodes', 
                'L': 'Leaf endpoint nodes',
                'F': 'Filter boundary nodes',
                'I': 'Intermediate nodes',
                'C': 'Convergence points'
            }
            for flag, count in sorted(flag_stats.items()):
                desc = flag_descriptions.get(flag, 'Unknown')
                print(f'    {flag} ({desc}): {count}')
            
            return created_path_ids
            
        except Exception as e:
            print(f'Error saving paths to database: {e}')
            raise

def find_network_downstream(db, algorithm: str = 'dijkstra', start_node_id: int = None, 
                          ignore_node_id: int = 0, utility_no: int = 0, toolset_id: int = 0, 
                          eq_poc_no: str = '', data_codes: str = '') -> List[int]:
    """
    Main function to find downstream paths and save to database.
    
    IMPORTANT: 
    - PATH FILTERS (utility_no, toolset_id, eq_poc_no) control traversable nodes
    - TARGET CODES (data_codes) define where paths can end
    - Start node is ALWAYS included regardless of path filters
    
    Args:
        db: Database connection
        algorithm: Algorithm to use ('dfs' or 'dijkstra')
        start_node_id: Starting node ID (always included)
        ignore_node_id: Node to ignore (0 = none)
        utility_no: PATH FILTER - Utility filter (0 = all)
        toolset_id: PATH FILTER - Toolset filter (0 = all)
        eq_poc_no: PATH FILTER - POC number filter ('' = all)
        data_codes: TARGET CODES - Where paths can end ('' = leaf/boundary only)
        
    Returns:
        List of created path IDs in database
    """
    if start_node_id is None:
        raise ValueError("start_node_id is required")
    
    algorithm = algorithm.lower()
    if algorithm not in ['dfs', 'dijkstra']:
        raise ValueError(f"Invalid algorithm '{algorithm}'. Use 'dfs' or 'dijkstra'")
    
    pathfinder = NetworkPathFinder(db)
    
    try:
        # Load network data with PATH filtering (start_node always included)
        pathfinder.load_network_data(start_node_id, utility_no, toolset_id, eq_poc_no)
        
        # Find downstream paths with specified algorithm
        print(f'\nUsing {algorithm.upper()} algorithm...')
        paths = pathfinder.find_downstream_paths(algorithm, ignore_node_id, data_codes)
        
        if not paths:
            print(f'No downstream paths found from node {start_node_id}')
            return []
        
        # Prepare algorithm name for database
        algorithm_name = f'{algorithm.upper()}_DOWNSTREAM'
        
        # Save paths to database
        created_path_ids = pathfinder.save_paths_to_db(paths, data_codes, algorithm_name)
        
        return created_path_ids
        
    except Exception as e:
        print(f'Error in downstream analysis: {e}')
        raise

# Example usage and testing
if __name__ == '__main__':
    from db import Database
    
    # Test the enhanced pathfinder
    db = Database()
    
    try:
        # Example 1: Find shortest paths using Dijkstra (default)
        print("=== Example 1: Dijkstra - Shortest paths to equipment ===")
        path_ids = find_network_downstream(
            db,
            algorithm='dijkstra',   # Find shortest paths only
            start_node_id=1709,     # Elbow node (always included)
            ignore_node_id=0,       # Don't ignore any nodes
            utility_no=13,          # Only traverse drainage utility nodes
            toolset_id=0,           # All toolsets
            eq_poc_no='',           # All POCs
            data_codes='15000'      # End at equipment nodes
        )
        print(f'Created {len(path_ids)} shortest paths: {path_ids}\n')
        
        # Example 2: Find ALL possible paths using DFS
        print("=== Example 2: DFS - ALL possible paths (be careful!) ===")
        path_ids = find_network_downstream(
            db,
            algorithm='dfs',        # Find ALL paths (can be many!)
            start_node_id=1709,     # Elbow node
            ignore_node_id=0,       # Don't ignore any nodes
            utility_no=13,          # Only traverse drainage utility nodes
            toolset_id=0,           # All toolsets
            eq_poc_no='',           # All POCs
            data_codes='15000'      # End at equipment nodes
        )
        print(f'Created {len(path_ids)} total paths: {path_ids}\n')
        
        # Example 3: Cross-utility shortest paths
        print("=== Example 3: Cross-utility shortest paths ===")
        path_ids = find_network_downstream(
            db,
            algorithm='dijkstra',   # Shortest paths only
            start_node_id=1709,     # Start node
            ignore_node_id=0,       # Don't ignore any nodes  
            utility_no=0,           # Allow all utilities (cross-utility paths)
            toolset_id=0,           # All toolsets
            eq_poc_no='',           # All POCs
            data_codes=''           # End at leaf nodes or filter boundaries
        )
        print(f'Created {len(path_ids)} shortest paths: {path_ids}\n')
        
        # Example 4: DFS with path limits (for safety)
        print("=== Example 4: DFS with manual limit check ===")
        pathfinder = NetworkPathFinder(db)
        pathfinder.load_network_data(1709, 13, 0, '')
        
        # Get a preview of how many paths DFS might find
        dijkstra_paths = pathfinder.find_downstream_paths('dijkstra', 0, '15000')
        print(f"Dijkstra found {len(dijkstra_paths)} unique endpoints")
        
        # Estimate: DFS might find many more paths if there are multiple routes to same endpoints
        estimated_dfs_paths = len(dijkstra_paths) * 5  # Rough estimate
        print(f"DFS might find ~{estimated_dfs_paths} paths (rough estimate)")
        
        if estimated_dfs_paths < 1000:  # Safety check
            dfs_paths = pathfinder.find_downstream_paths('dfs', 0, '15000')
            path_ids = pathfinder.save_paths_to_db(dfs_paths, '15000', 'DFS_DOWNSTREAM')
            print(f'Actually found {len(dfs_paths)} DFS paths: {path_ids[:10]}...')
        else:
            print("Skipping DFS due to potentially large result set. Use with caution!")
        
    except Exception as e:
        print(f"Error in examples: {e}")
    finally:
        db.close()