#!/usr/bin/env python3

"""
Fixed Network Path Finder using Dijkstra algorithm for downstream analysis.

Key fixes:
1. data_codes now only affects TARGET nodes (where paths can end), not path filtering
2. Path filtering (utility_no, toolset_id, eq_poc_no) affects traversable nodes
3. Always includes start_node_id in filtered nodes regardless of filters
4. Properly implements 3 ways paths can end: leaf nodes, target nodes, or filter boundaries
5. Enhanced node classification with proper convergence detection
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
    Efficient network path finder using Dijkstra algorithm.
    
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
    
    def _validate_search_parameters(self, ignore_node_id: int, target_data_codes: str) -> Tuple[set, str]:
        """
        Common validation and parsing for search parameters.
        
        Returns:
            Tuple of (target_codes_set, algorithm_description)
        """
        if not self.loaded:
            raise RuntimeError('Network data not loaded. Call load_network_data() first.')
        
        if self.start_node_id not in self.all_nodes:
            raise ValueError(f'Start node {self.start_node_id} not found in loaded network')
        
        # Parse target data codes for TARGET endpoints
        target_codes_set = set()
        if target_data_codes and target_data_codes.strip() and target_data_codes != '0':
            for code in target_data_codes.split(','):
                code = code.strip()
                if code.isdigit():
                    target_codes_set.add(int(code))
        
        return target_codes_set, f'Ignore: {ignore_node_id}, Targets: {target_codes_set if target_codes_set else "leaf/boundary"}'
    
    def _is_endpoint(self, node_id: int, target_codes_set: set, visited: set, ignore_node_id: int) -> Tuple[str, str]:
        """
        Determine if a node should be considered an endpoint and what type.
        
        Returns:
            Tuple of (endpoint_type, reason) or (None, '') if not an endpoint
        """
        if node_id == self.start_node_id or node_id == ignore_node_id:
            return None, ''
        
        current_node_obj = self.all_nodes.get(node_id)
        
        # Get all neighbors and traversable neighbors
        all_neighbors = self.graph.get(node_id, [])
        traversable_neighbors = []
        
        for neighbor_id, link_id, cost, reverse in all_neighbors:
            if neighbor_id == ignore_node_id or neighbor_id in visited:
                continue
            if neighbor_id in self.traversable_nodes:
                traversable_neighbors.append((neighbor_id, link_id, cost, reverse))
        
        # Rule 1: LEAF NODE - No outgoing connections in full network
        if len(all_neighbors) == 0:
            return 'LEAF', 'No outgoing connections (true leaf)'
        
        # Rule 2: TARGET NODE - Matches target data codes
        elif target_codes_set and current_node_obj and current_node_obj.data_code in target_codes_set:
            return 'TARGET', f'Matches target data code {current_node_obj.data_code}'
        
        # Rule 3: FILTER BOUNDARY - No more traversable neighbors
        elif len(traversable_neighbors) == 0 and len(all_neighbors) > 0:
            return 'BOUNDARY', f'Filter boundary ({len(all_neighbors)} total neighbors, 0 traversable)'
        
        return None, ''
    
    def _get_traversable_neighbors(self, node_id: int, visited: set, ignore_node_id: int) -> List[Tuple[int, int, float, bool]]:
        """
        Get list of traversable neighbors for a node.
        
        Returns:
            List of (neighbor_id, link_id, cost, reverse) tuples
        """
        neighbors = []
        for neighbor_id, link_id, cost, reverse in self.graph.get(node_id, []):
            if neighbor_id == ignore_node_id or neighbor_id in visited:
                continue
            if neighbor_id in self.traversable_nodes:
                neighbors.append((neighbor_id, link_id, cost, reverse))
        return neighbors
    
    def _reconstruct_path(self, endpoint_node: int, previous: dict, path_id: int) -> Optional[PathResult]:
        """
        Reconstruct a path from start to endpoint using the previous mapping.
        
        Returns:
            PathResult or None if no valid path
        """
        if endpoint_node not in previous and endpoint_node != self.start_node_id:
            return None
        
        path_links = []
        current = endpoint_node
        total_cost = 0.0
        
        while current in previous:
            prev_node, link_id, reverse, edge_cost = previous[current]
            
            link = self.links[link_id]
            path_links.append((
                0,  # seq will be set later
                link_id, 
                prev_node if not reverse else current,
                current if not reverse else prev_node, 
                edge_cost, 
                reverse
            ))
            total_cost += edge_cost
            current = prev_node
        
        # Reverse to get correct order (start to end) and set sequence numbers
        path_links.reverse()
        for i in range(len(path_links)):
            seq, link_id, start_node, end_node, cost, reverse = path_links[i]
            path_links[i] = (i + 1, link_id, start_node, end_node, cost, reverse)
        
        if path_links or endpoint_node == self.start_node_id:  # Valid path or single-node path
            return PathResult(
                path_id=path_id,
                start_node_id=self.start_node_id,
                end_node_id=endpoint_node,
                total_cost=total_cost,
                links=path_links
            )
        
        return None
    
    def _print_search_results(self, algorithm: str, paths: List[PathResult], endpoint_stats: dict):
        """
        Print search results and debugging information.
        """
        print(f'\n✓ Found {len(paths)} downstream paths using {algorithm} from node {self.start_node_id}:')
        print(f'  - {endpoint_stats.get("LEAF", 0)} paths to leaf nodes')
        print(f'  - {endpoint_stats.get("TARGET", 0)} paths to target nodes')  
        print(f'  - {endpoint_stats.get("BOUNDARY", 0)} paths to filter boundaries')
        
        if len(paths) == 0:
            print('\n⚠ No paths found - debugging info:')
            print(f'  - Start node {self.start_node_id} exists: {self.start_node_id in self.all_nodes}')
            print(f'  - Start node is traversable: {self.start_node_id in self.traversable_nodes}')
            start_neighbors = self.graph.get(self.start_node_id, [])
            print(f'  - Start node has {len(start_neighbors)} total neighbors')
            
            traversable_start_neighbors = [n for n, _, _, _ in start_neighbors if n in self.traversable_nodes]
            print(f'  - Start node has {len(traversable_start_neighbors)} traversable neighbors')
            
            if traversable_start_neighbors:
                print(f'  - Traversable neighbors: {traversable_start_neighbors[:5]}')

    def find_downstream_paths(self, ignore_node_id: int = 0, target_data_codes: str = '', 
                             algorithm: str = 'dijkstra') -> List[PathResult]:
        """
        Find downstream paths using the specified algorithm.
        
        Args:
            ignore_node_id: Node ID to ignore in traversal (0 = none)
            target_data_codes: Target data codes for endpoints ('15000,107' etc, '' = none)
            algorithm: Algorithm to use ('dijkstra' or 'dfs')
            
        Returns:
            List of PathResult objects for each valid path endpoint
        """
        algorithm = algorithm.lower()
        if algorithm == 'dijkstra':
            return self.find_downstream_paths_dijkstra(ignore_node_id, target_data_codes)
        elif algorithm == 'dfs':
            return self.find_downstream_paths_dfs(ignore_node_id, target_data_codes)
        else:
            raise ValueError(f"Unsupported algorithm: {algorithm}. Use 'dijkstra' or 'dfs'")

    def find_downstream_paths_dijkstra(self, ignore_node_id: int = 0, target_data_codes: str = '') -> List[PathResult]:
        """
        Find shortest downstream paths from start_node_id using Dijkstra's algorithm.
        
        This finds the SHORTEST path to each reachable endpoint.
        
        Args:
            ignore_node_id: Node ID to ignore in traversal (0 = none)
            target_data_codes: Target data codes for endpoints ('15000,107' etc, '' = none)
            
        Returns:
            List of PathResult objects for shortest paths to each endpoint
        """
        target_codes_set, desc = self._validate_search_parameters(ignore_node_id, target_data_codes)
        
        print(f'Finding shortest downstream paths (Dijkstra) from node {self.start_node_id}:')
        print(f'  - {desc}')
        print(f'  - Traversable nodes: {len(self.traversable_nodes)}')
        
        # Dijkstra's algorithm with path tracking
        distances = {self.start_node_id: 0.0}
        previous = {}  # node_id -> (previous_node_id, link_id, reverse, edge_cost)
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
            
            # Check if current node should be an endpoint
            endpoint_type, endpoint_reason = self._is_endpoint(current_node, target_codes_set, visited, ignore_node_id)
            if endpoint_type:
                potential_endpoints.append((current_node, endpoint_type, endpoint_reason))
                print(f'  Found {endpoint_type} endpoint: node {current_node} - {endpoint_reason}')
            
            # Continue exploring traversable neighbors
            traversable_neighbors = self._get_traversable_neighbors(current_node, visited, ignore_node_id)
            for neighbor_id, link_id, cost, reverse in traversable_neighbors:
                new_dist = current_dist + cost
                
                if neighbor_id not in distances or new_dist < distances[neighbor_id]:
                    distances[neighbor_id] = new_dist
                    previous[neighbor_id] = (current_node, link_id, reverse, cost)
                    heapq.heappush(heap, (new_dist, neighbor_id))
        
        # Build paths to all valid endpoints
        paths = []
        path_id = 1
        endpoint_stats = {'LEAF': 0, 'TARGET': 0, 'BOUNDARY': 0}
        
        for endpoint_node, endpoint_type, reason in potential_endpoints:
            path = self._reconstruct_path(endpoint_node, previous, path_id)
            if path:
                paths.append(path)
                path_id += 1
                endpoint_stats[endpoint_type] += 1
            else:
                print(f'  Warning: Could not reconstruct path to endpoint {endpoint_node}')
        
        self._print_search_results('Dijkstra', paths, endpoint_stats)
        return paths

    def find_downstream_paths_dfs(self, ignore_node_id: int = 0, target_data_codes: str = '') -> List[PathResult]:
        """
        Find ALL downstream paths from start_node_id using Depth-First Search.
        
        This finds ALL possible paths to each reachable endpoint (can be many paths per endpoint).
        
        Args:
            ignore_node_id: Node ID to ignore in traversal (0 = none)
            target_data_codes: Target data codes for endpoints ('15000,107' etc, '' = none)
            
        Returns:
            List of PathResult objects for all possible paths to endpoints
        """
        target_codes_set, desc = self._validate_search_parameters(ignore_node_id, target_data_codes)
        
        print(f'Finding all downstream paths (DFS) from node {self.start_node_id}:')
        print(f'  - {desc}')
        print(f'  - Traversable nodes: {len(self.traversable_nodes)}')
        
        all_paths = []
        path_id = 1
        endpoint_stats = {'LEAF': 0, 'TARGET': 0, 'BOUNDARY': 0}
        
        def dfs_recursive(current_node: int, visited: set, path_links: List, total_cost: float):
            """
            Recursive DFS to find all paths.
            """
            nonlocal path_id, endpoint_stats
            
            # Skip ignored node
            if current_node == ignore_node_id:
                return
            
            # Check if current node should be an endpoint
            endpoint_type, endpoint_reason = self._is_endpoint(current_node, target_codes_set, visited, ignore_node_id)
            if endpoint_type and current_node != self.start_node_id:
                # Found an endpoint - create a path
                path = PathResult(
                    path_id=path_id,
                    start_node_id=self.start_node_id,
                    end_node_id=current_node,
                    total_cost=total_cost,
                    links=[(i+1, link_id, start_node, end_node, cost, reverse) 
                           for i, (link_id, start_node, end_node, cost, reverse) in enumerate(path_links)]
                )
                all_paths.append(path)
                path_id += 1
                endpoint_stats[endpoint_type] += 1
                print(f'  Found {endpoint_type} endpoint: node {current_node} - {endpoint_reason} (path {path.path_id})')
            
            # Continue exploring if not a leaf endpoint
            # (TARGET and BOUNDARY endpoints can still have paths continuing through them)
            if endpoint_type != 'LEAF':
                traversable_neighbors = self._get_traversable_neighbors(current_node, visited, ignore_node_id)
                
                for neighbor_id, link_id, cost, reverse in traversable_neighbors:
                    if neighbor_id not in visited:  # Avoid cycles
                        # Add this edge to the path
                        link = self.links[link_id]
                        new_path_links = path_links + [(
                            link_id,
                            current_node if not reverse else neighbor_id,
                            neighbor_id if not reverse else current_node,
                            cost,
                            reverse
                        )]
                        
                        # Recursively explore
                        new_visited = visited.copy()
                        new_visited.add(neighbor_id)
                        dfs_recursive(neighbor_id, new_visited, new_path_links, total_cost + cost)
        
        # Start DFS from the start node
        initial_visited = {self.start_node_id}
        dfs_recursive(self.start_node_id, initial_visited, [], 0.0)
        
        self._print_search_results('DFS', all_paths, endpoint_stats)
        return all_paths
    
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
        target_codes_set = set()
        if target_data_codes and target_data_codes.strip() and target_data_codes != '0':
            for code in target_data_codes.split(','):
                code = code.strip()
                if code.isdigit():
                    target_codes_set.add(int(code))
        
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
            algorithm: Algorithm name for the paths (e.g., 'DIJKSTRA_DOWNSTREAM', 'DFS_DOWNSTREAM')
            
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

def find_network_downstream(db, start_node_id: int, algorithm: str = 'dijkstra', 
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
        start_node_id: Starting node ID (always included)
        algorithm: Algorithm to use ('dijkstra' for shortest paths, 'dfs' for all paths)
        ignore_node_id: Node to ignore (0 = none)
        utility_no: PATH FILTER - Utility filter (0 = all)
        toolset_id: PATH FILTER - Toolset filter (0 = all)
        eq_poc_no: PATH FILTER - POC number filter ('' = all)
        data_codes: TARGET CODES - Where paths can end ('' = leaf/boundary only)
        
    Returns:
        List of created path IDs in database
    """
    # Validate algorithm parameter
    algorithm = algorithm.lower()
    if algorithm not in ['dijkstra', 'dfs']:
        raise ValueError(f"Unsupported algorithm: {algorithm}. Use 'dijkstra' or 'dfs'")
    
    pathfinder = NetworkPathFinder(db)
    
    try:
        # Load network data with PATH filtering (start_node always included)
        pathfinder.load_network_data(start_node_id, utility_no, toolset_id, eq_poc_no)
        
        # Find downstream paths with TARGET filtering using specified algorithm
        paths = pathfinder.find_downstream_paths(ignore_node_id, data_codes, algorithm)
        
        if not paths:
            print(f'No downstream paths found from node {start_node_id} using {algorithm}')
            return []
        
        # Determine algorithm name for database
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
    
    # Test the pathfinder
    db = Database()
    
    try:
        # Example 1: Find shortest paths using Dijkstra
        print("=== Example 1: Dijkstra - Shortest paths ===")
        path_ids = find_network_downstream(
            db,
            start_node_id=1709,     # Elbow node (always included)
            algorithm='dijkstra',   # Find shortest paths
            ignore_node_id=0,       # Don't ignore any nodes
            utility_no=13,          # Only traverse drainage utility nodes
            toolset_id=0,           # All toolsets
            eq_poc_no='',           # All POCs
            data_codes='15000'      # End at equipment nodes
        )
        print(f'Created {len(path_ids)} shortest paths: {path_ids}\n')
        
        # Example 2: Find all possible paths using DFS
        print("=== Example 2: DFS - All possible paths ===")
        path_ids = find_network_downstream(
            db,
            start_node_id=1709,     # Node from dirty water utility (108)
            algorithm='dfs',        # Find all paths
            ignore_node_id=0,       # Don't ignore any nodes  
            utility_no=0,           # Allow all utilities (cross-utility paths)
            toolset_id=0,           # All toolsets
            eq_poc_no='',           # All POCs
            data_codes=''           # End at leaf nodes or filter boundaries
        )
        print(f'Created {len(path_ids)} total paths: {path_ids}\n')
        
        # Example 3: Compare algorithms
        print("=== Example 3: Algorithm Comparison ===")
        
        # Test with same parameters using both algorithms
        test_params = {
            'start_node_id': 1709,
            'ignore_node_id': 0,
            'utility_no': 13,
            'toolset_id': 0,
            'eq_poc_no': '',
            'data_codes': '15000'
        }
        
        print("Using Dijkstra:")
        dijkstra_paths = find_network_downstream(db, algorithm='dijkstra', **test_params)
        
        print("\nUsing DFS:")
        dfs_paths = find_network_downstream(db, algorithm='dfs', **test_params)
        
        print(f"\nComparison:")
        print(f"  Dijkstra found {len(dijkstra_paths)} paths (shortest to each endpoint)")
        print(f"  DFS found {len(dfs_paths)} paths (all possible paths)")
        
    finally:
        db.close()