def find_any_path_between_nodes(self, start_node_id: int, end_node_id: int, 
                               ignore_node_ids: int = 0, max_depth: int = 1000) -> Optional[PathResult]:
    """
    Find ANY path between two specific nodes using BFS (fast, not necessarily shortest).
    
    This method prioritizes speed over optimality. It returns the first path found,
    which is typically one with fewer hops but not necessarily the shortest cost.
    
    Args:
        start_node_id: Starting node ID
        end_node_id: Target node ID
        ignore_node_ids: Node ID to ignore in traversal (0 = none)
        max_depth: Maximum path length to prevent infinite loops (default 1000)
        
    Returns:
        PathResult object if path found, None otherwise
    """
    if not self.loaded:
        raise RuntimeError('Network data not loaded. Call load_network_data() first.')
    
    if start_node_id == end_node_id:
        # Same node - return empty path
        return PathResult(
            path_id=1,
            start_node_id=start_node_id,
            end_node_id=end_node_id,
            total_cost=0.0,
            links=[]
        )
    
    if start_node_id not in self.traversable_nodes or end_node_id not in self.traversable_nodes:
        print(f'Start node {start_node_id} or end node {end_node_id} not in traversable nodes')
        return None
    
    print(f'Finding any path from node {start_node_id} to node {end_node_id} using BFS...')
    
    # BFS with path tracking
    queue = deque([(start_node_id, [], 0.0, 0)])  # (node_id, path_links, total_cost, depth)
    visited = {start_node_id}
    
    while queue:
        current_node, path_links, total_cost, depth = queue.popleft()
        
        # Depth limit check
        if depth > max_depth:
            continue
        
        # Check if we reached the target
        if current_node == end_node_id:
            print(f'✓ Found path from {start_node_id} to {end_node_id} with cost {total_cost:.2f}')
            return self._build_path_result(1, end_node_id, total_cost, path_links)
        
        # Explore neighbors
        neighbors = self._get_traversable_neighbors(current_node, ignore_node_ids, visited)
        
        for neighbor_id, link_id, cost, reverse in neighbors:
            if neighbor_id not in visited:
                visited.add(neighbor_id)
                new_path_links = path_links + [(link_id, current_node, neighbor_id, 0, cost, reverse)]
                new_total_cost = total_cost + cost
                queue.append((neighbor_id, new_path_links, new_total_cost, depth + 1))
    
    print(f'✗ No path found from node {start_node_id} to node {end_node_id}')
    return None

def find_shortest_path_between_nodes(self, start_node_id: int, end_node_id: int, 
                                   ignore_node_ids: int = 0) -> Optional[PathResult]:
    """
    Find the shortest path between two specific nodes using Dijkstra's algorithm.
    
    This method finds the optimal (lowest cost) path between the specified nodes.
    
    Args:
        start_node_id: Starting node ID
        end_node_id: Target node ID
        ignore_node_ids: Node ID to ignore in traversal (0 = none)
        
    Returns:
        PathResult object if path found, None otherwise
    """
    if not self.loaded:
        raise RuntimeError('Network data not loaded. Call load_network_data() first.')
    
    if start_node_id == end_node_id:
        # Same node - return empty path
        return PathResult(
            path_id=1,
            start_node_id=start_node_id,
            end_node_id=end_node_id,
            total_cost=0.0,
            links=[]
        )
    
    if start_node_id not in self.traversable_nodes or end_node_id not in self.traversable_nodes:
        print(f'Start node {start_node_id} or end node {end_node_id} not in traversable nodes')
        return None
    
    print(f'Finding shortest path from node {start_node_id} to node {end_node_id} using Dijkstra...')
    
    # Dijkstra's algorithm with early termination
    distances = {start_node_id: 0.0}
    previous = {}  # node_id -> (previous_node_id, link_id, reverse)
    visited = set()
    heap = [(0.0, start_node_id)]
    
    while heap:
        current_dist, current_node = heapq.heappop(heap)
        
        if current_node in visited:
            continue
        
        visited.add(current_node)
        
        # Early termination - we found the shortest path to target
        if current_node == end_node_id:
            break
        
        # Skip ignored node
        if current_node == ignore_node_ids:
            continue
        
        # Explore neighbors
        neighbors = self._get_traversable_neighbors(current_node, ignore_node_ids, visited)
        
        for neighbor_id, link_id, cost, reverse in neighbors:
            new_dist = current_dist + cost
            
            if neighbor_id not in distances or new_dist < distances[neighbor_id]:
                distances[neighbor_id] = new_dist
                previous[neighbor_id] = (current_node, link_id, reverse)
                heapq.heappush(heap, (new_dist, neighbor_id))
    
    # Check if target was reached
    if end_node_id not in distances:
        print(f'✗ No path found from node {start_node_id} to node {end_node_id}')
        return None
    
    # Reconstruct the shortest path
    path_links = []
    current = end_node_id
    
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
    
    total_cost = distances[end_node_id]
    print(f'✓ Found shortest path from {start_node_id} to {end_node_id} with cost {total_cost:.2f}')
    
    return self._build_path_result(1, end_node_id, total_cost, path_links)
