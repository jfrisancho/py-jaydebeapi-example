def find_downstream_path_dijkstra(
        self,
        raw_ignore_node_ids: str = '',
        target_data_codes: str = ''
) -> List[PathResult]:
    """
    Find shortest downstream path using Dijkstra's algorithm.
    
    This finds the shortest path to the first reachable endpoint.
    If you need multiple paths, consider using a different algorithm.
    
    Params:
        :param str raw_ignore_node_ids: Node IDs to ignore in traversal ('12345', '12345,12349', '' = none)
        :param str target_data_codes: Target data codes for endpoints ('15000,107', '' = none)
        
    Returns:
        List with single PathResult object for the shortest path, or empty list if no path found
    """
    if not self.loaded:
        raise RuntimeError('Network data not loaded. Call load_network_data() first.')
    
    target_codes_set = self._parse_target_codes(target_data_codes)
    
    print(f'Finding shortest downstream path using Dijkstra from node {self.start_node_id}:')
    print(f'  - Ignore nodes: {raw_ignore_node_ids}')
    print(f'  - Target data codes: {target_codes_set if target_codes_set else "None (leaf/boundary only)"}')
    
    # Dijkstra's algorithm with early termination
    distances = {self.start_node_id: 0.0}
    previous = {}  # node_id -> (previous_node_id, link_id, reverse)
    visited = set()
    heap = [(0.0, self.start_node_id)]
    ignore_node_ids = [int(item) for item in raw_ignore_node_ids.split(',') if item.strip()] if raw_ignore_node_ids else []
    
    while heap:
        current_dist, current_node = heapq.heappop(heap)
        
        if current_node in visited:
            continue
            
        visited.add(current_node)
        
        # Skip ignored node (but don't mark as endpoint)
        if current_node in ignore_node_ids:
            continue
        
        # Check if current node should be an endpoint (except start node)
        if current_node != self.start_node_id:
            is_endpoint, endpoint_type, reason = self._is_endpoint(current_node, target_codes_set, ignore_node_ids)
            
            if is_endpoint:
                print(f'  Found {endpoint_type} endpoint: node {current_node} - {reason}')
                
                # Reconstruct the shortest path to this endpoint
                path_links = []
                current = current_node
                
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
                    path_result = self._build_path_result(1, current_node, current_dist, path_links)
                    
                    print(f'\n✓ Dijkstra found shortest downstream path from node {self.start_node_id}:')
                    print(f'  - Path to {endpoint_type.lower()} node {current_node}')
                    print(f'  - Distance: {current_dist}')
                    
                    return [path_result]
        
        # Explore traversable neighbors
        neighbors = self._get_traversable_neighbors(current_node, ignore_node_ids, visited)
        
        for neighbor_id, link_id, cost, reverse in neighbors:
            new_dist = current_dist + cost
            
            if neighbor_id not in distances or new_dist < distances[neighbor_id]:
                distances[neighbor_id] = new_dist
                previous[neighbor_id] = (current_node, link_id, reverse)
                heapq.heappush(heap, (new_dist, neighbor_id))
    
    # No endpoint found
    print(f'\n✗ No reachable endpoints found from node {self.start_node_id}')
    return []
