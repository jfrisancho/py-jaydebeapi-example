"""
Path storage and retrieval service for managing path definitions and pathfinding.
Handles path discovery, storage, and retrieval operations.
"""

import json
import hashlib
import logging
from typing import Optional, Dict, Any, List, Tuple
from datetime import datetime
import heapq
from collections import defaultdict, deque


class PathService:
    """Service for path discovery, storage, and retrieval."""
    
    def __init__(self, db_connection):
        self.db = db_connection
        self.logger = logging.getLogger(__name__)
        
        # Cache for network topology
        self._network_cache = {}
        self._adjacency_cache = {}
        self._node_info_cache = {}
        
        # Path finding configuration
        self.max_path_length = 50  # Maximum number of hops
        self.max_search_time = 10  # Maximum seconds for pathfinding
    
    def find_path(self, from_poc: Dict, to_poc: Dict, config) -> Optional[Dict]:
        """
        Find a path between two PoCs using Dijkstra's algorithm.
        Returns path information if found, None otherwise.
        """
        start_node = from_poc['node_id']
        end_node = to_poc['node_id']
        
        if start_node == end_node:
            self.logger.debug("Start and end nodes are the same")
            return None
        
        try:
            # Find path using Dijkstra's algorithm
            path_result = self._find_dijkstra_path(start_node, end_node, config)
            
            if not path_result:
                return None
            
            # Enrich path with additional information
            enriched_path = self._enrich_path_data(path_result, from_poc, to_poc, config)
            
            return enriched_path
            
        except Exception as e:
            self.logger.error(f"Error finding path from {start_node} to {end_node}: {str(e)}")
            return None
    
    def _find_dijkstra_path(self, start_node: int, end_node: int, config) -> Optional[Dict]:
        """Find shortest path using Dijkstra's algorithm."""
        # Load network adjacency if not cached
        if not self._adjacency_cache:
            self._load_network_adjacency(config)
        
        if start_node not in self._adjacency_cache or end_node not in self._adjacency_cache:
            self.logger.debug(f"Nodes {start_node} or {end_node} not in network")
            return None
        
        # Dijkstra's algorithm implementation
        distances = {start_node: 0}
        previous = {}
        visited = set()
        pq = [(0, start_node)]
        
        start_time = datetime.now()
        
        while pq:
            # Check timeout
            if (datetime.now() - start_time).seconds > self.max_search_time:
                self.logger.warning(f"Path search timeout for {start_node} -> {end_node}")
                break
            
            current_distance, current_node = heapq.heappop(pq)
            
            if current_node in visited:
                continue
            
            visited.add(current_node)
            
            # Found target
            if current_node == end_node:
                path = self._reconstruct_path(previous, start_node, end_node)
                return {
                    'path_nodes': path,
                    'path_length': len(path),
                    'total_cost': current_distance,
                    'search_time_ms': (datetime.now() - start_time).total_seconds() * 1000
                }
            
            # Check path length limit
            if len(self._reconstruct_path(previous, start_node, current_node)) >= self.max_path_length:
                continue
            
            # Explore neighbors
            for neighbor, edge_cost in self._adjacency_cache.get(current_node, []):
                if neighbor in visited:
                    continue
                
                new_distance = current_distance + edge_cost
                
                if neighbor not in distances or new_distance < distances[neighbor]:
                    distances[neighbor] = new_distance
                    previous[neighbor] = current_node
                    heapq.heappush(pq, (new_distance, neighbor))
        
        return None
    
    def _reconstruct_path(self, previous: Dict, start: int, end: int) -> List[int]:
        """Reconstruct path from Dijkstra's previous nodes."""
        path = []
        current = end
        
        while current is not None:
            path.append(current)
            current = previous.get(current)
            
            if current == start:
                path.append(start)
                break
        
        return list(reversed(path))
    
    def _load_network_adjacency(self, config) -> None:
        """Load network adjacency list from database."""
        self.logger.info("Loading network adjacency data")
        
        # Build adjacency list from connections
        query = """
        SELECT conn.from_poc_id, conn.to_poc_id, 
               from_poc.node_id as from_node, to_poc.node_id as to_node,
               1.0 as cost
        FROM tb_equipment_poc_connections conn
        JOIN tb_equipment_pocs from_poc ON conn.from_poc_id = from_poc.id
        JOIN tb_equipment_pocs to_poc ON conn.to_poc_id = to_poc.id
        WHERE conn.is_valid = 1 
        AND from_poc.is_active = 1 
        AND to_poc.is_active = 1
        """
        
        # Add filters based on config
        conditions = []
        params = []
        
        if config.fab:
            query += """
            AND EXISTS (
                SELECT 1 FROM tb_equipments e1 
                JOIN tb_toolsets t1 ON e1.toolset = t1.code
                WHERE e1.id = from_poc.equipment_id AND t1.fab = %s
            )
            AND EXISTS (
                SELECT 1 FROM tb_equipments e2 
                JOIN tb_toolsets t2 ON e2.toolset = t2.code
                WHERE e2.id = to_poc.equipment_id AND t2.fab = %s
            )
            """
            params.extend([config.fab, config.fab])
        
        if config.model_no:
            query += """
            AND EXISTS (
                SELECT 1 FROM tb_equipments e1 
                JOIN tb_toolsets t1 ON e1.toolset = t1.code
                WHERE e1.id = from_poc.equipment_id AND t1.model_no = %s
            )
            AND EXISTS (
                SELECT 1 FROM tb_equipments e2 
                JOIN tb_toolsets t2 ON e2.toolset = t2.code
                WHERE e2.id = to_poc.equipment_id AND t2.model_no = %s
            )
            """
            params.extend([config.model_no, config.model_no])
        
        if config.phase_no:
            query += """
            AND EXISTS (
                SELECT 1 FROM tb_equipments e1 
                JOIN tb_toolsets t1 ON e1.toolset = t1.code
                WHERE e1.id = from_poc.equipment_id AND t1.phase_no = %s
            )
            AND EXISTS (
                SELECT 1 FROM tb_equipments e2 
                JOIN tb_toolsets t2 ON e2.toolset = t2.code
                WHERE e2.id = to_poc.equipment_id AND t2.phase_no = %s
            )
            """
            params.extend([config.phase_no, config.phase_no])
        
        with self.db.cursor() as cursor:
            cursor.execute(query, params)
            connections = cursor.fetchall()
        
        # Build bidirectional adjacency list
        adjacency = defaultdict(list)
        
        for conn in connections:
            from_node, to_node, cost = conn[2], conn[3], conn[4]
            
            # Add both directions (undirected graph)
            adjacency[from_node].append((to_node, cost))
            adjacency[to_node].append((from_node, cost))
        
        self._adjacency_cache = dict(adjacency)
        self.logger.info(f"Loaded adjacency data for {len(self._adjacency_cache)} nodes")
    
    def _enrich_path_data(self, path_result: Dict, from_poc: Dict, to_poc: Dict, config) -> Dict:
        """Enrich path data with additional context information."""
        path_nodes = path_result['path_nodes']
        
        # Get node information for the path
        node_info = self._get_nodes_info(path_nodes)
        
        # Calculate path metrics
        metrics = self._calculate_path_metrics(path_nodes, node_info)
        
        # Build path context
        path_context = {
            'start_poc': {
                'node_id': from_poc['node_id'],
                'equipment_id': from_poc['equipment_id'],
                'utility_no': from_poc.get('utility_no'),
                'reference': from_poc.get('reference'),
                'flow': from_poc.get('flow')
            },
            'end_poc': {
                'node_id': to_poc['node_id'],
                'equipment_id': to_poc['equipment_id'],
                'utility_no': to_poc.get('utility_no'),
                'reference': to_poc.get('reference'),
                'flow': to_poc.get('flow')
            },
            'path_sequence': path_nodes,
            'node_details': node_info,
            'filters': {
                'fab': config.fab,
                'model_no': config.model_no,
                'phase_no': config.phase_no,
                'toolset': config.toolset
            }
        }
        
        # Collect scope information
        data_codes = set()
        utilities = set()
        references = set()
        
        for node_id in path_nodes:
            node = node_info.get(node_id, {})
            if node.get('data_code'):
                data_codes.add(node['data_code'])
            if node.get('utility_no'):
                utilities.add(node['utility_no'])
            if node.get('reference'):
                references.add(node['reference'])
        
        return {
            'path_nodes': path_nodes,
            'path_length': len(path_nodes),
            'link_count': len(path_nodes) - 1 if len(path_nodes) > 1 else 0,
            'total_cost': path_result['total_cost'],
            'total_length_mm': metrics.get('total_length_mm', 0.0),
            'search_time_ms': path_result['search_time_ms'],
            'data_codes_scope': list(data_codes),
            'utilities_scope': list(utilities),
            'references_scope': list(references),
            'path_context': path_context,
            'source_type': 'RANDOM',
            'scope': 'CONNECTIVITY'
        }
    
    def _get_nodes_info(self, node_ids: List[int]) -> Dict[int, Dict]:
        """Get detailed information for a list of node IDs."""
        if not node_ids:
            return {}
        
        # Check cache first
        cached_nodes = {}
        missing_nodes = []
        
        for node_id in node_ids:
            if node_id in self._node_info_cache:
                cached_nodes[node_id] = self._node_info_cache[node_id]
            else:
                missing_nodes.append(node_id)
        
        if not missing_nodes:
            return cached_nodes
        
        # Query missing nodes
        placeholders = ','.join(['%s'] * len(missing_nodes))
        query = f"""
        SELECT p.node_id, p.equipment_id, p.utility_no, p.reference, p.flow,
               p.markers, p.is_loopback, e.data_code, e.category_no, e.kind,
               e.guid as equipment_guid
        FROM tb_equipment_pocs p
        JOIN tb_equipments e ON p.equipment_id = e.id
        WHERE p.node_id IN ({placeholders})
        """
        
        with self.db.cursor() as cursor:
            cursor.execute(query, missing_nodes)
            results = cursor.fetchall()
        
        # Process results
        node_info = cached_nodes.copy()
        
        for row in results:
            node_id = row[0]
            node_data = {
                'node_id': row[0],
                'equipment_id': row[1],
                'utility_no': row[2],
                'reference': row[3],
                'flow': row[4],
                'markers': row[5],
                'is_loopback': bool(row[6]),
                'data_code': row[7],
                'category_no': row[8],
                'kind': row[9],
                'equipment_guid': row[10]
            }
            
            node_info[node_id] = node_data
            self._node_info_cache[node_id] = node_data
        
        return node_info
    
    def _calculate_path_metrics(self, path_nodes: List[int], node_info: Dict) -> Dict:
        """Calculate path metrics like total length."""
        # For now, return basic metrics
        # In a real implementation, you would calculate actual physical lengths
        return {
            'total_length_mm': len(path_nodes) * 1000.0,  # Placeholder calculation
            'equipment_count': len(set(node_info.get(n, {}).get('equipment_id') for n in path_nodes)),
            'utility_changes': self._count_utility_changes(path_nodes, node_info)
        }
    
    def _count_utility_changes(self, path_nodes: List[int], node_info: Dict) -> int:
        """Count the number of utility changes along the path."""
        changes = 0
        prev_utility = None
        
        for node_id in path_nodes:
            node = node_info.get(node_id, {})
            current_utility = node.get('utility_no')
            
            if prev_utility is not None and current_utility != prev_utility:
                changes += 1
            
            prev_utility = current_utility
        
        return changes
    
    def store_path_definition(self, run_id: str, path_data: Dict, config) -> int:
        """Store a path definition in the database."""
        # Generate path hash
        path_hash = self._generate_path_hash(path_data)
        
        # Check if path already exists
        existing_id = self._get_existing_path_id(path_hash)
        if existing_id:
            # Update attempt record with existing path
            self._link_attempt_to_path(run_id, existing_id, path_data)
            return existing_id
        
        # Insert new path definition
        query = """
        INSERT INTO tb_path_definitions (
            path_hash, source_type, scope, target_fab, target_model_no,
            target_phase_no, target_data_codes, target_utilities, target_references,
            node_count, link_count, total_length_mm, coverage,
            data_codes_scope, utilities_scope, references_scope, path_context
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """
        
        with self.db.cursor() as cursor:
            cursor.execute(query, (
                path_hash,
                path_data.get('source_type', 'RANDOM'),
                path_data.get('scope', 'CONNECTIVITY'),
                config.fab,
                config.model_no,
                config.phase_no,
                json.dumps(path_data.get('data_codes_scope', [])),
                json.dumps(path_data.get('utilities_scope', [])),
                json.dumps(path_data.get('references_scope', [])),
                path_data.get('path_length', 0),
                path_data.get('link_count', 0),
                path_data.get('total_length_mm', 0.0),
                0.0,  # Coverage will be calculated by coverage service
                json.dumps(path_data.get('data_codes_scope', [])),
                json.dumps(path_data.get('utilities_scope', [])),
                json.dumps(path_data.get('references_scope', [])),
                json.dumps(path_data.get('path_context', {}))
            ))
            
            path_definition_id = cursor.lastrowid
        
        self.db.commit()
        
        # Link attempt to new path
        self._link_attempt_to_path(run_id, path_definition_id, path_data)
        
        self.logger.debug(f"Stored new path definition {path_definition_id} with hash {path_hash}")
        return path_definition_id
    
    def _generate_path_hash(self, path_data: Dict) -> str:
        """Generate a unique hash for the path."""
        # Create a string representation of the path
        path_nodes = path_data.get('path_nodes', [])
        path_str = '->'.join(map(str, sorted(path_nodes)))
        
        # Include key attributes in hash
        hash_data = f"{path_str}|{path_data.get('source_type', '')}|{path_data.get('scope', '')}"
        
        return hashlib.md5(hash_data.encode()).hexdigest()
    
    def _get_existing_path_id(self, path_hash: str) -> Optional[int]:
        """Check if a path with this hash already exists."""
        query = "SELECT id FROM tb_path_definitions WHERE path_hash = %s"
        
        with self.db.cursor() as cursor:
            cursor.execute(query, (path_hash,))
            result = cursor.fetchone()
            return result[0] if result else None
    
    def _link_attempt_to_path(self, run_id: str, path_definition_id: int, path_data: Dict) -> None:
        """Link the most recent attempt to a path definition."""
        query = """
        UPDATE tb_attempt_paths 
        SET path_definition_id = %s, tested_at = %s, cost = %s
        WHERE run_id = %s AND path_definition_id IS NULL
        ORDER BY picked_at DESC 
        LIMIT 1
        """
        
        with self.db.cursor() as cursor:
            cursor.execute(query, (
                path_definition_id,
                datetime.now(),
                path_data.get('total_cost', 0.0),
                run_id
            ))
        self.db.commit()
    
    def get_path_definition(self, path_definition_id: int) -> Optional[Dict]:
        """Retrieve a path definition by ID."""
        query = """
        SELECT path_hash, source_type, scope, target_fab, target_model_no,
               target_phase_no, node_count, link_count, total_length_mm,
               coverage, data_codes_scope, utilities_scope, references_scope,
               path_context, created_at
        FROM tb_path_definitions
        WHERE id = %s
        """
        
        with self.db.cursor() as cursor:
            cursor.execute(query, (path_definition_id,))
            result = cursor.fetchone()
        
        if not result:
            return None
        
        return {
            'id': path_definition_id,
            'path_hash': result[0],
            'source_type': result[1],
            'scope': result[2],
            'target_fab': result[3],
            'target_model_no': result[4],
            'target_phase_no': result[5],
            'node_count': result[6],
            'link_count': result[7],
            'total_length_mm': result[8],
            'coverage': result[9],
            'data_codes_scope': json.loads(result[10]) if result[10] else [],
            'utilities_scope': json.loads(result[11]) if result[11] else [],
            'references_scope': json.loads(result[12]) if result[12] else [],
            'path_context': json.loads(result[13]) if result[13] else {},
            'created_at': result[14]
        }
    
    def clear_network_cache(self) -> None:
        """Clear network topology caches."""
        self._network_cache.clear()
        self._adjacency_cache.clear()
        self._node_info_cache.clear()
        self.logger.info("Network caches cleared")
    
    def get_path_statistics(self, run_id: str) -> Dict[str, Any]:
        """Get path statistics for a run."""
        query = """
        SELECT 
            COUNT(*) as total_paths,
            COUNT(DISTINCT path_hash) as unique_paths,
            AVG(node_count) as avg_node_count,
            AVG(link_count) as avg_link_count,
            AVG(total_length_mm) as avg_length,
            MIN(node_count) as min_nodes,
            MAX(node_count) as max_nodes
        FROM tb_path_definitions pd
        JOIN tb_attempt_paths ap ON pd.id = ap.path_definition_id
        WHERE ap.run_id = %s
        """
        
        with self.db.cursor() as cursor:
            cursor.execute(query, (run_id,))
            result = cursor.fetchone()
        
        if not result:
            return {}
        
        return {
            'total_paths': result[0] or 0,
            'unique_paths': result[1] or 0,
            'avg_node_count': float(result[2]) if result[2] else 0.0,
            'avg_link_count': float(result[3]) if result[3] else 0.0,
            'avg_length': float(result[4]) if result[4] else 0.0,
            'min_nodes': result[5] or 0,
            'max_nodes': result[6] or 0
        }
