import random
import logging
import hashlib
from typing import Optional, List, Dict, Any, Tuple, Set
from dataclasses import dataclass
from collections import defaultdict, deque


@dataclass
class BiasReduction:
    """Configuration for bias reduction in random sampling."""
    max_attempts_per_toolset: int = 5
    max_attempts_per_equipment: int = 3
    min_distance_between_nodes: int = 10
    utility_diversity_weight: float = 0.3    
    phase_diversity_weight: float = 0.2


@dataclass
class PathResult:
    """Result of a path finding operation."""
    start_node_id: int
    end_node_id: int
    path_nodes: List[int]
    path_links: List[int]
    total_cost: float
    total_length_mm: float
    data_codes: List[int]
    utilities: List[int]
    references: List[str]
    start_poc_id: int
    end_poc_id: int
    start_equipment_id: int
    end_equipment_id: int
    toolset_code: str


@dataclass
class SamplingStats:
    """Statistics for tracking sampling attempts and bias reduction."""
    toolset_attempts: Dict[str, int]
    equipment_attempts: Dict[int, int]
    utility_coverage: Set[int]
    phase_coverage: Set[int]
    recent_nodes: deque  # For minimum distance tracking
    
    def __init__(self):
        self.toolset_attempts = defaultdict(int)
        self.equipment_attempts = defaultdict(int)
        self.utility_coverage = set()
        self.phase_coverage = set()
        self.recent_nodes = deque(maxlen=100)  # Track last 100 nodes


class RandomManager:
    """Manages random path generation with bias mitigation."""
    
    def __init__(self, db_connection):
        self.db = db_connection
        self.logger = logging.getLogger(__name__)
        self.bias_config = BiasReduction()
        self.sampling_stats = SamplingStats()
        
    def generate_random_path(self, run_id: str, config: 'RandomRunConfig') -> Optional[PathResult]:
        """Generate a random path between two equipment PoCs."""
        try:
            # Select random PoC pair based on configuration
            poc_pair = self._select_random_poc_pair(config)
            if not poc_pair:
                self.logger.warning('No valid PoC pair found for random sampling')
                return None
            
            start_poc, end_poc = poc_pair
            
            # Record attempt
            self._record_attempt(run_id, start_poc['node_id'], end_poc['node_id'])
            
            # Find path between the PoCs
            path_result = self._find_path_between_pocs(start_poc, end_poc)
            
            if path_result:
                # Update sampling statistics
                self._update_sampling_stats(path_result)
                self.logger.debug(f'Path found: {path_result.start_node_id} -> {path_result.end_node_id}')
            
            return path_result
            
        except Exception as e:
            self.logger.error(f'Error generating random path: {str(e)}')
            return None
    
    def _select_random_poc_pair(self, config: 'RandomRunConfig') -> Optional[Tuple[Dict, Dict]]:
        """Select a random pair of PoCs based on configuration and bias reduction."""
        max_attempts = 50  # Prevent infinite loops
        
        for attempt in range(max_attempts):
            try:
                # Step 1: Select random building (if not specified)
                fab = config.fab or self._select_random_fab(config)
                if not fab:
                    continue
                
                # Step 2: Select random toolset (if not specified)
                toolset = config.toolset or self._select_random_toolset(fab, config)
                if not toolset:
                    continue
                
                # Apply bias reduction for toolset selection
                if self.sampling_stats.toolset_attempts[toolset] >= self.bias_config.max_attempts_per_toolset:
                    continue
                
                # Step 3: Select two random equipment from the toolset
                equipment_pair = self._select_random_equipment_pair(toolset, fab, config)
                if not equipment_pair:
                    continue
                
                eq1, eq2 = equipment_pair
                
                # Apply bias reduction for equipment selection
                if (self.sampling_stats.equipment_attempts[eq1['id']] >= self.bias_config.max_attempts_per_equipment or
                    self.sampling_stats.equipment_attempts[eq2['id']] >= self.bias_config.max_attempts_per_equipment):
                    continue
                
                # Step 4: Select random PoCs from each equipment
                poc1 = self._select_random_poc_from_equipment(eq1['id'])
                poc2 = self._select_random_poc_from_equipment(eq2['id'])
                
                if not poc1 or not poc2:
                    continue
                
                # Apply minimum distance bias reduction
                if self._is_too_close_to_recent_nodes(poc1['node_id'], poc2['node_id']):
                    continue
                
                # Apply utility diversity bias
                if not self._passes_utility_diversity_check(poc1, poc2):
                    continue
                
                return (poc1, poc2)
                
            except Exception as e:
                self.logger.warning(f'Error in PoC selection attempt {attempt}: {str(e)}')
                continue
        
        self.logger.warning('Failed to select valid PoC pair after maximum attempts')
        return None
    
    def _select_random_fab(self, config: 'RandomRunConfig') -> Optional[str]:
        """Select a random fab/building."""
        query = '''
            SELECT DISTINCT fab 
            FROM tb_toolsets 
            WHERE is_active = 1
        '''
        
        params = []
        if config.model_no:
            query += ' AND model_no = %s'
            params.append(config.model_no)
        
        if config.phase_no:
            query += ' AND phase_no = %s'
            params.append(config.phase_no)
        
        with self.db.cursor() as cursor:
            cursor.execute(query, params)
            fabs = [row['fab'] for row in cursor.fetchall()]
            
        return random.choice(fabs) if fabs else None
    
    def _select_random_toolset(self, fab: str, config: 'RandomRunConfig') -> Optional[str]:
        """Select a random toolset from the specified fab."""
        query = '''
            SELECT code 
            FROM tb_toolsets 
            WHERE fab = %s AND is_active = 1
        '''
        
        params = [fab]
        if config.model_no:
            query += ' AND model_no = %s'
            params.append(config.model_no)
        
        if config.phase_no:
            query += ' AND phase_no = %s'
            params.append(config.phase_no)
        
        with self.db.cursor() as cursor:
            cursor.execute(query, params)
            toolsets = [row['code'] for row in cursor.fetchall()]
        
        # Apply bias reduction - prefer less sampled toolsets
        if toolsets and len(toolsets) > 1:
            # Weight by inverse of attempts (less attempted = higher weight)
            weights = []
            for toolset in toolsets:
                attempts = self.sampling_stats.toolset_attempts[toolset]
                weight = 1.0 / (attempts + 1)  # +1 to avoid division by zero
                weights.append(weight)
            
            return random.choices(toolsets, weights=weights)[0]
        
        return random.choice(toolsets) if toolsets else None
    
    def _select_random_equipment_pair(self, toolset: str, fab: str, config: 'RandomRunConfig') -> Optional[Tuple[Dict, Dict]]:
        """Select a random pair of equipment from the toolset."""
        query = '''
            SELECT e.id, e.guid, e.node_id, e.data_code, e.category_no, e.kind
            FROM tb_equipments e
            WHERE e.toolset = %s AND e.is_active = 1
        '''
        
        with self.db.cursor() as cursor:
            cursor.execute(query, (toolset,))
            equipment_list = cursor.fetchall()
        
        if len(equipment_list) < 2:
            return None
        
        # Select two different equipment
        eq1, eq2 = random.sample(equipment_list, 2)
        return (eq1, eq2)
    
    def _select_random_poc_from_equipment(self, equipment_id: int) -> Optional[Dict]:
        """Select a random PoC from the specified equipment."""
        query = '''
            SELECT ep.id, ep.equipment_id, ep.node_id, ep.utility_no, ep.markers, 
                   ep.reference, ep.flow, ep.is_loopback, ep.is_used
            FROM tb_equipment_pocs ep
            WHERE ep.equipment_id = %s AND ep.is_active = 1
        '''
        
        with self.db.cursor() as cursor:
            cursor.execute(query, (equipment_id,))
            pocs = cursor.fetchall()
        
        if not pocs:
            return None
        
        # Prefer used PoCs but allow unused ones
        used_pocs = [poc for poc in pocs if poc['is_used']]
        if used_pocs:
            return random.choice(used_pocs)
        else:
            return random.choice(pocs)
    
    def _is_too_close_to_recent_nodes(self, node1_id: int, node2_id: int) -> bool:
        """Check if nodes are too close to recently sampled nodes."""
        min_distance = self.bias_config.min_distance_between_nodes
        
        for recent_node in self.sampling_stats.recent_nodes:
            if (abs(node1_id - recent_node) < min_distance or 
                abs(node2_id - recent_node) < min_distance):
                return True
        
        return False
    
    def _passes_utility_diversity_check(self, poc1: Dict, poc2: Dict) -> bool:
        """Check if PoC pair adds to utility diversity."""
        utility1 = poc1.get('utility_no')
        utility2 = poc2.get('utility_no')
        
        # If no utilities specified, always pass
        if not utility1 and not utility2:
            return True
        
        # Apply utility diversity weight
        existing_utilities = self.sampling_stats.utility_coverage
        new_utilities = {utility1, utility2} - {None}
        
        if not new_utilities.issubset(existing_utilities):
            # New utilities found - boost probability
            return random.random() < (1.0 - self.bias_config.utility_diversity_weight)
        
        # Existing utilities - reduce probability
        return random.random() < self.bias_config.utility_diversity_weight
    
    def _find_path_between_pocs(self, start_poc: Dict, end_poc: Dict) -> Optional[PathResult]:
        """Find path between two PoCs using network traversal."""
        start_node_id = start_poc['node_id']
        end_node_id = end_poc['node_id']
        
        # Use Dijkstra's algorithm to find shortest path
        path_data = self._dijkstra_path_finding(start_node_id, end_node_id)
        
        if not path_data:
            return None
        
        # Extract path information
        path_nodes, path_links, total_cost = path_data
        
        # Get additional path metadata
        path_metadata = self._extract_path_metadata(path_nodes, path_links)
        
        # Get toolset information
        toolset_query = '''
            SELECT toolset FROM tb_equipments 
            WHERE id = %s
        '''
        
        with self.db.cursor() as cursor:
            cursor.execute(toolset_query, (start_poc['equipment_id'],))
            toolset_result = cursor.fetchone()
            toolset_code = toolset_result['toolset'] if toolset_result else 'UNKNOWN'
        
        return PathResult(
            start_node_id=start_node_id,
            end_node_id=end_node_id,
            path_nodes=path_nodes,
            path_links=path_links,
            total_cost=total_cost,
            total_length_mm=path_metadata['total_length'],
            data_codes=path_metadata['data_codes'],
            utilities=path_metadata['utilities'],
            references=path_metadata['references'],
            start_poc_id=start_poc['id'],
            end_poc_id=end_poc['id'],
            start_equipment_id=start_poc['equipment_id'],
            end_equipment_id=end_poc['equipment_id'],
            toolset_code=toolset_code
        )
    
    def _dijkstra_path_finding(self, start_node_id: int, end_node_id: int) -> Optional[Tuple[List[int], List[int], float]]:
        """Find shortest path using Dijkstra's algorithm."""
        import heapq
        
        # Priority queue: (cost, current_node, path_nodes, path_links)
        pq = [(0, start_node_id, [start_node_id], [])]
        visited = set()
        
        max_iterations = 10000  # Prevent infinite loops
        iteration = 0
        
        while pq and iteration < max_iterations:
            iteration += 1
            current_cost, current_node, path_nodes, path_links = heapq.heappop(pq)
            
            if current_node in visited:
                continue
            
            visited.add(current_node)
            
            if current_node == end_node_id:
                return (path_nodes, path_links, current_cost)
            
            # Get neighboring nodes
            neighbors = self._get_node_neighbors(current_node)
            
            for neighbor_node, link_id, link_cost in neighbors:
                if neighbor_node not in visited:
                    new_cost = current_cost + link_cost
                    new_path_nodes = path_nodes + [neighbor_node]
                    new_path_links = path_links + [link_id]
                    
                    heapq.heappush(pq, (new_cost, neighbor_node, new_path_nodes, new_path_links))
        
        return None  # No path found
    
    def _get_node_neighbors(self, node_id: int) -> List[Tuple[int, int, float]]:
        """Get neighboring nodes and link information."""
        query = '''
            SELECT end_node_id as neighbor_id, id as link_id, cost
            FROM nw_links 
            WHERE start_node_id = %s
            UNION ALL
            SELECT start_node_id as neighbor_id, id as link_id, cost
            FROM nw_links 
            WHERE end_node_id = %s AND bidirected = 'Y'
        '''
        
        with self.db.cursor() as cursor:
            cursor.execute(query, (node_id, node_id))
            return [(row['neighbor_id'], row['link_id'], row['cost']) for row in cursor.fetchall()]
    
    def _extract_path_metadata(self, path_nodes: List[int], path_links: List[int]) -> Dict[str, Any]:
        """Extract metadata from path nodes and links."""
        # Get node data
        if path_nodes:
            node_placeholders = ','.join(['%s'] * len(path_nodes))
            node_query = f'''
                SELECT data_code, utility_no, markers, e2e_group_no
                FROM nw_nodes 
                WHERE id IN ({node_placeholders})
            '''
            
            with self.db.cursor() as cursor:
                cursor.execute(node_query, path_nodes)
                node_data = cursor.fetchall()
        else:
            node_data = []
        
        # Get link data for length calculation
        total_length = 0.0
        if path_links:
            link_placeholders = ','.join(['%s'] * len(path_links))
            link_query = f'''
                SELECT cost
                FROM nw_links 
                WHERE id IN ({link_placeholders})
            '''
            
            with self.db.cursor() as cursor:
                cursor.execute(link_query, path_links)
                link_data = cursor.fetchall()
                total_length = sum(row['cost'] for row in link_data)
        
        # Extract unique values
        data_codes = list(set(row['data_code'] for row in node_data if row['data_code']))
        utilities = list(set(row['utility_no'] for row in node_data if row['utility_no']))
        
        # Extract references from markers
        references = []
        for row in node_data:
            if row['markers']:
                # Parse markers to extract reference codes
                markers = row['markers'].split(',')
                for marker in markers:
                    if len(marker.strip()) <= 8:  # Reference format
                        references.append(marker.strip())
        
        references = list(set(references))
        
        return {
            'total_length': total_length,
            'data_codes': data_codes,
            'utilities': utilities,
            'references': references
        }
    
    def _record_attempt(self, run_id: str, start_node_id: int, end_node_id: int):
        """Record path attempt in database."""
        query = '''
            INSERT INTO tb_attempt_paths (run_id, path_definition_id, start_node_id, end_node_id, picked_at)
            VALUES (%s, NULL, %s, %s, NOW())
        '''
        
        with self.db.cursor() as cursor:
            cursor.execute(query, (run_id, start_node_id, end_node_id))
            self.db.commit()
    
    def _update_sampling_stats(self, path_result: PathResult):
        """Update sampling statistics for bias reduction."""
        # Update toolset attempts
        self.sampling_stats.toolset_attempts[path_result.toolset_code] += 1
        
        # Update equipment attempts
        self.sampling_stats.equipment_attempts[path_result.start_equipment_id] += 1
        self.sampling_stats.equipment_attempts[path_result.end_equipment_id] += 1
        
        # Update utility coverage
        self.sampling_stats.utility_coverage.update(path_result.utilities)
        
        # Update recent nodes for distance tracking
        self.sampling_stats.recent_nodes.append(path_result.start_node_id)
        self.sampling_stats.recent_nodes.append(path_result.end_node_id)
    
    def reset_sampling_stats(self):
        """Reset sampling statistics for a new run."""
        self.sampling_stats = SamplingStats()
    
    def get_sampling_stats(self) -> Dict[str, Any]:
        """Get current sampling statistics."""
        return {
            'toolset_attempts': dict(self.sampling_stats.toolset_attempts),
            'equipment_attempts': dict(self.sampling_stats.equipment_attempts),
            'utility_coverage': list(self.sampling_stats.utility_coverage),
            'recent_nodes_count': len(self.sampling_stats.recent_nodes)
        }
