
import logging
from typing import Optional, Set, Dict, Any, List, Tuple
from dataclasses import dataclass
from bitarray import bitarray
import json


@dataclass
class CoverageScope:
    """Defines the scope for coverage calculation."""
    fab: Optional[str] = None
    phase_no: Optional[int] = None
    model_no: Optional[int] = None
    toolset: Optional[str] = None
    total_nodes: int = 0
    total_links: int = 0
    node_id_mapping: Dict[int, int] = None  # Maps actual node_id to bitarray index
    link_id_mapping: Dict[int, int] = None  # Maps actual link_id to bitarray index


@dataclass
class CoverageMetrics:
    """Coverage metrics for a run."""
    total_nodes_in_scope: int
    total_links_in_scope: int
    covered_nodes: int
    covered_links: int
    node_coverage: float
    link_coverage: float
    overall_coverage: float
    unique_paths_contributing: int


class CoverageManager:
    """Manages coverage tracking using bitsets for efficient calculation."""
    
    def __init__(self, db_connection):
        self.db = db_connection
        self.logger = logging.getLogger(__name__)
        self._coverage_cache = {}  # Cache coverage data by run_id
        
    def initialize_coverage(self, run_id: str, fab: Optional[str] = None, 
                          phase_no: Optional[int] = None, model_no: Optional[int] = None,
                          toolset: Optional[str] = None) -> CoverageScope:
        """Initialize coverage tracking for a run with specified scope."""
        self.logger.info(f'Initializing coverage for run {run_id}')
        
        # Define the scope and get all nodes/links within scope
        scope = self._define_coverage_scope(fab, phase_no, model_no, toolset)
        
        # Initialize bitarrays for tracking coverage
        node_bitarray = bitarray(scope.total_nodes)
        link_bitarray = bitarray(scope.total_links)
        node_bitarray.setall(0)  # All uncovered initially
        link_bitarray.setall(0)  # All uncovered initially
        
        # Store in cache
        self._coverage_cache[run_id] = {
            'scope': scope,
            'node_coverage': node_bitarray,
            'link_coverage': link_bitarray,
            'covered_paths': set(),  # Track unique path hashes
            'last_updated': None
        }
        
        # Update run record with total counts
        self._update_run_totals(run_id, scope.total_nodes, scope.total_links)
        
        self.logger.info(f'Coverage initialized: {scope.total_nodes} nodes, {scope.total_links} links')
        return scope
    
    def _define_coverage_scope(self, fab: Optional[str], phase_no: Optional[int], 
                             model_no: Optional[int], toolset: Optional[str]) -> CoverageScope:
        """Define the scope for coverage calculation and create ID mappings."""
        
        # Build the query for nodes within scope
        node_query = 'SELECT id FROM nw_nodes WHERE 1=1'
        node_params = []
        
        # Add filters based on scope
        if fab or phase_no or model_no or toolset:
            # Join with equipment data to filter by scope
            node_query = '''
                SELECT DISTINCT n.id 
                FROM nw_nodes n
                JOIN tb_equipment_pocs ep ON n.id = ep.node_id
                JOIN tb_equipments e ON ep.equipment_id = e.id
                JOIN tb_toolsets t ON e.toolset = t.code
                WHERE 1=1
            '''
            
            if fab:
                node_query += ' AND t.fab = %s'
                node_params.append(fab)
            
            if phase_no:
                node_query += ' AND t.phase_no = %s'
                node_params.append(phase_no)
            
            if model_no:
                node_query += ' AND t.model_no = %s'
                node_params.append(model_no)
            
            if toolset:
                node_query += ' AND t.code = %s'
                node_params.append(toolset)
        
        node_query += ' ORDER BY id'
        
        # Get nodes within scope
        with self.db.cursor() as cursor:
            cursor.execute(node_query, node_params)
            node_ids = [row['id'] for row in cursor.fetchall()]
        
        # Build node ID mapping (node_id -> bitarray_index)
        node_id_mapping = {node_id: idx for idx, node_id in enumerate(node_ids)}
        
        # Build the query for links within scope
        if node_ids:
            # Only include links where both start and end nodes are in scope
            node_placeholders = ','.join(['%s'] * len(node_ids))
            link_query = f'''
                SELECT id 
                FROM nw_links 
                WHERE start_node_id IN ({node_placeholders}) 
                AND end_node_id IN ({node_placeholders})
                ORDER BY id
            '''
            link_params = node_ids + node_ids
        else:
            # If no nodes in scope, no links either
            link_query = 'SELECT id FROM nw_links WHERE 1=0'
            link_params = []
        
        # Get links within scope
        with self.db.cursor() as cursor:
            cursor.execute(link_query, link_params)
            link_ids = [row['id'] for row in cursor.fetchall()]
        
        # Build link ID mapping (link_id -> bitarray_index)
        link_id_mapping = {link_id: idx for idx, link_id in enumerate(link_ids)}
        
        return CoverageScope(
            fab=fab,
            phase_no=phase_no,
            model_no=model_no,
            toolset=toolset,
            total_nodes=len(node_ids),
            total_links=len(link_ids),
            node_id_mapping=node_id_mapping,
            link_id_mapping=link_id_mapping
        )
    
    def update_coverage(self, run_id: str, path_result: 'PathResult') -> float:
        """Update coverage with a new path and return new coverage percentage."""
        if run_id not in self._coverage_cache:
            self.logger.error(f'Coverage not initialized for run {run_id}')
            return 0.0
        
        cache_data = self._coverage_cache[run_id]
        scope = cache_data['scope']
        node_coverage = cache_data['node_coverage']
        link_coverage = cache_data['link_coverage']
        
        # Generate path hash for deduplication
        path_hash = self._generate_path_hash(path_result)
        
        # Skip if path already processed
        if path_hash in cache_data['covered_paths']:
            return self.get_current_coverage(run_id)
        
        # Mark nodes as covered
        nodes_added = 0
        for node_id in path_result.path_nodes:
            if node_id in scope.node_id_mapping:
                bit_index = scope.node_id_mapping[node_id]
                if not node_coverage[bit_index]:
                    node_coverage[bit_index] = 1
                    nodes_added += 1
        
        # Mark links as covered
        links_added = 0
        for link_id in path_result.path_links:
            if link_id in scope.link_id_mapping:
                bit_index = scope.link_id_mapping[link_id]
                if not link_coverage[bit_index]:
                    link_coverage[bit_index] = 1
                    links_added += 1
        
        # Add to covered paths set
        cache_data['covered_paths'].add(path_hash)
        cache_data['last_updated'] = path_result
        
        # Calculate new coverage
        current_coverage = self._calculate_coverage_percentage(node_coverage, link_coverage)
        
        # Update run record
        self._update_run_coverage(run_id, current_coverage, len(cache_data['covered_paths']))
        
        self.logger.debug(f'Coverage updated: +{nodes_added} nodes, +{links_added} links, total: {current_coverage:.4f}')
        return current_coverage
    
    def _generate_path_hash(self, path_result: 'PathResult') -> str:
        """Generate a hash for path deduplication."""
        import hashlib
        path_key = f"{sorted(path_result.path_nodes)}_{sorted(path_result.path_links)}"
        return hashlib.md5(path_key.encode()).hexdigest()
    
    def _calculate_coverage_percentage(self, node_coverage: bitarray, link_coverage: bitarray) -> float:
        """Calculate overall coverage percentage."""
        if len(node_coverage) == 0 and len(link_coverage) == 0:
            return 0.0
        
        # Weight nodes and links equally
        node_ratio = node_coverage.count() / len(node_coverage) if len(node_coverage) > 0 else 0.0
        link_ratio = link_coverage.count() / len(link_coverage) if len(link_coverage) > 0 else 0.0
        
        # Simple average of node and link coverage
        return (node_ratio + link_ratio) / 2.0
    
    def get_current_coverage(self, run_id: str) -> float:
        """Get current coverage percentage for a run."""
        if run_id not in self._coverage_cache:
            return 0.0
        
        cache_data = self._coverage_cache[run_id]
        node_coverage = cache_data['node_coverage']
        link_coverage = cache_data['link_coverage']
        
        return self._calculate_coverage_percentage(node_coverage, link_coverage)
    
    def is_target_reached(self, run_id: str, target_coverage: float) -> bool:
        """Check if target coverage has been reached."""
        current = self.get_current_coverage(run_id)
        return current >= target_coverage
    
    def get_coverage_metrics(self, run_id: str) -> Optional[CoverageMetrics]:
        """Get detailed coverage metrics for a run."""
        if run_id not in self._coverage_cache:
            return None
        
        cache_data = self._coverage_cache[run_id]
        scope = cache_data['scope']
        node_coverage = cache_data['node_coverage']
        link_coverage = cache_data['link_coverage']
        
        covered_nodes = node_coverage.count()
        covered_links = link_coverage.count()
        
        node_coverage_pct = covered_nodes / scope.total_nodes if scope.total_nodes > 0 else 0.0
        link_coverage_pct = covered_links / scope.total_links if scope.total_links > 0 else 0.0
        overall_coverage = self._calculate_coverage_percentage(node_coverage, link_coverage)
        
        return CoverageMetrics(
            total_nodes_in_scope=scope.total_nodes,
            total_links_in_scope=scope.total_links,
            covered_nodes=covered_nodes,
            covered_links=covered_links,
            node_coverage=node_coverage_pct,
            link_coverage=link_coverage_pct,
            overall_coverage=overall_coverage,
            unique_paths_contributing=len(cache_data['covered_paths'])
        )
    
    def get_uncovered_nodes(self, run_id: str, limit: int = 100) -> List[int]:
        """Get a list of uncovered node IDs."""
        if run_id not in self._coverage_cache:
            return []
        
        cache_data = self._coverage_cache[run_id]
        scope = cache_data['scope']
        node_coverage = cache_data['node_coverage']
        
        uncovered_nodes = []
        reverse_mapping = {idx: node_id for node_id, idx in scope.node_id_mapping.items()}
        
        for idx in range(len(node_coverage)):
            if not node_coverage[idx]:
                uncovered_nodes.append(reverse_mapping[idx])
                if len(uncovered_nodes) >= limit:
                    break
        
        return uncovered_nodes
    
    def get_uncovered_links(self, run_id: str, limit: int = 100) -> List[int]:
        """Get a list of uncovered link IDs."""
        if run_id not in self._coverage_cache:
            return []
        
        cache_data = self._coverage_cache[run_id]
        scope = cache_data['scope']
        link_coverage = cache_data['link_coverage']
        
        uncovered_links = []
        reverse_mapping = {idx: link_id for link_id, idx in scope.link_id_mapping.items()}
        
        for idx in range(len(link_coverage)):
            if not link_coverage[idx]:
                uncovered_links.append(reverse_mapping[idx])
                if len(uncovered_links) >= limit:
                    break
        
        return uncovered_links
    
    def get_coverage_gaps(self, run_id: str) -> Dict[str, Any]:
        """Analyze coverage gaps and provide insights."""
        if run_id not in self._coverage_cache:
            return {}
        
        uncovered_nodes = self.get_uncovered_nodes(run_id, 1000)
        uncovered_links = self.get_uncovered_links(run_id, 1000)
        
        # Analyze uncovered nodes by equipment type
        node_analysis = self._analyze_uncovered_nodes(uncovered_nodes)
        
        # Analyze uncovered links by type
        link_analysis = self._analyze_uncovered_links(uncovered_links)
        
        return {
            'uncovered_node_count': len(uncovered_nodes),
            'uncovered_link_count': len(uncovered_links),
            'node_analysis': node_analysis,
            'link_analysis': link_analysis,
            'recommendations': self._generate_coverage_recommendations(node_analysis, link_analysis)
        }
    
    def _analyze_uncovered_nodes(self, uncovered_nodes: List[int]) -> Dict[str, Any]:
        """Analyze uncovered nodes by various attributes."""
        if not uncovered_nodes:
            return {}
        
        # Query node attributes
        node_placeholders = ','.join(['%s'] * len(uncovered_nodes))
        query = f'''
            SELECT n.id, n.data_code, n.utility_no, n.e2e_group_no, n.nwo_type_no,
                   e.kind as equipment_kind, t.fab, t.phase_no, t.model_no
            FROM nw_nodes n
            LEFT JOIN tb_equipment_pocs ep ON n.id = ep.node_id
            LEFT JOIN tb_equipments e ON ep.equipment_id = e.id
            LEFT JOIN tb_toolsets t ON e.toolset = t.code
            WHERE n.id IN ({node_placeholders})
        '''
        
        with self.db.cursor() as cursor:
            cursor.execute(query, uncovered_nodes)
            node_data = cursor.fetchall()
        
        # Analyze by various dimensions
        analysis = {
            'by_data_code': {},
            'by_utility': {},
            'by_equipment_kind': {},
            'by_fab': {},
            'by_phase': {},
            'isolated_nodes': 0
        }
        
        for node in node_data:
            # Count by data code
            data_code = node.get('data_code', 'UNKNOWN')
            analysis['by_data_code'][data_code] = analysis['by_data_code'].get(data_code, 0) + 1
            
            # Count by utility
            utility = node.get('utility_no', 'UNKNOWN')
            analysis['by_utility'][utility] = analysis['by_utility'].get(utility, 0) + 1
            
            # Count by equipment kind
            eq_kind = node.get('equipment_kind', 'UNKNOWN')
            analysis['by_equipment_kind'][eq_kind] = analysis['by_equipment_kind'].get(eq_kind, 0) + 1
            
            # Count by fab
            fab = node.get('fab', 'UNKNOWN')
            analysis['by_fab'][fab] = analysis['by_fab'].get(fab, 0) + 1
            
            # Count by phase
            phase = node.get('phase_no', 'UNKNOWN')
            analysis['by_phase'][phase] = analysis['by_phase'].get(phase, 0) + 1
            
            # Check for isolated nodes (nodes with no equipment association)
            if not node.get('equipment_kind'):
                analysis['isolated_nodes'] += 1
        
        return analysis
    
    def _analyze_uncovered_links(self, uncovered_links: List[int]) -> Dict[str, Any]:
        """Analyze uncovered links by various attributes."""
        if not uncovered_links:
            return {}
        
        # Query link attributes
        link_placeholders = ','.join(['%s'] * len(uncovered_links))
        query = f'''
            SELECT id, start_node_id, end_node_id, cost, nwo_type_no, bidirected
            FROM nw_links 
            WHERE id IN ({link_placeholders})
        '''
        
        with self.db.cursor() as cursor:
            cursor.execute(query, uncovered_links)
            link_data = cursor.fetchall()
        
        analysis = {
            'by_type': {},
            'by_cost_range': {'low': 0, 'medium': 0, 'high': 0},
            'bidirectional_count': 0,
            'unidirectional_count': 0
        }
        
        for link in link_data:
            # Count by type
            link_type = link.get('nwo_type_no', 'UNKNOWN')
            analysis['by_type'][link_type] = analysis['by_type'].get(link_type, 0) + 1
            
            # Count by cost range
            cost = link.get('cost', 0)
            if cost < 1000:
                analysis['by_cost_range']['low'] += 1
            elif cost < 10000:
                analysis['by_cost_range']['medium'] += 1
            else:
                analysis['by_cost_range']['high'] += 1
            
            # Count directionality
            if link.get('bidirected') == 'Y':
                analysis['bidirectional_count'] += 1
            else:
                analysis['unidirectional_count'] += 1
        
        return analysis
    
    def _generate_coverage_recommendations(self, node_analysis: Dict, link_analysis: Dict) -> List[str]:
        """Generate recommendations to improve coverage."""
        recommendations = []
        
        # Check for concentration of uncovered nodes
        if node_analysis.get('by_fab'):
            max_fab = max(node_analysis['by_fab'].items(), key=lambda x: x[1])
            if max_fab[1] > 10:  # More than 10 uncovered nodes in one fab
                recommendations.append(f'Focus sampling on fab {max_fab[0]} ({max_fab[1]} uncovered nodes)')
        
        # Check for isolated nodes
        if node_analysis.get('isolated_nodes', 0) > 5:
            recommendations.append(f'Investigate {node_analysis["isolated_nodes"]} isolated nodes without equipment association')
        
        # Check for equipment type gaps
        if node_analysis.get('by_equipment_kind'):
            for eq_kind, count in node_analysis['by_equipment_kind'].items():
                if eq_kind != 'UNKNOWN' and count > 5:
                    recommendations.append(f'Target {eq_kind} equipment type ({count} uncovered nodes)')
        
        # Check for utility gaps
        if node_analysis.get('by_utility'):
            for utility, count in node_analysis['by_utility'].items():
                if utility != 'UNKNOWN' and count > 5:
                    recommendations.append(f'Target utility {utility} ({count} uncovered nodes)')
        
        return recommendations
    
    def _update_run_totals(self, run_id: str, total_nodes: int, total_links: int):
        """Update run record with total node and link counts."""
        query = '''
            UPDATE tb_runs 
            SET total_nodes = %s, total_links = %s 
            WHERE id = %s
        '''
        
        with self.db.cursor() as cursor:
            cursor.execute(query, (total_nodes, total_links, run_id))
            self.db.commit()
    
    def _update_run_coverage(self, run_id: str, coverage: float, unique_paths: int):
        """Update run record with current coverage."""
        query = '''
            UPDATE tb_runs 
            SET total_coverage = %s 
            WHERE id = %s
        '''
        
        with self.db.cursor() as cursor:
            cursor.execute(query, (coverage, run_id))
            self.db.commit()
    
    def export_coverage_data(self, run_id: str) -> Optional[Dict[str, Any]]:
        """Export complete coverage data for analysis."""
        if run_id not in self._coverage_cache:
            return None
        
        cache_data = self._coverage_cache[run_id]
        scope = cache_data['scope']
        
        # Convert bitarrays to lists for serialization
        node_coverage_list = cache_data['node_coverage'].tolist()
        link_coverage_list = cache_data['link_coverage'].tolist()
        
        return {
            'run_id': run_id,
            'scope': {
                'fab': scope.fab,
                'phase_no': scope.phase_no,
                'model_no': scope.model_no,
                'toolset': scope.toolset,
                'total_nodes': scope.total_nodes,
                'total_links': scope.total_links
            },
            'coverage': {
                'nodes': node_coverage_list,
                'links': link_coverage_list,
                'covered_paths_count': len(cache_data['covered_paths'])
            },
            'metrics': self.get_coverage_metrics(run_id).__dict__ if self.get_coverage_metrics(run_id) else None
        }
    
    def clear_coverage_cache(self, run_id: Optional[str] = None):
        """Clear coverage cache for a specific run or all runs."""
        if run_id:
            self._coverage_cache.pop(run_id, None)
        else:
            self._coverage_cache.clear()
