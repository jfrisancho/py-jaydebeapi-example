"""
Coverage tracking service using bitsets for efficient coverage calculation.
Tracks unique nodes and links covered by discovered paths.
"""

import logging
import json
from typing import Dict, Any, Set, List, Optional
from bitarray import bitarray
from datetime import datetime


class CoverageTracker:
    """Bitset-based coverage tracker for nodes and links."""
    
    def __init__(self):
        self.nodes_bitset = bitarray()
        self.links_bitset = bitarray()
        self.node_id_to_index = {}
        self.link_id_to_index = {}
        self.index_to_node_id = {}
        self.index_to_link_id = {}
        self.total_possible_nodes = 0
        self.total_possible_links = 0
    
    def initialize(self, node_universe: Set[int], link_universe: Set[Tuple[int, int]]):
        """Initialize bitsets with the universe of possible nodes and links."""
        # Sort for consistent indexing
        sorted_nodes = sorted(node_universe)
        sorted_links = sorted(link_universe)
        
        self.total_possible_nodes = len(sorted_nodes)
        self.total_possible_links = len(sorted_links)
        
        # Initialize bitsets
        self.nodes_bitset = bitarray(self.total_possible_nodes)
        self.links_bitset = bitarray(self.total_possible_links)
        self.nodes_bitset.setall(0)
        self.links_bitset.setall(0)
        
        # Build mapping dictionaries
        for i, node_id in enumerate(sorted_nodes):
            self.node_id_to_index[node_id] = i
            self.index_to_node_id[i] = node_id
        
        for i, link_id in enumerate(sorted_links):
            self.link_id_to_index[link_id] = i
            self.index_to_link_id[i] = link_id
    
    def add_path_coverage(self, path_nodes: List[int]) -> bool:
        """
        Add coverage for a path. Returns True if new coverage was added.
        """
        if len(path_nodes) < 2:
            return False
        
        new_coverage = False
        
        # Mark nodes as covered
        for node_id in path_nodes:
            if node_id in self.node_id_to_index:
                index = self.node_id_to_index[node_id]
                if not self.nodes_bitset[index]:
                    self.nodes_bitset[index] = 1
                    new_coverage = True
        
        # Mark links as covered
        for i in range(len(path_nodes) - 1):
            node_a, node_b = path_nodes[i], path_nodes[i + 1]
            # Create consistent link representation (smaller id first)
            link_id = (min(node_a, node_b), max(node_a, node_b))
            
            if link_id in self.link_id_to_index:
                index = self.link_id_to_index[link_id]
                if not self.links_bitset[index]:
                    self.links_bitset[index] = 1
                    new_coverage = True
        
        return new_coverage
    
    def get_coverage_ratio(self) -> float:
        """Get the current coverage ratio (0.0 to 1.0)."""
        if self.total_possible_nodes == 0 and self.total_possible_links == 0:
            return 0.0
        
        covered_nodes = self.nodes_bitset.count()
        covered_links = self.links_bitset.count()
        total_elements = self.total_possible_nodes + self.total_possible_links
        
        if total_elements == 0:
            return 0.0
        
        return (covered_nodes + covered_links) / total_elements
    
    def get_node_coverage_ratio(self) -> float:
        """Get node coverage ratio."""
        if self.total_possible_nodes == 0:
            return 0.0
        return self.nodes_bitset.count() / self.total_possible_nodes
    
    def get_link_coverage_ratio(self) -> float:
        """Get link coverage ratio."""
        if self.total_possible_links == 0:
            return 0.0
        return self.links_bitset.count() / self.total_possible_links
    
    def get_covered_count(self) -> Dict[str, int]:
        """Get counts of covered elements."""
        return {
            'nodes': self.nodes_bitset.count(),
            'links': self.links_bitset.count(),
            'total': self.nodes_bitset.count() + self.links_bitset.count()
        }
    
    def get_total_count(self) -> Dict[str, int]:
        """Get counts of total possible elements."""
        return {
            'nodes': self.total_possible_nodes,
            'links': self.total_possible_links,
            'total': self.total_possible_nodes + self.total_possible_links
        }


class CoverageService:
    """Service for tracking coverage of network elements during path discovery."""
    
    def __init__(self, db_connection):
        self.db = db_connection
        self.logger = logging.getLogger(__name__)
        
        # Coverage trackers by run_id
        self._coverage_trackers = {}
        
        # Cache for universe calculations
        self._universe_cache = {}
    
    def initialize_coverage(self, run_id: str, config) -> None:
        """Initialize coverage tracking for a run."""
        self.logger.info(f"Initializing coverage tracking for run {run_id}")
        
        # Get universe of nodes and links based on config
        node_universe, link_universe = self._get_coverage_universe(config)
        
        # Create coverage tracker
        tracker = CoverageTracker()
        tracker.initialize(node_universe, link_universe)
        
        self._coverage_trackers[run_id] = tracker
        
        self.logger.info(f"Coverage initialized: {len(node_universe)} nodes, {len(link_universe)} links")
    
    def update_coverage(self, run_id: str, path_data: Dict) -> bool:
        """
        Update coverage with a new path. Returns True if new coverage was added.
        """
        if run_id not in self._coverage_trackers:
            self.logger.error(f"Coverage tracker not initialized for run {run_id}")
            return False
        
        tracker = self._coverage_trackers[run_id]
        path_nodes = path_data.get('path_nodes', [])
        
        # Add path coverage
        new_coverage = tracker.add_path_coverage(path_nodes)
        
        if new_coverage:
            # Update run record with new coverage
            self._update_run_coverage(run_id, tracker)
            
            # Update path definition coverage
            path_coverage = self._calculate_path_contribution(path_nodes, tracker)
            self._update_path_coverage(path_data.get('path_definition_id'), path_coverage)
        
        return new_coverage
    
    def get_current_coverage(self, run_id: str) -> float:
        """Get current coverage ratio for a run."""
        if run_id not in self._coverage_trackers:
            return 0.0
        
        return self._coverage_trackers[run_id].get_coverage_ratio()
    
    def get_coverage_details(self, run_id: str) -> Dict[str, Any]:
        """Get detailed coverage information for a run."""
        if run_id not in self._coverage_trackers:
            return {}
        
        tracker = self._coverage_trackers[run_id]
        
        return {
            'overall_coverage': tracker.get_coverage_ratio(),
            'node_coverage': tracker.get_node_coverage_ratio(),
            'link_coverage': tracker.get_link_coverage_ratio(),
            'covered_count': tracker.get_covered_count(),
            'total_count': tracker.get_total_count(),
            'coverage_efficiency': self._calculate_coverage_efficiency(run_id)
        }
    
    def get_total_nodes_covered(self, run_id: str) -> int:
        """Get total number of nodes covered in a run."""
        if run_id not in self._coverage_trackers:
            return 0
        
        return self._coverage_trackers[run_id].get_covered_count()['nodes']
    
    def get_total_links_covered(self, run_id: str) -> int:
        """Get total number of links covered in a run."""
        if run_id not in self._coverage_trackers:
            return 0
        
        return self._coverage_trackers[run_id].get_covered_count()['links']
    
    def _get_coverage_universe(self, config) -> Tuple[Set[int], Set[Tuple[int, int]]]:
        """Get the universe of all possible nodes and links based on config."""
        cache_key = self._generate_universe_cache_key(config)
        
        if cache_key in self._universe_cache:
            return self._universe_cache[cache_key]
        
        # Build query for nodes universe
        node_query = """
        SELECT DISTINCT p.node_id
        FROM tb_equipment_pocs p
        JOIN tb_equipments e ON p.equipment_id = e.id
        JOIN tb_toolsets t ON e.toolset = t.code
        WHERE p.is_active = 1 AND e.is_active = 1 AND t.is_active = 1
        """
        
        # Build query for links universe
        link_query = """
        SELECT DISTINCT from_poc.node_id, to_poc.node_id
        FROM tb_equipment_poc_connections conn
        JOIN tb_equipment_pocs from_poc ON conn.from_poc_id = from_poc.id
        JOIN tb_equipment_pocs to_poc ON conn.to_poc_id = to_poc.id
        JOIN tb_equipments from_eq ON from_poc.equipment_id = from_eq.id
        JOIN tb_equipments to_eq ON to_poc.equipment_id = to_eq.id
        JOIN tb_toolsets from_ts ON from_eq.toolset = from_ts.code
        JOIN tb_toolsets to_ts ON to_eq.toolset = to_ts.code
        WHERE conn.is_valid = 1 
        AND from_poc.is_active = 1 AND to_poc.is_active = 1
        AND from_eq.is_active = 1 AND to_eq.is_active = 1
        AND from_ts.is_active = 1 AND to_ts.is_active = 1
        """
        
        # Add filters based on config
        conditions = []
        params = []
        
        if config.fab:
            conditions.append("t.fab = %s")
            params.append(config.fab)
        
        if config.model_no:
            conditions.append("t.model_no = %s")
            params.append(config.model_no)
        
        if config.phase_no:
            conditions.append("t.phase_no = %s")
            params.append(config.phase_no)
        
        if config.toolset:
            conditions.append("t.code = %s")
            params.append(config.toolset)
        
        if conditions:
            condition_str = " AND " + " AND ".join(conditions)
            node_query += condition_str
            
            # For link query, apply conditions to both sides
            link_conditions = []
            link_params = []
            
            for condition, param in zip(conditions, params):
                from_condition = condition.replace("t.", "from_ts.")
                to_condition = condition.replace("t.", "to_ts.")
                link_conditions.extend([from_condition, to_condition])
                link_params.extend([param, param])
            
            link_query += " AND " + " AND ".join(link_conditions)
            params = link_params
        
        # Execute queries
        with self.db.cursor() as cursor:
            # Get nodes
            cursor.execute(node_query, params[:len(conditions)] if conditions else [])
            node_results = cursor.fetchall()
            node_universe = {row[0] for row in node_results}
            
            # Get links
            cursor.execute(link_query, params if conditions else [])
            link_results = cursor.fetchall()
            link_universe = set()
            
            for from_node, to_node in link_results:
                # Create consistent link representation
                link_id = (min(from_node, to_node), max(from_node, to_node))
                link_universe.add(link_id)
        
        # Cache result
        result = (node_universe, link_universe)
        self._universe_cache[cache_key] = result
        
        self.logger.debug(f"Universe calculated: {len(node_universe)} nodes, {len(link_universe)} links")
        return result
    
    def _generate_universe_cache_key(self, config) -> str:
        """Generate cache key for universe calculation."""
        parts = [
            f"fab:{config.fab or 'None'}",
            f"model:{config.model_no or 'None'}",
            f"phase:{config.phase_no or 'None'}",
            f"toolset:{config.toolset or 'None'}"
        ]
        return "|".join(parts)
    
    def _update_run_coverage(self, run_id: str, tracker: CoverageTracker) -> None:
        """Update run record with current coverage information."""
        coverage_ratio = tracker.get_coverage_ratio()
        covered_count = tracker.get_covered_count()
        
        query = """
        UPDATE tb_runs SET
            total_coverage = %s,
            total_nodes = %s,
            total_links = %s
        WHERE id = %s
        """
        
        with self.db.cursor() as cursor:
            cursor.execute(query, (
                coverage_ratio,
                covered_count['nodes'],
                covered_count['links'],
                run_id
            ))
        self.db.commit()
    
    def _calculate_path_contribution(self, path_nodes: List[int], tracker: CoverageTracker) -> float:
        """Calculate how much coverage this path contributes."""
        if not path_nodes or len(path_nodes) < 2:
            return 0.0
        
        # Create temporary tracker to see contribution
        temp_nodes = set()
        temp_links = set()
        
        # Count nodes in this path that are in universe
        for node_id in path_nodes:
            if node_id in tracker.node_id_to_index:
                temp_nodes.add(node_id)
        
        # Count links in this path that are in universe
        for i in range(len(path_nodes) - 1):
            node_a, node_b = path_nodes[i], path_nodes[i + 1]
            link_id = (min(node_a, node_b), max(node_a, node_b))
            if link_id in tracker.link_id_to_index:
                temp_links.add(link_id)
        
        total_elements = tracker.get_total_count()['total']
        if total_elements == 0:
            return 0.0
        
        path_elements = len(temp_nodes) + len(temp_links)
        return path_elements / total_elements
    
    def _update_path_coverage(self, path_definition_id: Optional[int], coverage: float) -> None:
        """Update path definition with its coverage contribution."""
        if not path_definition_id:
            return
        
        query = """
        UPDATE tb_path_definitions 
        SET coverage = %s 
        WHERE id = %s
        """
        
        with self.db.cursor() as cursor:
            cursor.execute(query, (coverage, path_definition_id))
        self.db.commit()
    
    def _calculate_coverage_efficiency(self, run_id: str) -> float:
        """Calculate coverage efficiency (coverage per path found)."""
        query = """
        SELECT COUNT(*) as path_count
        FROM tb_attempt_paths ap
        JOIN tb_path_definitions pd ON ap.path_definition_id = pd.id
        WHERE ap.run_id = %s
        """
        
        with self.db.cursor() as cursor:
            cursor.execute(query, (run_id,))
            result = cursor.fetchone()
            path_count = result[0] if result else 0
        
        if path_count == 0:
            return 0.0
        
        current_coverage = self.get_current_coverage(run_id)
        return current_coverage / path_count
    
    def clear_coverage_tracker(self, run_id: str) -> None:
        """Clear coverage tracker for a run to free memory."""
        if run_id in self._coverage_trackers:
            del self._coverage_trackers[run_id]
            self.logger.debug(f"Coverage tracker cleared for run {run_id}")
    
    def clear_universe_cache(self) -> None:
        """Clear universe cache to free memory."""
        self._universe_cache.clear()
        self.logger.info("Universe cache cleared")
    
    def get_coverage_gaps(self, run_id: str) -> Dict[str, Any]:
        """Analyze coverage gaps to identify uncovered areas."""
        if run_id not in self._coverage_trackers:
            return {}
        
        tracker = self._coverage_trackers[run_id]
        
        # Find uncovered nodes
        uncovered_nodes = []
        for i in range(tracker.total_possible_nodes):
            if not tracker.nodes_bitset[i]:
                node_id = tracker.index_to_node_id[i]
                uncovered_nodes.append(node_id)
        
        # Find uncovered links
        uncovered_links = []
        for i in range(tracker.total_possible_links):
            if not tracker.links_bitset[i]:
                link_id = tracker.index_to_link_id[i]
                uncovered_links.append(link_id)
        
        # Sample some uncovered elements for analysis
        sample_size = min(10, len(uncovered_nodes))
        sampled_nodes = uncovered_nodes[:sample_size] if uncovered_nodes else []
        
        sample_size = min(10, len(uncovered_links))
        sampled_links = uncovered_links[:sample_size] if uncovered_links else []
        
        return {
            'uncovered_nodes_count': len(uncovered_nodes),
            'uncovered_links_count': len(uncovered_links),
            'sample_uncovered_nodes': sampled_nodes,
            'sample_uncovered_links': sampled_links,
            'coverage_ratio': tracker.get_coverage_ratio(),
            'node_coverage_ratio': tracker.get_node_coverage_ratio(),
            'link_coverage_ratio': tracker.get_link_coverage_ratio()
        }
    
    def export_coverage_state(self, run_id: str) -> Optional[Dict[str, Any]]:
        """Export coverage state for persistence or analysis."""
        if run_id not in self._coverage_trackers:
            return None
        
        tracker = self._coverage_trackers[run_id]
        
        return {
            'run_id': run_id,
            'nodes_bitset': tracker.nodes_bitset.tobytes().hex(),
            'links_bitset': tracker.links_bitset.tobytes().hex(),
            'node_id_to_index': tracker.node_id_to_index,
            'link_id_to_index': {str(k): v for k, v in tracker.link_id_to_index.items()},
            'total_possible_nodes': tracker.total_possible_nodes,
            'total_possible_links': tracker.total_possible_links,
            'coverage_ratio': tracker.get_coverage_ratio(),
            'export_timestamp': datetime.now().isoformat()
        }
    
    def import_coverage_state(self, coverage_data: Dict[str, Any]) -> bool:
        """Import coverage state from exported data."""
        try:
            run_id = coverage_data['run_id']
            
            tracker = CoverageTracker()
            tracker.total_possible_nodes = coverage_data['total_possible_nodes']
            tracker.total_possible_links = coverage_data['total_possible_links']
            
            # Restore bitsets
            tracker.nodes_bitset = bitarray()
            tracker.nodes_bitset.frombytes(bytes.fromhex(coverage_data['nodes_bitset']))
            
            tracker.links_bitset = bitarray()
            tracker.links_bitset.frombytes(bytes.fromhex(coverage_data['links_bitset']))
            
            # Restore mappings
            tracker.node_id_to_index = coverage_data['node_id_to_index']
            tracker.index_to_node_id = {v: k for k, v in tracker.node_id_to_index.items()}
            
            # Convert link keys back to tuples
            tracker.link_id_to_index = {}
            tracker.index_to_link_id = {}
            
            for key_str, index in coverage_data['link_id_to_index'].items():
                key_tuple = eval(key_str)  # Convert string back to tuple
                tracker.link_id_to_index[key_tuple] = index
                tracker.index_to_link_id[index] = key_tuple
            
            self._coverage_trackers[run_id] = tracker
            
            self.logger.info(f"Coverage state imported for run {run_id}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error importing coverage state: {str(e)}")
            return False
        