"""
Coverage tracking with bitsets for efficient node and link coverage calculation.
"""
import logging
import sqlite3
from typing import Optional, Dict, Any, List, Set, Tuple
from dataclasses import dataclass
import json


@dataclass
class CoverageMetrics:
    """Coverage metrics for a run."""
    run_id: str
    total_nodes: int
    total_links: int
    covered_nodes: int
    covered_links: int
    coverage_percentage: float
    unique_paths: int
    
    @property
    def node_coverage_percentage(self) -> float:
        return (self.covered_nodes / self.total_nodes * 100) if self.total_nodes > 0 else 0.0
    
    @property
    def link_coverage_percentage(self) -> float:
        return (self.covered_links / self.total_links * 100) if self.total_links > 0 else 0.0


class CoverageManager:
    """Manages coverage tracking using efficient bitset-like operations."""
    
    def __init__(self, db_connection: sqlite3.Connection):
        self.db = db_connection
        self.logger = logging.getLogger(__name__)
        
        # In-memory coverage tracking for active runs
        self._coverage_cache = {}  # run_id -> {'nodes': set, 'links': set}
        
    def initialize_coverage(self, run_id: str, fab: Optional[str] = None,
                          model_no: Optional[int] = None,
                          phase_no: Optional[int] = None,
                          toolset: Optional[str] = None) -> CoverageMetrics:
        """Initialize coverage tracking for a run."""
        
        # Get total nodes and links for the scope
        total_nodes, total_links = self.get_total_nodes_links(fab, model_no, phase_no, toolset)
        
        # Initialize empty coverage sets
        self._coverage_cache[run_id] = {
            'nodes': set(),
            'links': set(),
            'scope': {
                'fab': fab,
                'model_no': model_no,
                'phase_no': phase_no,
                'toolset': toolset
            },
            'total_nodes': total_nodes,
            'total_links': total_links
        }
        
        self.logger.info(f"Coverage initialized for run {run_id}: {total_nodes} nodes, {total_links} links")
        
        return CoverageMetrics(
            run_id=run_id,
            total_nodes=total_nodes,
            total_links=total_links,
            covered_nodes=0,
            covered_links=0,
            coverage_percentage=0.0,
            unique_paths=0
        )
    
    def get_total_nodes_links(self, fab: Optional[str] = None,
                            model_no: Optional[int] = None,
                            phase_no: Optional[int] = None,
                            toolset: Optional[str] = None) -> Tuple[int, int]:
        """Get total number of nodes and links in scope."""
        
        # Base query for nodes
        node_query = """
        SELECT COUNT(DISTINCT ep.node_id)
        FROM tb_equipment_pocs ep
        JOIN tb_equipments e ON ep.equipment_id = e.id
        JOIN tb_toolsets ts ON e.toolset = ts.code
        WHERE ep.is_active = 1 AND e.is_active = 1 AND ts.is_active = 1
        """
        
        # Base query for links
        link_query = """
        SELECT COUNT(DISTINCT conn.id)
        FROM tb_equipment_poc_connections conn
        JOIN tb_equipment_pocs ep1 ON conn.from_poc_id = ep1.id
        JOIN tb_equipment_pocs ep2 ON conn.to_poc_id = ep2.id
        JOIN tb_equipments e1 ON ep1.equipment_id = e1.id
        JOIN tb_equipments e2 ON ep2.equipment_id = e2.id
        JOIN tb_toolsets ts1 ON e1.toolset = ts1.code
        JOIN tb_toolsets ts2 ON e2.toolset = ts2.code
        WHERE conn.is_valid = 1 
          AND ep1.is_active = 1 AND ep2.is_active = 1
          AND e1.is_active = 1 AND e2.is_active = 1
          AND ts1.is_active = 1 AND ts2.is_active = 1
        """
        
        params = []
        conditions = []
        
        # Add filters
        if fab:
            conditions.append("ts.fab = ?")
            params.append(fab)
        
        if model_no:
            conditions.append("ts.model_no = ?")
            params.append(model_no)
        
        if phase_no:
            conditions.append("ts.phase_no = ?")
            params.append(phase_no)
        
        if toolset:
            conditions.append("ts.code = ?")
            params.append(toolset)
        
        # Apply conditions to both queries
        if conditions:
            condition_str = " AND " + " AND ".join(conditions)
            node_query += condition_str
            
            # For link query, we need to apply conditions to both toolsets
            link_conditions = []
            link_params = []
            
            for condition in conditions:
                link_conditions.append(condition.replace("ts.", "ts1."))
                link_conditions.append(condition.replace("ts.", "ts2."))
                link_params.extend([params[conditions.index(condition)]] * 2)
            
            link_query += " AND " + " AND ".join(link_conditions)
            link_params = params + params  # Duplicate params for both sides
        else:
            link_params = []
        
        # Execute queries
        cursor = self.db.execute(node_query, params)
        total_nodes = cursor.fetchone()[0] or 0
        
        cursor = self.db.execute(link_query, link_params)
        total_links = cursor.fetchone()[0] or 0
        
        return total_nodes, total_links
    
    def update_coverage(self, run_id: str, path_nodes: List[int], 
                       path_links: List[int]) -> CoverageMetrics:
        """Update coverage with new path nodes and links."""
        
        if run_id not in self._coverage_cache:
            self.logger.warning(f"Coverage not initialized for run {run_id}")
            return self.get_coverage_metrics(run_id)
        
        # Add new nodes and links to coverage sets
        coverage_data = self._coverage_cache[run_id]
        
        # Convert to sets and add
        new_nodes = set(path_nodes)
        new_links = set(path_links)
        
        old_node_count = len(coverage_data['nodes'])
        old_link_count = len(coverage_data['links'])
        
        # Update coverage sets
        coverage_data['nodes'].update(new_nodes)
        coverage_data['links'].update(new_links)
        
        new_node_count = len(coverage_data['nodes'])
        new_link_count = len(coverage_data['links'])
        
        # Log the update
        nodes_added = new_node_count - old_node_count
        links_added = new_link_count - old_link_count
        
        if nodes_added > 0 or links_added > 0:
            self.logger.debug(f"Coverage updated for {run_id}: +{nodes_added} nodes, +{links_added} links")
        
        return self.get_coverage_metrics(run_id)
    
    def get_coverage_metrics(self, run_id: str) -> CoverageMetrics:
        """Get current coverage metrics for a run."""
        
        if run_id not in self._coverage_cache:
            # Try to reconstruct from database
            return self._reconstruct_coverage_from_db(run_id)
        
        coverage_data = self._coverage_cache[run_id]
        
        covered_nodes = len(coverage_data['nodes'])
        covered_links = len(coverage_data['links'])
        total_nodes = coverage_data['total_nodes']
        total_links = coverage_data['total_links']
        
        # Calculate overall coverage percentage
        if total_nodes + total_links > 0:
            coverage_percentage = ((covered_nodes + covered_links) / (total_nodes + total_links)) * 100
        else:
            coverage_percentage = 0.0
        
        # Get unique path count
        unique_paths = self._get_unique_path_count(run_id)
        
        return CoverageMetrics(
            run_id=run_id,
            total_nodes=total_nodes,
            total_links=total_links,
            covered_nodes=covered_nodes,
            covered_links=covered_links,
            coverage_percentage=coverage_percentage,
            unique_paths=unique_paths
        )
    
    def get_current_coverage(self, run_id: str) -> float:
        """Get current coverage percentage for a run."""
        metrics = self.get_coverage_metrics(run_id)
        return metrics.coverage_percentage / 100.0  # Return as decimal
    
    def _reconstruct_coverage_from_db(self, run_id: str) -> CoverageMetrics:
        """Reconstruct coverage from database records."""
        
        # Get run configuration
        run_query = """
        SELECT fab, model_no, phase_no, toolset, total_nodes, total_links
        FROM tb_runs
        WHERE id = ?
        """
        
        cursor = self.db.execute(run_query, [run_id])
        run_data = cursor.fetchone()
        
        if not run_data:
            self.logger.error(f"Run {run_id} not found")
            return CoverageMetrics(run_id, 0, 0, 0, 0, 0.0, 0)
        
        fab, model_no, phase_no, toolset, total_nodes, total_links = run_data
        
        # Get all paths for this run and collect nodes/links
        paths_query = """
        SELECT DISTINCT pd.path_context
        FROM tb_path_definitions pd
        JOIN tb_attempt_paths ap ON pd.id = ap.path_definition_id
        WHERE ap.run_id = ? AND pd.path_context IS NOT NULL
        """
        
        cursor = self.db.execute(paths_query, [run_id])
        path_contexts = cursor.fetchall()
        
        # Reconstruct coverage sets
        all_nodes = set()
        all_links = set()
        
        for (context_json,) in path_contexts:
            try:
                context = json.loads(context_json)
                nodes = context.get('nodes', [])
                links = context.get('links', [])
                
                all_nodes.update(nodes)
                all_links.update(links)
                
            except json.JSONDecodeError:
                continue
        
        # Store in cache for future use
        self._coverage_cache[run_id] = {
            'nodes': all_nodes,
            'links': all_links,
            'scope': {
                'fab': fab,
                'model_no': model_no,
                'phase_no': phase_no,
                'toolset': toolset
            },
            'total_nodes': total_nodes or 0,
            'total_links': total_links or 0
        }
        
        # Calculate metrics
        covered_nodes = len(all_nodes)
        covered_links = len(all_links)
        
        if (total_nodes or 0) + (total_links or 0) > 0:
            coverage_percentage = ((covered_nodes + covered_links) / ((total_nodes or 0) + (total_links or 0))) * 100
        else:
            coverage_percentage = 0.0
        
        unique_paths = len(path_contexts)
        
        return CoverageMetrics(
            run_id=run_id,
            total_nodes=total_nodes or 0,
            total_links=total_links or 0,
            covered_nodes=covered_nodes,
            covered_links=covered_links,
            coverage_percentage=coverage_percentage,
            unique_paths=unique_paths
        )
    
    def _get_unique_path_count(self, run_id: str) -> int:
        """Get count of unique paths for a run."""
        
        query = """
        SELECT COUNT(DISTINCT ap.path_definition_id)
        FROM tb_attempt_paths ap
        WHERE ap.run_id = ? AND ap.path_definition_id IS NOT NULL
        """
        
        cursor = self.db.execute(query, [run_id])
        return cursor.fetchone()[0] or 0
    
    def get_uncovered_elements(self, run_id: str, element_type: str = 'both',
                             limit: int = 100) -> Dict[str, List[int]]:
        """Get uncovered nodes and/or links."""
        
        if run_id not in self._coverage_cache:
            self._reconstruct_coverage_from_db(run_id)
        
        coverage_data = self._coverage_cache[run_id]
        scope = coverage_data['scope']
        
        uncovered = {'nodes': [], 'links': []}
        
        if element_type in ['nodes', 'both']:
            # Get all nodes in scope
            node_query = """
            SELECT DISTINCT ep.node_id
            FROM tb_equipment_pocs ep
            JOIN tb_equipments e ON ep.equipment_id = e.id
            JOIN tb_toolsets ts ON e.toolset = ts.code
            WHERE ep.is_active = 1 AND e.is_active = 1 AND ts.is_active = 1
            """
            
            params = []
            conditions = []
            
            for key, value in scope.items():
                if value is not None:
                    if key == 'toolset':
                        conditions.append("ts.code = ?")
                    else:
                        conditions.append(f"ts.{key} = ?")
                    params.append(value)
            
            if conditions:
                node_query += " AND " + " AND ".join(conditions)
            
            node_query += f" LIMIT {limit}"
            
            cursor = self.db.execute(node_query, params)
            all_nodes = {row[0] for row in cursor.fetchall()}
            
            uncovered['nodes'] = list(all_nodes - coverage_data['nodes'])[:limit]
        
        if element_type in ['links', 'both']:
            # Get all links in scope
            link_query = """
            SELECT DISTINCT conn.id
            FROM tb_equipment_poc_connections conn
            JOIN tb_equipment_pocs ep1 ON conn.from_poc_id = ep1.id
            JOIN tb_equipment_pocs ep2 ON conn.to_poc_id = ep2.id
            JOIN tb_equipments e1 ON ep1.equipment_id = e1.id
            JOIN tb_equipments e2 ON ep2.equipment_id = e2.id
            JOIN tb_toolsets ts1 ON e1.toolset = ts1.code
            JOIN tb_toolsets ts2 ON e2.toolset = ts2.code
            WHERE conn.is_valid = 1 
              AND ep1.is_active = 1 AND ep2.is_active = 1
              AND e1.is_active = 1 AND e2.is_active = 1
              AND ts1.is_active = 1 AND ts2.is_active = 1
            """
            
            link_params = []
            link_conditions = []
            
            for key, value in scope.items():
                if value is not None:
                    if key == 'toolset':
                        link_conditions.append("ts1.code = ? AND ts2.code = ?")
                        link_params.extend([value, value])
                    else:
                        link_conditions.append(f"ts1.{key} = ? AND ts2.{key} = ?")
                        link_params.extend([value, value])
            
            if link_conditions:
                link_query += " AND " + " AND ".join(link_conditions)
            
            link_query += f" LIMIT {limit}"
            
            cursor = self.db.execute(link_query, link_params)
            all_links = {row[0] for row in cursor.fetchall()}
            
            uncovered['links'] = list(all_links - coverage_data['links'])[:limit]
        
        return uncovered
    
    def get_coverage_gaps(self, run_id: str) -> Dict[str, Any]:
        """Identify coverage gaps and suggest improvements."""
        
        metrics = self.get_coverage_metrics(run_id)
        uncovered = self.get_uncovered_elements(run_id, 'both', 50)
        
        # Analyze gap patterns
        gap_analysis = {
            'total_gaps': len(uncovered['nodes']) + len(uncovered['links']),
            'node_gaps': len(uncovered['nodes']),
            'link_gaps': len(uncovered['links']),
            'coverage_efficiency': metrics.coverage_percentage,
            'suggestions': []
        }
        
        # Generate suggestions based on gaps
        if metrics.coverage_percentage < 50:
            gap_analysis['suggestions'].append({
                'type': 'INCREASE_SAMPLING',
                'priority': 'HIGH',
                'description': 'Coverage is low. Consider increasing sampling attempts or adjusting bias configuration.'
            })
        
        if len(uncovered['nodes']) > len(uncovered['links']):
            gap_analysis['suggestions'].append({
                'type': 'FOCUS_ON_NODES',
                'priority': 'MEDIUM',
                'description': 'Node coverage is lagging behind link coverage. Focus on paths with more diverse nodes.'
            })
        
        # Check for toolset distribution
        if run_id in self._coverage_cache:
            scope = self._coverage_cache[run_id]['scope']
            if not scope.get('toolset'):  # Multiple toolsets
                gap_analysis['suggestions'].append({
                    'type': 'TOOLSET_ANALYSIS',
                    'priority': 'LOW',
                    'description': 'Consider analyzing coverage by individual toolsets to identify under-sampled areas.'
                })
        
        return gap_analysis
    
    def compare_coverage(self, run_id1: str, run_id2: str) -> Dict[str, Any]:
        """Compare coverage between two runs."""
        
        metrics1 = self.get_coverage_metrics(run_id1)
        metrics2 = self.get_coverage_metrics(run_id2)
        
        # Calculate overlaps if both runs are in cache
        overlap_analysis = {}
        if run_id1 in self._coverage_cache and run_id2 in self._coverage_cache:
            nodes1 = self._coverage_cache[run_id1]['nodes']
            nodes2 = self._coverage_cache[run_id2]['nodes']
            links1 = self._coverage_cache[run_id1]['links']
            links2 = self._coverage_cache[run_id2]['links']
            
            node_overlap = len(nodes1 & nodes2)
            link_overlap = len(links1 & links2)
            
            overlap_analysis = {
                'node_overlap': node_overlap,
                'link_overlap': link_overlap,
                'node_overlap_percentage': (node_overlap / len(nodes1 | nodes2) * 100) if (nodes1 | nodes2) else 0,
                'link_overlap_percentage': (link_overlap / len(links1 | links2) * 100) if (links1 | links2) else 0,
                'unique_to_run1_nodes': len(nodes1 - nodes2),
                'unique_to_run2_nodes': len(nodes2 - nodes1),
                'unique_to_run1_links': len(links1 - links2),
                'unique_to_run2_links': len(links2 - links1)
            }
        
        return {
            'run1': {
                'run_id': run_id1,
                'metrics': metrics1
            },
            'run2': {
                'run_id': run_id2,
                'metrics': metrics2
            },
            'comparison': {
                'coverage_difference': metrics2.coverage_percentage - metrics1.coverage_percentage,
                'node_difference': metrics2.covered_nodes - metrics1.covered_nodes,
                'link_difference': metrics2.covered_links - metrics1.covered_links,
                'path_difference': metrics2.unique_paths - metrics1.unique_paths
            },
            'overlap': overlap_analysis
        }
    
    def get_coverage_distribution(self, run_id: str) -> Dict[str, Any]:
        """Get coverage distribution by various dimensions."""
        
        if run_id not in self._coverage_cache:
            self._reconstruct_coverage_from_db(run_id)
        
        # Get paths with their attributes
        paths_query = """
        SELECT 
            pd.target_fab,
            pd.target_model_no,
            pd.target_phase_no,
            pd.target_toolset_no,
            pd.utilities_scope,
            pd.data_codes_scope,
            pd.node_count,
            pd.link_count
        FROM tb_path_definitions pd
        JOIN tb_attempt_paths ap ON pd.id = ap.path_definition_id
        WHERE ap.run_id = ?
        """
        
        cursor = self.db.execute(paths_query, [run_id])
        paths = cursor.fetchall()
        
        # Analyze distribution
        distribution = {
            'by_fab': {},
            'by_model': {},
            'by_phase': {},
            'by_toolset': {},
            'by_utility': {},
            'by_complexity': {'SIMPLE': 0, 'MEDIUM': 0, 'COMPLEX': 0}
        }
        
        for path in paths:
            fab, model_no, phase_no, toolset, utilities_json, data_codes_json, node_count, link_count = path
            
            # Count by dimensions
            if fab:
                distribution['by_fab'][fab] = distribution['by_fab'].get(fab, 0) + 1
            
            if model_no:
                distribution['by_model'][str(model_no)] = distribution['by_model'].get(str(model_no), 0) + 1
            
            if phase_no:
                distribution['by_phase'][str(phase_no)] = distribution['by_phase'].get(str(phase_no), 0) + 1
            
            if toolset:
                distribution['by_toolset'][toolset] = distribution['by_toolset'].get(toolset, 0) + 1
            
            # Utilities
            if utilities_json:
                try:
                    utilities = json.loads(utilities_json)
                    for utility in utilities:
                        distribution['by_utility'][str(utility)] = distribution['by_utility'].get(str(utility), 0) + 1
                except json.JSONDecodeError:
                    pass
            
            # Complexity
            if node_count <= 5:
                distribution['by_complexity']['SIMPLE'] += 1
            elif node_count <= 15:
                distribution['by_complexity']['MEDIUM'] += 1
            else:
                distribution['by_complexity']['COMPLEX'] += 1
        
        return distribution
    
    def export_coverage_report(self, run_id: str) -> Dict[str, Any]:
        """Generate comprehensive coverage report."""
        
        metrics = self.get_coverage_metrics(run_id)
        gaps = self.get_coverage_gaps(run_id)
        distribution = self.get_coverage_distribution(run_id)
        
        # Get sampling efficiency
        efficiency_query = """
        SELECT 
            COUNT(*) as total_attempts,
            COUNT(CASE WHEN path_definition_id IS NOT NULL THEN 1 END) as successful_attempts
        FROM tb_attempt_paths
        WHERE run_id = ?
        """
        
        cursor = self.db.execute(efficiency_query, [run_id])
        efficiency_data = cursor.fetchone()
        
        success_rate = 0.0
        if efficiency_data and efficiency_data[0] > 0:
            success_rate = (efficiency_data[1] / efficiency_data[0]) * 100
        
        return {
            'run_id': run_id,
            'timestamp': datetime.now().isoformat(),
            'coverage_metrics': {
                'total_nodes': metrics.total_nodes,
                'total_links': metrics.total_links,
                'covered_nodes': metrics.covered_nodes,
                'covered_links': metrics.covered_links,
                'node_coverage_percentage': metrics.node_coverage_percentage,
                'link_coverage_percentage': metrics.link_coverage_percentage,
                'overall_coverage_percentage': metrics.coverage_percentage,
                'unique_paths': metrics.unique_paths
            },
            'sampling_efficiency': {
                'total_attempts': efficiency_data[0] if efficiency_data else 0,
                'successful_attempts': efficiency_data[1] if efficiency_data else 0,
                'success_rate_percentage': success_rate
            },
            'coverage_gaps': gaps,
            'coverage_distribution': distribution,
            'recommendations': self._generate_coverage_recommendations(metrics, gaps, distribution)
        }
    
    def _generate_coverage_recommendations(self, metrics: CoverageMetrics, 
                                         gaps: Dict[str, Any], 
                                         distribution: Dict[str, Any]) -> List[Dict[str, str]]:
        """Generate recommendations based on coverage analysis."""
        
        recommendations = []
        
        # Coverage-based recommendations
        if metrics.coverage_percentage < 30:
            recommendations.append({
                'type': 'COVERAGE_LOW',
                'priority': 'HIGH',
                'action': 'Increase sampling attempts significantly',
                'rationale': f'Current coverage is only {metrics.coverage_percentage:.1f}%'
            })
        elif metrics.coverage_percentage < 60:
            recommendations.append({
                'type': 'COVERAGE_MEDIUM',
                'priority': 'MEDIUM',
                'action': 'Continue sampling with adjusted bias configuration',
                'rationale': f'Coverage is moderate at {metrics.coverage_percentage:.1f}%'
            })
        
        # Balance recommendations
        node_coverage = metrics.node_coverage_percentage
        link_coverage = metrics.link_coverage_percentage
        
        if abs(node_coverage - link_coverage) > 15:
            if node_coverage < link_coverage:
                recommendations.append({
                    'type': 'BALANCE_NODES',
                    'priority': 'MEDIUM',
                    'action': 'Focus on paths with more diverse nodes',
                    'rationale': f'Node coverage ({node_coverage:.1f}%) lags behind link coverage ({link_coverage:.1f}%)'
                })
            else:
                recommendations.append({
                    'type': 'BALANCE_LINKS',
                    'priority': 'MEDIUM',
                    'action': 'Focus on longer paths with more connections',
                    'rationale': f'Link coverage ({link_coverage:.1f}%) lags behind node coverage ({node_coverage:.1f}%)'
                })
        
        # Distribution-based recommendations
        complexity_dist = distribution.get('by_complexity', {})
        total_paths = sum(complexity_dist.values())
        
        if total_paths > 0:
            simple_ratio = complexity_dist.get('SIMPLE', 0) / total_paths
            if simple_ratio > 0.7:
                recommendations.append({
                    'type': 'COMPLEXITY_BIAS',
                    'priority': 'LOW',
                    'action': 'Encourage more complex path discovery',
                    'rationale': f'{simple_ratio:.1%} of paths are simple, consider adjusting minimum distance'
                })
        
        # Toolset diversity recommendations
        toolset_count = len(distribution.get('by_toolset', {}))
        if toolset_count > 0 and toolset_count < 5:
            recommendations.append({
                'type': 'TOOLSET_DIVERSITY',
                'priority': 'MEDIUM',
                'action': 'Expand sampling to more toolsets',
                'rationale': f'Only {toolset_count} toolsets covered'
            })
        
        return recommendations
    
    def clear_coverage_cache(self, run_id: Optional[str] = None) -> None:
        """Clear coverage cache for specific run or all runs."""
        
        if run_id:
            if run_id in self._coverage_cache:
                del self._coverage_cache[run_id]
                self.logger.info(f"Coverage cache cleared for run {run_id}")
        else:
            self._coverage_cache.clear()
            self.logger.info("All coverage cache cleared")
    
    def get_cache_statistics(self) -> Dict[str, Any]:
        """Get coverage cache statistics."""
        
        cache_stats = {
            'cached_runs': len(self._coverage_cache),
            'run_ids': list(self._coverage_cache.keys()),
            'memory_usage': {}
        }
        
        for run_id, data in self._coverage_cache.items():
            cache_stats['memory_usage'][run_id] = {
                'nodes_count': len(data['nodes']),
                'links_count': len(data['links']),
                'estimated_bytes': len(data['nodes']) * 8 + len(data['links']) * 8  # Rough estimate
            }
        
        return cache_stats
    
    def optimize_coverage_strategy(self, run_id: str) -> Dict[str, Any]:
        """Suggest optimizations for coverage strategy."""
        
        metrics = self.get_coverage_metrics(run_id)
        gaps = self.get_coverage_gaps(run_id)
        
        # Analyze current sampling patterns
        sampling_query = """
        SELECT 
            ap.start_node_id,
            ap.end_node_id,
            ap.cost,
            CASE WHEN ap.path_definition_id IS NOT NULL THEN 1 ELSE 0 END as success
        FROM tb_attempt_paths ap
        WHERE ap.run_id = ?
        ORDER BY ap.picked_at DESC
        LIMIT 100
        """
        
        cursor = self.db.execute(sampling_query, [run_id])
        recent_attempts = cursor.fetchall()
        
        # Calculate patterns
        total_attempts = len(recent_attempts)
        successful_attempts = sum(1 for attempt in recent_attempts if attempt[3])
        
        success_rate = successful_attempts / total_attempts if total_attempts > 0 else 0
        
        # Analyze cost distribution
        costs = [attempt[2] for attempt in recent_attempts if attempt[2] is not None]
        avg_cost = sum(costs) / len(costs) if costs else 0
        
        optimizations = {
            'current_performance': {
                'success_rate': success_rate * 100,
                'average_cost': avg_cost,
                'recent_attempts': total_attempts
            },
            'recommendations': [],
            'suggested_parameters': {}
        }
        
        # Generate optimization recommendations
        if success_rate < 0.3:
            optimizations['recommendations'].append({
                'type': 'LOW_SUCCESS_RATE',
                'action': 'Reduce minimum distance or adjust bias configuration',
                'impact': 'HIGH'
            })
            optimizations['suggested_parameters']['min_distance_between_nodes'] = max(5, int(avg_cost * 0.5))
        
        if metrics.coverage_percentage < 50 and success_rate > 0.7:
            optimizations['recommendations'].append({
                'type': 'INCREASE_DIVERSITY',
                'action': 'Increase utility and phase diversity weights',
                'impact': 'MEDIUM'
            })
            optimizations['suggested_parameters']['utility_diversity_weight'] = 0.4
            optimizations['suggested_parameters']['phase_diversity_weight'] = 0.3
        
        if gaps['total_gaps'] > 1000:
            optimizations['recommendations'].append({
                'type': 'LARGE_GAPS',
                'action': 'Consider exhaustive approach for remaining coverage',
                'impact': 'HIGH'
            })
        
        return optimizations