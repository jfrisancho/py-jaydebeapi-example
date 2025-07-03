"""
Coverage tracking service with bitsets for efficient coverage calculation.
Tracks which POCs and equipment are covered by successful paths.
"""

import logging
from typing import List, Dict, Set, Optional, Tuple
from dataclasses import dataclass
from bitarray import bitarray
import json

logger = logging.getLogger(__name__)


@dataclass
class CoverageMetrics:
    """Coverage metrics for a run"""
    total_pocs: int
    covered_pocs: int
    poc_coverage_percentage: float
    total_equipment: int
    covered_equipment: int
    equipment_coverage_percentage: float
    total_connections: int
    successful_connections: int
    connection_success_rate: float
    unique_paths: int
    total_attempts: int


@dataclass
class CoverageBreakdown:
    """Detailed coverage breakdown by categories"""
    by_fab: Dict[str, CoverageMetrics]
    by_toolset: Dict[str, CoverageMetrics]
    by_model: Dict[int, CoverageMetrics]
    by_phase: Dict[int, CoverageMetrics]
    by_utility: Dict[int, CoverageMetrics]
    by_data_code: Dict[int, CoverageMetrics]


class CoverageService:
    """Service for tracking and calculating coverage metrics"""
    
    def __init__(self, db_connection):
        self.db = db_connection
        self._poc_index_map = {}
        self._equipment_index_map = {}
        self._coverage_bitset = None
        self._equipment_bitset = None
        
    def initialize_coverage_tracking(self, run_id: str, config: Optional[Dict] = None) -> Dict:
        """
        Initialize coverage tracking for a run with intelligent strategy support.
        
        Args:
            run_id: Run identifier
            config: Configuration dict from RandomGenerationConfig
            
        Returns:
            Dictionary with initialization results
        """
        logger.info(f"Initializing coverage tracking for run {run_id}")
        
        # Extract filters from config
        filters = {}
        if config:
            if config.get('fab'):
                filters['fab'] = config['fab']
            if config.get('model_no'):
                filters['model_no'] = config['model_no']
            if config.get('phase_no'):
                filters['phase_no'] = config['phase_no']
            if config.get('expanded_toolsets'):
                filters['expanded_toolsets'] = config['expanded_toolsets']
            elif config.get('toolset'):
                filters['toolset'] = config['toolset']
        
        # Get all relevant POCs and equipment
        pocs_data = self._get_pocs_for_coverage(filters)
        equipment_data = self._get_equipment_for_coverage(filters)
        
        # Create index mappings
        self._poc_index_map = {poc['id']: idx for idx, poc in enumerate(pocs_data)}
        self._equipment_index_map = {eq['id']: idx for idx, eq in enumerate(equipment_data)}
        
        # Initialize bitsets
        self._coverage_bitset = bitarray(len(pocs_data))
        self._coverage_bitset.setall(0)
        
        self._equipment_bitset = bitarray(len(equipment_data))
        self._equipment_bitset.setall(0)
        
        logger.info(f"Initialized coverage tracking: {len(pocs_data)} POCs, {len(equipment_data)} equipment")
        
        return {
            'total_pocs': len(pocs_data),
            'total_equipment': len(equipment_data),
            'pocs_data': pocs_data,
            'equipment_data': equipment_data,
            'strategy_info': config.get('coverage_strategy', 'standard') if config else 'standard'
        }
        
    def update_coverage_from_successful_path(self, from_poc_id: int, to_poc_id: int, 
                                           from_equipment_id: int, to_equipment_id: int) -> bool:
        """
        Update coverage from a successful path.
        
        Args:
            from_poc_id: Starting POC ID
            to_poc_id: Ending POC ID
            from_equipment_id: Starting equipment ID
            to_equipment_id: Ending equipment ID
            
        Returns:
            True if coverage was updated, False if already covered
        """
        updated = False
        
        # Update POC coverage
        if from_poc_id in self._poc_index_map:
            idx = self._poc_index_map[from_poc_id]
            if not self._coverage_bitset[idx]:
                self._coverage_bitset[idx] = 1
                updated = True
                
        if to_poc_id in self._poc_index_map:
            idx = self._poc_index_map[to_poc_id]
            if not self._coverage_bitset[idx]:
                self._coverage_bitset[idx] = 1
                updated = True
                
        # Update equipment coverage
        if from_equipment_id in self._equipment_index_map:
            idx = self._equipment_index_map[from_equipment_id]
            if not self._equipment_bitset[idx]:
                self._equipment_bitset[idx] = 1
                updated = True
                
        if to_equipment_id in self._equipment_index_map:
            idx = self._equipment_index_map[to_equipment_id]
            if not self._equipment_bitset[idx]:
                self._equipment_bitset[idx] = 1
                updated = True
                
        return updated
        
    def calculate_current_coverage(self) -> float:
        """
        Calculate current coverage percentage.
        
        Returns:
            Coverage percentage (0.0 to 1.0)
        """
        if not self._coverage_bitset or len(self._coverage_bitset) == 0:
            return 0.0
            
        covered_count = self._coverage_bitset.count(1)
        total_count = len(self._coverage_bitset)
        
        return covered_count / total_count if total_count > 0 else 0.0
        
    def calculate_equipment_coverage(self) -> float:
        """
        Calculate current equipment coverage percentage.
        
        Returns:
            Equipment coverage percentage (0.0 to 1.0)
        """
        if not self._equipment_bitset or len(self._equipment_bitset) == 0:
            return 0.0
            
        covered_count = self._equipment_bitset.count(1)
        total_count = len(self._equipment_bitset)
        
        return covered_count / total_count if total_count > 0 else 0.0
        
    def get_coverage_metrics_for_run(self, run_id: str) -> CoverageMetrics:
        """
        Get comprehensive coverage metrics for a run.
        
        Args:
            run_id: Run identifier
            
        Returns:
            Coverage metrics object
        """
        # Get basic statistics
        stats_query = """
            SELECT 
                COUNT(*) as total_attempts,
                COUNT(CASE WHEN ap.cost IS NOT NULL THEN 1 END) as successful_connections,
                COUNT(DISTINCT pd.path_hash) as unique_paths
            FROM tb_attempt_paths ap
            JOIN tb_path_definitions pd ON ap.path_definition_id = pd.id
            WHERE ap.run_id = ?
        """
        
        cursor = self.db.cursor()
        cursor.execute(stats_query, (run_id,))
        stats = cursor.fetchone()
        
        # Get POC coverage
        poc_coverage = self._calculate_poc_coverage_for_run(run_id)
        equipment_coverage = self._calculate_equipment_coverage_for_run(run_id)
        
        return CoverageMetrics(
            total_pocs=poc_coverage['total_pocs'],
            covered_pocs=poc_coverage['covered_pocs'],
            poc_coverage_percentage=poc_coverage['coverage_percentage'],
            total_equipment=equipment_coverage['total_equipment'],
            covered_equipment=equipment_coverage['covered_equipment'],
            equipment_coverage_percentage=equipment_coverage['coverage_percentage'],
            total_connections=stats['total_attempts'] or 0,
            successful_connections=stats['successful_connections'] or 0,
            connection_success_rate=(stats['successful_connections'] or 0) / max(stats['total_attempts'] or 1, 1),
            unique_paths=stats['unique_paths'] or 0,
            total_attempts=stats['total_attempts'] or 0
        )
        
    def get_coverage_breakdown_for_run(self, run_id: str) -> CoverageBreakdown:
        """
        Get detailed coverage breakdown by categories.
        
        Args:
            run_id: Run identifier
            
        Returns:
            Coverage breakdown object
        """
        breakdown = CoverageBreakdown(
            by_fab={},
            by_toolset={},
            by_model={},
            by_phase={},
            by_utility={},
            by_data_code={}
        )
        
        # Get all successful paths for the run
        successful_paths = self._get_successful_paths_with_details(run_id)
        
        # Group by categories
        fab_groups = self._group_by_attribute(successful_paths, 'fab')
        toolset_groups = self._group_by_attribute(successful_paths, 'toolset')
        model_groups = self._group_by_attribute(successful_paths, 'model_no')
        phase_groups = self._group_by_attribute(successful_paths, 'phase_no')
        
        # Calculate metrics for each group
        for fab, paths in fab_groups.items():
            breakdown.by_fab[fab] = self._calculate_metrics_for_paths(paths)
            
        for toolset, paths in toolset_groups.items():
            breakdown.by_toolset[toolset] = self._calculate_metrics_for_paths(paths)
            
        for model, paths in model_groups.items():
            breakdown.by_model[model] = self._calculate_metrics_for_paths(paths)
            
        for phase, paths in phase_groups.items():
            breakdown.by_phase[phase] = self._calculate_metrics_for_paths(paths)
            
        return breakdown
        
    def get_uncovered_pocs(self, run_id: str, filters: Optional[Dict] = None) -> List[Dict]:
        """
        Get list of POCs that are not covered by any successful path.
        
        Args:
            run_id: Run identifier
            filters: Optional filters
            
        Returns:
            List of uncovered POC details
        """
        # Get all POCs in scope
        all_pocs = self._get_pocs_for_coverage(filters)
        
        # Get covered POC IDs
        covered_pocs = self._get_covered_poc_ids_for_run(run_id)
        
        # Filter uncovered
        uncovered = [poc for poc in all_pocs if poc['id'] not in covered_pocs]
        
        logger.info(f"Found {len(uncovered)} uncovered POCs out of {len(all_pocs)} total")
        return uncovered
        
    def get_coverage_gaps(self, run_id: str) -> Dict[str, List[Dict]]:
        """
        Identify coverage gaps by category.
        
        Args:
            run_id: Run identifier
            
        Returns:
            Dictionary with coverage gaps by category
        """
        gaps = {
            'uncovered_fabs': [],
            'uncovered_toolsets': [],
            'low_coverage_fabs': [],
            'low_coverage_toolsets': [],
            'isolated_equipment': []
        }
        
        # Get coverage breakdown
        breakdown = self.get_coverage_breakdown_for_run(run_id)
        
        # Identify gaps
        for fab, metrics in breakdown.by_fab.items():
            if metrics.poc_coverage_percentage == 0:
                gaps['uncovered_fabs'].append({'fab': fab, 'metrics': metrics})
            elif metrics.poc_coverage_percentage < 0.5:  # Less than 50% coverage
                gaps['low_coverage_fabs'].append({'fab': fab, 'metrics': metrics})
                
        for toolset, metrics in breakdown.by_toolset.items():
            if metrics.poc_coverage_percentage == 0:
                gaps['uncovered_toolsets'].append({'toolset': toolset, 'metrics': metrics})
            elif metrics.poc_coverage_percentage < 0.5:
                gaps['low_coverage_toolsets'].append({'toolset': toolset, 'metrics': metrics})
                
        # Find isolated equipment (equipment with no successful connections)
        gaps['isolated_equipment'] = self._find_isolated_equipment(run_id)
        
        return gaps
        
    def generate_coverage_report(self, run_id: str, strategy_info: Optional[Dict] = None) -> Dict:
        """
        Generate comprehensive coverage report with strategy awareness.
        
        Args:
            run_id: Run identifier
            strategy_info: Information about the coverage strategy applied
            
        Returns:
            Comprehensive coverage report
        """
        metrics = self.get_coverage_metrics_for_run(run_id)
        breakdown = self.get_coverage_breakdown_for_run(run_id)
        gaps = self.get_coverage_gaps(run_id)
        
        # Calculate additional insights
        insights = self._generate_coverage_insights(metrics, breakdown, gaps)
        
        # Add strategy-specific insights
        if strategy_info:
            strategy_insights = self._generate_strategy_insights(strategy_info, metrics)
            insights.extend(strategy_insights)
        
        return {
            'run_id': run_id,
            'overall_metrics': metrics,
            'breakdown': breakdown,
            'gaps': gaps,
            'insights': insights,
            'recommendations': self._generate_coverage_recommendations(metrics, gaps, strategy_info),
            'strategy_info': strategy_info
        }
        
    def _generate_strategy_insights(self, strategy_info: Dict, metrics: CoverageMetrics) -> List[str]:
        """Generate insights specific to the coverage strategy used"""
        insights = []
        
        strategy = strategy_info.get('coverage_strategy', 'standard')
        
        if strategy == 'adaptive':
            insights.append("Coverage target was adapted to match scope limitations")
            
        elif strategy == 'intensive':
            insights.append("Intensive sampling strategy applied to critical toolset")
            if metrics.poc_coverage_percentage > 0.7:
                insights.append("Intensive strategy achieved high coverage within scope")
                
        elif strategy == 'grouped':
            expanded_count = len(strategy_info.get('expanded_toolsets', []))
            if expanded_count > 1:
                insights.append(f"Scope expanded to {expanded_count} related toolsets for better coverage")
                
        if strategy_info.get('original_coverage_target'):
            original = strategy_info['original_coverage_target']
            adjusted = strategy_info.get('coverage_target', original)
            if adjusted != original:
                insights.append(f"Coverage target adjusted from {original:.1%} to {adjusted:.1%}")
                
        return insights
        
    def _generate_coverage_recommendations(self, metrics: CoverageMetrics, gaps: Dict, 
                                         strategy_info: Optional[Dict] = None) -> List[str]:
        """Generate recommendations based on coverage analysis with strategy awareness"""
        recommendations = []
        
        if metrics.poc_coverage_percentage < 0.5:
            if strategy_info and strategy_info.get('coverage_strategy') == 'intensive':
                recommendations.append("Consider expanding scope beyond current toolset for broader coverage")
            else:
                recommendations.append("Increase target coverage or sampling iterations")
                
        if gaps['uncovered_fabs']:
            recommendations.append("Focus additional sampling on uncovered buildings")
            
        if gaps['low_coverage_fabs']:
            recommendations.append("Increase sampling density in low-coverage buildings")
            
        if metrics.connection_success_rate < 0.7:
            recommendations.append("Investigate network connectivity issues")
            recommendations.append("Review pathfinding algorithm parameters")
            
        if gaps['isolated_equipment']:
            recommendations.append("Verify connectivity of isolated equipment")
            recommendations.append("Check if isolated equipment should be excluded from analysis")
            
        # Strategy-specific recommendations
        if strategy_info:
            strategy = strategy_info.get('coverage_strategy', 'standard')
            
            if strategy == 'grouped' and metrics.poc_coverage_percentage > 0.8:
                recommendations.append("Consider reducing scope - high coverage achieved with grouped strategy")
                
            if strategy == 'adaptive' and metrics.connection_success_rate < 0.5:
                recommendations.append("Adaptive strategy may need further scope expansion")
                
            if strategy_info.get('scope_expanded'):
                recommendations.append("Validate that expanded scope aligns with analysis objectives")
                
        return recommendations
        
    def _get_pocs_for_coverage(self, filters: Optional[Dict] = None) -> List[Dict]:
        """Get POCs for coverage calculation with optional filters"""
        query = """
            SELECT DISTINCT poc.id, poc.equipment_id, poc.node_id, poc.utility_no, 
                   poc.reference, poc.flow, poc.markers,
                   eq.toolset, eq.data_code, eq.category_no, eq.kind,
                   ts.fab, ts.model_no, ts.phase_no
            FROM tb_equipment_pocs poc
            JOIN tb_equipments eq ON poc.equipment_id = eq.id
            JOIN tb_toolsets ts ON eq.toolset = ts.code
            WHERE poc.is_active = 1 AND poc.is_used = 1 
              AND eq.is_active = 1 AND ts.is_active = 1
        """
        
        params = []
        
        if filters:
            if filters.get('fab'):
                query += " AND ts.fab = ?"
                params.append(filters['fab'])
            if filters.get('model_no'):
                query += " AND ts.model_no = ?"
                params.append(filters['model_no'])
            if filters.get('phase_no'):
                query += " AND ts.phase_no = ?"
                params.append(filters['phase_no'])
            if filters.get('expanded_toolsets'):
                placeholders = ','.join(['?' for _ in filters['expanded_toolsets']])
                query += f" AND eq.toolset IN ({placeholders})"
                params.extend(filters['expanded_toolsets'])
            elif filters.get('toolset'):
                query += " AND eq.toolset = ?"
                params.append(filters['toolset'])
                
        query += " ORDER BY poc.id"
        
        cursor = self.db.cursor()
        cursor.execute(query, params)
        return cursor.fetchall()
        
    def _get_equipment_for_coverage(self, filters: Optional[Dict] = None) -> List[Dict]:
        """Get equipment for coverage calculation with optional filters"""
        query = """
            SELECT DISTINCT eq.id, eq.toolset, eq.data_code, eq.category_no, eq.kind,
                   ts.fab, ts.model_no, ts.phase_no
            FROM tb_equipments eq
            JOIN tb_toolsets ts ON eq.toolset = ts.code
            WHERE eq.is_active = 1 AND ts.is_active = 1
        """
        
        params = []
        
        if filters:
            if filters.get('fab'):
                query += " AND ts.fab = ?"
                params.append(filters['fab'])
            if filters.get('model_no'):
                query += " AND ts.model_no = ?"
                params.append(filters['model_no'])
            if filters.get('phase_no'):
                query += " AND ts.phase_no = ?"
                params.append(filters['phase_no'])
            if filters.get('expanded_toolsets'):
                placeholders = ','.join(['?' for _ in filters['expanded_toolsets']])
                query += f" AND eq.toolset IN ({placeholders})"
                params.extend(filters['expanded_toolsets'])
            elif filters.get('toolset'):
                query += " AND eq.toolset = ?"
                params.append(filters['toolset'])
                
        query += " ORDER BY eq.id"
        
        cursor = self.db.cursor()
        cursor.execute(query, params)
        return cursor.fetchall()
        
    def _calculate_poc_coverage_for_run(self, run_id: str) -> Dict:
        """Calculate POC coverage for a run"""
        query = """
            SELECT 
                COUNT(DISTINCT all_pocs.id) as total_pocs,
                COUNT(DISTINCT covered_pocs.poc_id) as covered_pocs
            FROM (
                SELECT DISTINCT poc.id
                FROM tb_equipment_pocs poc
                JOIN tb_equipments eq ON poc.equipment_id = eq.id
                JOIN tb_toolsets ts ON eq.toolset = ts.code
                WHERE poc.is_active = 1 AND poc.is_used = 1 
                  AND eq.is_active = 1 AND ts.is_active = 1
            ) all_pocs
            LEFT JOIN (
                SELECT DISTINCT poc.id as poc_id
                FROM tb_attempt_paths ap
                JOIN tb_path_definitions pd ON ap.path_definition_id = pd.id
                JOIN tb_equipment_pocs poc ON (poc.node_id = ap.start_node_id OR poc.node_id = ap.end_node_id)
                WHERE ap.run_id = ? AND ap.cost IS NOT NULL
            ) covered_pocs ON all_pocs.id = covered_pocs.poc_id
        """
        
        cursor = self.db.cursor()
        cursor.execute(query, (run_id,))
        result = cursor.fetchone()
        
        total_pocs = result['total_pocs'] or 0
        covered_pocs = result['covered_pocs'] or 0
        
        return {
            'total_pocs': total_pocs,
            'covered_pocs': covered_pocs,
            'coverage_percentage': covered_pocs / total_pocs if total_pocs > 0 else 0.0
        }
        
    def _calculate_equipment_coverage_for_run(self, run_id: str) -> Dict:
        """Calculate equipment coverage for a run"""
        query = """
            SELECT 
                COUNT(DISTINCT all_eq.id) as total_equipment,
                COUNT(DISTINCT covered_eq.equipment_id) as covered_equipment
            FROM (
                SELECT DISTINCT eq.id
                FROM tb_equipments eq
                JOIN tb_toolsets ts ON eq.toolset = ts.code
                WHERE eq.is_active = 1 AND ts.is_active = 1
            ) all_eq
            LEFT JOIN (
                SELECT DISTINCT poc.equipment_id
                FROM tb_attempt_paths ap
                JOIN tb_path_definitions pd ON ap.path_definition_id = pd.id
                JOIN tb_equipment_pocs poc ON (poc.node_id = ap.start_node_id OR poc.node_id = ap.end_node_id)
                WHERE ap.run_id = ? AND ap.cost IS NOT NULL
            ) covered_eq ON all_eq.id = covered_eq.equipment_id
        """
        
        cursor = self.db.cursor()
        cursor.execute(query, (run_id,))
        result = cursor.fetchone()
        
        total_equipment = result['total_equipment'] or 0
        covered_equipment = result['covered_equipment'] or 0
        
        return {
            'total_equipment': total_equipment,
            'covered_equipment': covered_equipment,
            'coverage_percentage': covered_equipment / total_equipment if total_equipment > 0 else 0.0
        }
        
    def _get_successful_paths_with_details(self, run_id: str) -> List[Dict]:
        """Get successful paths with detailed information"""
        query = """
            SELECT pd.*, ap.start_node_id, ap.end_node_id, ap.cost,
                   start_poc.equipment_id as start_equipment_id,
                   end_poc.equipment_id as end_equipment_id,
                   start_ts.fab, start_ts.model_no, start_ts.phase_no,
                   start_eq.toolset
            FROM tb_attempt_paths ap
            JOIN tb_path_definitions pd ON ap.path_definition_id = pd.id
            LEFT JOIN tb_equipment_pocs start_poc ON start_poc.node_id = ap.start_node_id
            LEFT JOIN tb_equipment_pocs end_poc ON end_poc.node_id = ap.end_node_id
            LEFT JOIN tb_equipments start_eq ON start_poc.equipment_id = start_eq.id
            LEFT JOIN tb_toolsets start_ts ON start_eq.toolset = start_ts.code
            WHERE ap.run_id = ? AND ap.cost IS NOT NULL
        """
        
        cursor = self.db.cursor()
        cursor.execute(query, (run_id,))
        return cursor.fetchall()
        
    def _group_by_attribute(self, paths: List[Dict], attribute: str) -> Dict:
        """Group paths by a specific attribute"""
        groups = {}
        for path in paths:
            key = path.get(attribute)
            if key not in groups:
                groups[key] = []
            groups[key].append(path)
        return groups
        
    def _calculate_metrics_for_paths(self, paths: List[Dict]) -> CoverageMetrics:
        """Calculate coverage metrics for a group of paths"""
        if not paths:
            return CoverageMetrics(0, 0, 0.0, 0, 0, 0.0, 0, 0, 0.0, 0, 0)
            
        unique_pocs = set()
        unique_equipment = set()
        
        for path in paths:
            # Extract POC and equipment info from path context if available
            if path.get('path_context'):
                try:
                    context = json.loads(path['path_context'])
                    if 'nodes' in context:
                        unique_pocs.update(context['nodes'])
                except:
                    pass
                    
            # Also add start/end nodes
            if path.get('start_node_id'):
                unique_pocs.add(path['start_node_id'])
            if path.get('end_node_id'):
                unique_pocs.add(path['end_node_id'])
                
            # Add equipment
            if path.get('start_equipment_id'):
                unique_equipment.add(path['start_equipment_id'])
            if path.get('end_equipment_id'):
                unique_equipment.add(path['end_equipment_id'])
                
        return CoverageMetrics(
            total_pocs=len(unique_pocs),
            covered_pocs=len(unique_pocs),
            poc_coverage_percentage=1.0,  # All POCs in this group are covered
            total_equipment=len(unique_equipment),
            covered_equipment=len(unique_equipment),
            equipment_coverage_percentage=1.0,  # All equipment in this group are covered
            total_connections=len(paths),
            successful_connections=len(paths),
            connection_success_rate=1.0,
            unique_paths=len(set(path.get('path_hash') for path in paths)),
            total_attempts=len(paths)
        )
        
    def _get_covered_poc_ids_for_run(self, run_id: str) -> Set[int]:
        """Get set of POC IDs covered by successful paths"""
        query = """
            SELECT DISTINCT poc.id
            FROM tb_attempt_paths ap
            JOIN tb_equipment_pocs poc ON (poc.node_id = ap.start_node_id OR poc.node_id = ap.end_node_id)
            WHERE ap.run_id = ? AND ap.cost IS NOT NULL
        """
        
        cursor = self.db.cursor()
        cursor.execute(query, (run_id,))
        return set(row['id'] for row in cursor.fetchall())
        
    def _find_isolated_equipment(self, run_id: str) -> List[Dict]:
        """Find equipment with no successful connections"""
        query = """
            SELECT DISTINCT eq.id, eq.toolset, eq.kind, eq.name,
                   ts.fab, ts.model_no, ts.phase_no
            FROM tb_equipments eq
            JOIN tb_toolsets ts ON eq.toolset = ts.code
            WHERE eq.is_active = 1 AND ts.is_active = 1
              AND eq.id NOT IN (
                  SELECT DISTINCT poc.equipment_id
                  FROM tb_attempt_paths ap
                  JOIN tb_equipment_pocs poc ON (poc.node_id = ap.start_node_id OR poc.node_id = ap.end_node_id)
                  WHERE ap.run_id = ? AND ap.cost IS NOT NULL
              )
        """
        
        cursor = self.db.cursor()
        cursor.execute(query, (run_id,))
        return cursor.fetchall()
        
    def _generate_coverage_insights(self, metrics: CoverageMetrics, 
                                  breakdown: CoverageBreakdown, gaps: Dict) -> List[str]:
        """Generate insights from coverage analysis"""
        insights = []
        
        # Overall coverage insights
        if metrics.poc_coverage_percentage < 0.3:
            insights.append("Low overall POC coverage - consider increasing sampling or targeting specific areas")
        elif metrics.poc_coverage_percentage > 0.8:
            insights.append("High POC coverage achieved - good sampling distribution")
            
        # Connection success rate insights
        if metrics.connection_success_rate < 0.5:
            insights.append("Low connection success rate - possible connectivity issues in the network")
        elif metrics.connection_success_rate > 0.9:
            insights.append("High connection success rate - network appears well connected")
            
        # Gap insights
        if gaps['uncovered_fabs']:
            insights.append(f"Found {len(gaps['uncovered_fabs'])} completely uncovered buildings")
            
        if gaps['isolated_equipment']:
            insights.append(f"Found {len(gaps['isolated_equipment'])} isolated equipment items")
            
        return insights
        
    def _generate_coverage_recommendations(self, metrics: CoverageMetrics, gaps: Dict) -> List[str]:
        """Generate recommendations based on coverage analysis"""
        recommendations = []
        
        if metrics.poc_coverage_percentage < 0.5:
            recommendations.append("Increase target coverage or sampling iterations")
            
        if gaps['uncovered_fabs']:
            recommendations.append("Focus additional sampling on uncovered buildings")
            
        if gaps['low_coverage_fabs']:
            recommendations.append("Increase sampling density in low-coverage buildings")
            
        if metrics.connection_success_rate < 0.7:
            recommendations.append("Investigate network connectivity issues")
            recommendations.append("Review pathfinding algorithm parameters")
            
        if gaps['isolated_equipment']:
            recommendations.append("Verify connectivity of isolated equipment")
            recommendations.append("Check if isolated equipment should be excluded from analysis")
            
        return recommendations