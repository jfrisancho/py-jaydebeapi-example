"""
Random path generation with bias mitigation for equipment PoC sampling.
"""
import random
import logging
from typing import Optional, Dict, Any, List, Tuple, Set
from dataclasses import dataclass
from collections import defaultdict
import sqlite3


@dataclass
class BiasReduction:
    """Configuration for bias reduction in random sampling."""
    max_attempts_per_toolset: int = 5
    max_attempts_per_equipment: int = 3
    min_distance_between_nodes: int = 10
    utility_diversity_weight: float = 0.3    
    phase_diversity_weight: float = 0.2
    max_consecutive_failures: int = 50
    cooldown_period: int = 10


class RandomManager:
    """Manages random PoC pair generation with bias mitigation."""
    
    def __init__(self, db_connection: sqlite3.Connection):
        self.db = db_connection
        self.logger = logging.getLogger(__name__)
        self.bias_config = BiasReduction()
        
        # Tracking for bias mitigation
        self.toolset_attempts = defaultdict(int)
        self.equipment_attempts = defaultdict(int)
        self.recently_used_toolsets = set()
        self.recently_used_equipment = set()
        self.consecutive_failures = 0
        self.last_successful_approach = None
    
    def generate_random_poc_pair(self, fab: Optional[str] = None,
                               model_no: Optional[int] = None,
                               phase_no: Optional[int] = None,
                               toolset: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """Generate a random PoC pair with bias mitigation."""
        
        # If toolset is specified, use toolset-focused approach
        if toolset:
            return self._generate_toolset_poc_pair(toolset, fab, model_no, phase_no)
        
        # Otherwise, use hierarchical random selection
        return self._generate_hierarchical_poc_pair(fab, model_no, phase_no)
    
    def _generate_hierarchical_poc_pair(self, fab: Optional[str] = None,
                                      model_no: Optional[int] = None,
                                      phase_no: Optional[int] = None) -> Optional[Dict[str, Any]]:
        """Generate PoC pair using hierarchical random selection."""
        
        max_attempts = 100
        attempts = 0
        
        while attempts < max_attempts:
            try:
                # Step 1: Select random building (if not specified)
                selected_fab = fab
                if not selected_fab:
                    selected_fab = self._select_random_fab(model_no, phase_no)
                    if not selected_fab:
                        attempts += 1
                        continue
                
                # Step 2: Select random toolset with bias mitigation
                selected_toolset = self._select_random_toolset(
                    selected_fab, model_no, phase_no
                )
                if not selected_toolset:
                    attempts += 1
                    continue
                
                # Step 3: Generate PoC pair from selected toolset
                poc_pair = self._generate_toolset_poc_pair(
                    selected_toolset, selected_fab, model_no, phase_no
                )
                
                if poc_pair:
                    self._update_bias_tracking(selected_toolset, success=True)
                    return poc_pair
                
                self._update_bias_tracking(selected_toolset, success=False)
                attempts += 1
                
            except Exception as e:
                self.logger.warning(f"Error in hierarchical generation: {e}")
                attempts += 1
        
        self.logger.warning("Failed to generate PoC pair after maximum attempts")
        return None
    
    def _generate_toolset_poc_pair(self, toolset: str, fab: Optional[str] = None,
                                 model_no: Optional[int] = None,
                                 phase_no: Optional[int] = None) -> Optional[Dict[str, Any]]:
        """Generate PoC pair from a specific toolset."""
        
        # Get all equipment in toolset
        equipment_sql = """
        SELECT e.id, e.guid, e.node_id, e.data_code, e.category_no,
               e.kind, e.name
        FROM tb_equipments e
        JOIN tb_toolsets ts ON e.toolset = ts.code
        WHERE ts.code = ?
        """
        
        params = [toolset]
        conditions = []
        
        if fab:
            conditions.append("ts.fab = ?")
            params.append(fab)
        if model_no:
            conditions.append("ts.model_no = ?")
            params.append(model_no)
        if phase_no:
            conditions.append("ts.phase_no = ?")
            params.append(phase_no)
        
        if conditions:
            equipment_sql += " AND " + " AND ".join(conditions)
        
        equipment_sql += " AND e.is_active = 1"
        
        cursor = self.db.execute(equipment_sql, params)
        equipment_list = cursor.fetchall()
        
        if len(equipment_list) < 2:
            return None
        
        # Try to find PoC pair with bias mitigation
        max_attempts = 50
        attempts = 0
        
        while attempts < max_attempts:
            # Select two different equipment with bias consideration
            eq1, eq2 = self._select_equipment_pair(equipment_list)
            if not eq1 or not eq2:
                attempts += 1
                continue
            
            # Get PoCs for each equipment
            poc1 = self._select_poc_from_equipment(eq1[0])  # equipment_id
            poc2 = self._select_poc_from_equipment(eq2[0])
            
            if not poc1 or not poc2:
                attempts += 1
                continue
            
            # Validate distance and diversity
            if self._validate_poc_pair(poc1, poc2):
                return {
                    'from_equipment_id': eq1[0],
                    'to_equipment_id': eq2[0],
                    'from_node_id': poc1['node_id'],
                    'to_node_id': poc2['node_id'],
                    'from_poc_id': poc1['id'],
                    'to_poc_id': poc2['id'],
                    'toolset': toolset,
                    'fab': fab,
                    'model_no': model_no,
                    'phase_no': phase_no,
                    'cost': self._calculate_cost(poc1, poc2)
                }
            
            attempts += 1
        
        return None
    
    def _select_random_fab(self, model_no: Optional[int] = None,
                          phase_no: Optional[int] = None) -> Optional[str]:
        """Select random fab with available toolsets."""
        
        fab_sql = """
        SELECT DISTINCT ts.fab
        FROM tb_toolsets ts
        WHERE ts.is_active = 1
        """
        
        params = []
        if model_no:
            fab_sql += " AND ts.model_no = ?"
            params.append(model_no)
        if phase_no:
            fab_sql += " AND ts.phase_no = ?"
            params.append(phase_no)
        
        cursor = self.db.execute(fab_sql, params)
        fabs = [row[0] for row in cursor.fetchall()]
        
        if not fabs:
            return None
        
        return random.choice(fabs)
    
    def _select_random_toolset(self, fab: str, model_no: Optional[int] = None,
                             phase_no: Optional[int] = None) -> Optional[str]:
        """Select random toolset with bias mitigation."""
        
        # Get available toolsets
        toolset_sql = """
        SELECT ts.code, COUNT(e.id) as equipment_count
        FROM tb_toolsets ts
        LEFT JOIN tb_equipments e ON ts.code = e.toolset AND e.is_active = 1
        WHERE ts.fab = ? AND ts.is_active = 1
        """
        
        params = [fab]
        if model_no:
            toolset_sql += " AND ts.model_no = ?"
            params.append(model_no)
        if phase_no:
            toolset_sql += " AND ts.phase_no = ?"
            params.append(phase_no)
        
        toolset_sql += " GROUP BY ts.code HAVING equipment_count >= 2"
        
        cursor = self.db.execute(toolset_sql, params)
        toolsets = cursor.fetchall()
        
        if not toolsets:
            return None
        
        # Apply bias mitigation
        available_toolsets = []
        
        for toolset_code, equipment_count in toolsets:
            # Skip recently overused toolsets
            if (toolset_code not in self.recently_used_toolsets and 
                self.toolset_attempts[toolset_code] < self.bias_config.max_attempts_per_toolset):
                # Weight by equipment count and inverse of attempts
                weight = equipment_count / (1 + self.toolset_attempts[toolset_code])
                available_toolsets.append((toolset_code, weight))
        
        # If no toolsets available due to bias restrictions, reset and try again
        if not available_toolsets:
            self._reset_toolset_bias()
            available_toolsets = [(ts[0], ts[1]) for ts in toolsets]
        
        # Weighted random selection
        if available_toolsets:
            toolset_codes, weights = zip(*available_toolsets)
            return random.choices(toolset_codes, weights=weights, k=1)[0]
        
        return None
    
    def _select_equipment_pair(self, equipment_list: List[Tuple]) -> Tuple[Optional[Tuple], Optional[Tuple]]:
        """Select two different equipment with bias consideration."""
        
        if len(equipment_list) < 2:
            return None, None
        
        # Filter out recently overused equipment
        available_equipment = [
            eq for eq in equipment_list 
            if (eq[0] not in self.recently_used_equipment and 
                self.equipment_attempts[eq[0]] < self.bias_config.max_attempts_per_equipment)
        ]
        
        # If not enough available, reset bias
        if len(available_equipment) < 2:
            self._reset_equipment_bias()
            available_equipment = equipment_list
        
        # Select two different equipment
        eq1 = random.choice(available_equipment)
        remaining = [eq for eq in available_equipment if eq[0] != eq1[0]]
        
        if not remaining:
            return None, None
        
        eq2 = random.choice(remaining)
        
        return eq1, eq2
    
    def _select_poc_from_equipment(self, equipment_id: int) -> Optional[Dict[str, Any]]:
        """Select random PoC from equipment with preference for used PoCs."""
        
        poc_sql = """
        SELECT id, node_id, is_used, markers, utility_no, reference, flow, is_loopback
        FROM tb_equipment_pocs
        WHERE equipment_id = ? AND is_active = 1
        """
        
        cursor = self.db.execute(poc_sql, [equipment_id])
        pocs = cursor.fetchall()
        
        if not pocs:
            return None
        
        # Prefer used PoCs (they should have paths)
        used_pocs = [poc for poc in pocs if poc[2]]  # is_used = True
        if used_pocs:
            selected_poc = random.choice(used_pocs)
        else:
            selected_poc = random.choice(pocs)
        
        return {
            'id': selected_poc[0],
            'node_id': selected_poc[1],
            'is_used': selected_poc[2],
            'markers': selected_poc[3],
            'utility_no': selected_poc[4],
            'reference': selected_poc[5],
            'flow': selected_poc[6],
            'is_loopback': selected_poc[7]
        }
    
    def _validate_poc_pair(self, poc1: Dict[str, Any], poc2: Dict[str, Any]) -> bool:
        """Validate PoC pair for distance and diversity."""
        
        # Check minimum distance (simple node ID difference as proxy)
        distance = abs(poc1['node_id'] - poc2['node_id'])
        if distance < self.bias_config.min_distance_between_nodes:
            return False
        
        # Check that they're not the same PoC
        if poc1['id'] == poc2['id']:
            return False
        
        # Prefer pairs with different utilities for diversity
        if (poc1['utility_no'] and poc2['utility_no'] and 
            poc1['utility_no'] == poc2['utility_no']):
            # Same utility - apply diversity weight
            return random.random() < (1.0 - self.bias_config.utility_diversity_weight)
        
        return True
    
    def _calculate_cost(self, poc1: Dict[str, Any], poc2: Dict[str, Any]) -> float:
        """Calculate cost estimate for path finding."""
        
        # Simple distance-based cost
        base_cost = abs(poc1['node_id'] - poc2['node_id'])
        
        # Penalty for unused PoCs
        if not poc1['is_used']:
            base_cost *= 1.5
        if not poc2['is_used']:
            base_cost *= 1.5
        
        # Bonus for different utilities (encourages diversity)
        if (poc1['utility_no'] and poc2['utility_no'] and 
            poc1['utility_no'] != poc2['utility_no']):
            base_cost *= 0.8
        
        return float(base_cost)
    
    def _update_bias_tracking(self, toolset: str, success: bool) -> None:
        """Update bias tracking counters."""
        
        self.toolset_attempts[toolset] += 1
        
        if success:
            self.consecutive_failures = 0
            self.last_successful_approach = toolset
        else:
            self.consecutive_failures += 1
            
            # Add to recently used if too many failures
            if self.consecutive_failures >= self.bias_config.max_consecutive_failures:
                self.recently_used_toolsets.add(toolset)
    
    def _reset_toolset_bias(self) -> None:
        """Reset toolset bias tracking."""
        self.recently_used_toolsets.clear()
        
        # Reduce attempt counts rather than clearing completely
        for toolset in self.toolset_attempts:
            self.toolset_attempts[toolset] = max(0, self.toolset_attempts[toolset] - 2)
    
    def _reset_equipment_bias(self) -> None:
        """Reset equipment bias tracking."""
        self.recently_used_equipment.clear()
        
        # Reduce attempt counts
        for equipment in self.equipment_attempts:
            self.equipment_attempts[equipment] = max(0, self.equipment_attempts[equipment] - 1)
    
    def get_sampling_statistics(self) -> Dict[str, Any]:
        """Get current sampling bias statistics."""
        
        return {
            'toolset_attempts': dict(self.toolset_attempts),
            'equipment_attempts': dict(self.equipment_attempts),
            'recently_used_toolsets': list(self.recently_used_toolsets),
            'recently_used_equipment': list(self.recently_used_equipment),
            'consecutive_failures': self.consecutive_failures,
            'last_successful_approach': self.last_successful_approach
        }
    
    def reset_bias_tracking(self) -> None:
        """Completely reset all bias tracking."""
        
        self.toolset_attempts.clear()
        self.equipment_attempts.clear()
        self.recently_used_toolsets.clear()
        self.recently_used_equipment.clear()
        self.consecutive_failures = 0
        self.last_successful_approach = None
        
        self.logger.info("Bias tracking reset")
    
    def update_bias_config(self, config: BiasReduction) -> None:
        """Update bias reduction configuration."""
        self.bias_config = config
        self.logger.info(f"Bias configuration updated: {config}")
    
    def get_toolset_diversity_stats(self, fab: Optional[str] = None,
                                  model_no: Optional[int] = None,
                                  phase_no: Optional[int] = None) -> Dict[str, Any]:
        """Get diversity statistics for available toolsets."""
        
        # Get toolset distribution
        stats_sql = """
        SELECT 
            ts.code,
            ts.fab,
            ts.phase_no,
            ts.model_no,
            COUNT(e.id) as equipment_count,
            COUNT(DISTINCT ep.utility_no) as utility_diversity,
            COUNT(ep.id) as total_pocs,
            SUM(CASE WHEN ep.is_used = 1 THEN 1 ELSE 0 END) as used_pocs
        FROM tb_toolsets ts
        LEFT JOIN tb_equipments e ON ts.code = e.toolset AND e.is_active = 1
        LEFT JOIN tb_equipment_pocs ep ON e.id = ep.equipment_id AND ep.is_active = 1
        WHERE ts.is_active = 1
        """
        
        params = []
        if fab:
            stats_sql += " AND ts.fab = ?"
            params.append(fab)
        if model_no:
            stats_sql += " AND ts.model_no = ?"
            params.append(model_no)
        if phase_no:
            stats_sql += " AND ts.phase_no = ?"
            params.append(phase_no)
        
        stats_sql += """
        GROUP BY ts.code, ts.fab, ts.phase_no, ts.model_no
        HAVING equipment_count >= 2
        ORDER BY equipment_count DESC
        """
        
        cursor = self.db.execute(stats_sql, params)
        toolsets = cursor.fetchall()
        
        # Calculate statistics
        total_toolsets = len(toolsets)
        if total_toolsets == 0:
            return {'total_toolsets': 0, 'toolsets': []}
        
        total_equipment = sum(row[4] for row in toolsets)
        total_pocs = sum(row[6] for row in toolsets)
        total_used_pocs = sum(row[7] for row in toolsets)
        
        toolset_data = []
        for row in toolsets:
            toolset_data.append({
                'code': row[0],
                'fab': row[1],
                'phase_no': row[2],
                'model_no': row[3],
                'equipment_count': row[4],
                'utility_diversity': row[5],
                'total_pocs': row[6],
                'used_pocs': row[7],
                'usage_rate': row[7] / row[6] if row[6] > 0 else 0,
                'attempts': self.toolset_attempts.get(row[0], 0)
            })
        
        return {
            'total_toolsets': total_toolsets,
            'total_equipment': total_equipment,
            'total_pocs': total_pocs,
            'total_used_pocs': total_used_pocs,
            'overall_usage_rate': total_used_pocs / total_pocs if total_pocs > 0 else 0,
            'toolsets': toolset_data
        }
    
    def suggest_optimal_sampling_strategy(self, fab: Optional[str] = None,
                                        model_no: Optional[int] = None,
                                        phase_no: Optional[int] = None) -> Dict[str, Any]:
        """Suggest optimal sampling strategy based on data distribution."""
        
        diversity_stats = self.get_toolset_diversity_stats(fab, model_no, phase_no)
        
        if diversity_stats['total_toolsets'] == 0:
            return {'strategy': 'NO_DATA', 'reason': 'No suitable toolsets found'}
        
        # Analyze distribution
        toolsets = diversity_stats['toolsets']
        high_poc_toolsets = [ts for ts in toolsets if ts['total_pocs'] >= 10]
        high_usage_toolsets = [ts for ts in toolsets if ts['usage_rate'] >= 0.7]
        diverse_toolsets = [ts for ts in toolsets if ts['utility_diversity'] >= 3]
        
        # Determine strategy
        if len(high_usage_toolsets) >= 5:
            strategy = 'FOCUS_HIGH_USAGE'
            reason = 'Multiple toolsets with high PoC usage rates available'
        elif len(diverse_toolsets) >= 3:
            strategy = 'FOCUS_DIVERSE'
            reason = 'Multiple toolsets with utility diversity available'
        elif len(high_poc_toolsets) >= 5:
            strategy = 'BALANCED'
            reason = 'Good distribution of toolsets with adequate PoC counts'
        else:
            strategy = 'EXHAUSTIVE'
            reason = 'Limited options, exhaustive search recommended'
        
        return {
            'strategy': strategy,
            'reason': reason,
            'recommended_bias_config': self._suggest_bias_config(strategy),
            'priority_toolsets': self._get_priority_toolsets(toolsets, strategy),
            'diversity_stats': diversity_stats
        }
    
    def _suggest_bias_config(self, strategy: str) -> BiasReduction:
        """Suggest bias configuration based on strategy."""
        
        if strategy == 'FOCUS_HIGH_USAGE':
            return BiasReduction(
                max_attempts_per_toolset=10,
                max_attempts_per_equipment=5,
                min_distance_between_nodes=5,
                utility_diversity_weight=0.2,
                phase_diversity_weight=0.1
            )
        elif strategy == 'FOCUS_DIVERSE':
            return BiasReduction(
                max_attempts_per_toolset=7,
                max_attempts_per_equipment=4,
                min_distance_between_nodes=15,
                utility_diversity_weight=0.5,
                phase_diversity_weight=0.3
            )
        elif strategy == 'EXHAUSTIVE':
            return BiasReduction(
                max_attempts_per_toolset=15,
                max_attempts_per_equipment=8,
                min_distance_between_nodes=5,
                utility_diversity_weight=0.1,
                phase_diversity_weight=0.1
            )
        else:  # BALANCED
            return self.bias_config  # Use default
    
    def _get_priority_toolsets(self, toolsets: List[Dict], strategy: str) -> List[str]:
        """Get priority toolsets based on strategy."""
        
        if strategy == 'FOCUS_HIGH_USAGE':
            # Sort by usage rate and equipment count
            sorted_toolsets = sorted(toolsets, 
                                   key=lambda x: (x['usage_rate'], x['equipment_count']), 
                                   reverse=True)
        elif strategy == 'FOCUS_DIVERSE':
            # Sort by utility diversity and total PoCs
            sorted_toolsets = sorted(toolsets,
                                   key=lambda x: (x['utility_diversity'], x['total_pocs']),
                                   reverse=True)
        else:
            # Sort by equipment count and total PoCs
            sorted_toolsets = sorted(toolsets,
                                   key=lambda x: (x['equipment_count'], x['total_pocs']),
                                   reverse=True)
        
        # Return top 10 priority toolsets
        return [ts['code'] for ts in sorted_toolsets[:10]]
        