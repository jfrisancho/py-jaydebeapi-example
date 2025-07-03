    def _get_adjustment_reason(self, config: RandomGenerationConfig) -> str:
        """Get reason for coverage adjustment"""
        if config.original_coverage_target == config.coverage_target:
            return "No adjustment needed"
        elif config.expanded_toolsets:
            return f"Expanded to {len(config.expanded_toolsets)} related toolsets"
        elif config.coverage_strategy == "intensive":
            return "Intensive sampling of critical toolset"
        elif config.coverage_strategy == "adaptive":
            return "Adjusted to realistic coverage potential"
        else:
            return "Strategy-based adjustment"
            
    def _get_total_factory_pocs(self) -> int:
        """Get total POC count in factory"""
        query = """
            SELECT COUNT(DISTINCT poc.id) as total_pocs
            FROM tb_equipment_pocs poc
            JOIN tb_equipments eq ON poc.equipment_id = eq.id
            JOIN tb_toolsets ts ON eq.toolset = ts.code
            WHERE poc.is_active = 1 AND poc.is_used = 1 AND ts.is_active = 1
        """
        
        cursor = self.db.cursor()
        cursor.execute(query)
        result = cursor.fetchone()
        return result['total_pocs'] if result else 0
        
    def generate_coverage_strategy_report(self, config: RandomGenerationConfig) -> Dict:
        """Generate a report about the coverage strategy applied"""
        original_config = RandomGenerationConfig(
            coverage_target=config.original_coverage_target or config.coverage_target,
            model=config.model,
            phase=config.phase,
            fab=config.fab,
            toolset=config.toolset if not config.expanded_toolsets else config.toolset,
            allow_scope_expansion=False
        )
        
        # Assess original potential
        original_potential = 0.0
        if original_config.toolset:
            original_potential = self._assess_toolset_coverage_potential(original_config.toolset)
        elif original_config.phase:
            original_potential = self._assess_phase_coverage_potential(original_config.phase, original_config.fab)
        elif original_config.fab:
            original_potential = self._assess_fab_coverage_potential(original_config.fab)
        else:
            original_potential = 1.0
            
        # Assess current potential
        current_potential = 0.0
        if config.expanded_toolsets:
            for toolset in config.expanded_toolsets:
                current_potential += self._assess_toolset_coverage_potential(toolset)
        elif config.toolset:
            current_potential = self._assess_toolset_coverage_potential(config.toolset)
        elif config.phase:
            current_potential = self._assess_phase_coverage_potential(config.phase, config.fab)
        elif config.fab:
            current_potential = self._assess_fab_coverage_potential(config.fab)
        else:
            current_potential = 1.0
            
        return {
            'original_request': {
                'coverage_target': config.original_coverage_target or config.coverage_target,
                'scope': self._describe_scope(original_config),
                'potential': original_potential
            },
            'strategy_applied': {
                'name': config.coverage_strategy,
                'adjusted_target': config.coverage_target,
                'scope': self._describe_scope(config),
                'potential': current_potential
            },
            'improvements': {
                'potential_increase': current_potential - original_potential,
                'target_achievable': current_potential >= config.coverage_target,
                'scope_expansion': config.expanded_toolsets is not None
            },
            'recommendations': self._generate_strategy_recommendations(config, original_potential, current_potential)
        }
        
    def _describe_scope(self, config: RandomGenerationConfig) -> str:
        """Describe the scope of the configuration"""
        scope_parts = []
        
        if config.expanded_toolsets:
            scope_parts.append(f"{len(config.expanded_toolsets)} related toolsets")
        elif config.toolset:
            scope_parts.append(f"toolset {config.toolset}")
        
        if config.fab:
            scope_parts.append(f"fab {config.fab}")
            
        if config.phase:
            scope_parts.append(f"phase {config.phase.value}")
            
        if config.model:
            scope_parts.append(f"model {config.model.value}")
            
        return ", ".join(scope_parts) if scope_parts else "factory-wide"
        
    def _generate_strategy_recommendations(self, config: RandomGenerationConfig, 
                                         original_potential: float, current_potential: float) -> List[str]:
        """Generate recommendations based on the strategy applied"""
        recommendations = []
        
        if current_potential >= config.coverage_target:
            recommendations.append("✓ Target coverage is achievable with current strategy")
        else:
            recommendations.append("⚠ Target coverage may still be challenging - consider further scope expansion")
            
        if config.expanded_toolsets:
            recommendations.append(f"Consider validating that all {len(config.expanded_toolsets)} toolsets are relevant")
            
        if config.coverage_strategy == "intensive":
            recommendations.append("Focus on thorough coverage within the critical toolset")
            
        if original_potential < 0.05:  # Very small scope
            recommendations.append("Consider if this scope is representative of your analysis goals")
            
        potential_improvement = current_potential - original_potential
        if potential_improvement > 0.1:
            recommendations.append(f"Strategy significantly improved coverage potential by {potential_improvement:.1%}")
            
        return recommendations
        
    # New methods for intelligent coverage strategy
    
    def _assess_toolset_coverage_potential(self, toolset: str) -> float:
        """Assess coverage potential for a specific toolset"""
        # Get POC count for this toolset
        toolset_query = """
            SELECT COUNT(DISTINCT poc.id) as toolset_pocs
            FROM tb_equipment_pocs poc
            JOIN tb_equipments eq ON poc.equipment_id = eq.id
            WHERE eq.toolset = ? AND poc.is_active = 1 AND poc.is_used = 1
        """
        
        # Get total POC count in factory
        total_query = """
            SELECT COUNT(DISTINCT poc.id) as total_pocs
            FROM tb_equipment_pocs poc
            JOIN tb_equipments eq ON poc.equipment_id = eq.id
            JOIN tb_toolsets ts ON eq.toolset = ts.code
            WHERE poc.is_active = 1 AND poc.is_used = 1 AND ts.is_active = 1
        """
        
        cursor = self.db.cursor()
        cursor.execute(toolset_query, (toolset,))
        toolset_result = cursor.fetchone()
        
        cursor.execute(total_query)
        total_result = cursor.fetchone()
        
        toolset_pocs = toolset_result['toolset_pocs'] or 0
        total_pocs = total_result['total_pocs'] or 1
        
        return toolset_pocs / total_pocs
        
    def _assess_phase_coverage_potential(self, phase: Phase, fab: Optional[str] = None) -> float:
        """Assess coverage potential for a specific phase"""
        phase_map = {Phase.P1: 1, Phase.P2: 2, Phase.A: 1, Phase.B: 2, Phase.C: 3, Phase.D: 4}
        phase_no = phase_map[phase]
        
        phase_query = """
            SELECT COUNT(DISTINCT poc.id) as phase_pocs
            FROM tb_equipment_pocs poc
            JOIN tb_equipments eq ON poc.equipment_id = eq.id
            JOIN tb_toolsets ts ON eq.toolset = ts.code
            WHERE ts.phase_no = ? AND poc.is_active = 1 AND poc.is_used = 1
        """
        
        params = [phase_no]
        if fab:
            phase_query += " AND ts.fab = ?"
            params.append(fab)
            
        cursor = self.db.cursor()
        cursor.execute(phase_query, params)
        phase_result = cursor.fetchone()
        
        cursor.execute("""
            SELECT COUNT(DISTINCT poc.id) as total_pocs
            FROM tb_equipment_pocs poc
            JOIN tb_equipments eq ON poc.equipment_id = eq.id
            JOIN tb_toolsets ts ON eq.toolset = ts.code
            WHERE poc.is_active = 1 AND poc.is_used = 1 AND ts.is_active = 1
        """)
        total_result = cursor.fetchone()
        
        phase_pocs = phase_result['phase_pocs'] or 0
        total_pocs = total_result['total_pocs'] or 1
        
        return phase_pocs / total_pocs
        
    def _assess_fab_coverage_potential(self, fab: str) -> float:
        """Assess coverage potential for a specific fab"""
        fab_query = """
            SELECT COUNT(DISTINCT poc.id) as fab_pocs
            FROM tb_equipment_pocs poc
            JOIN tb_equipments eq ON poc.equipment_id = eq.id
            JOIN tb_toolsets ts ON eq.toolset = ts.code
            WHERE ts.fab = ? AND poc.is_active = 1 AND poc.is_used = 1
        """
        
        cursor = self.db.cursor()
        cursor.execute(fab_query, (fab,))
        fab_result = cursor.fetchone()
        
        cursor.execute("""
            SELECT COUNT(DISTINCT poc.id) as total_pocs
            FROM tb_equipment_pocs poc
            JOIN tb_equipments eq ON poc.equipment_id = eq.id
            JOIN tb_toolsets ts ON eq.toolset = ts.code
            WHERE poc.is_active = 1 AND poc.is_used = 1 AND ts.is_active = 1
        """)
        total_result = cursor.fetchone()
        
        fab_pocs = fab_result['fab_pocs'] or 0
        total_pocs = total_result['total_pocs'] or 1
        
        return fab_pocs / total_pocs
        
    def _get_toolset_info(self, toolset: str) -> Dict:
        """Get information about a toolset"""
        query = """
            SELECT fab, model_no, phase_no, COUNT(DISTINCT eq.id) as equipment_count
            FROM tb_toolsets ts
            JOIN tb_equipments eq ON ts.code = eq.toolset
            WHERE ts.code = ? AND ts.is_active = 1 AND eq.is_active = 1
            GROUP BY fab, model_no, phase_no
        """
        
        cursor = self.db.cursor()
        cursor.execute(query, (toolset,))
        result = cursor.fetchone()
        
        return result if result else {}
        
    def _get_toolset_weight(self, toolset: str) -> float:
        """Get toolset weight/importance (placeholder - customize based on business logic)"""
        # This could be based on:
        # - Equipment criticality
        # - Process importance
        # - Historical failure rates
        # - Business priority
        
        # For now, return a default weight
        # You can customize this based on your business rules
        toolset_info = self._get_toolset_info(toolset)
        equipment_count = toolset_info.get('equipment_count', 0)
        
        # Simple heuristic: more equipment = higher weight
        if equipment_count > 100:
            return 0.9
        elif equipment_count > 50:
            return 0.7
        elif equipment_count > 20:
            return 0.5
        else:
            return 0.3
            
    def _get_related_toolsets(self, toolset: str, target_coverage: float) -> List[str]:
        """Get related toolsets to achieve target coverage"""
        toolset_info = self._get_toolset_info(toolset)
        if not toolset_info:
            return []
            
        # Find toolsets with similar characteristics
        query = """
            SELECT ts.code, COUNT(DISTINCT eq.id) as equipment_count
            FROM tb_toolsets ts
            JOIN tb_equipments eq ON ts.code = eq.toolset
            WHERE ts.fab = ? AND ts.phase_no = ? AND ts.model_no = ?
              AND ts.code != ? AND ts.is_active = 1 AND eq.is_active = 1
            GROUP BY ts.code
            ORDER BY equipment_count DESC
            LIMIT 10
        """
        
        cursor = self.db.cursor()
        cursor.execute(query, (
            toolset_info['fab'],
            toolset_info['phase_no'],
            toolset_info['model_no'],
            toolset
        ))
        
        related = cursor.fetchall()
        
        # Calculate how many toolsets needed to reach target
        related_toolsets = [toolset]  # Start with original
        current_potential = self._assess_toolset_coverage_potential(toolset)
        
        for related_toolset in related:
            if current_potential >= target_coverage:
                break
                
            related_code = related_toolset['code']
            additional_potential = self._assess_toolset_coverage_potential(related_code)
            current_potential += additional_potential
            related_toolsets.append(related_code)
            
        return related_toolsets if len(related_toolsets) > 1 else []
        
    def _get_coverage_analysis(self, config: RandomGenerationConfig, available_equipment: List[Dict]) -> Dict:
        """Get detailed coverage analysis"""
        total_factory_pocs = self._get_total_factory_pocs()
        scope_pocs = len(set(eq['equipment_id'] for eq in available_equipment))  # Approximate
        
        analysis = {
            'total_factory_pocs': total_factory_pocs,
            'scope_pocs': scope_pocs,
            'scope_percentage': scope_pocs / total_factory_pocs if total_factory_pocs > 0 else 0,
            'strategy_applied': config.coverage_strategy,
            'original_target': config.original_coverage_target,
            'adjusted_target': config.coverage_target,
            'adjustment_reason': self._get_adjustment_reason(config)
        }
        
        return analysis
        
    def _get"""
Random path generation service with bias mitigation.
Handles random selection of equipment POCs until coverage target is achieved.
"""

import random
import logging
from typing import List, Dict, Optional, Tuple, Set
from dataclasses import dataclass
from enum import Enum
import hashlib
import json

logger = logging.getLogger(__name__)


class ModelType(Enum):
    BIM = "BIM"
    FIVE_D = "5D"


class Phase(Enum):
    # BIM phases
    P1 = "P1"  # Bottom part of building
    P2 = "P2"  # Cleanroom
    # 5D phases  
    A = "A"
    B = "B"
    C = "C"
    D = "D"


@dataclass
class POCPair:
    """Represents a pair of POCs for path generation"""
    from_poc_id: int
    to_poc_id: int
    from_equipment_id: int
    to_equipment_id: int
    from_node_id: int
    to_node_id: int
    toolset_code: str
    fab: str
    model_no: int
    phase_no: int


@dataclass
class RandomGenerationConfig:
    """Configuration for random path generation"""
    coverage_target: float
    model: Optional[ModelType] = None
    phase: Optional[Phase] = None
    fab: Optional[str] = None
    toolset: Optional[str] = None
    max_attempts: int = 10000
    bias_mitigation: bool = True
    min_path_length: int = 2
    max_path_length: int = 50
    # New intelligent coverage fields
    allow_scope_expansion: bool = True
    coverage_strategy: str = "adaptive"  # adaptive, intensive, representative, grouped
    original_coverage_target: Optional[float] = None
    expanded_toolsets: Optional[List[str]] = None


class RandomService:
    """Service for random path generation with bias mitigation"""
    
    def __init__(self, db_connection):
        self.db = db_connection
        self.generation_stats = {}
        self.bias_tracker = {}
        
        # Coverage strategy configuration
        self.coverage_hierarchy = {
            'factory': 1.0,      # 100% of factory
            'fab': 0.3,          # 30% of factory (if selecting by fab)
            'phase': 0.15,       # 15% of factory (if selecting by phase)
            'toolset': 0.01      # 1% of factory (if selecting by toolset)
        }
        
    def generate_random_paths(self, config: RandomGenerationConfig) -> Dict:
        """
        Generate random paths until coverage target is achieved.
        
        Args:
            config: Random generation configuration
            
        Returns:
            Dictionary with generation results and statistics
        """
        # Apply intelligent coverage strategy
        config = self.intelligent_coverage_strategy(config)
        
        logger.info(f"Starting random path generation with adjusted target coverage: {config.coverage_target}")
        
        # Initialize bias tracking
        self._initialize_bias_tracking(config)
        
        # Get available equipment and POCs
        available_equipment = self._get_available_equipment(config)
        if not available_equipment:
            raise ValueError("No equipment found matching the criteria")
            
        logger.info(f"Found {len(available_equipment)} equipment items")
        
        # Generate POC pairs
        generated_pairs = []
        attempts = 0
        coverage_achieved = 0.0
        
        while coverage_achieved < config.coverage_target and attempts < config.max_attempts:
            attempts += 1
            
            # Select random POC pair
            poc_pair = self._select_random_poc_pair(available_equipment, config)
            if not poc_pair:
                logger.warning("Could not select random POC pair")
                continue
                
            # Check if this pair would contribute to coverage
            if self._would_contribute_to_coverage(poc_pair, generated_pairs):
                generated_pairs.append(poc_pair)
                coverage_achieved = self._calculate_coverage(generated_pairs, available_equipment)
                
                logger.debug(f"Added pair {len(generated_pairs)}: {poc_pair.from_poc_id} -> {poc_pair.to_poc_id}, "
                           f"Coverage: {coverage_achieved:.2%}")
            
            # Update bias tracking
            self._update_bias_tracking(poc_pair)
            
            # Log progress periodically
            if attempts % 1000 == 0:
                logger.info(f"Attempts: {attempts}, Pairs: {len(generated_pairs)}, "
                          f"Coverage: {coverage_achieved:.2%}")
        
        # Generate results
        results = {
            'pairs_generated': len(generated_pairs),
            'attempts_made': attempts,
            'coverage_achieved': coverage_achieved,
            'target_coverage': config.coverage_target,
            'original_target_coverage': config.original_coverage_target,
            'coverage_strategy': config.coverage_strategy,
            'scope_expanded': config.expanded_toolsets is not None,
            'expanded_toolsets': config.expanded_toolsets,
            'success': coverage_achieved >= config.coverage_target,
            'poc_pairs': generated_pairs,
            'bias_stats': self._get_bias_statistics(),
            'generation_stats': self._get_generation_statistics(available_equipment),
            'coverage_analysis': self._get_coverage_analysis(config, available_equipment)
        }
        
        logger.info(f"Random generation completed: {len(generated_pairs)} pairs, "
                   f"{coverage_achieved:.2%} coverage in {attempts} attempts")
        
        return results
        
    def intelligent_coverage_strategy(self, config: RandomGenerationConfig) -> RandomGenerationConfig:
        """Apply intelligent coverage strategy based on constraints"""
        
        # Store original target for reporting
        if config.original_coverage_target is None:
            config.original_coverage_target = config.coverage_target
            
        # Step 1: Assess coverage potential
        if config.toolset:
            potential = self._assess_toolset_coverage_potential(config.toolset)
            toolset_info = self._get_toolset_info(config.toolset)
            
            logger.info(f"Toolset {config.toolset} coverage potential: {potential:.1%} "
                       f"(requested: {config.coverage_target:.1%})")
            
            if potential < config.coverage_target:
                logger.warning(f"Toolset {config.toolset} can only achieve {potential:.1%} coverage")
                
                # Determine strategy based on toolset characteristics
                toolset_weight = self._get_toolset_weight(config.toolset)
                
                if config.allow_scope_expansion:
                    # Strategy 1: Expand to related toolsets
                    if toolset_weight > 0.8:  # Critical toolset
                        config.coverage_strategy = "intensive"
                        config.coverage_target = min(config.coverage_target, potential * 0.8)
                        logger.info(f"Critical toolset - intensive sampling at {config.coverage_target:.1%}")
                    else:
                        # Expand to similar toolsets
                        config.coverage_strategy = "grouped"
                        expanded_toolsets = self._get_related_toolsets(config.toolset, config.coverage_target)
                        if expanded_toolsets:
                            config.expanded_toolsets = expanded_toolsets
                            config.toolset = None  # Remove single toolset filter
                            logger.info(f"Expanded to {len(expanded_toolsets)} related toolsets")
                        else:
                            # Fallback to fab level
                            config.coverage_strategy = "adaptive"
                            config.toolset = None
                            config.fab = toolset_info.get('fab')
                            logger.info(f"Expanded to fab level: {config.fab}")
                else:
                    # Strategy 2: Adjust target to realistic level
                    config.coverage_strategy = "adaptive"
                    config.coverage_target = min(config.coverage_target, potential * 0.8)
                    logger.info(f"Adjusted target to realistic level: {config.coverage_target:.1%}")
                    
        elif config.phase:
            potential = self._assess_phase_coverage_potential(config.phase, config.fab)
            if potential < config.coverage_target:
                config.coverage_strategy = "adaptive"
                config.coverage_target = min(config.coverage_target, potential * 0.9)
                logger.info(f"Adjusted phase coverage target: {config.coverage_target:.1%}")
                
        elif config.fab:
            potential = self._assess_fab_coverage_potential(config.fab)
            if potential < config.coverage_target:
                config.coverage_strategy = "adaptive"
                config.coverage_target = min(config.coverage_target, potential * 0.9)
                logger.info(f"Adjusted fab coverage target: {config.coverage_target:.1%}")
        
        return config
    
    def _get_available_equipment(self, config: RandomGenerationConfig) -> List[Dict]:
        """Get available equipment based on configuration filters"""
        query = """
            SELECT DISTINCT 
                e.id as equipment_id,
                e.toolset,
                e.guid,
                e.node_id,
                e.data_code,
                e.category_no,
                e.kind,
                e.name,
                t.fab,
                t.model_no,
                t.phase_no
            FROM tb_equipments e
            JOIN tb_toolsets t ON e.toolset = t.code
            WHERE e.is_active = 1 AND t.is_active = 1
        """
        
        params = []
        
        # Apply filters
        if config.model:
            model_map = {ModelType.BIM: 1, ModelType.FIVE_D: 2}  # Assuming 1=BIM, 2=5D
            query += " AND t.model_no = ?"
            params.append(model_map[config.model])
            
        if config.phase:
            phase_map = {Phase.P1: 1, Phase.P2: 2, Phase.A: 1, Phase.B: 2, Phase.C: 3, Phase.D: 4}
            query += " AND t.phase_no = ?"
            params.append(phase_map[config.phase])
            
        if config.fab:
            query += " AND t.fab = ?"
            params.append(config.fab)
            
        # Handle expanded toolsets or single toolset
        if config.expanded_toolsets:
            placeholders = ','.join(['?' for _ in config.expanded_toolsets])
            query += f" AND e.toolset IN ({placeholders})"
            params.extend(config.expanded_toolsets)
        elif config.toolset:
            query += " AND e.toolset = ?"
            params.append(config.toolset)
            
        query += " ORDER BY e.id"
        
        cursor = self.db.cursor()
        cursor.execute(query, params)
        return cursor.fetchall()
    
    def _select_random_poc_pair(self, available_equipment: List[Dict], 
                               config: RandomGenerationConfig) -> Optional[POCPair]:
        """Select a random POC pair with bias mitigation"""
        
        # Step 1: Select random building (fab) if not specified
        if not config.fab:
            fabs = list(set(eq['fab'] for eq in available_equipment))
            if config.bias_mitigation:
                fab = self._select_with_bias_mitigation(fabs, 'fab')
            else:
                fab = random.choice(fabs)
        else:
            fab = config.fab
            
        # Filter equipment by selected fab
        fab_equipment = [eq for eq in available_equipment if eq['fab'] == fab]
        if not fab_equipment:
            return None
            
        # Step 2: Select random toolset - now handles expanded toolsets
        if config.expanded_toolsets:
            # When using expanded toolsets, select from available ones
            toolsets = list(set(eq['toolset'] for eq in fab_equipment 
                              if eq['toolset'] in config.expanded_toolsets))
            if not toolsets:
                return None
            if config.bias_mitigation:
                toolset = self._select_with_bias_mitigation(toolsets, 'toolset')
            else:
                toolset = random.choice(toolsets)
        elif config.toolset:
            toolset = config.toolset
        else:
            toolsets = list(set(eq['toolset'] for eq in fab_equipment))
            if config.bias_mitigation:
                toolset = self._select_with_bias_mitigation(toolsets, 'toolset')
            else:
                toolset = random.choice(toolsets)
            
        # Filter equipment by selected toolset
        toolset_equipment = [eq for eq in fab_equipment if eq['toolset'] == toolset]
        if len(toolset_equipment) < 2:
            return None
            
        # Step 3: Select two different equipment items
        if config.bias_mitigation:
            eq1 = self._select_with_bias_mitigation(toolset_equipment, 'equipment')
            eq2 = self._select_with_bias_mitigation(
                [eq for eq in toolset_equipment if eq['equipment_id'] != eq1['equipment_id']], 
                'equipment'
            )
        else:
            eq1, eq2 = random.sample(toolset_equipment, 2)
            
        # Step 4: Get POCs for selected equipment
        poc1 = self._get_random_poc_for_equipment(eq1['equipment_id'])
        poc2 = self._get_random_poc_for_equipment(eq2['equipment_id'])
        
        if not poc1 or not poc2:
            return None
            
        return POCPair(
            from_poc_id=poc1['id'],
            to_poc_id=poc2['id'],
            from_equipment_id=eq1['equipment_id'],
            to_equipment_id=eq2['equipment_id'],
            from_node_id=poc1['node_id'],
            to_node_id=poc2['node_id'],
            toolset_code=toolset,
            fab=fab,
            model_no=eq1['model_no'],
            phase_no=eq1['phase_no']
        )
    
    def _get_random_poc_for_equipment(self, equipment_id: int) -> Optional[Dict]:
        """Get a random POC for the given equipment"""
        query = """
            SELECT id, node_id, utility_no, reference, flow, markers, is_loopback
            FROM tb_equipment_pocs 
            WHERE equipment_id = ? AND is_active = 1 AND is_used = 1
        """
        
        cursor = self.db.cursor()
        cursor.execute(query, (equipment_id,))
        pocs = cursor.fetchall()
        
        if not pocs:
            return None
            
        return random.choice(pocs)
    
    def _select_with_bias_mitigation(self, items: List, category: str):
        """Select item with bias mitigation to ensure fair distribution"""
        if not items:
            return None
            
        if category not in self.bias_tracker:
            self.bias_tracker[category] = {}
            
        # Calculate selection weights (inverse of usage frequency)
        weights = []
        for item in items:
            item_key = str(item) if isinstance(item, (str, int)) else str(item.get('equipment_id', item))
            usage_count = self.bias_tracker[category].get(item_key, 0)
            # Higher weight for less used items
            weight = 1.0 / (usage_count + 1)
            weights.append(weight)
        
        # Weighted random selection
        return random.choices(items, weights=weights)[0]
    
    def _update_bias_tracking(self, poc_pair: POCPair):
        """Update bias tracking statistics"""
        categories = {
            'fab': poc_pair.fab,
            'toolset': poc_pair.toolset_code,
            'equipment': poc_pair.from_equipment_id,
            'equipment': poc_pair.to_equipment_id
        }
        
        for category, value in categories.items():
            if category not in self.bias_tracker:
                self.bias_tracker[category] = {}
            self.bias_tracker[category][str(value)] = self.bias_tracker[category].get(str(value), 0) + 1
    
    def _would_contribute_to_coverage(self, poc_pair: POCPair, existing_pairs: List[POCPair]) -> bool:
        """Check if this POC pair would contribute to coverage (basic check)"""
        # Simple duplicate check based on POC IDs
        for existing in existing_pairs:
            if ((existing.from_poc_id == poc_pair.from_poc_id and existing.to_poc_id == poc_pair.to_poc_id) or
                (existing.from_poc_id == poc_pair.to_poc_id and existing.to_poc_id == poc_pair.from_poc_id)):
                return False
        return True
    
    def _calculate_coverage(self, generated_pairs: List[POCPair], available_equipment: List[Dict]) -> float:
        """Calculate current coverage percentage"""
        if not available_equipment:
            return 0.0
            
        # Simple coverage calculation: unique POCs covered / total POCs
        covered_pocs = set()
        for pair in generated_pairs:
            covered_pocs.add(pair.from_poc_id)
            covered_pocs.add(pair.to_poc_id)
            
        # Get total POC count
        total_pocs = self._get_total_poc_count(available_equipment)
        if total_pocs == 0:
            return 0.0
            
        return len(covered_pocs) / total_pocs
    
    def _get_total_poc_count(self, available_equipment: List[Dict]) -> int:
        """Get total count of POCs for available equipment"""
        equipment_ids = [eq['equipment_id'] for eq in available_equipment]
        if not equipment_ids:
            return 0
            
        placeholders = ','.join(['?' for _ in equipment_ids])
        query = f"""
            SELECT COUNT(*) as total
            FROM tb_equipment_pocs 
            WHERE equipment_id IN ({placeholders}) AND is_active = 1 AND is_used = 1
        """
        
        cursor = self.db.cursor()
        cursor.execute(query, equipment_ids)
        result = cursor.fetchone()
        return result['total'] if result else 0
    
    def _initialize_bias_tracking(self, config: RandomGenerationConfig):
        """Initialize bias tracking for fair distribution"""
        self.bias_tracker = {
            'fab': {},
            'toolset': {},
            'equipment': {}
        }
        
    def _get_bias_statistics(self) -> Dict:
        """Get bias statistics for analysis"""
        stats = {}
        for category, counts in self.bias_tracker.items():
            if counts:
                stats[category] = {
                    'total_selections': sum(counts.values()),
                    'unique_items': len(counts),
                    'distribution': counts.copy()
                }
        return stats
    
    def _get_generation_statistics(self, available_equipment: List[Dict]) -> Dict:
        """Get generation statistics"""
        fabs = set(eq['fab'] for eq in available_equipment)
        toolsets = set(eq['toolset'] for eq in available_equipment)
        
        return {
            'total_equipment': len(available_equipment),
            'unique_fabs': len(fabs),
            'unique_toolsets': len(toolsets),
            'fabs': list(fabs),
            'toolsets': list(toolsets)
        }
    
    def generate_path_hash(self, poc_pair: POCPair) -> str:
        """Generate a unique hash for a POC pair"""
        # Create deterministic hash based on POC pair
        data = f"{min(poc_pair.from_poc_id, poc_pair.to_poc_id)}-{max(poc_pair.from_poc_id, poc_pair.to_poc_id)}"
        return hashlib.md5(data.encode()).hexdigest()
