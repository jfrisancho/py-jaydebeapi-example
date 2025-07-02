"""
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


class RandomService:
    """Service for random path generation with bias mitigation"""
    
    def __init__(self, db_connection):
        self.db = db_connection
        self.generation_stats = {}
        self.bias_tracker = {}
        
    def generate_random_paths(self, config: RandomGenerationConfig) -> Dict:
        """
        Generate random paths until coverage target is achieved.
        
        Args:
            config: Random generation configuration
            
        Returns:
            Dictionary with generation results and statistics
        """
        logger.info(f"Starting random path generation with target coverage: {config.coverage_target}")
        
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
            'success': coverage_achieved >= config.coverage_target,
            'poc_pairs': generated_pairs,
            'bias_stats': self._get_bias_statistics(),
            'generation_stats': self._get_generation_statistics(available_equipment)
        }
        
        logger.info(f"Random generation completed: {len(generated_pairs)} pairs, "
                   f"{coverage_achieved:.2%} coverage in {attempts} attempts")
        
        return results
    
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
            
        if config.toolset:
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
            
        # Step 2: Select random toolset if not specified
        if not config.toolset:
            toolsets = list(set(eq['toolset'] for eq in fab_equipment))
            if config.bias_mitigation:
                toolset = self._select_with_bias_mitigation(toolsets, 'toolset')
            else:
                toolset = random.choice(toolsets)
        else:
            toolset = config.toolset
            
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
