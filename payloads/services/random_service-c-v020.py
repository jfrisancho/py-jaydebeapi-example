"""
Random path generation service with bias mitigation and intelligent sampling.
Handles random selection of PoC pairs for path discovery.
"""

import random
import logging
from typing import Optional, Dict, Any, List, Tuple, Set
from dataclasses import dataclass
from collections import defaultdict


@dataclass
class SamplingStats:
    """Statistics for tracking sampling effectiveness."""
    total_attempts: int = 0
    successful_samples: int = 0
    failed_samples: int = 0
    fab_distribution: Dict[str, int] = None
    toolset_distribution: Dict[str, int] = None
    
    def __post_init__(self):
        if self.fab_distribution is None:
            self.fab_distribution = defaultdict(int)
        if self.toolset_distribution is None:
            self.toolset_distribution = defaultdict(int)


class RandomService:
    """Service for generating random PoC pairs with bias mitigation."""
    
    def __init__(self, db_connection):
        self.db = db_connection
        self.logger = logging.getLogger(__name__)
        
        # Cache for frequently accessed data
        self._fab_cache = None
        self._toolset_cache = {}
        self._equipment_cache = {}
        
        # Sampling statistics
        self.stats = SamplingStats()
        
        # Bias mitigation
        self._used_poc_pairs: Set[Tuple[int, int]] = set()
        self._failed_toolsets: Set[str] = set()
        self._toolset_attempt_counts = defaultdict(int)
        
        # Maximum attempts before giving up on a toolset
        self.max_toolset_attempts = 10
    
    def generate_random_poc_pair(self, config) -> Optional[Tuple[Dict, Dict]]:
        """
        Generate a random pair of PoCs based on configuration.
        Returns (from_poc, to_poc) or None if no valid pair found.
        """
        self.stats.total_attempts += 1
        
        try:
            # Step 1: Select random fab if not specified
            fab = config.fab if config.fab else self._select_random_fab(config)
            if not fab:
                self.logger.warning("No fabs available for sampling")
                return None
            
            # Step 2: Select random toolset if not specified
            toolset = config.toolset if config.toolset else self._select_random_toolset(fab, config)
            if not toolset:
                self.logger.warning(f"No toolsets available for fab {fab}")
                return None
            
            # Step 3: Get equipment list for toolset
            equipment_list = self._get_toolset_equipment(toolset, config)
            if len(equipment_list) < 2:
                self.logger.debug(f"Toolset {toolset} has insufficient equipment for pairing")
                self._failed_toolsets.add(toolset)
                return None
            
            # Step 4: Select two different equipment randomly
            from_equipment, to_equipment = self._select_equipment_pair(equipment_list)
            if not from_equipment or not to_equipment:
                return None
            
            # Step 5: Select random PoCs from each equipment
            from_poc = self._select_random_poc(from_equipment['id'])
            to_poc = self._select_random_poc(to_equipment['id'])
            
            if not from_poc or not to_poc:
                return None
            
            # Step 6: Check if pair already used (bias mitigation)
            poc_pair_key = (min(from_poc['node_id'], to_poc['node_id']), 
                           max(from_poc['node_id'], to_poc['node_id']))
            
            if poc_pair_key in self._used_poc_pairs:
                # Try a few more times with same equipment
                for retry in range(3):
                    from_poc = self._select_random_poc(from_equipment['id'])
                    to_poc = self._select_random_poc(to_equipment['id'])
                    
                    if from_poc and to_poc:
                        new_key = (min(from_poc['node_id'], to_poc['node_id']), 
                                  max(from_poc['node_id'], to_poc['node_id']))
                        if new_key not in self._used_poc_pairs:
                            break
                else:
                    # All retries failed, increment toolset attempts
                    self._toolset_attempt_counts[toolset] += 1
                    if self._toolset_attempt_counts[toolset] >= self.max_toolset_attempts:
                        self._failed_toolsets.add(toolset)
                    return None
            
            # Step 7: Record successful sampling
            self._used_poc_pairs.add(poc_pair_key)
            self.stats.successful_samples += 1
            self.stats.fab_distribution[fab] += 1
            self.stats.toolset_distribution[toolset] += 1
            
            # Add context information
            from_poc['equipment_info'] = from_equipment
            from_poc['toolset'] = toolset
            from_poc['fab'] = fab
            
            to_poc['equipment_info'] = to_equipment
            to_poc['toolset'] = toolset
            to_poc['fab'] = fab
            
            self.logger.debug(f"Generated PoC pair: {from_poc['node_id']} -> {to_poc['node_id']} "
                             f"(toolset: {toolset}, fab: {fab})")
            
            return (from_poc, to_poc)
            
        except Exception as e:
            self.logger.error(f"Error generating random PoC pair: {str(e)}")
            self.stats.failed_samples += 1
            return None
    
    def _select_random_fab(self, config) -> Optional[str]:
        """Select a random fab based on configuration filters."""
        if self._fab_cache is None:
            self._load_fab_cache(config)
        
        if not self._fab_cache:
            return None
        
        return random.choice(self._fab_cache)
    
    def _load_fab_cache(self, config) -> None:
        """Load available fabs into cache."""
        query = """
        SELECT DISTINCT fab 
        FROM tb_toolsets 
        WHERE is_active = 1
        """
        conditions = []
        params = []
        
        if config.model_no:
            conditions.append("model_no = %s")
            params.append(config.model_no)
        
        if config.phase_no:
            conditions.append("phase_no = %s")
            params.append(config.phase_no)
        
        if conditions:
            query += " AND " + " AND ".join(conditions)
        
        query += " ORDER BY fab"
        
        with self.db.cursor() as cursor:
            cursor.execute(query, params)
            self._fab_cache = [row[0] for row in cursor.fetchall()]
        
        self.logger.info(f"Loaded {len(self._fab_cache)} fabs into cache")
    
    def _select_random_toolset(self, fab: str, config) -> Optional[str]:
        """Select a random toolset for the given fab."""
        cache_key = f"{fab}_{config.model_no}_{config.phase_no}"
        
        if cache_key not in self._toolset_cache:
            self._load_toolset_cache(fab, config, cache_key)
        
        available_toolsets = [ts for ts in self._toolset_cache[cache_key] 
                             if ts not in self._failed_toolsets]
        
        if not available_toolsets:
            # Reset failed toolsets if we've exhausted all options
            if len(self._failed_toolsets) >= len(self._toolset_cache[cache_key]) * 0.8:
                self.logger.info("Resetting failed toolsets cache")
                self._failed_toolsets.clear()
                self._toolset_attempt_counts.clear()
                available_toolsets = self._toolset_cache[cache_key]
            
            if not available_toolsets:
                return None
        
        return random.choice(available_toolsets)
    
    def _load_toolset_cache(self, fab: str, config, cache_key: str) -> None:
        """Load toolsets for a specific fab into cache."""
        query = """
        SELECT code 
        FROM tb_toolsets 
        WHERE fab = %s AND is_active = 1
        """
        conditions = ["fab = %s"]
        params = [fab]
        
        if config.model_no:
            conditions.append("model_no = %s")
            params.append(config.model_no)
        
        if config.phase_no:
            conditions.append("phase_no = %s")
            params.append(config.phase_no)
        
        query = f"SELECT code FROM tb_toolsets WHERE {' AND '.join(conditions)} ORDER BY code"
        
        with self.db.cursor() as cursor:
            cursor.execute(query, params)
            self._toolset_cache[cache_key] = [row[0] for row in cursor.fetchall()]
        
        self.logger.debug(f"Loaded {len(self._toolset_cache[cache_key])} toolsets for {cache_key}")
    
    def _get_toolset_equipment(self, toolset: str, config) -> List[Dict]:
        """Get all equipment for a toolset."""
        cache_key = f"eq_{toolset}"
        
        if cache_key not in self._equipment_cache:
            query = """
            SELECT id, guid, node_id, data_code, category_no, kind, name
            FROM tb_equipments 
            WHERE toolset = %s AND is_active = 1
            ORDER BY id
            """
            
            with self.db.cursor() as cursor:
                cursor.execute(query, (toolset,))
                equipment_list = []
                for row in cursor.fetchall():
                    equipment_list.append({
                        'id': row[0],
                        'guid': row[1],
                        'node_id': row[2],
                        'data_code': row[3],
                        'category_no': row[4],
                        'kind': row[5],
                        'name': row[6]
                    })
                
                self._equipment_cache[cache_key] = equipment_list
        
        return self._equipment_cache[cache_key]
    
    def _select_equipment_pair(self, equipment_list: List[Dict]) -> Tuple[Optional[Dict], Optional[Dict]]:
        """Select two different equipment randomly from the list."""
        if len(equipment_list) < 2:
            return None, None
        
        # Ensure we get two different equipment
        selected = random.sample(equipment_list, 2)
        return selected[0], selected[1]
    
    def _select_random_poc(self, equipment_id: int) -> Optional[Dict]:
        """Select a random PoC from the given equipment."""
        query = """
        SELECT id, equipment_id, node_id, is_used, markers, utility_no, 
               reference, flow, is_loopback
        FROM tb_equipment_pocs 
        WHERE equipment_id = %s AND is_active = 1
        """
        
        with self.db.cursor() as cursor:
            cursor.execute(query, (equipment_id,))
            pocs = cursor.fetchall()
        
        if not pocs:
            return None
        
        # Convert to dict format
        poc_list = []
        for poc in pocs:
            poc_dict = {
                'id': poc[0],
                'equipment_id': poc[1],
                'node_id': poc[2],
                'is_used': bool(poc[3]),
                'markers': poc[4],
                'utility_no': poc[5],
                'reference': poc[6],
                'flow': poc[7],
                'is_loopback': bool(poc[8])
            }
            poc_list.append(poc_dict)
        
        # Prefer used PoCs for better connectivity chances
        used_pocs = [poc for poc in poc_list if poc['is_used']]
        if used_pocs and random.random() < 0.7:  # 70% chance to pick used PoC
            return random.choice(used_pocs)
        
        return random.choice(poc_list)
    
    def get_sampling_statistics(self) -> Dict[str, Any]:
        """Get current sampling statistics."""
        success_rate = 0.0
        if self.stats.total_attempts > 0:
            success_rate = (self.stats.successful_samples / self.stats.total_attempts) * 100
        
        return {
            'total_attempts': self.stats.total_attempts,
            'successful_samples': self.stats.successful_samples,
            'failed_samples': self.stats.failed_samples,
            'success_rate': success_rate,
            'unique_poc_pairs': len(self._used_poc_pairs),
            'failed_toolsets': len(self._failed_toolsets),
            'fab_distribution': dict(self.stats.fab_distribution),
            'toolset_distribution': dict(self.stats.toolset_distribution),
            'cache_sizes': {
                'fabs': len(self._fab_cache) if self._fab_cache else 0,
                'toolsets': len(self._toolset_cache),
                'equipment': len(self._equipment_cache)
            }
        }
    
    def reset_sampling_state(self) -> None:
        """Reset sampling state for a fresh start."""
        self._used_poc_pairs.clear()
        self._failed_toolsets.clear()
        self._toolset_attempt_counts.clear()
        self.stats = SamplingStats()
        self.logger.info("Sampling state reset")
    
    def clear_caches(self) -> None:
        """Clear all caches to free memory."""
        self._fab_cache = None
        self._toolset_cache.clear()
        self._equipment_cache.clear()
        self.logger.info("All caches cleared")
    
    def get_toolset_equipment_count(self, toolset: str) -> int:
        """Get the number of equipment in a toolset."""
        query = """
        SELECT COUNT(*) 
        FROM tb_equipments 
        WHERE toolset = %s AND is_active = 1
        """
        
        with self.db.cursor() as cursor:
            cursor.execute(query, (toolset,))
            return cursor.fetchone()[0]
    
    def get_equipment_poc_count(self, equipment_id: int) -> int:
        """Get the number of PoCs for an equipment."""
        query = """
        SELECT COUNT(*) 
        FROM tb_equipment_pocs 
        WHERE equipment_id = %s AND is_active = 1
        """
        
        with self.db.cursor() as cursor:
            cursor.execute(query, (equipment_id,))
            return cursor.fetchone()[0]
    
    def validate_toolset_viability(self, toolset: str) -> Dict[str, Any]:
        """Validate if a toolset has sufficient data for random sampling."""
        equipment_count = self.get_toolset_equipment_count(toolset)
        
        if equipment_count < 2:
            return {
                'viable': False,
                'reason': 'Insufficient equipment count',
                'equipment_count': equipment_count
            }
        
        # Check PoC counts for equipment
        query = """
        SELECT e.id, COUNT(p.id) as poc_count
        FROM tb_equipments e
        LEFT JOIN tb_equipment_pocs p ON e.id = p.equipment_id AND p.is_active = 1
        WHERE e.toolset = %s AND e.is_active = 1
        GROUP BY e.id
        HAVING poc_count > 0
        """
        
        with self.db.cursor() as cursor:
            cursor.execute(query, (toolset,))
            equipment_with_pocs = cursor.fetchall()
        
        equipment_with_pocs_count = len(equipment_with_pocs)
        
        if equipment_with_pocs_count < 2:
            return {
                'viable': False,
                'reason': 'Insufficient equipment with PoCs',
                'equipment_count': equipment_count,
                'equipment_with_pocs': equipment_with_pocs_count
            }
        
        # Calculate average PoCs per equipment
        total_pocs = sum(row[1] for row in equipment_with_pocs)
        avg_pocs = total_pocs / equipment_with_pocs_count if equipment_with_pocs_count > 0 else 0
        
        return {
            'viable': True,
            'equipment_count': equipment_count,
            'equipment_with_pocs': equipment_with_pocs_count,
            'total_pocs': total_pocs,
            'avg_pocs_per_equipment': avg_pocs
        }