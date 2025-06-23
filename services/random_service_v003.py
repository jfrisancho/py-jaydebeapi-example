"""
Service for random path generation with bias mitigation.
Updated for simplified schema with new toolset/equipment structure.
"""

import random
import hashlib
from typing import List, Dict, Tuple, Optional, Set
from collections import defaultdict
from datetime import datetime

from models import RunConfig, PathResult, PathDefinition, Equipment, Toolset, BiasReduction, ValidationError, ReviewFlag
from enums import Method, ObjectType, ErrorType, Severity, SourceType, FlagType
from db import Database


class RandomService:
    """Service for generating random paths with bias mitigation."""
    
    def __init__(self, db: Database, building_code: str):
        self.db = db
        self.building_code = building_code
        self.bias_config = BiasReduction()
        
        # Caches for bias mitigation
        self._toolset_attempt_counts: Dict[str, int] = defaultdict(int)
        self._equipment_attempt_counts: Dict[str, int] = defaultdict(int)
        self._used_node_pairs: Set[Tuple[int, int]] = set()
        self._utility_usage_counts: Dict[str, int] = defaultdict(int)
        self._category_usage_counts: Dict[str, int] = defaultdict(int)
        
        # Load toolsets for the building (simplified structure)
        self._toolsets = self._load_toolsets()
    
    def generate_random_path(self, config: RunConfig) -> PathResult:
        """Generate a single random path attempt."""
        
        # Select toolset with bias mitigation
        toolset = self._select_toolset_with_bias_mitigation(config.toolset)
        if not toolset:
            return PathResult(
                path_found=False,
                errors=[ValidationError(
                    id=None,
                    run_id=config.run_id,
                    path_definition_id=None,
                    validation_test_id=None,
                    severity=Severity.ERROR,
                    error_scope="TOOLSET_SELECTION",
                    error_type=ErrorType.MISSING_NODE,
                    object_type=ObjectType.NODE,
                    building_code=self.building_code,
                    notes="No available toolsets for path generation"
                )]
            )
        
        # Select two different equipment pieces
        if len(toolset.equipment_list) < 2:
            return PathResult(
                path_found=False,
                errors=[ValidationError(
                    id=None,
                    run_id=config.run_id,
                    path_definition_id=None,
                    validation_test_id=None,
                    severity=Severity.ERROR,
                    error_scope="EQUIPMENT_SELECTION", 
                    error_type=ErrorType.MISSING_NODE,
                    object_type=ObjectType.NODE,
                    building_code=self.building_code,
                    notes=f"Toolset {toolset.name} has insufficient equipment"
                )]
            )
        
        equipment_pair = self._select_equipment_pair(toolset)
        if not equipment_pair:
            return PathResult(
                path_found=False,
                errors=[ValidationError(
                    id=None,
                    run_id=config.run_id,
                    path_definition_id=None,
                    validation_test_id=None,
                    severity=Severity.ERROR,
                    error_scope="EQUIPMENT_SELECTION",
                    error_type=ErrorType.MISSING_NODE,
                    object_type=ObjectType.NODE,
                    building_code=self.building_code,
                    notes="Could not select valid equipment pair"
                )]
            )
        
        # Select points of contact
        start_poc = self._select_point_of_contact(equipment_pair[0])
        end_poc = self._select_point_of_contact(equipment_pair[1])
        
        if not start_poc or not end_poc:
            return PathResult(
                path_found=False,
                errors=[ValidationError(
                    id=None,
                    run_id=config.run_id,
                    path_definition_id=None,
                    validation_test_id=None,
                    severity=Severity.ERROR,
                    error_scope="POC_SELECTION",
                    error_type=ErrorType.MISSING_NODE,
                    object_type=ObjectType.POC,
                    building_code=self.building_code,
                    notes="Could not select valid points of contact"
                )]
            )
        
        start_node = start_poc.node_id
        end_node = end_poc.node_id
        
        # Check if this node pair was already attempted (bias mitigation)
        node_pair = (min(start_node, end_node), max(start_node, end_node))
        if node_pair in self._used_node_pairs:
            return PathResult(
                path_found=False,
                errors=[ValidationError(
                    id=None,
                    run_id=config.run_id,
                    path_definition_id=None,
                    validation_test_id=None,
                    severity=Severity.WARNING,
                    error_scope="BIAS_MITIGATION",
                    error_type=ErrorType.PATH_NOT_FOUND,
                    object_type=ObjectType.NODE,
                    building_code=self.building_code,
                    notes=f"Node pair ({start_node}, {end_node}) already attempted"
                )]
            )
        
        # Find shortest path
        path_data = self._find_shortest_path(start_node, end_node)
        if not path_data:
            # Flag for review
            review_flag = ReviewFlag(
                id=None,
                run_id=config.run_id,
                flag_type=FlagType.MANUAL_REVIEW,
                created_at=datetime.now(),
                severity=Severity.MEDIUM,
                reason="No path found between selected nodes",
                object_type=ObjectType.NODE,
                start_node_id=start_node,
                end_node_id=end_node,
                building_code=self.building_code,
                utility=start_poc.utility_code,
                notes=f"Toolset: {toolset.name}, Equipment: {equipment_pair[0].name} -> {equipment_pair[1].name}"
            )
            
            return PathResult(
                path_found=False,
                review_flags=[review_flag]
            )
        
        # Create path definition
        path_definition = self._create_path_definition(
            path_data, toolset, equipment_pair, start_poc, end_poc
        )
        
        # Update bias mitigation tracking
        self._update_bias_tracking(toolset, equipment_pair, node_pair)
        
        return PathResult(
            path_found=True,
            path_definition=path_definition,
            coverage_contribution=path_definition.coverage
        )
    
    def _load_toolsets(self) -> List[Toolset]:
        """Load toolsets for the building with simplified structure."""
        try:
            # Query simplified toolset structure
            sql = """
            SELECT code, fab, phase, name, description, is_active
            FROM tb_toolsets 
            WHERE fab = ? AND is_active = TRUE
            ORDER BY code
            """
            
            results = self.db.query(sql, [self.building_code])
            toolsets = []
            
            for row in results:
                code, fab, phase, name, description, is_active = row
                
                toolset = Toolset(
                    code=code,
                    fab=fab,
                    phase=phase,
                    name=name or "",
                    description=description,
                    is_active=is_active
                )
                
                # Load equipment for this toolset
                toolset.equipment_list = self._load_equipment_for_toolset(code)
                toolsets.append(toolset)
            
            return toolsets
            
        except Exception as e:
            print(f"Error loading toolsets for building {self.building_code}: {e}")
            return []
    
    def _load_equipment_for_toolset(self, toolset_code: str) -> List[Equipment]:
        """Load equipment for a specific toolset."""
        try:
            # Query simplified equipment structure
            sql = """
            SELECT id, toolset, name, guid, node_id, kind, is_active
            FROM tb_equipments 
            WHERE toolset = ? AND is_active = TRUE
            ORDER BY name
            """
            
            results = self.db.query(sql, [toolset_code])
            equipment_list = []
            
            for row in results:
                eq_id, toolset, name, guid, node_id, kind, is_active = row
                
                equipment = Equipment(
                    id=eq_id,
                    toolset_code=toolset,
                    name=name,
                    guid=guid,
                    node_id=node_id,
                    kind=kind,
                    is_active=is_active
                )
                
                # Load PoCs for this equipment
                equipment.pocs = self._load_pocs_for_equipment(eq_id)
                equipment_list.append(equipment)
            
            return equipment_list
            
        except Exception as e:
            print(f"Error loading equipment for toolset {toolset_code}: {e}")
            return []
    
    def _load_pocs_for_equipment(self, equipment_id: int) -> List['EquipmentPoC']:
        """Load PoCs for a specific equipment."""
        try:
            from models import EquipmentPoC
            
            sql = """
            SELECT id, equipment_id, code, node_id, utility, flow, is_used, is_active
            FROM tb_equipment_pocs 
            WHERE equipment_id = ? AND is_active = TRUE
            ORDER BY code
            """
            
            results = self.db.query(sql, [equipment_id])
            pocs = []
            
            for row in results:
                poc_id, eq_id, code, node_id, utility, flow, is_used, is_active = row
                
                poc = EquipmentPoC(
                    id=poc_id,
                    equipment_id=eq_id,
                    code=code,
                    node_id=node_id,
                    utility_code=utility,
                    flow_direction=flow,
                    is_used=is_used,
                    is_active=is_active
                )
                pocs.append(poc)
            
            return pocs
            
        except Exception as e:
            print(f"Error loading PoCs for equipment {equipment_id}: {e}")
            return []
    
    def _select_toolset_with_bias_mitigation(self, target_toolset: str = "") -> Optional[Toolset]:
        """Select a toolset while mitigating selection bias."""
        # If specific toolset requested, try to find it
        if target_toolset and target_toolset != "ALL":
            specific_toolsets = [ts for ts in self._toolsets if ts.code == target_toolset]
            if specific_toolsets:
                return specific_toolsets[0]
        
        # Filter available toolsets based on attempt limits
        available_toolsets = [
            ts for ts in self._toolsets 
            if self._toolset_attempt_counts[ts.code] < self.bias_config.max_attempts_per_toolset
        ]
        
        if not available_toolsets:
            # Reset counters if all toolsets exhausted
            self._toolset_attempt_counts.clear()
            available_toolsets = self._toolsets
        
        if not available_toolsets:
            return None
        
        # Weight selection by diversity (favor less-used utilities/categories)
        weights = []
        for toolset in available_toolsets:
            weight = 1.0
            
            # Get utility codes for this toolset
            utility_codes = self._get_toolset_utilities(toolset)
            
            # Reduce weight for frequently used utilities
            for utility in utility_codes:
                usage_factor = 1.0 - (self._utility_usage_counts[utility] * self.bias_config.utility_diversity_weight / 100)
                weight *= max(0.1, usage_factor)
            
            # Get category for this toolset (simplified - using first equipment's category)
            category = self._get_toolset_category(toolset)
            if category:
                category_factor = 1.0 - (self._category_usage_counts[category] * self.bias_config.category_diversity_weight / 100)
                weight *= max(0.1, category_factor)
            
            weights.append(weight)
        
        # Weighted random selection
        if weights:
            selected_toolset = random.choices(available_toolsets, weights=weights)[0]
            return selected_toolset
        
        return random.choice(available_toolsets) if available_toolsets else None
    
    def _select_equipment_pair(self, toolset: Toolset) -> Optional[Tuple[Equipment, Equipment]]:
        """Select a pair of equipment for path generation."""
        available_equipment = [eq for eq in toolset.equipment_list if eq.is_active]
        
        if len(available_equipment) < 2:
            return None
        
        # Filter equipment based on attempt limits
        filtered_equipment = [
            eq for eq in available_equipment
            if self._equipment_attempt_counts[eq.guid] < self.bias_config.max_attempts_per_equipment
        ]
        
        if len(filtered_equipment) < 2:
            # Reset counters if not enough equipment available
            for eq in available_equipment:
                self._equipment_attempt_counts[eq.guid] = 0
            filtered_equipment = available_equipment
        
        # Select two different equipment pieces
        equipment_1 = random.choice(filtered_equipment)
        equipment_2 = random.choice([eq for eq in filtered_equipment if eq.id != equipment_1.id])
        
        return (equipment_1, equipment_2)
    
    def _select_point_of_contact(self, equipment: Equipment) -> Optional['EquipmentPoC']:
        """Select a point of contact from equipment."""
        available_pocs = equipment.get_available_pocs()
        if not available_pocs:
            return None
        
        # Prefer PoCs with utilities defined
        pocs_with_utility = [poc for poc in available_pocs if poc.utility_code]
        if pocs_with_utility:
            return random.choice(pocs_with_utility)
        
        return random.choice(available_pocs)
    
    def _find_shortest_path(self, start_node: int, end_node: int) -> Optional[dict]:
        """Find shortest path between two nodes (placeholder implementation)."""
        # This is a placeholder - in reality this would implement
        # Dijkstra's algorithm or similar pathfinding
        
        # Simulate path finding
        if random.random() < 0.7:  # 70% success rate
            # Simulate path data
            path_length = random.randint(5, 50)
            nodes = list(range(start_node, start_node + path_length))
            links = list(range(100, 100 + path_length - 1))
            
            return {
                'nodes': nodes,
                'links': links,
                'total_length_mm': random.uniform(1000.0, 50000.0),
                'utilities': ['N2', 'CDA'],
                'category': 'PRODUCTION'
            }
        
        return None  # No path found
    
    def _create_path_definition(self, path_data: dict, toolset: Toolset, 
                              equipment_pair: Tuple[Equipment, Equipment],
                              start_poc, end_poc) -> PathDefinition:
        """Create a path definition from path data."""
        
        # Generate path hash
        path_str = f"{path_data['nodes']}_{path_data['links']}"
        path_hash = hashlib.md5(path_str.encode()).hexdigest()
        
        # Calculate coverage (simplified)
        coverage = len(path_data['nodes']) / 1000.0  # Normalize to percentage
        
        # Create path context
        path_context = {
            'nodes': path_data['nodes'],
            'links': path_data['links'],
            'start_node_id': path_data['nodes'][0] if path_data['nodes'] else None,
            'end_node_id': path_data['nodes'][-1] if path_data['nodes'] else None,
            'toolset_code': toolset.code,
            'equipment_ids': [equipment_pair[0].id, equipment_pair[1].id],
            'start_equipment': equipment_pair[0].name,
            'end_equipment': equipment_pair[1].name,
            'start_poc': start_poc.code,
            'end_poc': end_poc.code
        }
        
        return PathDefinition(
            id=None,
            path_hash=path_hash,
            source_type=SourceType.RANDOM,
            building_code=self.building_code,
            category=path_data['category'],
            scope='CONNECTIVITY',
            node_count=len(path_data['nodes']),
            link_count=len(path_data['links']),
            total_length_mm=path_data['total_length_mm'],
            coverage=coverage,
            utilities=path_data['utilities'],
            path_context=path_context
        )
    
    def _update_bias_tracking(self, toolset: Toolset, equipment_pair: Tuple[Equipment, Equipment],
                            node_pair: Tuple[int, int]):
        """Update bias mitigation tracking."""
        # Update attempt counts
        self._toolset_attempt_counts[toolset.code] += 1
        self._equipment_attempt_counts[equipment_pair[0].guid] += 1
        self._equipment_attempt_counts[equipment_pair[1].guid] += 1
        
        # Track used node pairs
        self._used_node_pairs.add(node_pair)
        
        # Update utility usage (simplified)
        for utility in ['N2', 'CDA']:  # Example utilities
            self._utility_usage_counts[utility] += 1
        
        # Update category usage
        self._category_usage_counts['PRODUCTION'] += 1
    
    def _get_toolset_utilities(self, toolset: Toolset) -> List[str]:
        """Get utility codes used by a toolset."""
        utilities = set()
        for equipment in toolset.equipment_list:
            for poc in equipment.pocs:
                if poc.utility_code:
                    utilities.add(poc.utility_code)
        return list(utilities)
    
    def _get_toolset_category(self, toolset: Toolset) -> Optional[str]:
        """Get primary category for a toolset (simplified)."""
        if toolset.equipment_list:
            # Use equipment kind as category proxy
            return toolset.equipment_list[0].kind
        return None