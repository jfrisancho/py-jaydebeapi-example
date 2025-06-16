"""
Service for random path generation with bias mitigation.
"""

import random
import hashlib
from typing import List, Dict, Tuple, Optional, Set
from collections import defaultdict
from datetime import datetime

from models import RunConfig, PathResult, PathDefinition, Equipment, Toolset, BiasReduction, ValidationError, ReviewFlag
from enums import Method, ObjectType, ErrorType, Severity
from db import Database


class RandomService:
    """Service for generating random paths with bias mitigation."""
    
    def __init__(self, db: Database, fab: str):
        self.db = db
        self.fab = fab
        self.bias_config = BiasReduction()
        
        # Caches for bias mitigation
        self._toolset_attempt_counts: Dict[str, int] = defaultdict(int)
        self._equipment_attempt_counts: Dict[str, int] = defaultdict(int)
        self._used_node_pairs: Set[Tuple[int, int]] = set()
        self._utility_usage_counts: Dict[str, int] = defaultdict(int)
        self._category_usage_counts: Dict[str, int] = defaultdict(int)
        
        # Load toolsets for the fab
        self._toolsets = self._load_toolsets()
    
    def generate_random_path(self, config: RunConfig) -> PathResult:
        """Generate a single random path attempt."""
        
        # Select toolset with bias mitigation
        toolset = self._select_toolset_with_bias_mitigation()
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
                    notes="Could not select valid equipment pair"
                )]
            )
        
        # Select points of contact
        start_node = self._select_point_of_contact(equipment_pair[0])
        end_node = self._select_point_of_contact(equipment_pair[1])
        
        if not start_node or not end_node:
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
                    object_type=ObjectType.NODE,
                    notes="Could not select valid points of contact"
                )]
            )
        
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
                created_at=datetime.now(),
                severity=Severity.MEDIUM,
                reason="No path found between selected nodes",
                object_type=ObjectType.NODE,
                start_node_id=start_node,
                end_node_id=end_node,
                utility=toolset.utility_codes[0] if toolset.utility_codes else None,
                notes=f"Toolset: {toolset.name}, Equipment: {equipment_pair[0].name} -> {equipment_pair[1].name}"
            )
            
            return PathResult(
                path_found=False,
                review_flags=[review_flag]
            )
        
        # Create path definition
        path_definition = self._create_path_definition(
            path_data, toolset, equipment_pair, start_node, end_node
        )
        
        # Update bias mitigation tracking
        self._update_bias_tracking(toolset, equipment_pair, node_pair)
        
        return PathResult(
            path_found=True,
            path_definition=path_definition,
            coverage_contribution=path_definition.coverage
        )
    
    def _select_toolset_with_bias_mitigation(self) -> Optional[Toolset]:
        """Select a toolset while mitigating selection bias."""
        available_toolsets = [
            ts for ts in self._toolsets 
            if self._toolset_attempt_counts[ts.id] < self.bias_config.max_attempts_per_toolset
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
            
            # Reduce weight for frequently used utilities
            for utility in toolset.utility_codes:
                usage_factor = 1.0 - (self._utility_usage_counts[utility] * self.bias_config.utility_diversity_weight / 100)
                weight *= max(0.1, usage_factor)
            
            # Reduce weight for frequently used categories
            category_factor = 1.0 - (self._category_usage_counts[toolset.category] * self.bias_config.category_diversity_weight / 100)
            weight *= max(0.1, category_factor)
            
            weights.append(weight)
        
        # Weighted random selection
        return random.choices(available_toolsets, weights=weights)[0]
    
    def _select_equipment_pair(self, toolset: Toolset) -> Optional[Tuple[Equipment, Equipment]]:
        """Select a pair of different equipment from the toolset."""
        available_equipment = [
            eq for eq in toolset.equipment_list
            if (self._equipment_attempt_counts[eq.id] < self.bias_config.max_attempts_per_equipment
                and len(eq.points_of_contact) > 0)
        ]
        
        if len(available_equipment) < 2:
            return None
        
        # Select two different equipment pieces
        equipment1 = random.choice(available_equipment)
        available_equipment2 = [eq for eq in available_equipment if eq.id != equipment1.id]
        
        if not available_equipment2:
            return None
        
        equipment2 = random.choice(available_equipment2)
        return (equipment1, equipment2)
    
    def _select_point_of_contact(self, equipment: Equipment) -> Optional[int]:
        """Select a random point of contact from equipment."""
        if not equipment.points_of_contact:
            return None
        return random.choice(equipment.points_of_contact)
    
    def _find_shortest_path(self, start_node: int, end_node: int) -> Optional[Dict]:
        """Find the shortest path between two nodes using database function."""
        try:
            # Call database stored procedure or function to find shortest path
            # This is a placeholder - actual implementation depends on your pathfinding logic
            sql = "SELECT find_shortest_path(?, ?) as path_data"
            result = self.db.query(sql, [start_node, end_node])
            
            if result and result[0] and result[0][0]:
                # Parse path data (assuming JSON or similar format)
                import json
                path_data = json.loads(result[0][0]) if isinstance(result[0][0], str) else result[0][0]
                return path_data
            
            return None
            
        except Exception as e:
            print(f"Error finding path from {start_node} to {end_node}: {e}")
            return None
    
    def _create_path_definition(self, path_data: Dict, toolset: Toolset, 
                              equipment_pair: Tuple[Equipment, Equipment],
                              start_node: int, end_node: int) -> PathDefinition:
        """Create a PathDefinition from discovered path data."""
        
        # Extract path information
        nodes = path_data.get('nodes', [])
        links = path_data.get('links', [])
        total_length = path_data.get('total_length_mm', 0.0)
        
        # Create path context
        path_context = {
            'nodes': nodes,
            'links': links,
            'start_node_id': start_node,
            'end_node_id': end_node,
            'toolset_id': toolset.id,
            'equipment_ids': [equipment_pair[0].id, equipment_pair[1].id]
        }
        
        # Create path hash for uniqueness
        path_hash = self._generate_path_hash(path_context)
        
        # Calculate coverage (simplified - actual implementation may vary)
        coverage = len(nodes) + len(links)  # Simple node + link count
        
        return PathDefinition(
            id=None,  # Will be set when stored
            path_hash=path_hash,
            fab=self.fab,
            category=toolset.category,
            scope="CONNECTIVITY",  # Default scope
            node_count=len(nodes),
            link_count=len(links),
            total_length_mm=total_length,
            coverage=coverage,
            utilities=toolset.utility_codes,
            path_context=path_context
        )
    
    def _generate_path_hash(self, path_context: Dict) -> str:
        """Generate a unique hash for the path."""
        # Create a deterministic string representation
        hash_input = f"{path_context['start_node_id']}-{path_context['end_node_id']}-"
        hash_input += "-".join(map(str, sorted(path_context['nodes'])))
        hash_input += "-" + "-".join(map(str, sorted(path_context['links'])))
        
        return hashlib.sha256(hash_input.encode()).hexdigest()
    
    def _update_bias_tracking(self, toolset: Toolset, equipment_pair: Tuple[Equipment, Equipment],
                            node_pair: Tuple[int, int]):
        """Update tracking counters for bias mitigation."""
        self._toolset_attempt_counts[toolset.id] += 1
        self._equipment_attempt_counts[equipment_pair[0].id] += 1
        self._equipment_attempt_counts[equipment_pair[1].id] += 1
        self._used_node_pairs.add(node_pair)
        
        for utility in toolset.utility_codes:
            self._utility_usage_counts[utility] += 1
        
        self._category_usage_counts[toolset.category] += 1
    
    def _load_toolsets(self) -> List[Toolset]:
        """Load available toolsets for the fab from database."""
        # This is a placeholder implementation
        # In reality, you would query your actual toolset/equipment tables
        
        sql = """
        SELECT DISTINCT 
            toolset_id, toolset_name, category, utility_codes
        FROM toolsets 
        WHERE fab = ?
        ORDER BY toolset_id
        """
        
        try:
            rows = self.db.query(sql, [self.fab])
            toolsets = []
            
            for row in rows:
                toolset_id, name, category, utility_codes_str = row
                
                # Parse utility codes (assuming comma-separated)
                utility_codes = utility_codes_str.split(',') if utility_codes_str else []
                
                # Load equipment for this toolset
                equipment_list = self._load_equipment_for_toolset(toolset_id)
                
                toolset = Toolset(
                    id=toolset_id,
                    name=name,
                    fab=self.fab,
                    category=category,
                    equipment_list=equipment_list,
                    utility_codes=utility_codes
                )
                
                toolsets.append(toolset)
            
            return toolsets
            
        except Exception as e:
            print(f"Error loading toolsets for fab {self.fab}: {e}")
            return []
    
    def _load_equipment_for_toolset(self, toolset_id: str) -> List[Equipment]:
        """Load equipment list for a specific toolset."""
        # Placeholder implementation
        sql = """
        SELECT 
            equipment_id, equipment_name, utility_codes, category,
            poc_node_ids
        FROM equipment 
        WHERE toolset_id = ?
        ORDER BY equipment_id
        """
        
        try:
            rows = self.db.query(sql, [toolset_id])
            equipment_list = []
            
            for row in rows:
                eq_id, name, utility_codes_str, category, poc_nodes_str = row
                
                # Parse utility codes and PoC node IDs
                utility_codes = utility_codes_str.split(',') if utility_codes_str else []
                poc_nodes = [int(x.strip()) for x in poc_nodes_str.split(',') if x.strip().isdigit()] if poc_nodes_str else []
                
                equipment = Equipment(
                    id=eq_id,
                    name=name,
                    toolset_id=toolset_id,
                    points_of_contact=poc_nodes,
                    utility_codes=utility_codes,
                    category=category
                )
                
                equipment_list.append(equipment)
            
            return equipment_list
            
        except Exception as e:
            print(f"Error loading equipment for toolset {toolset_id}: {e}")
            return []