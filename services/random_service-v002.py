"""
Service for random path generation with bias mitigation.
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
        
        # Load toolsets for the building
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
                category_factor = 1.0 - (self._category_usage_counts[category] * self.bias_config.category_diversity_weight / 100
