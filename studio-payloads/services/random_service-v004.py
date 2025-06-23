"""
Service for random path generation with bias mitigation.
Updated for simplified schema with new toolset/equipment structure.
"""

import random
import hashlib
from typing import List, Dict, Tuple, Optional, Set
from collections import defaultdict
from datetime import datetime

from models import RunConfig, PathResult, PathDefinition, Equipment, Toolset, EquipmentPoC, BiasReduction, ValidationError, ReviewFlag
from enums import Method, ObjectType, ErrorType, Severity, SourceType, FlagType
from db import Database


class RandomService:
    """Service for generating random paths with bias mitigation."""
    
    def __init__(self, db: Database, building_code: str): # building_code is fab
        self.db = db
        self.building_code = building_code # This is fab
        self.bias_config = BiasReduction()
        
        self._toolset_attempt_counts: Dict[str, int] = defaultdict(int)
        self._equipment_attempt_counts: Dict[str, int] = defaultdict(int) # Keyed by equipment.guid
        self._used_node_pairs: Set[Tuple[int, int]] = set()
        self._utility_usage_counts: Dict[str, int] = defaultdict(int)
        self._category_usage_counts: Dict[str, int] = defaultdict(int) # Category from equipment.kind
        
        self._toolsets = self._load_toolsets()
    
    def generate_random_path(self, config: RunConfig) -> PathResult:
        """Generate a single random path attempt."""
        
        toolset = self._select_toolset_with_bias_mitigation(config.toolset, config.phase)
        if not toolset:
            return PathResult(
                path_found=False,
                errors=[ValidationError(
                    id=None, run_id=config.run_id, path_definition_id=None, validation_test_id=None,
                    severity=Severity.ERROR, error_scope="TOOLSET_SELECTION", error_type=ErrorType.MISSING_NODE,
                    object_type=ObjectType.TOOLSET, building_code=self.building_code,
                    notes=f"No available toolsets for fab {self.building_code}, phase {config.phase}, toolset code {config.toolset}"
                )]
            )
        
        if len(toolset.equipment_list) < 2:
            return PathResult(
                path_found=False,
                errors=[ValidationError(
                    id=None, run_id=config.run_id, path_definition_id=None, validation_test_id=None,
                    severity=Severity.ERROR, error_scope="EQUIPMENT_SELECTION", error_type=ErrorType.EQUIPMENT_ERROR,
                    object_type=ObjectType.EQUIPMENT, building_code=self.building_code,
                    notes=f"Toolset {toolset.code} has insufficient equipment ({len(toolset.equipment_list)})"
                )]
            )
        
        equipment_pair = self._select_equipment_pair(toolset)
        if not equipment_pair:
            return PathResult(
                path_found=False,
                errors=[ValidationError(
                    id=None, run_id=config.run_id, path_definition_id=None, validation_test_id=None,
                    severity=Severity.ERROR, error_scope="EQUIPMENT_SELECTION", error_type=ErrorType.EQUIPMENT_ERROR,
                    object_type=ObjectType.EQUIPMENT, building_code=self.building_code,
                    notes=f"Could not select valid equipment pair from toolset {toolset.code}"
                )]
            )
        
        start_poc = self._select_point_of_contact(equipment_pair[0])
        end_poc = self._select_point_of_contact(equipment_pair[1])
        
        if not start_poc or not end_poc or start_poc.node_id == end_poc.node_id:
            return PathResult(
                path_found=False,
                errors=[ValidationError(
                    id=None, run_id=config.run_id, path_definition_id=None, validation_test_id=None,
                    severity=Severity.ERROR, error_scope="POC_SELECTION", error_type=ErrorType.POC_ERROR,
                    object_type=ObjectType.POC, building_code=self.building_code,
                    notes="Could not select valid, distinct points of contact"
                )]
            )
        
        start_node, end_node = start_poc.node_id, end_poc.node_id
        
        node_pair = tuple(sorted((start_node, end_node)))
        if node_pair in self._used_node_pairs:
            # This is part of bias mitigation, not necessarily an error, but a reason for no path this attempt.
            return PathResult(path_found=False, review_flags=[ReviewFlag(
                id=None, run_id=config.run_id, flag_type=FlagType.ANOMALY, severity=Severity.LOW,
                reason="Node pair already attempted due to bias config.", object_type=ObjectType.PATH,
                start_node_id=start_node, end_node_id=end_node, building_code=self.building_code,
                notes=f"Skipped due to bias_config.min_distance_between_nodes or repeated pair: {node_pair}"
            )])

        path_data = self._find_shortest_path(start_node, end_node, common_utility=start_poc.utility)
        if not path_data:
            return PathResult(
                path_found=False,
                review_flags=[ReviewFlag(
                    id=None, run_id=config.run_id, flag_type=FlagType.MANUAL_REVIEW, created_at=datetime.now(),
                    severity=Severity.MEDIUM, reason="No path found between selected PoC nodes",
                    object_type=ObjectType.PATH, start_node_id=start_node, end_node_id=end_node,
                    building_code=self.building_code, utility=start_poc.utility,
                    notes=f"From toolset: {toolset.code}, Equip: {equipment_pair[0].name}({start_poc.code}) -> {equipment_pair[1].name}({end_poc.code})"
                )]
            )
        
        path_definition = self._create_path_definition(
            config, path_data, toolset, equipment_pair, start_poc, end_poc
        )
        
        self._update_bias_tracking(toolset, equipment_pair, node_pair, path_definition)
        
        return PathResult(
            path_found=True,
            path_definition=path_definition,
            coverage_contribution=path_definition.coverage # This is path specific, not total.
        )
    
    def _load_toolsets(self) -> List[Toolset]:
        """Load toolsets for the building (fab) with simplified structure."""
        try:
            sql = """
            SELECT code, fab, phase, name, description, is_active
            FROM tb_toolsets 
            WHERE fab = ? AND is_active = TRUE
            ORDER BY code
            """
            results = self.db.query(sql, [self.building_code])
            toolsets = []
            for row_data in results:
                toolset = Toolset(
                    code=row_data[0], fab=row_data[1], phase=row_data[2], 
                    name=row_data[3], description=row_data[4], is_active=bool(row_data[5])
                )
                toolset.equipment_list = self._load_equipment_for_toolset(toolset.code)
                toolsets.append(toolset)
            return toolsets
        except Exception as e:
            print(f"Error loading toolsets for building {self.building_code}: {e}")
            return []
    
    def _load_equipment_for_toolset(self, toolset_code: str) -> List[Equipment]:
        """Load equipment for a specific toolset."""
        try:
            # tb_equipments: id, toolset, name, guid, node_id, kind, description, is_active
            sql = """
            SELECT id, toolset, name, guid, node_id, kind, description, is_active
            FROM tb_equipments 
            WHERE toolset = ? AND is_active = TRUE
            ORDER BY name
            """
            results = self.db.query(sql, [toolset_code])
            equipment_list = []
            for row_data in results:
                equipment = Equipment(
                    id=row_data[0], toolset_code=row_data[1], name=row_data[2], guid=row_data[3],
                    node_id=row_data[4], kind=row_data[5], description=row_data[6], is_active=bool(row_data[7])
                )
                equipment.pocs = self._load_pocs_for_equipment(equipment.id)
                equipment_list.append(equipment)
            return equipment_list
        except Exception as e:
            print(f"Error loading equipment for toolset {toolset_code}: {e}")
            return []
    
    def _load_pocs_for_equipment(self, equipment_id: int) -> List[EquipmentPoC]:
        """Load PoCs for a specific equipment."""
        try:
            # tb_equipment_pocs: id, equipment_id, code, node_id, utility, flow, is_used, is_active
            sql = """
            SELECT id, equipment_id, code, node_id, utility, flow, is_used, is_active
            FROM tb_equipment_pocs 
            WHERE equipment_id = ? AND is_active = TRUE
            ORDER BY code
            """
            results = self.db.query(sql, [equipment_id])
            pocs = []
            for row_data in results:
                poc = EquipmentPoC(
                    id=row_data[0], equipment_id=row_data[1], code=row_data[2], node_id=row_data[3],
                    utility=row_data[4], flow=row_data[5], is_used=bool(row_data[6]), is_active=bool(row_data[7])
                )
                pocs.append(poc)
            return pocs
        except Exception as e:
            print(f"Error loading PoCs for equipment {equipment_id}: {e}")
            return []
    
    def _select_toolset_with_bias_mitigation(self, target_toolset_code: str = "", target_phase: str = "") -> Optional[Toolset]:
        """Select a toolset considering bias, specific code, and phase."""
        candidate_toolsets = self._toolsets

        if target_toolset_code and target_toolset_code.upper() != "ALL":
            candidate_toolsets = [ts for ts in candidate_toolsets if ts.code == target_toolset_code]
        
        if target_phase: # target_phase is A, B, C, D
            candidate_toolsets = [ts for ts in candidate_toolsets if ts.phase == target_phase]

        if not candidate_toolsets:
            return None

        available_toolsets = [
            ts for ts in candidate_toolsets
            if self._toolset_attempt_counts[ts.code] < self.bias_config.max_attempts_per_toolset
        ]
        
        if not available_toolsets:
            # If specific filters led to no available toolsets due to attempt limits, try resetting for this subset
            for ts_code in self._toolset_attempt_counts.keys():
                if ts_code in [ts.code for ts in candidate_toolsets]:
                    self._toolset_attempt_counts[ts_code] = 0 # Reset for this filtered subset
            available_toolsets = candidate_toolsets # Retry with reset counts for the filtered set

        if not available_toolsets:
            return None # Still no toolsets after targeted reset
        
        # Simple random choice for now, can add weighting later
        return random.choice(available_toolsets)

    def _select_equipment_pair(self, toolset: Toolset) -> Optional[Tuple[Equipment, Equipment]]:
        """Select a pair of equipment from the toolset, mitigating bias."""
        active_equipment = [eq for eq in toolset.equipment_list if eq.is_active and eq.pocs]

        if len(active_equipment) < 2:
            return None

        # Filter by attempt count
        eligible_equipment = [
            eq for eq in active_equipment
            if self._equipment_attempt_counts[eq.guid] < self.bias_config.max_attempts_per_equipment
        ]

        if len(eligible_equipment) < 2:
             # Reset attempt counts for equipment in this toolset if not enough are eligible
            for eq_guid in self._equipment_attempt_counts.keys():
                 if eq_guid in [eq.guid for eq in active_equipment]:
                    self._equipment_attempt_counts[eq_guid] = 0
            eligible_equipment = active_equipment # Retry with all active equipment from this toolset

        if len(eligible_equipment) < 2:
            return None

        eq1 = random.choice(eligible_equipment)
        # Ensure eq2 is different and also eligible
        possible_eq2 = [eq for eq in eligible_equipment if eq.guid != eq1.guid]
        if not possible_eq2:
            return None
        eq2 = random.choice(possible_eq2)
        
        return eq1, eq2

    def _select_point_of_contact(self, equipment: Equipment) -> Optional[EquipmentPoC]:
        """Select an available, active PoC from equipment."""
        available_pocs = [poc for poc in equipment.pocs if poc.is_active]
        if not available_pocs:
            return None
        
        # Prefer PoCs with utilities defined, then by priority (if exists), then random
        pocs_with_utility = [poc for poc in available_pocs if poc.utility]
        if pocs_with_utility:
            return random.choice(pocs_with_utility)
        return random.choice(available_pocs)
    
    def _find_shortest_path(self, start_node_id: int, end_node_id: int, common_utility: Optional[str]) -> Optional[dict]:
        """
        Placeholder: find shortest path between two nodes, optionally considering common utility.
        In a real system, this would query nw_nodes and nw_links using an algorithm like Dijkstra's or A*.
        The query would filter links based on `building_code` (fab) and potentially `utility_codes` matching `common_utility`.
        """
        # Simulate path finding success rate
        if random.random() < 0.75:  # 75% success
            # Simulate path data (nodes are just IDs, links are just IDs)
            num_intermediate_nodes = random.randint(0, 8) # 0 to 8 intermediate nodes
            path_nodes = [start_node_id] + [random.randint(1000, 9999) for _ in range(num_intermediate_nodes)] + [end_node_id]
            path_links = [random.randint(10000, 99999) for _ in range(len(path_nodes) -1)]
            
            # Determine primary utility for the path
            path_utility = common_utility if common_utility else random.choice(['N2', 'CDA', 'PW', 'VAC'])
            
            # Simulate category based on utility or randomly
            category = "UTILITY_LINE" if path_utility else "GENERAL_PATH"
            if path_utility == 'N2': category = "NITROGEN_SUPPLY"
            elif path_utility == 'CDA': category = "AIR_SUPPLY"

            return {
                'nodes': path_nodes,
                'links': path_links,
                'total_length_mm': random.uniform(100.0, 20000.0) * len(path_links),
                'utilities': [path_utility] if path_utility else [],
                'category': category, # Example category
                'scope': 'CONNECTIVITY' # Example scope
            }
        return None

    def _create_path_definition(self, config: RunConfig, path_data: dict, toolset: Toolset, 
                              equipment_pair: Tuple[Equipment, Equipment],
                              start_poc: EquipmentPoC, end_poc: EquipmentPoC) -> PathDefinition:
        """Create a PathDefinition object from discovered path data."""
        
        nodes_str = "_".join(map(str, sorted(path_data['nodes'])))
        links_str = "_".join(map(str, sorted(path_data['links'])))
        path_hash_input = f"{self.building_code}_{nodes_str}_{links_str}"
        path_hash = hashlib.md5(path_hash_input.encode()).hexdigest()
        
        # Simplified coverage calculation: (nodes + links) / (total_nodes_in_fab + total_links_in_fab)
        # Requires knowing total_nodes/links for the fab. For now, a placeholder.
        # This will be refined when CoverageService calculates it.
        # Here, it's more like "potential contribution" if totals were known.
        # The actual coverage update is handled by CoverageService.
        # For PathDefinition itself, coverage could be num_newly_covered_items / total_items.
        # Placeholder coverage value:
        num_items = len(path_data['nodes']) + len(path_data['links'])
        # A more realistic path-specific coverage might be its own size relative to some constant, or 0 here
        # and let CoverageService.calculate_path_coverage_contribution fill it.
        # For PathDefinition storage, it can represent the coverage this path *would* add if it's new.
        # Let's assume coverage here is a placeholder and real calculation is done elsewhere.
        path_coverage_value = 0.01 * num_items # Dummy value

        path_context = {
            'nodes': path_data['nodes'],
            'links': path_data['links'],
            'start_node_id': start_poc.node_id,
            'end_node_id': end_poc.node_id,
            'toolset_code': toolset.code,
            'start_equipment_guid': equipment_pair[0].guid,
            'end_equipment_guid': equipment_pair[1].guid,
            'start_equipment_name': equipment_pair[0].name,
            'end_equipment_name': equipment_pair[1].name,
            'start_poc_code': start_poc.code,
            'end_poc_code': end_poc.code,
            'phase': toolset.phase, # Phase of the toolset used
        }
        
        return PathDefinition(
            id=None, # Will be set upon DB insertion
            path_hash=path_hash,
            source_type=SourceType.RANDOM,
            building_code=self.building_code, # This is fab
            category=path_data.get('category', 'UNKNOWN'),
            scope=path_data.get('scope', 'CONNECTIVITY'),
            node_count=len(path_data['nodes']),
            link_count=len(path_data['links']),
            total_length_mm=path_data.get('total_length_mm', 0.0),
            coverage=path_coverage_value, # Placeholder
            utilities=path_data.get('utilities', []),
            path_context=path_context,
            scenario_id=None,
            scenario_context=None
        )

    def _update_bias_tracking(self, toolset: Toolset, equipment_pair: Tuple[Equipment, Equipment],
                              node_pair: Tuple[int, int], path_def: PathDefinition):
        """Update internal counters for bias mitigation."""
        self._toolset_attempt_counts[toolset.code] += 1
        self._equipment_attempt_counts[equipment_pair[0].guid] += 1
        self._equipment_attempt_counts[equipment_pair[1].guid] += 1
        self._used_node_pairs.add(node_pair)

        for utility in path_def.utilities:
            self._utility_usage_counts[utility] += 1
        if path_def.category:
            self._category_usage_counts[path_def.category] +=1
        # Could also track phase usage from toolset.phase if desired for bias

    def _get_toolset_utilities(self, toolset: Toolset) -> List[str]:
        """Get all unique utility codes associated with a toolset's equipment PoCs."""
        all_utilities: Set[str] = set()
        for equipment in toolset.equipment_list:
            for poc in equipment.pocs:
                if poc.utility:
                    all_utilities.add(poc.utility)
        return list(all_utilities)

    def _get_toolset_category(self, toolset: Toolset) -> Optional[str]:
        """Simplistic way to get a 'category' for a toolset, e.g., from its first equipment."""
        # This could be refined, e.g. most common equipment.kind in the toolset
        if toolset.equipment_list and toolset.equipment_list[0].kind:
            return toolset.equipment_list[0].kind
        return None