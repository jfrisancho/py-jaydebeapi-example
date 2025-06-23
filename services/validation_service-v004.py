"""
Service for validating paths, testing connectivity and utility consistency.
Updated for simplified schema with new field names.
"""

import json
from datetime import datetime
from typing import List, Dict, Set, Optional

from models import PathDefinition, ValidationError
from enums import Severity, ErrorType, ObjectType, ValidationScope, SourceType
from db import Database


class ValidationService:
    """Service for path validation and consistency checking."""
    
    def __init__(self, db: Database):
        self.db = db
        self._validation_tests = self._load_validation_tests() # Load known test types
    
    def validate_path(self, run_id: str, path_definition: PathDefinition) -> List[ValidationError]:
        """Perform comprehensive validation on a path."""
        errors: List[ValidationError] = []
        
        if not path_definition or not path_definition.id:
            # Cannot validate a path without definition or ID (for linking errors)
            print("ValidationService: Path definition or path_definition.id is missing.")
            # Optionally, create a generic error here
            return errors

        path_def_id = path_definition.id # Path ID for error logging

        nodes = path_definition.path_context.get('nodes', [])
        links = path_definition.path_context.get('links', [])
        
        if path_definition.source_type == SourceType.RANDOM:
            errors.extend(self._validate_random_path_specifics(run_id, path_definition, nodes, links))
        elif path_definition.source_type == SourceType.SCENARIO:
            errors.extend(self._validate_scenario_path_specifics(run_id, path_definition, nodes, links))
        
        # Common validation tests
        errors.extend(self._validate_connectivity(run_id, path_definition, nodes, links))
        errors.extend(self._validate_utility_consistency(run_id, path_definition, nodes, links))
        errors.extend(self._validate_flow_direction(run_id, path_definition, nodes, links))
        errors.extend(self._validate_material_consistency(run_id, path_definition, nodes, links))
        errors.extend(self._validate_path_integrity(run_id, path_definition, nodes, links))
        
        for error in errors:
            self._store_validation_error(error) # Store individual errors
        
        return errors
    
    def _validate_random_path_specifics(self, run_id: str, path_def: PathDefinition, 
                             nodes: List[int], links: List[int]) -> List[ValidationError]:
        """Validate random path specific requirements."""
        errors = []
        path_def_id = path_def.id
        
        if not path_def.building_code: # building_code is fab
            errors.append(ValidationError(
                id=None, run_id=run_id, path_definition_id=path_def_id, 
                validation_test_id=self._get_test_id('RANDOM_BUILDING_REQUIRED'),
                severity=Severity.ERROR, error_scope=ValidationScope.QA.value, 
                error_type=ErrorType.RANDOM_BUILDING_REQUIRED, object_type=ObjectType.PATH,
                building_code=path_def.building_code, category=path_def.category,
                error_message="Random paths must have a building code (fab).", notes="Path source is RANDOM."
            ))
        
        equipment_ids = path_def.path_context.get('equipment_ids', []) # Assuming this key exists from RandomService
        if len(equipment_ids) < 2:
            errors.append(ValidationError(
                id=None, run_id=run_id, path_definition_id=path_def_id,
                validation_test_id=self._get_test_id('RANDOM_EQUIPMENT_REQUIRED'),
                severity=Severity.ERROR, error_scope=ValidationScope.EQUIPMENT.value,
                error_type=ErrorType.RANDOM_EQUIPMENT_REQUIRED, object_type=ObjectType.PATH,
                building_code=path_def.building_code, category=path_def.category,
                error_message="Random paths must connect at least 2 equipment pieces.",
                error_data={'equipment_ids_found': equipment_ids}
            ))
        return errors

    def _validate_scenario_path_specifics(self, run_id: str, path_def: PathDefinition,
                               nodes: List[int], links: List[int]) -> List[ValidationError]:
        """Validate scenario path specific requirements."""
        errors = []
        path_def_id = path_def.id

        if not path_def.scenario_id:
            errors.append(ValidationError(
                id=None, run_id=run_id, path_definition_id=path_def_id,
                validation_test_id=self._get_test_id('SCENARIO_ID_REQUIRED'),
                severity=Severity.ERROR, error_scope=ValidationScope.SCENARIO.value,
                error_type=ErrorType.SCENARIO_ID_REQUIRED, object_type=ObjectType.PATH,
                scenario_id=path_def.scenario_id, category=path_def.category,
                error_message="Scenario paths must have a scenario_id."
            ))
        
        if not path_def.scenario_context: # scenario_context is optional but good for QA
            errors.append(ValidationError(
                id=None, run_id=run_id, path_definition_id=path_def_id,
                validation_test_id=self._get_test_id('SCENARIO_CONTEXT_REQUIRED'),
                severity=Severity.WARNING, error_scope=ValidationScope.SCENARIO.value,
                error_type=ErrorType.SCENARIO_CONTEXT_REQUIRED, object_type=ObjectType.PATH,
                scenario_id=path_def.scenario_id, category=path_def.category,
                error_message="Scenario paths should ideally have scenario_context."
            ))
        return errors

    def _validate_connectivity(self, run_id: str, path_def: PathDefinition, 
                             nodes: List[int], links: List[int]) -> List[ValidationError]:
        errors = []
        path_def_id = path_def.id
        building_code = path_def.building_code # This is fab

        for node_id in nodes:
            if not self._node_exists(node_id, building_code):
                errors.append(ValidationError(
                    id=None, run_id=run_id, path_definition_id=path_def_id,
                    validation_test_id=self._get_test_id('CONNECTIVITY_NODE_EXISTS'),
                    severity=Severity.CRITICAL, error_scope=ValidationScope.CONNECTIVITY.value,
                    error_type=ErrorType.MISSING_NODE, object_type=ObjectType.NODE, node_id=node_id,
                    building_code=building_code, category=path_def.category,
                    error_message=f"Node {node_id} in path does not exist in fab {building_code}."
                ))
        
        for link_id in links:
            if not self._link_exists(link_id, building_code):
                errors.append(ValidationError(
                    id=None, run_id=run_id, path_definition_id=path_def_id,
                    validation_test_id=self._get_test_id('CONNECTIVITY_LINK_EXISTS'),
                    severity=Severity.CRITICAL, error_scope=ValidationScope.CONNECTIVITY.value,
                    error_type=ErrorType.MISSING_LINK, object_type=ObjectType.LINK, link_id=link_id,
                    building_code=building_code, category=path_def.category,
                    error_message=f"Link {link_id} in path does not exist in fab {building_code}."
                ))

        for i in range(len(nodes) - 1):
            node1, node2 = nodes[i], nodes[i+1]
            # Assumes links provided are in order and correspond to node pairs.
            # A more robust check would verify the link connects node1 and node2.
            # For now, check if nodes themselves are connected by *any* link if links list is unreliable.
            if not self._nodes_connected(node1, node2, building_code):
                 errors.append(ValidationError(
                    id=None, run_id=run_id, path_definition_id=path_def_id,
                    validation_test_id=self._get_test_id('CONNECTIVITY_BREAK'),
                    severity=Severity.CRITICAL, error_scope=ValidationScope.CONNECTIVITY.value,
                    error_type=ErrorType.CONNECTIVITY_BREAK, object_type=ObjectType.NODE,
                    node_id=node1, # Could also use link_id if available for this segment
                    building_code=building_code, category=path_def.category,
                    error_message=f"Connectivity break: No direct link found between node {node1} and {node2}.",
                    error_data={'from_node': node1, 'to_node': node2}
                ))
        return errors

    def _validate_utility_consistency(self, run_id: str, path_def: PathDefinition,
                                    nodes: List[int], links: List[int]) -> List[ValidationError]:
        errors = []
        path_def_id = path_def.id
        building_code = path_def.building_code # This is fab
        expected_path_utilities = set(path_def.utilities)

        node_db_utilities = self._get_node_utilities(nodes, building_code)
        for node_id, actual_utils_list in node_db_utilities.items():
            actual_utils_set = set(actual_utils_list)
            if expected_path_utilities and not actual_utils_set.intersection(expected_path_utilities):
                errors.append(ValidationError(
                    id=None, run_id=run_id, path_definition_id=path_def_id,
                    validation_test_id=self._get_test_id('UTILITY_NODE_MISMATCH'),
                    severity=Severity.HIGH, error_scope=ValidationScope.UTILITY.value,
                    error_type=ErrorType.UTILITY_MISMATCH, object_type=ObjectType.NODE, node_id=node_id,
                    building_code=building_code, category=path_def.category, utility=",".join(actual_utils_list),
                    error_message=f"Node {node_id} utilities ({actual_utils_list}) don't match expected path utilities ({list(expected_path_utilities)}).",
                    error_data={'expected': list(expected_path_utilities), 'actual': actual_utils_list}
                ))

        link_db_utilities = self._get_link_utilities(links, building_code)
        for link_id, actual_utils_list in link_db_utilities.items():
            actual_utils_set = set(actual_utils_list)
            if expected_path_utilities and not actual_utils_set.intersection(expected_path_utilities):
                errors.append(ValidationError(
                    id=None, run_id=run_id, path_definition_id=path_def_id,
                    validation_test_id=self._get_test_id('UTILITY_LINK_MISMATCH'),
                    severity=Severity.HIGH, error_scope=ValidationScope.UTILITY.value,
                    error_type=ErrorType.UTILITY_MISMATCH, object_type=ObjectType.LINK, link_id=link_id,
                    building_code=building_code, category=path_def.category, utility=",".join(actual_utils_list),
                    error_message=f"Link {link_id} utilities ({actual_utils_list}) don't match expected path utilities ({list(expected_path_utilities)}).",
                    error_data={'expected': list(expected_path_utilities), 'actual': actual_utils_list}
                ))
        return errors
    
    def _validate_flow_direction(self, run_id: str, path_def: PathDefinition,
                               nodes: List[int], links: List[int]) -> List[ValidationError]:
        # This is a complex validation. A simplified version:
        # Check if PoC flows are compatible with path direction if defined.
        # Check if link flows are consistent.
        # For now, this is a placeholder for more detailed logic.
        errors = []
        # Example: if path_context has start/end PoC flow info, validate against link flows.
        # path_def_id = path_def.id
        # building_code = path_def.building_code
        # link_flows = self._get_link_flows(links, building_code) # from nw_links
        # ... logic to check flow consistency ...
        return errors
        
    def _validate_material_consistency(self, run_id: str, path_def: PathDefinition,
                                     nodes: List[int], links: List[int]) -> List[ValidationError]:
        errors = []
        path_def_id = path_def.id
        building_code = path_def.building_code # This is fab
        
        # Get materials from nw_nodes and nw_links based on path_def context
        node_materials = self._get_node_materials(nodes, building_code)
        link_materials = self._get_link_materials(links, building_code)
        
        all_materials_in_path: Set[str] = set()
        for mat_dict in [node_materials, link_materials]:
            for item_id, material_val in mat_dict.items():
                if material_val: # Only consider non-empty material values
                    all_materials_in_path.add(material_val)
        
        if len(all_materials_in_path) > 1: # More than one distinct material
            errors.append(ValidationError(
                id=None, run_id=run_id, path_definition_id=path_def_id,
                validation_test_id=self._get_test_id('MATERIAL_INCONSISTENCY'),
                severity=Severity.WARNING, error_scope=ValidationScope.MATERIAL.value,
                error_type=ErrorType.INVALID_MATERIAL, object_type=ObjectType.PATH, # Error on the path
                building_code=building_code, category=path_def.category,
                material=",".join(sorted(list(all_materials_in_path))),
                error_message=f"Path contains multiple materials: {sorted(list(all_materials_in_path))}.",
                error_data={'materials_found': sorted(list(all_materials_in_path))}
            ))
        return errors

    def _validate_path_integrity(self, run_id: str, path_def: PathDefinition,
                               nodes: List[int], links: List[int]) -> List[ValidationError]:
        errors = []
        path_def_id = path_def.id
        building_code = path_def.building_code # This is fab

        if len(nodes) < 2 and len(links) < 1 : # A path needs at least 2 nodes and 1 link
            errors.append(ValidationError(
                id=None, run_id=run_id, path_definition_id=path_def_id,
                validation_test_id=self._get_test_id('PATH_TOO_SHORT'),
                severity=Severity.ERROR, error_scope=ValidationScope.QA.value,
                error_type=ErrorType.PATH_LENGTH, object_type=ObjectType.PATH,
                building_code=building_code, category=path_def.category,
                error_message=f"Path is too short: Nodes={len(nodes)}, Links={len(links)}. Requires at least 2 nodes and 1 link.",
                error_data={'node_count': len(nodes), 'link_count': len(links)}
            ))
        
        # Example max reasonable length (could be a config)
        max_nodes_config = 1000 
        if len(nodes) > max_nodes_config:
            errors.append(ValidationError(
                id=None, run_id=run_id, path_definition_id=path_def_id,
                validation_test_id=self._get_test_id('PATH_TOO_LONG'),
                severity=Severity.WARNING, error_scope=ValidationScope.QA.value,
                error_type=ErrorType.PATH_LENGTH, object_type=ObjectType.PATH,
                building_code=building_code, category=path_def.category,
                error_message=f"Path is very long: {len(nodes)} nodes. Max recommended: {max_nodes_config}.",
                error_data={'node_count': len(nodes), 'max_recommended': max_nodes_config}
            ))

        if len(set(nodes)) != len(nodes): # Check for duplicate nodes (simple loops)
            duplicate_nodes = list(set(n_id for n_id in nodes if nodes.count(n_id) > 1))
            errors.append(ValidationError(
                id=None, run_id=run_id, path_definition_id=path_def_id,
                validation_test_id=self._get_test_id('PATH_HAS_LOOPS'),
                severity=Severity.MEDIUM, error_scope=ValidationScope.QA.value,
                error_type=ErrorType.PATH_LOOPS, object_type=ObjectType.PATH,
                building_code=building_code, category=path_def.category,
                error_message=f"Path contains loops/duplicate nodes: {duplicate_nodes}.",
                error_data={'duplicate_nodes': duplicate_nodes}
            ))
        return errors
    
    def _store_validation_error(self, error: ValidationError):
        """Store a validation error in the database. (tb_validation_errors)"""
        sql = """
        INSERT INTO tb_validation_errors (
            run_id, path_definition_id, validation_test_id, severity,
            error_scope, error_type, object_type, node_id, link_id, scenario_id,
            building_code, category, utility, material, flow, item_name, 
            error_message, error_data, created_at, notes
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        try:
            error_data_json = json.dumps(error.error_data) if error.error_data else None
            params = [
                error.run_id, error.path_definition_id, error.validation_test_id,
                error.severity.value, error.error_scope, error.error_type.value,
                error.object_type.value, error.node_id, error.link_id, error.scenario_id,
                error.building_code, error.category, error.utility, error.material, error.flow,
                error.item_name, error.error_message, error_data_json,
                error.created_at, error.notes
            ]
            self.db.update(sql, params)
        except Exception as e:
            print(f"Error storing validation error for run {error.run_id}, path_def {error.path_definition_id}: {e}")

    def _load_validation_tests(self) -> Dict[str, dict]:
        """Load validation test definitions from tb_validation_tests."""
        sql = """
        SELECT id, code, name, description, scope, severity, test_type,
               applies_to_random, applies_to_scenario, building_specific, test_config, is_active
        FROM tb_validation_tests
        WHERE is_active = TRUE
        """
        tests = {}
        try:
            results = self.db.query(sql)
            for row in results:
                (test_id, code, name, description, scope, severity, test_type, 
                 applies_random, applies_scenario, bldg_specific, test_cfg, is_active) = row
                tests[code] = {
                    'id': test_id, 'name': name, 'description': description,
                    'scope': scope, 'severity': severity, 'test_type': test_type,
                    'applies_to_random': bool(applies_random), 
                    'applies_to_scenario': bool(applies_scenario),
                    'building_specific': bool(bldg_specific),
                    'test_config': json.loads(test_cfg) if test_cfg else None
                }
        except Exception as e:
            print(f"Error loading validation tests: {e}")
        return tests

    def _get_test_id(self, test_code: str) -> Optional[int]:
        """Get validation test ID by its unique code."""
        test_info = self._validation_tests.get(test_code)
        return test_info['id'] if test_info else None

    # Helper DB methods using nw_nodes and nw_links for path element properties
    # These assume nw_nodes and nw_links have building_code, utility_codes, flow_direction, material fields.
    
    def _node_exists(self, node_id: int, building_code: Optional[str]) -> bool:
        """Check if a node exists in nw_nodes."""
        # building_code is fab. If None or "SCENARIO", query without it.
        if building_code and building_code != "SCENARIO":
            sql = "SELECT 1 FROM nw_nodes WHERE id = ? AND building_code = ? LIMIT 1"
            params = [node_id, building_code]
        else:
            sql = "SELECT 1 FROM nw_nodes WHERE id = ? LIMIT 1"
            params = [node_id]
        try:
            return bool(self.db.query(sql, params))
        except Exception: return False

    def _link_exists(self, link_id: int, building_code: Optional[str]) -> bool:
        """Check if a link exists in nw_links."""
        if building_code and building_code != "SCENARIO":
            sql = "SELECT 1 FROM nw_links WHERE id = ? AND building_code = ? LIMIT 1"
            params = [link_id, building_code]
        else:
            sql = "SELECT 1 FROM nw_links WHERE id = ? LIMIT 1"
            params = [link_id]
        try:
            return bool(self.db.query(sql, params))
        except Exception: return False

    def _nodes_connected(self, node1_id: int, node2_id: int, building_code: Optional[str]) -> bool:
        """Check if two nodes are directly connected by a link in nw_links."""
        if building_code and building_code != "SCENARIO":
            sql = """SELECT 1 FROM nw_links WHERE building_code = ? AND 
                     ((from_node_id = ? AND to_node_id = ?) OR (from_node_id = ? AND to_node_id = ?)) LIMIT 1"""
            params = [building_code, node1_id, node2_id, node2_id, node1_id]
        else:
            sql = """SELECT 1 FROM nw_links WHERE 
                     ((from_node_id = ? AND to_node_id = ?) OR (from_node_id = ? AND to_node_id = ?)) LIMIT 1"""
            params = [node1_id, node2_id, node2_id, node1_id]
        try:
            return bool(self.db.query(sql, params))
        except Exception: return False

    def _get_element_properties(self, element_ids: List[int], building_code: Optional[str], table_name: str, property_column: str) -> Dict[int, Any]:
        """Generic helper to get properties for a list of elements (nodes or links)."""
        if not element_ids: return {}
        placeholders = ','.join(['?'] * len(element_ids))
        
        if building_code and building_code != "SCENARIO":
            sql = f"SELECT id, {property_column} FROM {table_name} WHERE id IN ({placeholders}) AND building_code = ?"
            params = element_ids + [building_code]
        else:
            sql = f"SELECT id, {property_column} FROM {table_name} WHERE id IN ({placeholders})"
            params = element_ids
        
        properties: Dict[int, Any] = {}
        try:
            results = self.db.query(sql, params)
            for row_id, prop_val_str in results:
                if property_column == 'utility_codes': # Assuming comma-separated string
                     properties[row_id] = [u.strip() for u in prop_val_str.split(',') if u.strip()] if prop_val_str else []
                else: # Direct value
                    properties[row_id] = prop_val_str
        except Exception as e:
            print(f"Error getting {property_column} from {table_name} for fab {building_code}: {e}")
        return properties

    def _get_node_utilities(self, node_ids: List[int], building_code: Optional[str]) -> Dict[int, List[str]]:
        return self._get_element_properties(node_ids, building_code, "nw_nodes", "utility_codes")

    def _get_link_utilities(self, link_ids: List[int], building_code: Optional[str]) -> Dict[int, List[str]]:
        return self._get_element_properties(link_ids, building_code, "nw_links", "utility_codes")

    def _get_link_flows(self, link_ids: List[int], building_code: Optional[str]) -> Dict[int, str]:
        return self._get_element_properties(link_ids, building_code, "nw_links", "flow_direction")

    def _get_node_materials(self, node_ids: List[int], building_code: Optional[str]) -> Dict[int, str]:
        return self._get_element_properties(node_ids, building_code, "nw_nodes", "material")
    
    def _get_link_materials(self, link_ids: List[int], building_code: Optional[str]) -> Dict[int, str]:
        return self._get_element_properties(link_ids, building_code, "nw_links", "material")

    def _flows_compatible(self, flow1: Optional[str], flow2: Optional[str]) -> bool:
        """Basic check for flow compatibility (can be expanded)."""
        if not flow1 or not flow2: return True # Treat unknown as compatible for now
        if flow1 == 'BIDIRECTIONAL' or flow2 == 'BIDIRECTIONAL': return True
        return flow1 != flow2 # e.g. IN -> OUT is okay, IN -> IN is not. Needs context of connection.
                               # This simplified check might be too basic.