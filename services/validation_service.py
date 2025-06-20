"""
Service for validating paths, testing connectivity and utility consistency.
"""

import json
from datetime import datetime
from typing import List, Dict, Set, Optional

from models import PathDefinition, ValidationError
from enums import Severity, ErrorType, ObjectType, ValidationScope
from db import Database


class ValidationService:
    """Service for path validation and consistency checking."""
    
    def __init__(self, db: Database):
        self.db = db
        self._validation_tests = self._load_validation_tests()
    
    def validate_path(self, run_id: str, path_definition: PathDefinition) -> List[ValidationError]:
        """Perform comprehensive validation on a path."""
        errors = []
        
        # Extract path data
        nodes = path_definition.path_context.get('nodes', [])
        links = path_definition.path_context.get('links', [])
        
        # Run all validation tests
        errors.extend(self._validate_connectivity(run_id, path_definition, nodes, links))
        errors.extend(self._validate_utility_consistency(run_id, path_definition, nodes, links))
        errors.extend(self._validate_flow_direction(run_id, path_definition, nodes, links))
        errors.extend(self._validate_material_consistency(run_id, path_definition, nodes, links))
        errors.extend(self._validate_path_integrity(run_id, path_definition, nodes, links))
        
        # Store validation errors in database
        for error in errors:
            self._store_validation_error(error)
        
        return errors
    
    def _validate_connectivity(self, run_id: str, path_def: PathDefinition, 
                             nodes: List[int], links: List[int]) -> List[ValidationError]:
        """Validate that all nodes and links in the path are properly connected."""
        errors = []
        
        # Check that all nodes exist
        for node_id in nodes:
            if not self._node_exists(node_id, path_def.fab):
                errors.append(ValidationError(
                    id=None,
                    run_id=run_id,
                    path_definition_id=path_def.id,
                    validation_test_id=self._get_test_id('CONNECTIVITY_NODE_EXISTS'),
                    severity=Severity.ERROR,
                    error_scope=ValidationScope.CONNECTIVITY.value,
                    error_type=ErrorType.MISSING_NODE,
                    object_type=ObjectType.NODE,
                    node_id=node_id,
                    category=path_def.category,
                    notes=f"Node {node_id} does not exist in fab {path_def.fab}"
                ))
        
        # Check that all links exist
        for link_id in links:
            if not self._link_exists(link_id, path_def.fab):
                errors.append(ValidationError(
                    id=None,
                    run_id=run_id,
                    path_definition_id=path_def.id,
                    validation_test_id=self._get_test_id('CONNECTIVITY_LINK_EXISTS'),
                    severity=Severity.ERROR,
                    error_scope=ValidationScope.CONNECTIVITY.value,
                    error_type=ErrorType.MISSING_LINK,
                    object_type=ObjectType.LINK,
                    link_id=link_id,
                    category=path_def.category,
                    notes=f"Link {link_id} does not exist in fab {path_def.fab}"
                ))
        
        # Check connectivity between consecutive nodes
        for i in range(len(nodes) - 1):
            node1, node2 = nodes[i], nodes[i + 1]
            if not self._nodes_connected(node1, node2, path_def.fab):
                errors.append(ValidationError(
                    id=None,
                    run_id=run_id,
                    path_definition_id=path_def.id,
                    validation_test_id=self._get_test_id('CONNECTIVITY_BREAK'),
                    severity=Severity.CRITICAL,
                    error_scope=ValidationScope.CONNECTIVITY.value,
                    error_type=ErrorType.CONNECTIVITY_BREAK,
                    object_type=ObjectType.NODE,
                    node_id=node1,
                    category=path_def.category,
                    notes=f"No connection between nodes {node1} and {node2}"
                ))
        
        return errors
    
    def _validate_utility_consistency(self, run_id: str, path_def: PathDefinition,
                                    nodes: List[int], links: List[int]) -> List[ValidationError]:
        """Validate that utilities are consistent throughout the path."""
        errors = []
        
        # Get utilities for all nodes and links in the path
        node_utilities = self._get_node_utilities(nodes, path_def.fab)
        link_utilities = self._get_link_utilities(links, path_def.fab)
        
        # Expected utilities from path definition
        expected_utilities = set(path_def.utilities)
        
        # Check node utility consistency
        for node_id, utilities in node_utilities.items():
            node_utility_set = set(utilities)
            if not node_utility_set.intersection(expected_utilities):
                errors.append(ValidationError(
                    id=None,
                    run_id=run_id,
                    path_definition_id=path_def.id,
                    validation_test_id=self._get_test_id('UTILITY_MISMATCH'),
                    severity=Severity.HIGH,
                    error_scope=ValidationScope.CONNECTIVITY.value,
                    error_type=ErrorType.UTILITY_MISMATCH,
                    object_type=ObjectType.NODE,
                    node_id=node_id,
                    category=path_def.category,
                    utility=','.join(utilities),
                    notes=f"Node utilities {utilities} don't match expected {list(expected_utilities)}"
                ))
        
        # Check link utility consistency  
        for link_id, utilities in link_utilities.items():
            link_utility_set = set(utilities)
            if not link_utility_set.intersection(expected_utilities):
                errors.append(ValidationError(
                    id=None,
                    run_id=run_id,
                    path_definition_id=path_def.id,
                    validation_test_id=self._get_test_id('UTILITY_MISMATCH'),
                    severity=Severity.HIGH,
                    error_scope=ValidationScope.CONNECTIVITY.value,
                    error_type=ErrorType.UTILITY_MISMATCH,
                    object_type=ObjectType.LINK,
                    link_id=link_id,
                    category=path_def.category,
                    utility=','.join(utilities),
                    notes=f"Link utilities {utilities} don't match expected {list(expected_utilities)}"
                ))
        
        return errors
    
    def _validate_flow_direction(self, run_id: str, path_def: PathDefinition,
                               nodes: List[int], links: List[int]) -> List[ValidationError]:
        """Validate flow direction consistency in the path."""
        errors = []
        
        # Get flow directions for links
        link_flows = self._get_link_flows(links, path_def.fab)
        
        # Check for flow consistency
        for i in range(len(links) - 1):
            current_link = links[i]
            next_link = links[i + 1]
            
            current_flow = link_flows.get(current_link)
            next_flow = link_flows.get(next_link)
            
            if current_flow and next_flow:
                # Check if flows are compatible (simplified logic)
                if not self._flows_compatible(current_flow, next_flow):
                    errors.append(ValidationError(
                        id=None,
                        run_id=run_id,
                        path_definition_id=path_def.id,
                        validation_test_id=self._get_test_id('FLOW_DIRECTION'),
                        severity=Severity.MEDIUM,
                        error_scope=ValidationScope.FLOW.value,
                        error_type=ErrorType.WRONG_DIRECTION,
                        object_type=ObjectType.LINK,
                        link_id=current_link,
                        category=path_def.category,
                        flow=current_flow,
                        notes=f"Flow mismatch between links {current_link} ({current_flow}) and {next_link} ({next_flow})"
                    ))
        
        return errors
    
    def _validate_material_consistency(self, run_id: str, path_def: PathDefinition,
                                     nodes: List[int], links: List[int]) -> List[ValidationError]:
        """Validate material consistency throughout the path."""
        errors = []
        
        # Get materials for nodes and links
        node_materials = self._get_node_materials(nodes, path_def.fab)
        link_materials = self._get_link_materials(links, path_def.fab)
        
        # Check for material compatibility
        all_materials = set()
        all_materials.update(node_materials.values())
        all_materials.update(link_materials.values())
        
        # Remove None values
        all_materials.discard(None)
        all_materials.discard('')
        
        # Check if materials are compatible (simplified - assumes same material throughout)
        if len(all_materials) > 1:
            # Multiple materials found - might be an issue
            material_list = list(all_materials)
            errors.append(ValidationError(
                id=None,
                run_id=run_id,
                path_definition_id=path_def.id,
                validation_test_id=self._get_test_id('MATERIAL_CONSISTENCY'),
                severity=Severity.WARNING,
                error_scope=ValidationScope.MATERIAL.value,
                error_type=ErrorType.INVALID_MATERIAL,
                object_type=ObjectType.NODE,
                category=path_def.category,
                material=','.join(material_list),
                notes=f"Multiple materials found in path: {material_list}"
            ))
        
        return errors
    
    def _validate_path_integrity(self, run_id: str, path_def: PathDefinition,
                               nodes: List[int], links: List[int]) -> List[ValidationError]:
        """Validate overall path integrity and quality."""
        errors = []
        
        # Check minimum path length
        if len(nodes) < 2:
            errors.append(ValidationError(
                id=None,
                run_id=run_id,
                path_definition_id=path_def.id,
                validation_test_id=self._get_test_id('PATH_LENGTH'),
                severity=Severity.ERROR,
                error_scope=ValidationScope.QA.value,
                error_type=ErrorType.PATH_NOT_FOUND,
                object_type=ObjectType.NODE,
                category=path_def.category,
                notes=f"Path too short: only {len(nodes)} nodes"
            ))
        
        # Check for reasonable path length (not too long)
        max_reasonable_length = 1000  # Configurable
        if len(nodes) > max_reasonable_length:
            errors.append(ValidationError(
                id=None,
                run_id=run_id,
                path_definition_id=path_def.id,
                validation_test_id=self._get_test_id('PATH_LENGTH'),
                severity=Severity.WARNING,
                error_scope=ValidationScope.QA.value,
                error_type=ErrorType.PATH_NOT_FOUND,
                object_type=ObjectType.NODE,
                category=path_def.category,
                notes=f"Path very long: {len(nodes)} nodes (max recommended: {max_reasonable_length})"
            ))
        
        # Check for duplicate nodes (loops)
        unique_nodes = set(nodes)
        if len(unique_nodes) != len(nodes):
            duplicates = [node for node in nodes if nodes.count(node) > 1]
            errors.append(ValidationError(
                id=None,
                run_id=run_id,
                path_definition_id=path_def.id,
                validation_test_id=self._get_test_id('PATH_LOOPS'),
                severity=Severity.MEDIUM,
                error_scope=ValidationScope.QA.value,
                error_type=ErrorType.CONNECTIVITY_BREAK,
                object_type=ObjectType.NODE,
                category=path_def.category,
                notes=f"Path contains loops - duplicate nodes: {list(set(duplicates))}"
            ))
        
        return errors
    
    def _store_validation_error(self, error: ValidationError):
        """Store a validation error in the database."""
        sql = """
        INSERT INTO tb_validation_errors (
            run_id, path_definition_id, validation_test_id, severity,
            error_scope, error_type, object_type, node_id, link_id,
            category, utility, material, flow, item_name, created_at, notes
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        try:
            self.db.update(sql, [
                error.run_id,
                error.path_definition_id,
                error.validation_test_id,
                error.severity.value,
                error.error_scope,
                error.error_type.value,
                error.object_type.value,
                error.node_id,
                error.link_id,
                error.category,
                error.utility,
                error.material,
                error.flow,
                error.item_name,
                error.created_at,
                error.notes
            ])
        except Exception as e:
            print(f"Error storing validation error: {e}")
    
    def _load_validation_tests(self) -> Dict[str, dict]:
        """Load validation test definitions from database."""
        sql = """
        SELECT id, code, name, description, scope, severity, reason, is_active
        FROM tb_validation_tests
        WHERE is_active = 1
        """
        
        tests = {}
        try:
            results = self.db.query(sql)
            for row in results:
                test_id, code, name, description, scope, severity, reason, is_active = row
                tests[code] = {
                    'id': test_id,
                    'name': name,
                    'description': description,
                    'scope': scope,
                    'severity': severity,
                    'reason': reason
                }
        except Exception as e:
            print(f"Error loading validation tests: {e}")
        
        return tests
    
    def _get_test_id(self, test_code: str) -> Optional[int]:
        """Get validation test ID by code."""
        test_info = self._validation_tests.get(test_code)
        return test_info['id'] if test_info else None
    
    # Helper methods for database queries
    
    def _node_exists(self, node_id: int, fab: str) -> bool:
        """Check if a node exists in the database."""
        sql = "SELECT 1 FROM nodes WHERE id = ? AND fab = ? LIMIT 1"
        try:
            result = self.db.query(sql, [node_id, fab])
            return len(result) > 0
        except Exception:
            return False
    
    def _link_exists(self, link_id: int, fab: str) -> bool:
        """Check if a link exists in the database."""
        sql = "SELECT 1 FROM links WHERE id = ? AND fab = ? LIMIT 1"
        try:
            result = self.db.query(sql, [link_id, fab])
            return len(result) > 0
        except Exception:
            return False
    
    def _nodes_connected(self, node1: int, node2: int, fab: str) -> bool:
        """Check if two nodes are directly connected."""
        sql = """
        SELECT 1 FROM links 
        WHERE fab = ? AND (
            (from_node_id = ? AND to_node_id = ?) OR 
            (from_node_id = ? AND to_node_id = ?)
        ) LIMIT 1
        """
        try:
            result = self.db.query(sql, [fab, node1, node2, node2, node1])
            return len(result) > 0
        except Exception:
            return False
    
    def _get_node_utilities(self, node_ids: List[int], fab: str) -> Dict[int, List[str]]:
        """Get utilities for a list of nodes."""
        if not node_ids:
            return {}
        
        placeholders = ','.join(['?'] * len(node_ids))
        sql = f"""
        SELECT id, utility_codes 
        FROM nodes 
        WHERE id IN ({placeholders}) AND fab = ?
        """
        
        utilities = {}
        try:
            params = node_ids + [fab]
            results = self.db.query(sql, params)
            for row in results:
                node_id, utility_str = row
                utility_list = utility_str.split(',') if utility_str else []
                utilities[node_id] = [u.strip() for u in utility_list if u.strip()]
        except Exception as e:
            print(f"Error getting node utilities: {e}")
        
        return utilities
    
    def _get_link_utilities(self, link_ids: List[int], fab: str) -> Dict[int, List[str]]:
        """Get utilities for a list of links."""
        if not link_ids:
            return {}
        
        placeholders = ','.join(['?'] * len(link_ids))
        sql = f"""
        SELECT id, utility_codes 
        FROM links 
        WHERE id IN ({placeholders}) AND fab = ?
        """
        
        utilities = {}
        try:
            params = link_ids + [fab]
            results = self.db.query(sql, params)
            for row in results:
                link_id, utility_str = row
                utility_list = utility_str.split(',') if utility_str else []
                utilities[link_id] = [u.strip() for u in utility_list if u.strip()]
        except Exception as e:
            print(f"Error getting link utilities: {e}")
        
        return utilities
    
    def _get_link_flows(self, link_ids: List[int], fab: str) -> Dict[int, str]:
        """Get flow directions for a list of links."""
        if not link_ids:
            return {}
        
        placeholders = ','.join(['?'] * len(link_ids))
        sql = f"""
        SELECT id, flow_direction 
        FROM links 
        WHERE id IN ({placeholders}) AND fab = ?
        """
        
        flows = {}
        try:
            params = link_ids + [fab]
            results = self.db.query(sql, params)
            for row in results:
                link_id, flow_direction = row
                flows[link_id] = flow_direction
        except Exception as e:
            print(f"Error getting link flows: {e}")
        
        return flows
    
    def _get_node_materials(self, node_ids: List[int], fab: str) -> Dict[int, str]:
        """Get materials for a list of nodes."""
        if not node_ids:
            return {}
        
        placeholders = ','.join(['?'] * len(node_ids))
        sql = f"""
        SELECT id, material 
        FROM nodes 
        WHERE id IN ({placeholders}) AND fab = ?
        """
        
        materials = {}
        try:
            params = node_ids + [fab]
            results = self.db.query(sql, params)
            for row in results:
                node_id, material = row
                materials[node_id] = material
        except Exception as e:
            print(f"Error getting node materials: {e}")
        
        return materials
    
    def _get_link_materials(self, link_ids: List[int], fab: str) -> Dict[int, str]:
        """Get materials for a list of links."""
        if not link_ids:
            return {}
        
        placeholders = ','.join(['?'] * len(link_ids))
        sql = f"""
        SELECT id, material 
        FROM links 
        WHERE id IN ({placeholders}) AND fab = ?
        """
        
        materials = {}
        try:
            params = link_ids + [fab]
            results = self.db.query(sql, params)
            for row in results:
                link_id, material = row
                materials[link_id] = material
        except Exception as e:
            print(f"Error getting link materials: {e}")
        
        return materials
    
    def _flows_compatible(self, flow1: str, flow2: str) -> bool:
        """Check if two flow directions are compatible."""
        # Simplified logic - in reality this would be more complex
        if not flow1 or not flow2:
            return True  # Unknown flows are assumed compatible
        
        # Define compatible flow combinations
        compatible_flows = {
            ('IN', 'OUT'),
            ('OUT', 'IN'),
            ('BIDIRECTIONAL', 'IN'),
            ('BIDIRECTIONAL', 'OUT'),
            ('IN', 'BIDIRECTIONAL'),
            ('OUT', 'BIDIRECTIONAL'),
            ('BIDIRECTIONAL', 'BIDIRECTIONAL')
        }
        
        return (flow1, flow2) in compatible_flows or flow1 == flow2