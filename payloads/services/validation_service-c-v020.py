"""
Comprehensive path validation service for ensuring path integrity and consistency.
Validates connectivity, utility consistency, and other path properties.
"""

import json
import logging
from typing import Dict, Any, List, Optional, Set, Tuple
from datetime import datetime
from enum import Enum


class ValidationSeverity(Enum):
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


class ValidationScope(Enum):
    CONNECTIVITY = "CONNECTIVITY"
    FLOW = "FLOW"
    MATERIAL = "MATERIAL"
    QA = "QA"
    SCENARIO = "SCENARIO"


class ValidationTestType(Enum):
    STRUCTURAL = "STRUCTURAL"
    LOGICAL = "LOGICAL"
    PERFORMANCE = "PERFORMANCE"
    COMPLIANCE = "COMPLIANCE"


class ValidationService:
    """Service for comprehensive path validation."""
    
    def __init__(self, db_connection):
        self.db = db_connection
        self.logger = logging.getLogger(__name__)
        
        # Cache for validation rules and test definitions
        self._validation_tests_cache = None
        self._utility_compatibility_cache = None
        
        # Initialize validation tests
        self._initialize_validation_tests()
    
    def validate_path_definition(self, run_id: str, path_definition_id: int, 
                                path_context: Dict) -> Dict[str, Any]:
        """
        Validate a complete path definition.
        Returns validation results with errors and warnings.
        """
        validation_result = {
            'path_definition_id': path_definition_id,
            'validation_timestamp': datetime.now().isoformat(),
            'errors': [],
            'warnings': [],
            'passed_tests': [],
            'failed_tests': [],
            'overall_status': 'UNKNOWN'
        }
        
        try:
            # Parse path context
            if isinstance(path_context, str):
                path_context = json.loads(path_context)
            
            # Extract path information
            path_nodes = path_context.get('path_sequence', [])
            start_poc = path_context.get('start_poc', {})
            end_poc = path_context.get('end_poc', {})
            
            if not path_nodes:
                self._add_validation_error(
                    validation_result, run_id, path_definition_id,
                    "STRUCTURAL", "CRITICAL", "PATH_EMPTY",
                    "Path contains no nodes", start_poc
                )
                validation_result['overall_status'] = 'FAILED'
                return validation_result
            
            # Run all validation tests
            self._validate_connectivity(validation_result, run_id, path_definition_id, 
                                      path_nodes, start_poc, end_poc)
            
            self._validate_utility_consistency(validation_result, run_id, path_definition_id,
                                             path_nodes, start_poc, end_poc)
            
            self._validate_poc_properties(validation_result, run_id, path_definition_id,
                                        start_poc, end_poc)
            
            self._validate_path_continuity(validation_result, run_id, path_definition_id,
                                         path_nodes)
            
            self._validate_flow_direction(validation_result, run_id, path_definition_id,
                                        path_nodes, start_poc, end_poc)
            
            # Determine overall status
            if validation_result['errors']:
                critical_errors = [e for e in validation_result['errors'] 
                                 if e.get('severity') == 'CRITICAL']
                high_errors = [e for e in validation_result['errors'] 
                             if e.get('severity') == 'HIGH']
                
                if critical_errors:
                    validation_result['overall_status'] = 'CRITICAL_FAILURE'
                elif high_errors:
                    validation_result['overall_status'] = 'FAILED'
                else:
                    validation_result['overall_status'] = 'WARNING'
            else:
                validation_result['overall_status'] = 'PASSED'
            
            # Store validation results
            self._store_validation_results(run_id, path_definition_id, validation_result)
            
        except Exception as e:
            self.logger.error(f"Error validating path {path_definition_id}: {str(e)}")
            self._add_validation_error(
                validation_result, run_id, path_definition_id,
                "STRUCTURAL", "CRITICAL", "VALIDATION_ERROR",
                f"Validation process failed: {str(e)}", {}
            )
            validation_result['overall_status'] = 'ERROR'
        
        return validation_result
    
    def _validate_connectivity(self, validation_result: Dict, run_id: str, 
                              path_definition_id: int, path_nodes: List[int],
                              start_poc: Dict, end_poc: Dict) -> None:
        """Validate basic connectivity requirements."""
        test_name = "CONNECTIVITY_BASIC"
        
        try:
            # Test 1: Path has minimum length
            if len(path_nodes) < 2:
                self._add_validation_error(
                    validation_result, run_id, path_definition_id,
                    "STRUCTURAL", "HIGH", "INSUFFICIENT_PATH_LENGTH",
                    "Path must have at least 2 nodes", start_poc
                )
                validation_result['failed_tests'].append(test_name)
                return
            
            # Test 2: Start and end nodes match PoCs
            if path_nodes[0] != start_poc.get('node_id'):
                self._add_validation_error(
                    validation_result, run_id, path_definition_id,
                    "STRUCTURAL", "HIGH", "START_NODE_MISMATCH",
                    f"Path start node {path_nodes[0]} doesn't match PoC node {start_poc.get('node_id')}", 
                    start_poc
                )
                validation_result['failed_tests'].append(test_name)
                return
            
            if path_nodes[-1] != end_poc.get('node_id'):
                self._add_validation_error(
                    validation_result, run_id, path_definition_id,
                    "STRUCTURAL", "HIGH", "END_NODE_MISMATCH",
                    f"Path end node {path_nodes[-1]} doesn't match PoC node {end_poc.get('node_id')}", 
                    end_poc
                )
                validation_result['failed_tests'].append(test_name)
                return
            
            # Test 3: Verify physical connectivity between adjacent nodes
            disconnected_links = self._check_physical_connectivity(path_nodes)
            if disconnected_links:
                for from_node, to_node in disconnected_links:
                    self._add_validation_error(
                        validation_result, run_id, path_definition_id,
                        "LOGICAL", "HIGH", "MISSING_PHYSICAL_CONNECTION",
                        f"No physical connection between nodes {from_node} and {to_node}",
                        {'node_id': from_node}
                    )
                validation_result['failed_tests'].append(test_name)
                return
            
            validation_result['passed_tests'].append(test_name)
            
        except Exception as e:
            self.logger.error(f"Error in connectivity validation: {str(e)}")
            self._add_validation_error(
                validation_result, run_id, path_definition_id,
                "STRUCTURAL", "MEDIUM", "CONNECTIVITY_VALIDATION_ERROR",
                f"Connectivity validation failed: {str(e)}", start_poc
            )
            validation_result['failed_tests'].append(test_name)
    
    def _validate_utility_consistency(self, validation_result: Dict, run_id: str,
                                    path_definition_id: int, path_nodes: List[int],
                                    start_poc: Dict, end_poc: Dict) -> None:
        """Validate utility consistency along the path."""
        test_name = "UTILITY_CONSISTENCY"
        
        try:
            # Get utility information for all nodes in path
            node_utilities = self._get_node_utilities(path_nodes)
            
            if not node_utilities:
                validation_result['warnings'].append({
                    'test': test_name,
                    'message': "No utility information found for path nodes"
                })
                return
            
            # Check for utility transitions
            utility_changes = []
            prev_utility = None
            
            for i, node_id in enumerate(path_nodes):
                node_utility = node_utilities.get(node_id)
                
                if node_utility and prev_utility and node_utility != prev_utility:
                    # Check if utility change is valid
                    if not self._is_valid_utility_transition(prev_utility, node_utility):
                        utility_changes.append({
                            'from_node': path_nodes[i-1],
                            'to_node': node_id,
                            'from_utility': prev_utility,
                            'to_utility': node_utility
                        })
                
                if node_utility:
                    prev_utility = node_utility
            
            # Report invalid utility transitions
            if utility_changes:
                for change in utility_changes:
                    self._add_validation_error(
                        validation_result, run_id, path_definition_id,
                        "LOGICAL", "MEDIUM", "INVALID_UTILITY_TRANSITION",
                        f"Invalid utility transition from {change['from_utility']} to {change['to_utility']} "
                        f"between nodes {change['from_node']} and {change['to_node']}",
                        {'node_id': change['to_node']}
                    )
                validation_result['failed_tests'].append(test_name)
            else:
                validation_result['passed_tests'].append(test_name)
            
            # Check start and end PoC utilities
            start_utility = start_poc.get('utility_no')
            end_utility = end_poc.get('utility_no')
            
            if start_utility and end_utility and start_utility != end_utility:
                # This might be valid in some cases, so it's a warning
                validation_result['warnings'].append({
                    'test': test_name,
                    'message': f"Start PoC utility ({start_utility}) differs from end PoC utility ({end_utility})"
                })
            
        except Exception as e:
            self.logger.error(f"Error in utility validation: {str(e)}")
            self._add_validation_error(
                validation_result, run_id, path_definition_id,
                "LOGICAL", "MEDIUM", "UTILITY_VALIDATION_ERROR",
                f"Utility validation failed: {str(e)}", start_poc
            )
            validation_result['failed_tests'].append(test_name)
    
    def _validate_poc_properties(self, validation_result: Dict, run_id: str,
                               path_definition_id: int, start_poc: Dict, end_poc: Dict) -> None:
        """Validate PoC properties and completeness."""
    def _validate_poc_properties(self, validation_result: Dict, run_id: str,
                               path_definition_id: int, start_poc: Dict, end_poc: Dict) -> None:
        """Validate PoC properties and completeness."""
        test_name = "POC_PROPERTIES"
        
        try:
            # Required properties for PoCs
            required_properties = ['node_id', 'equipment_id']
            recommended_properties = ['utility_no', 'markers', 'reference', 'flow']
            
            errors_found = False
            
            # Validate start PoC
            for prop in required_properties:
                if not start_poc.get(prop):
                    self._add_validation_error(
                        validation_result, run_id, path_definition_id,
                        "STRUCTURAL", "HIGH", "MISSING_REQUIRED_PROPERTY",
                        f"Start PoC missing required property: {prop}",
                        start_poc
                    )
                    errors_found = True
            
            # Validate end PoC
            for prop in required_properties:
                if not end_poc.get(prop):
                    self._add_validation_error(
                        validation_result, run_id, path_definition_id,
                        "STRUCTURAL", "HIGH", "MISSING_REQUIRED_PROPERTY",
                        f"End PoC missing required property: {prop}",
                        end_poc
                    )
                    errors_found = True
            
            # Check recommended properties (warnings only)
            for prop in recommended_properties:
                if not start_poc.get(prop):
                    validation_result['warnings'].append({
                        'test': test_name,
                        'message': f"Start PoC missing recommended property: {prop}"
                    })
                
                if not end_poc.get(prop):
                    validation_result['warnings'].append({
                        'test': test_name,
                        'message': f"End PoC missing recommended property: {prop}"
                    })
            
            # Validate PoC usage status
            if start_poc.get('is_used') is False:
                validation_result['warnings'].append({
                    'test': test_name,
                    'message': "Start PoC is marked as unused"
                })
            
            if end_poc.get('is_used') is False:
                validation_result['warnings'].append({
                    'test': test_name,
                    'message': "End PoC is marked as unused"
                })
            
            # Check for loopback PoCs (might affect path validity)
            if start_poc.get('is_loopback'):
                validation_result['warnings'].append({
                    'test': test_name,
                    'message': "Start PoC is marked as loopback"
                })
            
            if end_poc.get('is_loopback'):
                validation_result['warnings'].append({
                    'test': test_name,
                    'message': "End PoC is marked as loopback"
                })
            
            if errors_found:
                validation_result['failed_tests'].append(test_name)
            else:
                validation_result['passed_tests'].append(test_name)
            
        except Exception as e:
            self.logger.error(f"Error in PoC properties validation: {str(e)}")
            self._add_validation_error(
                validation_result, run_id, path_definition_id,
                "STRUCTURAL", "MEDIUM", "POC_VALIDATION_ERROR",
                f"PoC properties validation failed: {str(e)}", start_poc
            )
            validation_result['failed_tests'].append(test_name)
    
    def _validate_path_continuity(self, validation_result: Dict, run_id: str,
                                path_definition_id: int, path_nodes: List[int]) -> None:
        """Validate that the path is continuous and doesn't have gaps."""
        test_name = "PATH_CONTINUITY"
        
        try:
            if len(path_nodes) < 2:
                return
            
            # Check for duplicate nodes (except valid cases)
            node_counts = {}
            for node in path_nodes:
                node_counts[node] = node_counts.get(node, 0) + 1
            
            duplicates = [(node, count) for node, count in node_counts.items() if count > 1]
            
            if duplicates:
                for node, count in duplicates:
                    # Check if this is a valid loop/revisit
                    if not self._is_valid_node_revisit(node, path_nodes):
                        self._add_validation_error(
                            validation_result, run_id, path_definition_id,
                            "LOGICAL", "MEDIUM", "INVALID_NODE_REVISIT",
                            f"Node {node} appears {count} times in path without valid justification",
                            {'node_id': node}
                        )
                        validation_result['failed_tests'].append(test_name)
                        return
            
            # Check for unreasonably long paths
            if len(path_nodes) > 100:  # Configurable threshold
                validation_result['warnings'].append({
                    'test': test_name,
                    'message': f"Path is very long ({len(path_nodes)} nodes) - may indicate inefficient routing"
                })
            
            validation_result['passed_tests'].append(test_name)
            
        except Exception as e:
            self.logger.error(f"Error in path continuity validation: {str(e)}")
            self._add_validation_error(
                validation_result, run_id, path_definition_id,
                "LOGICAL", "MEDIUM", "CONTINUITY_VALIDATION_ERROR",
                f"Path continuity validation failed: {str(e)}", {'node_id': path_nodes[0] if path_nodes else 0}
            )
            validation_result['failed_tests'].append(test_name)
    
    def _validate_flow_direction(self, validation_result: Dict, run_id: str,
                               path_definition_id: int, path_nodes: List[int],
                               start_poc: Dict, end_poc: Dict) -> None:
        """Validate flow direction consistency."""
        test_name = "FLOW_DIRECTION"
        
        try:
            start_flow = start_poc.get('flow')
            end_flow = end_poc.get('flow')
            
            if not start_flow or not end_flow:
                validation_result['warnings'].append({
                    'test': test_name,
                    'message': "Flow direction information missing for one or both PoCs"
                })
                return
            
            # Check for valid flow combinations
            valid_combinations = [
                ('OUT', 'IN'),   # Normal flow from output to input
                ('IN', 'OUT'),   # Reverse flow (might be valid in some cases)
            ]
            
            flow_combination = (start_flow, end_flow)
            
            if flow_combination not in valid_combinations:
                self._add_validation_error(
                    validation_result, run_id, path_definition_id,
                    "LOGICAL", "MEDIUM", "INVALID_FLOW_COMBINATION",
                    f"Invalid flow combination: {start_flow} -> {end_flow}",
                    start_poc
                )
                validation_result['failed_tests'].append(test_name)
            elif flow_combination == ('IN', 'OUT'):
                validation_result['warnings'].append({
                    'test': test_name,
                    'message': "Reverse flow detected (IN -> OUT) - verify if intentional"
                })
                validation_result['passed_tests'].append(test_name)
            else:
                validation_result['passed_tests'].append(test_name)
            
        except Exception as e:
            self.logger.error(f"Error in flow direction validation: {str(e)}")
            self._add_validation_error(
                validation_result, run_id, path_definition_id,
                "LOGICAL", "LOW", "FLOW_VALIDATION_ERROR",
                f"Flow direction validation failed: {str(e)}", start_poc
            )
            validation_result['failed_tests'].append(test_name)
    
    def _check_physical_connectivity(self, path_nodes: List[int]) -> List[Tuple[int, int]]:
        """Check if adjacent nodes in path have physical connections."""
        disconnected_links = []
        
        if len(path_nodes) < 2:
            return disconnected_links
        
        # Build query to check all adjacent pairs at once
        pairs = [(path_nodes[i], path_nodes[i+1]) for i in range(len(path_nodes) - 1)]
        
        if not pairs:
            return disconnected_links
        
        # Create query with all pairs
        pair_conditions = []
        params = []
        
        for from_node, to_node in pairs:
            pair_conditions.append("""
                (from_poc.node_id = %s AND to_poc.node_id = %s) OR
                (from_poc.node_id = %s AND to_poc.node_id = %s)
            """)
            params.extend([from_node, to_node, to_node, from_node])
        
        query = f"""
        SELECT DISTINCT from_poc.node_id, to_poc.node_id
        FROM tb_equipment_poc_connections conn
        JOIN tb_equipment_pocs from_poc ON conn.from_poc_id = from_poc.id
        JOIN tb_equipment_pocs to_poc ON conn.to_poc_id = to_poc.id
        WHERE conn.is_valid = 1 AND ({' OR '.join(pair_conditions)})
        """
        
        with self.db.cursor() as cursor:
            cursor.execute(query, params)
            connected_pairs = set()
            
            for row in cursor.fetchall():
                from_node, to_node = row[0], row[1]
                # Add both directions
                connected_pairs.add((min(from_node, to_node), max(from_node, to_node)))
        
        # Check which pairs are missing
        for from_node, to_node in pairs:
            normalized_pair = (min(from_node, to_node), max(from_node, to_node))
            if normalized_pair not in connected_pairs:
                disconnected_links.append((from_node, to_node))
        
        return disconnected_links
    
    def _get_node_utilities(self, path_nodes: List[int]) -> Dict[int, int]:
        """Get utility information for path nodes."""
        if not path_nodes:
            return {}
        
        placeholders = ','.join(['%s'] * len(path_nodes))
        query = f"""
        SELECT node_id, utility_no
        FROM tb_equipment_pocs
        WHERE node_id IN ({placeholders}) AND utility_no IS NOT NULL
        """
        
        node_utilities = {}
        
        with self.db.cursor() as cursor:
            cursor.execute(query, path_nodes)
            for row in cursor.fetchall():
                node_utilities[row[0]] = row[1]
        
        return node_utilities
    
    def _is_valid_utility_transition(self, from_utility: int, to_utility: int) -> bool:
        """Check if a utility transition is valid."""
        if self._utility_compatibility_cache is None:
            self._load_utility_compatibility()
        
        # For now, implement basic rules
        # In a real system, this would be based on engineering rules
        
        # Same utility is always valid
        if from_utility == to_utility:
            return True
        
        # Example rules (would be loaded from configuration):
        # Water can transition to steam, steam can condense to water, etc.
        valid_transitions = {
            1: [2, 3],  # Water -> Steam, Water -> Ice (example)
            2: [1],     # Steam -> Water
            3: [1],     # Ice -> Water
            # Add more utility transition rules
        }
        
        return to_utility in valid_transitions.get(from_utility, [])
    
    def _is_valid_node_revisit(self, node_id: int, path_nodes: List[int]) -> bool:
        """Check if revisiting a node is valid (e.g., in loops)."""
        # Find all positions of this node
        positions = [i for i, node in enumerate(path_nodes) if node == node_id]
        
        if len(positions) <= 1:
            return True
        
        # For now, allow revisits only if they're not consecutive
        # (which would indicate a loop or valid routing pattern)
        for i in range(len(positions) - 1):
            if positions[i+1] - positions[i] == 1:
                return False  # Consecutive duplicate
        
        return True
    
    def _add_validation_error(self, validation_result: Dict, run_id: str,
                            path_definition_id: int, error_scope: str,
                            severity: str, error_type: str, message: str,
                            object_data: Dict) -> None:
        """Add a validation error to the result and store in database."""
        error = {
            'error_scope': error_scope,
            'severity': severity,
            'error_type': error_type,
            'message': message,
            'object_data': object_data,
            'timestamp': datetime.now().isoformat()
        }
        
        validation_result['errors'].append(error)
        
        # Store in database
        self._store_validation_error(run_id, path_definition_id, error)
    
    def _store_validation_error(self, run_id: str, path_definition_id: int, error: Dict) -> None:
        """Store validation error in database."""
        query = """
        INSERT INTO tb_validation_errors (
            run_id, path_definition_id, severity, error_scope, error_type,
            object_type, object_id, object_guid, error_message, error_data,
            object_utility_no, object_flow, object_is_loopback
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """
        
        object_data = error.get('object_data', {})
        
        with self.db.cursor() as cursor:
            cursor.execute(query, (
                run_id,
                path_definition_id,
                error['severity'],
                error['error_scope'],
                error['error_type'],
                'NODE',  # Default object type
                object_data.get('node_id', 0),
                object_data.get('equipment_guid', ''),
                error['message'],
                json.dumps(error),
                object_data.get('utility_no'),
                object_data.get('flow'),
                object_data.get('is_loopback', False)
            ))
        self.db.commit()
    
    def _store_validation_results(self, run_id: str, path_definition_id: int, 
                                validation_result: Dict) -> None:
        """Store overall validation results as tags."""
        # Create tags for validation status
        tag_type = "QA"
        
        if validation_result['overall_status'] == 'PASSED':
            tag_code = "VALIDATED_OK"
            tag = "Path validation passed"
        elif validation_result['overall_status'] == 'WARNING':
            tag_code = "VALIDATED_WARN"
            tag = "Path validation passed with warnings"
        elif validation_result['overall_status'] == 'FAILED':
            tag_code = "VALIDATED_FAIL"
            tag = "Path validation failed"
        elif validation_result['overall_status'] == 'CRITICAL_FAILURE':
            tag_code = "VALIDATED_CRIT"
            tag = "Path validation critical failure"
        else:
            tag_code = "VALIDATED_ERR"
            tag = "Path validation error"
        
        # Store validation tag
        query = """
        INSERT INTO tb_path_tags (
            run_id, path_definition_id, tag_type, tag_code, tag,
            source, confidence, notes
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        notes = f"Errors: {len(validation_result['errors'])}, Warnings: {len(validation_result['warnings'])}"
        
        with self.db.cursor() as cursor:
            cursor.execute(query, (
                run_id, path_definition_id, tag_type, tag_code, tag,
                "SYSTEM", 1.0, notes
            ))
        self.db.commit()
    
    def _initialize_validation_tests(self) -> None:
        """Initialize validation test definitions in database."""
        validation_tests = [
            {
                'code': 'CONN_BASIC',
                'name': 'Basic Connectivity Validation',
                'scope': 'CONNECTIVITY',
                'severity': 'HIGH',
                'test_type': 'STRUCTURAL',
                'description': 'Validates basic path connectivity requirements'
            },
            {
                'code': 'UTIL_CONSIST',
                'name': 'Utility Consistency Validation',
                'scope': 'FLOW',
                'severity': 'MEDIUM',
                'test_type': 'LOGICAL',
                'description': 'Validates utility consistency along path'
            },
            {
                'code': 'POC_PROPS',
                'name': 'PoC Properties Validation',
                'scope': 'QA',
                'severity': 'HIGH',
                'test_type': 'STRUCTURAL',
                'description': 'Validates PoC properties completeness'
            },
            {
                'code': 'PATH_CONT',
                'name': 'Path Continuity Validation',
                'scope': 'CONNECTIVITY',
                'severity': 'MEDIUM',
                'test_type': 'LOGICAL',
                'description': 'Validates path continuity and structure'
            },
            {
                'code': 'FLOW_DIR',
                'name': 'Flow Direction Validation',
                'scope': 'FLOW',
                'severity': 'MEDIUM',
                'test_type': 'LOGICAL',
                'description': 'Validates flow direction consistency'
            }
        ]
        
        # Check if tests already exist
        with self.db.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM tb_validation_tests")
            count = cursor.fetchone()[0]
            
            if count == 0:
                # Insert validation tests
                for test in validation_tests:
                    insert_query = """
                    INSERT INTO tb_validation_tests (
                        code, name, scope, severity, test_type, is_active, description
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """
                    
                    cursor.execute(insert_query, (
                        test['code'], test['name'], test['scope'],
                        test['severity'], test['test_type'], True, test['description']
                    ))
                
                self.db.commit()
                self.logger.info(f"Initialized {len(validation_tests)} validation tests")
    
    def _load_utility_compatibility(self) -> None:
        """Load utility compatibility rules."""
        # This would load from configuration or database
        # For now, using hardcoded rules
        self._utility_compatibility_cache = {
            # Add utility compatibility rules here
        }
    
    def get_validation_summary(self, run_id: str) -> Dict[str, Any]:
        """Get validation summary for a run."""
        query = """
        SELECT 
            severity,
            error_scope,
            error_type,
            COUNT(*) as count
        FROM tb_validation_errors
        WHERE run_id = %s
        GROUP BY severity, error_scope, error_type
        ORDER BY severity, error_scope, error_type
        """
        
        summary = {
            'total_errors': 0,
            'by_severity': {},
            'by_scope': {},
            'by_type': {},
            'error_details': []
        }
        
        with self.db.cursor() as cursor:
            cursor.execute(query, (run_id,))
            results = cursor.fetchall()
            
            for row in results:
                severity, scope, error_type, count = row
                
                summary['total_errors'] += count
                summary['by_severity'][severity] = summary['by_severity'].get(severity, 0) + count
                summary['by_scope'][scope] = summary['by_scope'].get(scope, 0) + count
                summary['by_type'][error_type] = summary['by_type'].get(error_type, 0) + count
                
                summary['error_details'].append({
                    'severity': severity,
                    'scope': scope,
                    'type': error_type,
                    'count': count
                })
        
        return summary
    
    def get_validation_tests(self) -> List[Dict[str, Any]]:
        """Get all available validation tests."""
        if self._validation_tests_cache is None:
            query = """
            SELECT code, name, scope, severity, test_type, is_active, description
            FROM tb_validation_tests
            WHERE is_active = 1
            ORDER BY severity DESC, scope, code
            """
            
            tests = []
            with self.db.cursor() as cursor:
                cursor.execute(query)
                for row in cursor.fetchall():
                    tests.append({
                        'code': row[0],
                        'name': row[1],
                        'scope': row[2],
                        'severity': row[3],
                        'test_type': row[4],
                        'is_active': bool(row[5]),
                        'description': row[6]
                    })
            
            self._validation_tests_cache = tests
        
        return self._validation_tests_cache
    
    def clear_validation_cache(self) -> None:
        """Clear validation caches."""
        self._validation_tests_cache = None
        self._utility_compatibility_cache = None
        self.logger.info("Validation caches cleared")
        