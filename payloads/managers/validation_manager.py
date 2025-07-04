"""
Comprehensive path validation framework for connectivity and utility consistency.
"""
import logging
import json
from typing import Optional, Dict, Any, List, Set, Tuple
from dataclasses import dataclass
from datetime import datetime
import sqlite3


@dataclass
class ValidationResult:
    """Result of a validation test."""
    test_code: str
    test_name: str
    severity: str
    passed: bool
    error_count: int
    warning_count: int
    details: List[Dict[str, Any]]
    
    @property
    def status(self) -> str:
        if self.passed:
            return "PASSED"
        elif self.severity in ["LOW", "MEDIUM"]:
            return "WARNING"
        else:
            return "FAILED"


@dataclass
class UtilityFlow:
    """Represents utility flow through a path segment."""
    from_node: int
    to_node: int
    utility_no: int
    flow_direction: str
    is_valid: bool
    consistency_issues: List[str]


class ValidationManager:
    """Manages comprehensive path validation including connectivity and utility consistency."""
    
    def __init__(self, db_connection: sqlite3.Connection):
        self.db = db_connection
        self.logger = logging.getLogger(__name__)
        
        # Initialize validation tests
        self._initialize_validation_tests()
    
    def _initialize_validation_tests(self) -> None:
        """Initialize standard validation tests in database."""
        
        standard_tests = [
            {
                'code': 'CONN_001',
                'name': 'PoC Connectivity Validation',
                'scope': 'CONNECTIVITY',
                'severity': 'CRITICAL',
                'test_type': 'STRUCTURAL',
                'description': 'Validates that all PoCs in path have required connectivity attributes'
            },
            {
                'code': 'CONN_002',
                'name': 'Path Continuity Check',
                'scope': 'CONNECTIVITY',
                'severity': 'HIGH',
                'test_type': 'LOGICAL',
                'description': 'Ensures path has continuous connections between all nodes'
            },
            {
                'code': 'UTY_001',
                'name': 'Utility Consistency Check',
                'scope': 'FLOW',
                'severity': 'HIGH',
                'test_type': 'LOGICAL',
                'description': 'Validates utility consistency throughout the path'
            },
            {
                'code': 'UTY_002',
                'name': 'Utility Flow Direction',
                'scope': 'FLOW',
                'severity': 'MEDIUM',
                'test_type': 'LOGICAL',
                'description': 'Checks utility flow direction consistency'
            },
            {
                'code': 'DATA_001',
                'name': 'Required Attributes Check',
                'scope': 'CONNECTIVITY',
                'severity': 'HIGH',
                'test_type': 'STRUCTURAL',
                'description': 'Validates that PoCs have required attributes (utility_no, markers, reference)'
            },
            {
                'code': 'QA_001',
                'name': 'Path Quality Assessment',
                'scope': 'QA',
                'severity': 'LOW',
                'test_type': 'PERFORMANCE',
                'description': 'Assesses overall path quality metrics'
            },
            {
                'code': 'QA_002',
                'name': 'Loopback Detection',
                'scope': 'QA',
                'severity': 'MEDIUM',
                'test_type': 'LOGICAL',
                'description': 'Detects and validates loopback connections'
            }
        ]
        
        # Insert tests if they don't exist
        for test in standard_tests:
            existing_query = "SELECT id FROM tb_validation_tests WHERE code = ?"
            cursor = self.db.execute(existing_query, [test['code']])
            
            if not cursor.fetchone():
                insert_sql = """
                INSERT INTO tb_validation_tests (
                    code, name, scope, severity, test_type, is_active, description
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """
                
                self.db.execute(insert_sql, [
                    test['code'], test['name'], test['scope'], test['severity'],
                    test['test_type'], 1, test['description']
                ])
        
        self.db.commit()
    
    def validate_path(self, run_id: str, path_def_id: int, path_context: Dict[str, Any],
                     utilities_scope: List[int], data_codes_scope: List[int]) -> List[ValidationResult]:
        """Run all validation tests on a path."""
        
        self.logger.debug(f"Validating path {path_def_id} for run {run_id}")
        
        # Get active validation tests
        tests_query = """
        SELECT id, code, name, scope, severity, test_type, description
        FROM tb_validation_tests
        WHERE is_active = 1
        ORDER BY severity DESC, code
        """
        
        cursor = self.db.execute(tests_query)
        tests = cursor.fetchall()
        
        results = []
        
        for test_row in tests:
            test_id, code, name, scope, severity, test_type, description = test_row
            
            try:
                # Run specific validation test
                result = self._run_validation_test(
                    run_id, path_def_id, test_id, code, name, scope, severity,
                    path_context, utilities_scope, data_codes_scope
                )
                
                results.append(result)
                
            except Exception as e:
                self.logger.error(f"Error running validation test {code}: {e}")
                # Create error result
                error_result = ValidationResult(
                    test_code=code,
                    test_name=name,
                    severity=severity,
                    passed=False,
                    error_count=1,
                    warning_count=0,
                    details=[{
                        'error_type': 'VALIDATION_ERROR',
                        'message': f"Test execution failed: {str(e)}",
                        'object_type': 'PATH',
                        'object_id': path_def_id
                    }]
                )
                results.append(error_result)
        
        return results
    
    def _run_validation_test(self, run_id: str, path_def_id: int, test_id: int,
                           code: str, name: str, scope: str, severity: str,
                           path_context: Dict[str, Any], utilities_scope: List[int],
                           data_codes_scope: List[int]) -> ValidationResult:
        """Run a specific validation test."""
        
        if code == 'CONN_001':
            return self._validate_poc_connectivity(
                run_id, path_def_id, test_id, code, name, severity, path_context
            )
        elif code == 'CONN_002':
            return self._validate_path_continuity(
                run_id, path_def_id, test_id, code, name, severity, path_context
            )
        elif code == 'UTY_001':
            return self._validate_utility_consistency(
                run_id, path_def_id, test_id, code, name, severity, path_context, utilities_scope
            )
        elif code == 'UTY_002':
            return self._validate_utility_flow_direction(
                run_id, path_def_id, test_id, code, name, severity, path_context
            )
        elif code == 'DATA_001':
            return self._validate_required_attributes(
                run_id, path_def_id, test_id, code, name, severity, path_context
            )
        elif code == 'QA_001':
            return self._assess_path_quality(
                run_id, path_def_id, test_id, code, name, severity, path_context
            )
        elif code == 'QA_002':
            return self._detect_loopbacks(
                run_id, path_def_id, test_id, code, name, severity, path_context
            )
        else:
            # Unknown test - return passed
            return ValidationResult(
                test_code=code,
                test_name=name,
                severity=severity,
                passed=True,
                error_count=0,
                warning_count=0,
                details=[]
            )
    
    def _validate_poc_connectivity(self, run_id: str, path_def_id: int, test_id: int,
                                 code: str, name: str, severity: str,
                                 path_context: Dict[str, Any]) -> ValidationResult:
        """Validate PoC connectivity attributes."""
        
        nodes = path_context.get('nodes', [])
        errors = []
        warnings = []
        
        if not nodes:
            errors.append({
                'error_type': 'EMPTY_PATH',
                'message': 'Path contains no nodes',
                'object_type': 'PATH',
                'object_id': path_def_id
            })
        else:
            # Check each PoC for required connectivity
            node_placeholders = ','.join(['?'] * len(nodes))
            poc_query = f"""
            SELECT 
                ep.node_id, ep.is_used, ep.utility_no, ep.markers, 
                ep.reference, ep.flow, ep.is_loopback,
                e.id as equipment_id, e.guid as equipment_guid,
                e.data_code, e.category_no
            FROM tb_equipment_pocs ep
            JOIN tb_equipments e ON ep.equipment_id = e.id
            WHERE ep.node_id IN ({node_placeholders})
            """
            
            cursor = self.db.execute(poc_query, nodes)
            poc_data = {row[0]: row[1:] for row in cursor.fetchall()}
            
            for node_id in nodes:
                if node_id not in poc_data:
                    errors.append({
                        'error_type': 'MISSING_POC',
                        'message': f'PoC for node {node_id} not found in database',
                        'object_type': 'NODE',
                        'object_id': node_id
                    })
                    continue
                
                poc_info = poc_data[node_id]
                is_used, utility_no, markers, reference, flow, is_loopback = poc_info[:6]
                equipment_id, equipment_guid, data_code, category_no = poc_info[6:]
                
                # Record validation error for missing connectivity
                if not is_used:
                    self._record_validation_error(
                        run_id, path_def_id, test_id, 'MEDIUM', 'CONNECTIVITY',
                        'UNUSED_POC', 'POC', equipment_id, equipment_guid,
                        f'PoC node {node_id} is not marked as used but appears in path'
                    )
                    
                    warnings.append({
                        'error_type': 'UNUSED_POC',
                        'message': f'PoC node {node_id} is not marked as used',
                        'object_type': 'POC',
                        'object_id': equipment_id
                    })
        
        passed = len(errors) == 0
        
        return ValidationResult(
            test_code=code,
            test_name=name,
            severity=severity,
            passed=passed,
            error_count=len(errors),
            warning_count=len(warnings),
            details=errors + warnings
        )
    
    def _validate_path_continuity(self, run_id: str, path_def_id: int, test_id: int,
                                code: str, name: str, severity: str,
                                path_context: Dict[str, Any]) -> ValidationResult:
        """Validate path continuity through connections."""
        
        nodes = path_context.get('nodes', [])
        links = path_context.get('links', [])
        errors = []
        
        if len(nodes) < 2:
            return ValidationResult(
                test_code=code, test_name=name, severity=severity, passed=True,
                error_count=0, warning_count=0, details=[]
            )
        
        # Check if all consecutive nodes are connected
        if links:
            link_placeholders = ','.join(['?'] * len(links))
            connection_query = f"""
            SELECT 
                conn.id, conn.from_poc_id, conn.to_poc_id, conn.is_valid,
                ep1.node_id as from_node, ep2.node_id as to_node
            FROM tb_equipment_poc_connections conn
            JOIN tb_equipment_pocs ep1 ON conn.from_poc_id = ep1.id
            JOIN tb_equipment_pocs ep2 ON conn.to_poc_id = ep2.id
            WHERE conn.id IN ({link_placeholders})
            """
            
            cursor = self.db.execute(connection_query, links)
            connections = cursor.fetchall()
            
            # Build connection map
            connection_map = {}
            for conn in connections:
                conn_id, from_poc, to_poc, is_valid, from_node, to_node = conn
                
                if not is_valid:
                    self._record_validation_error(
                        run_id, path_def_id, test_id, 'HIGH', 'CONNECTIVITY',
                        'INVALID_CONNECTION', 'LINK', conn_id, f'CONN_{conn_id}',
                        f'Connection {conn_id} is marked as invalid'
                    )
                    
                    errors.append({
                        'error_type': 'INVALID_CONNECTION',
                        'message': f'Connection {conn_id} between nodes {from_node}-{to_node} is invalid',
                        'object_type': 'CONNECTION',
                        'object_id': conn_id
                    })
                
                connection_map[from_node] = to_node
            
            # Validate path continuity
            for i in range(len(nodes) - 1):
                current_node = nodes[i]
                next_node = nodes[i + 1]
                
                if current_node not in connection_map or connection_map[current_node] != next_node:
                    self._record_validation_error(
                        run_id, path_def_id, test_id, 'CRITICAL', 'CONNECTIVITY',
                        'DISCONTINUOUS_PATH', 'PATH', path_def_id, f'PATH_{path_def_id}',
                        f'No connection found between consecutive nodes {current_node} and {next_node}'
                    )
                    
                    errors.append({
                        'error_type': 'DISCONTINUOUS_PATH',
                        'message': f'Missing connection between nodes {current_node} and {next_node}',
                        'object_type': 'PATH',
                        'object_id': path_def_id
                    })
        
        passed = len(errors) == 0
        
        return ValidationResult(
            test_code=code,
            test_name=name,
            severity=severity,
            passed=passed,
            error_count=len(errors),
            warning_count=0,
            details=errors
        )
    
    def _validate_utility_consistency(self, run_id: str, path_def_id: int, test_id: int,
                                    code: str, name: str, severity: str,
                                    path_context: Dict[str, Any], 
                                    utilities_scope: List[int]) -> ValidationResult:
        """Validate utility consistency throughout the path."""
        
        nodes = path_context.get('nodes', [])
        errors = []
        warnings = []
        
        if len(nodes) < 2:
            return ValidationResult(
                test_code=code, test_name=name, severity=severity, passed=True,
                error_count=0, warning_count=0, details=[]
            )
        
        # Get utility information for all nodes
        node_placeholders = ','.join(['?'] * len(nodes))
        utility_query = f"""
        SELECT 
            ep.node_id, ep.utility_no, ep.flow, ep.markers, ep.reference,
            e.id as equipment_id, e.guid as equipment_guid, e.kind
        FROM tb_equipment_pocs ep
        JOIN tb_equipments e ON ep.equipment_id = e.id
        WHERE ep.node_id IN ({node_placeholders})
        ORDER BY ep.node_id
        """
        
        cursor = self.db.execute(utility_query, nodes)
        node_utilities = {row[0]: row[1:] for row in cursor.fetchall()}
        
        # Analyze utility flow through path
        utility_flows = self._analyze_utility_flow(nodes, node_utilities)
        
        # Check for utility consistency issues
        current_utility = None
        utility_changes = []
        
        for i, node_id in enumerate(nodes):
            if node_id not in node_utilities:
                continue
            
            utility_no, flow, markers, reference, equipment_id, equipment_guid, kind = node_utilities[node_id]
            
            if utility_no is None:
                warnings.append({
                    'error_type': 'MISSING_UTILITY',
                    'message': f'Node {node_id} has no utility assignment',
                    'object_type': 'POC',
                    'object_id': equipment_id
                })
                continue
            
            if current_utility is None:
                current_utility = utility_no
            elif current_utility != utility_no:
                # Utility change detected - check if it's valid
                is_valid_change = self._is_valid_utility_transition(
                    current_utility, utility_no, kind, equipment_id
                )
                
                if not is_valid_change:
                    self._record_validation_error(
                        run_id, path_def_id, test_id, 'HIGH', 'FLOW',
                        'INVALID_UTILITY_CHANGE', 'POC', equipment_id, equipment_guid,
                        f'Invalid utility transition from {current_utility} to {utility_no} at equipment {equipment_guid}'
                    )
                    
                    errors.append({
                        'error_type': 'INVALID_UTILITY_CHANGE',
                        'message': f'Invalid utility change from {current_utility} to {utility_no} at node {node_id}',
                        'object_type': 'POC',
                        'object_id': equipment_id
                    })
                else:
                    utility_changes.append({
                        'from_utility': current_utility,
                        'to_utility': utility_no,
                        'node_id': node_id,
                        'equipment_type': kind
                    })
                
                current_utility = utility_no
        
        # Check for utility flow consistency
        for flow in utility_flows:
            if not flow.is_valid:
                for issue in flow.consistency_issues:
                    warnings.append({
                        'error_type': 'FLOW_INCONSISTENCY',
                        'message': f'Flow inconsistency between nodes {flow.from_node}-{flow.to_node}: {issue}',
                        'object_type': 'FLOW',
                        'object_id': f'{flow.from_node}-{flow.to_node}'
                    })
        
        passed = len(errors) == 0
        
        return ValidationResult(
            test_code=code,
            test_name=name,
            severity=severity,
            passed=passed,
            error_count=len(errors),
            warning_count=len(warnings),
            details=errors + warnings
        )
    
    def _analyze_utility_flow(self, nodes: List[int], 
                            node_utilities: Dict[int, Tuple]) -> List[UtilityFlow]:
        """Analyze utility flow through path segments."""
        
        flows = []
        
        for i in range(len(nodes) - 1):
            from_node = nodes[i]
            to_node = nodes[i + 1]
            
            if from_node not in node_utilities or to_node not in node_utilities:
                continue
            
            from_utility = node_utilities[from_node][0]  # utility_no
            to_utility = node_utilities[to_node][0]
            
            from_flow = node_utilities[from_node][1]  # flow direction
            to_flow = node_utilities[to_node][1]
            
            # Analyze flow consistency
            consistency_issues = []
            is_valid = True
            
            # Check utility consistency
            if from_utility != to_utility and from_utility is not None and to_utility is not None:
                # Different utilities - check if transition is valid
                if not self._is_compatible_utility_flow(from_utility, to_utility):
                    consistency_issues.append(f"Incompatible utility flow: {from_utility} -> {to_utility}")
                    is_valid = False
            
            # Check flow direction consistency
            if from_flow and to_flow:
                if from_flow == 'OUT' and to_flow == 'OUT':
                    consistency_issues.append("Both nodes have OUT flow direction")
                    is_valid = False
                elif from_flow == 'IN' and to_flow == 'IN':
                    consistency_issues.append("Both nodes have IN flow direction")
                    is_valid = False
            
            flow = UtilityFlow(
                from_node=from_node,
                to_node=to_node,
                utility_no=from_utility or to_utility,
                flow_direction=from_flow or to_flow or 'UNKNOWN',
                is_valid=is_valid,
                consistency_issues=consistency_issues
            )
            
            flows.append(flow)
        
        return flows
    
    def _is_valid_utility_transition(self, from_utility: int, to_utility: int,
                                   equipment_kind: str, equipment_id: int) -> bool:
        """Check if utility transition is valid based on equipment type."""
        
        # Define valid utility transitions
        # This is a simplified version - in practice, you'd have more complex rules
        
        valid_transitions = {
            # Water system transitions
            1: [2, 3],     # Water -> Hot Water, Steam
            2: [1, 3],     # Hot Water -> Water, Steam  
            3: [1, 2, 4],  # Steam -> Water, Hot Water, Condensate
            4: [1],        # Condensate -> Water
            
            # Gas system transitions
            10: [11, 12],  # Natural Gas -> Compressed Gas, Processed Gas
            11: [10, 12],  # Compressed Gas -> Natural Gas, Processed Gas
            
            # Electrical (no transitions typically)
            20: [],        # Electrical stays electrical
        }
        
        # Check if equipment type allows utility changes
        converting_equipment = ['PROCESSING', 'SUPPLY', 'TREATMENT']
        
        if equipment_kind not in converting_equipment:
            # Equipment doesn't convert utilities
            return from_utility == to_utility
        
        # Check valid transitions
        allowed_transitions = valid_transitions.get(from_utility, [])
        return to_utility in allowed_transitions
    
    def _is_compatible_utility_flow(self, from_utility: int, to_utility: int) -> bool:
        """Check if utilities are compatible for flow."""
        
        # Define utility compatibility groups
        compatible_groups = [
            [1, 2, 3, 4],      # Water systems
            [10, 11, 12, 13],  # Gas systems
            [20, 21, 22],      # Electrical systems
            [30, 31, 32],      # HVAC systems
        ]
        
        # Check if both utilities are in the same compatibility group
        for group in compatible_groups:
            if from_utility in group and to_utility in group:
                return True
        
        return False
    
    def _validate_utility_flow_direction(self, run_id: str, path_def_id: int, test_id: int,
                                       code: str, name: str, severity: str,
                                       path_context: Dict[str, Any]) -> ValidationResult:
        """Validate utility flow direction consistency."""
        
        nodes = path_context.get('nodes', [])
        warnings = []
        
        if len(nodes) < 2:
            return ValidationResult(
                test_code=code, test_name=name, severity=severity, passed=True,
                error_count=0, warning_count=0, details=[]
            )
        
        # Get flow direction information
        node_placeholders = ','.join(['?'] * len(nodes))
        flow_query = f"""
        SELECT ep.node_id, ep.flow, ep.utility_no, e.id as equipment_id, e.guid
        FROM tb_equipment_pocs ep
        JOIN tb_equipments e ON ep.equipment_id = e.id
        WHERE ep.node_id IN ({node_placeholders}) AND ep.flow IS NOT NULL
        """
        
        cursor = self.db.execute(flow_query, nodes)
        flow_data = {row[0]: row[1:] for row in cursor.fetchall()}
        
        # Analyze flow direction patterns
        in_flows = []
        out_flows = []
        
        for node_id in nodes:
            if node_id in flow_data:
                flow_direction, utility_no, equipment_id, equipment_guid = flow_data[node_id]
                
                if flow_direction == 'IN':
                    in_flows.append((node_id, utility_no, equipment_id))
                elif flow_direction == 'OUT':
                    out_flows.append((node_id, utility_no, equipment_id))
        
        # Check for flow direction issues
        if len(in_flows) > 1:
            warnings.append({
                'error_type': 'MULTIPLE_INFLOWS',
                'message': f'Path has multiple IN flow points: {[node for node, _, _ in in_flows]}',
                'object_type': 'PATH',
                'object_id': path_def_id
            })
        
        if len(out_flows) > 1:
            warnings.append({
                'error_type': 'MULTIPLE_OUTFLOWS',
                'message': f'Path has multiple OUT flow points: {[node for node, _, _ in out_flows]}',
                'object_type': 'PATH',
                'object_id': path_def_id
            })
        
        if not in_flows and not out_flows:
            warnings.append({
                'error_type': 'NO_FLOW_DIRECTION',
                'message': 'Path has no defined flow directions',
                'object_type': 'PATH',
                'object_id': path_def_id
            })
        
        return ValidationResult(
            test_code=code,
            test_name=name,
            severity=severity,
            passed=True,  # Flow direction issues are warnings, not failures
            error_count=0,
            warning_count=len(warnings),
            details=warnings
        )
    
    def _validate_required_attributes(self, run_id: str, path_def_id: int, test_id: int,
                                    code: str, name: str, severity: str,
                                    path_context: Dict[str, Any]) -> ValidationResult:
        """Validate that PoCs have required attributes."""
        
        nodes = path_context.get('nodes', [])
        errors = []
        warnings = []
        
        if not nodes:
            return ValidationResult(
                test_code=code, test_name=name, severity=severity, passed=True,
                error_count=0, warning_count=0, details=[]
            )
        
        # Check required attributes for each PoC
        node_placeholders = ','.join(['?'] * len(nodes))
        attr_query = f"""
        SELECT 
            ep.node_id, ep.utility_no, ep.markers, ep.reference,
            e.id as equipment_id, e.guid as equipment_guid
        FROM tb_equipment_pocs ep
        JOIN tb_equipments e ON ep.equipment_id = e.id
        WHERE ep.node_id IN ({node_placeholders})
        """
        
        cursor = self.db.execute(attr_query, nodes)
        poc_attributes = cursor.fetchall()
        
        for node_id, utility_no, markers, reference, equipment_id, equipment_guid in poc_attributes:
            missing_attrs = []
            
            if utility_no is None:
                missing_attrs.append('utility_no')
            
            if not markers or markers.strip() == '':
                missing_attrs.append('markers')
            
            if not reference or reference.strip() == '':
                missing_attrs.append('reference')
            
            if missing_attrs:
                severity_level = 'HIGH' if 'utility_no' in missing_attrs else 'MEDIUM'
                
                self._record_validation_error(
                    run_id, path_def_id, test_id, severity_level, 'CONNECTIVITY',
                    'MISSING_REQUIRED_ATTRIBUTES', 'POC', equipment_id, equipment_guid,
                    f'PoC node {node_id} missing required attributes: {", ".join(missing_attrs)}'
                )
                
                if 'utility_no' in missing_attrs:
                    errors.append({
                        'error_type': 'MISSING_UTILITY',
                        'message': f'PoC node {node_id} missing utility assignment',
                        'object_type': 'POC',
                        'object_id': equipment_id
                    })
                else:
                    warnings.append({
                        'error_type': 'MISSING_ATTRIBUTES',
                        'message': f'PoC node {node_id} missing attributes: {", ".join(missing_attrs)}',
                        'object_type': 'POC',
                        'object_id': equipment_id
                    })
        
        passed = len(errors) == 0
        
        return ValidationResult(
            test_code=code,
            test_name=name,
            severity=severity,
            passed=passed,
            error_count=len(errors),
            warning_count=len(warnings),
            details=errors + warnings
        )
    
    def _assess_path_quality(self, run_id: str, path_def_id: int, test_id: int,
                           code: str, name: str, severity: str,
                           path_context: Dict[str, Any]) -> ValidationResult:
        """Assess overall path quality metrics."""
        
        nodes = path_context.get('nodes', [])
        links = path_context.get('links', [])
        warnings = []
        
        node_count = len(nodes)
        link_count = len(links)
        
        # Quality assessments
        if node_count <= 2:
            warnings.append({
                'error_type': 'SHORT_PATH',
                'message': f'Path is very short with only {node_count} nodes',
                'object_type': 'PATH',
                'object_id': path_def_id
            })
        
        if node_count > 50:
            warnings.append({
                'error_type': 'LONG_PATH',
                'message': f'Path is very long with {node_count} nodes - may indicate inefficient routing',
                'object_type': 'PATH',
                'object_id': path_def_id
            })
        
        # Check link-to-node ratio
        if node_count > 1:
            expected_links = node_count - 1
            if link_count != expected_links:
                warnings.append({
                    'error_type': 'LINK_COUNT_MISMATCH',
                    'message': f'Expected {expected_links} links for {node_count} nodes, found {link_count}',
                    'object_type': 'PATH',
                    'object_id': path_def_id
                })
        
        return ValidationResult(
            test_code=code,
            test_name=name,
            severity=severity,
            passed=True,  # Quality issues are warnings
            error_count=0,
            warning_count=len(warnings),
            details=warnings
        )
    
    def _detect_loopbacks(self, run_id: str, path_def_id: int, test_id: int,
                        code: str, name: str, severity: str,
                        path_context: Dict[str, Any]) -> ValidationResult:
        """Detect and validate loopback connections."""
        
        nodes = path_context.get('nodes', [])
        warnings = []
        
        if len(nodes) < 3:  # Need at least 3 nodes for a meaningful loopback
            return ValidationResult(
                test_code=code, test_name=name, severity=severity, passed=True,
                error_count=0, warning_count=0, details=[]
            )
        
        # Check for loopback PoCs in the path
        node_placeholders = ','.join(['?'] * len(nodes))
        loopback_query = f"""
        SELECT ep.node_id, ep.is_loopback, e.id as equipment_id, e.guid
        FROM tb_equipment_pocs ep
        JOIN tb_equipments e ON ep.equipment_id = e.id
        WHERE ep.node_id IN ({node_placeholders}) AND ep.is_loopback = 1
        """
        
        cursor = self.db.execute(loopback_query, nodes)
        loopback_pocs = cursor.fetchall()
        
        # Check for duplicate nodes (actual loops in path)
        node_positions = {}
        for i, node_id in enumerate(nodes):
            if node_id in node_positions:
                warnings.append({
                    'error_type': 'CIRCULAR_PATH',
                    'message': f'Node {node_id} appears multiple times in path (positions {node_positions[node_id]} and {i})',
                    'object_type': 'PATH',
                    'object_id': path_def_id
                })
            else:
                node_positions[node_id] = i
        
        # Report loopback PoCs found
        for node_id, is_loopback, equipment_id, equipment_guid in loopback_pocs:
            warnings.append({
                'error_type': 'LOOPBACK_POC',
                'message': f'Node {node_id} is marked as loopback PoC',
                'object_type': 'POC',
                'object_id': equipment_id
            })
        
        return ValidationResult(
            test_code=code,
            test_name=name,
            severity=severity,
            passed=True,
            error_count=0,
            warning_count=len(warnings),
            details=warnings
        )
    
    def _record_validation_error(self, run_id: str, path_def_id: Optional[int], 
                               validation_test_id: int, severity: str, error_scope: str,
                               error_type: str, object_type: str, object_id: int,
                               object_guid: str, error_message: str) -> None:
        """Record validation error in database."""
        
        insert_sql = """
        INSERT INTO tb_validation_errors (
            run_id, path_definition_id, validation_test_id, severity, error_scope,
            error_type, object_type, object_id, object_guid, error_message,
            created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        self.db.execute(insert_sql, [
            run_id, path_def_id, validation_test_id, severity, error_scope,
            error_type, object_type, object_id, object_guid, error_message,
            datetime.now()
        ])
        self.db.commit()
    
    def get_validation_summary(self, run_id: str) -> Dict[str, Any]:
        """Get validation summary for a run."""
        
        summary_query = """
        SELECT 
            severity,
            error_scope,
            error_type,
            COUNT(*) as error_count
        FROM tb_validation_errors
        WHERE run_id = ?
        GROUP BY severity, error_scope, error_type
        ORDER BY 
            CASE severity 
                WHEN 'CRITICAL' THEN 1 
                WHEN 'HIGH' THEN 2 
                WHEN 'MEDIUM' THEN 3 
                ELSE 4 
            END,
            error_scope, error_type
        """
        
        cursor = self.db.execute(summary_query, [run_id])
        error_summary = cursor.fetchall()
        
        # Get total counts by severity
        severity_query = """
        SELECT severity, COUNT(*) as count
        FROM tb_validation_errors
        WHERE run_id = ?
        GROUP BY severity
        """
        
        cursor = self.db.execute(severity_query, [run_id])
        severity_counts = {row[0]: row[1] for row in cursor.fetchall()}
        
        # Get most common errors
        common_errors_query = """
        SELECT error_type, COUNT(*) as count
        FROM tb_validation_errors
        WHERE run_id = ?
        GROUP BY error_type
        ORDER BY count DESC
        LIMIT 10
        """
        
        cursor = self.db.execute(common_errors_query, [run_id])
        common_errors = cursor.fetchall()
        
        return {
            'run_id': run_id,
            'total_errors': sum(severity_counts.values()),
            'severity_breakdown': severity_counts,
            'error_details': [
                {
                    'severity': row[0],
                    'scope': row[1],
                    'type': row[2],
                    'count': row[3]
                }
                for row in error_summary
            ],
            'most_common_errors': [
                {'type': row[0], 'count': row[1]}
                for row in common_errors
            ]
        }
    
    def get_validation_report(self, run_id: str, include_details: bool = False) -> Dict[str, Any]:
        """Generate comprehensive validation report."""
        
        summary = self.get_validation_summary(run_id)
        
        report = {
            'run_id': run_id,
            'generated_at': datetime.now().isoformat(),
            'summary': summary,
            'recommendations': self._generate_validation_recommendations(summary)
        }
        
        if include_details:
            # Get detailed error information
            details_query = """
            SELECT 
                ve.severity, ve.error_scope, ve.error_type, ve.object_type,
                ve.object_id, ve.object_guid, ve.error_message, ve.created_at,
                vt.name as test_name, vt.description as test_description
            FROM tb_validation_errors ve
            JOIN tb_validation_tests vt ON ve.validation_test_id = vt.id
            WHERE ve.run_id = ?
            ORDER BY ve.created_at DESC
            LIMIT 100
            """
            
            cursor = self.db.execute(details_query, [run_id])
            error_details = []
            
            for row in cursor.fetchall():
                error_details.append({
                    'severity': row[0],
                    'scope': row[1],
                    'type': row[2],
                    'object_type': row[3],
                    'object_id': row[4],
                    'object_guid': row[5],
                    'message': row[6],
                    'timestamp': row[7],
                    'test_name': row[8],
                    'test_description': row[9]
                })
            
            report['error_details'] = error_details
        
        return report
    
    def _generate_validation_recommendations(self, summary: Dict[str, Any]) -> List[Dict[str, str]]:
        """Generate recommendations based on validation results."""
        
        recommendations = []
        severity_counts = summary.get('severity_breakdown', {})
        
        # Critical issues
        if severity_counts.get('CRITICAL', 0) > 0:
            recommendations.append({
                'priority': 'URGENT',
                'category': 'CRITICAL_ERRORS',
                'action': 'Immediate investigation required',
                'description': f"{severity_counts['CRITICAL']} critical validation errors found"
            })
        
        # High severity issues
        high_count = severity_counts.get('HIGH', 0)
        if high_count > 10:
            recommendations.append({
                'priority': 'HIGH',
                'category': 'DATA_QUALITY',
                'action': 'Review data quality and PoC configurations',
                'description': f"{high_count} high-severity validation errors suggest systemic data issues"
            })
        
        # Common error patterns
        common_errors = summary.get('most_common_errors', [])
        if common_errors:
            top_error = common_errors[0]
            if top_error['count'] > 5:
                recommendations.append({
                    'priority': 'MEDIUM',
                    'category': 'PATTERN_ANALYSIS',
                    'action': f'Address recurring {top_error["type"]} errors',
                    'description': f"Error type '{top_error['type']}' occurs {top_error['count']} times"
                })
        
        return recommendations