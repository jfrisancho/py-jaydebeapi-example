"""
Comprehensive path validation service.
Validates connectivity, POC completeness, and utility consistency.
"""

import logging
from typing import List, Dict, Optional, Set, Tuple, Any
from dataclasses import dataclass
from enum import Enum
import json

logger = logging.getLogger(__name__)


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


@dataclass
class ValidationError:
    """Represents a validation error"""
    test_id: int
    severity: ValidationSeverity
    error_scope: str
    error_type: str
    object_type: str
    object_id: int
    object_guid: str
    error_message: str
    error_data: Optional[Dict] = None
    object_fab: Optional[str] = None
    object_model_no: Optional[int] = None
    object_data_code: Optional[int] = None
    object_utility_no: Optional[int] = None
    object_markers: Optional[str] = None
    object_flow: Optional[str] = None
    object_is_loopback: Optional[bool] = None
    notes: Optional[str] = None


@dataclass
class ValidationResult:
    """Represents validation results for a path or run"""
    total_tests: int
    passed_tests: int
    failed_tests: int
    critical_errors: int
    high_errors: int
    medium_errors: int
    low_errors: int
    errors: List[ValidationError]
    success_rate: float


@dataclass
class UtilityTransition:
    """Represents a utility transition in a path"""
    from_utility: int
    to_utility: int
    transition_point: str
    is_valid: bool
    reason: Optional[str] = None


class ValidationService:
    """Service for comprehensive path validation"""
    
    def __init__(self, db_connection):
        self.db = db_connection
        self.validation_tests = {}
        self._load_validation_tests()
        
        # Define utility compatibility rules
        self.utility_transitions = self._initialize_utility_transitions()
        
    def validate_run(self, run_id: str) -> ValidationResult:
        """
        Validate all paths in a run.
        
        Args:
            run_id: Run identifier
            
        Returns:
            Validation results for the entire run
        """
        logger.info(f"Starting validation for run {run_id}")
        
        # Get all successful paths for the run
        successful_paths = self._get_successful_paths_for_run(run_id)
        failed_paths = self._get_failed_paths_for_run(run_id)
        
        all_errors = []
        total_tests = 0
        passed_tests = 0
        
        # Validate successful paths
        for path in successful_paths:
            result = self.validate_path(run_id, path)
            all_errors.extend(result.errors)
            total_tests += result.total_tests
            passed_tests += result.passed_tests
            
        # Validate failed paths (connectivity issues)
        for path in failed_paths:
            result = self.validate_failed_path(run_id, path)
            all_errors.extend(result.errors)
            total_tests += result.total_tests
            passed_tests += result.passed_tests
            
        # Store validation errors
        self._store_validation_errors(run_id, all_errors)
        
        # Calculate summary
        failed_tests = total_tests - passed_tests
        severity_counts = self._count_errors_by_severity(all_errors)
        
        result = ValidationResult(
            total_tests=total_tests,
            passed_tests=passed_tests,
            failed_tests=failed_tests,
            critical_errors=severity_counts.get(ValidationSeverity.CRITICAL, 0),
            high_errors=severity_counts.get(ValidationSeverity.HIGH, 0),
            medium_errors=severity_counts.get(ValidationSeverity.MEDIUM, 0),
            low_errors=severity_counts.get(ValidationSeverity.LOW, 0),
            errors=all_errors,
            success_rate=passed_tests / total_tests if total_tests > 0 else 0.0
        )
        
        logger.info(f"Validation completed: {passed_tests}/{total_tests} tests passed, "
                   f"{len(all_errors)} errors found")
        
        return result
        
    def validate_path(self, run_id: str, path: Dict) -> ValidationResult:
        """
        Validate a single successful path.
        
        Args:
            run_id: Run identifier
            path: Path dictionary with details
            
        Returns:
            Validation results for the path
        """
        errors = []
        total_tests = 0
        passed_tests = 0
        
        # Get POC details for the path
        start_poc = self._get_poc_by_node_id(path['start_node_id'])
        end_poc = self._get_poc_by_node_id(path['end_node_id'])
        
        if not start_poc or not end_poc:
            error = ValidationError(
                test_id=self._get_test_id("CONNECTIVITY_POC_EXISTS"),
                severity=ValidationSeverity.CRITICAL,
                error_scope="CONNECTIVITY",
                error_type="POC_NOT_FOUND",
                object_type="PATH",
                object_id=path['id'],
                object_guid=path.get('path_hash', ''),
                error_message=f"POC not found for nodes {path['start_node_id']} or {path['end_node_id']}"
            )
            errors.append(error)
            total_tests += 1
            return ValidationResult(1, 0, 1, 1, 0, 0, 0, errors, 0.0)
            
        # Test 1: POC Completeness Validation
        poc_result = self._validate_poc_completeness(start_poc, end_poc, path)
        errors.extend(poc_result['errors'])
        total_tests += poc_result['total_tests']
        passed_tests += poc_result['passed_tests']
        
        # Test 2: Connectivity Validation
        connectivity_result = self._validate_connectivity(start_poc, end_poc, path)
        errors.extend(connectivity_result['errors'])
        total_tests += connectivity_result['total_tests']
        passed_tests += connectivity_result['passed_tests']
        
        # Test 3: Utility Consistency Validation
        utility_result = self._validate_utility_consistency(start_poc, end_poc, path)
        errors.extend(utility_result['errors'])
        total_tests += utility_result['total_tests']
        passed_tests += utility_result['passed_tests']
        
        # Test 4: Flow Direction Validation
        flow_result = self._validate_flow_direction(start_poc, end_poc, path)
        errors.extend(flow_result['errors'])
        total_tests += flow_result['total_tests']
        passed_tests += flow_result['passed_tests']
        
        # Test 5: Equipment Compatibility Validation
        equipment_result = self._validate_equipment_compatibility(start_poc, end_poc, path)
        errors.extend(equipment_result['errors'])
        total_tests += equipment_result['total_tests']
        passed_tests += equipment_result['passed_tests']
        
        failed_tests = total_tests - passed_tests
        severity_counts = self._count_errors_by_severity(errors)
        
        return ValidationResult(
            total_tests=total_tests,
            passed_tests=passed_tests,
            failed_tests=failed_tests,
            critical_errors=severity_counts.get(ValidationSeverity.CRITICAL, 0),
            high_errors=severity_counts.get(ValidationSeverity.HIGH, 0),
            medium_errors=severity_counts.get(ValidationSeverity.MEDIUM, 0),
            low_errors=severity_counts.get(ValidationSeverity.LOW, 0),
            errors=errors,
            success_rate=passed_tests / total_tests if total_tests > 0 else 0.0
        )
        
    def validate_failed_path(self, run_id: str, path: Dict) -> ValidationResult:
        """
        Validate a failed path to understand why it failed.
        
        Args:
            run_id: Run identifier
            path: Failed path dictionary
            
        Returns:
            Validation results for the failed path
        """
        errors = []
        total_tests = 1
        passed_tests = 0
        
        # Get POC details
        start_poc = self._get_poc_by_node_id(path['start_node_id'])
        end_poc = self._get_poc_by_node_id(path['end_node_id'])
        
        if start_poc and end_poc:
            # Analyze why the path failed
            failure_reason = self._analyze_path_failure(start_poc, end_poc, path)
            
            error = ValidationError(
                test_id=self._get_test_id("CONNECTIVITY_PATH_FAILED"),
                severity=ValidationSeverity.HIGH,
                error_scope="CONNECTIVITY",
                error_type="PATH_NOT_FOUND",
                object_type="PATH",
                object_id=path['id'],
                object_guid=path.get('path_hash', ''),
                error_message=f"No path found between nodes {path['start_node_id']} and {path['end_node_id']}",
                error_data={'failure_reason': failure_reason},
                notes=f"Path failure analysis: {failure_reason}"
            )
            errors.append(error)
        else:
            error = ValidationError(
                test_id=self._get_test_id("CONNECTIVITY_POC_EXISTS"),
                severity=ValidationSeverity.CRITICAL,
                error_scope="CONNECTIVITY",
                error_type="POC_NOT_FOUND",
                object_type="PATH",
                object_id=path['id'],
                object_guid=path.get('path_hash', ''),
                error_message=f"POC not found for failed path nodes {path['start_node_id']} or {path['end_node_id']}"
            )
            errors.append(error)
            
        return ValidationResult(
            total_tests=total_tests,
            passed_tests=passed_tests,
            failed_tests=total_tests - passed_tests,
            critical_errors=1 if errors and errors[0].severity == ValidationSeverity.CRITICAL else 0,
            high_errors=1 if errors and errors[0].severity == ValidationSeverity.HIGH else 0,
            medium_errors=0,
            low_errors=0,
            errors=errors,
            success_rate=0.0
        )
        
    def _validate_poc_completeness(self, start_poc: Dict, end_poc: Dict, path: Dict) -> Dict:
        """Validate that POCs have all required fields"""
        errors = []
        total_tests = 0
        passed_tests = 0
        
        required_fields = ['utility_no', 'markers', 'reference', 'flow']
        
        for poc_name, poc in [('start', start_poc), ('end', end_poc)]:
            for field in required_fields:
                total_tests += 1
                if poc.get(field) is None or poc.get(field) == '':
                    error = ValidationError(
                        test_id=self._get_test_id("POC_COMPLETENESS"),
                        severity=ValidationSeverity.HIGH,
                        error_scope="QA",
                        error_type="MISSING_FIELD",
                        object_type="POC",
                        object_id=poc['id'],
                        object_guid=poc.get('guid', ''),
                        error_message=f"Missing required field '{field}' in {poc_name} POC",
                        object_utility_no=poc.get('utility_no'),
                        object_markers=poc.get('markers'),
                        object_flow=poc.get('flow')
                    )
                    errors.append(error)
                else:
                    passed_tests += 1
                    
        return {
            'errors': errors,
            'total_tests': total_tests,
            'passed_tests': passed_tests
        }
        
    def _validate_connectivity(self, start_poc: Dict, end_poc: Dict, path: Dict) -> Dict:
        """Validate connectivity requirements"""
        errors = []
        total_tests = 2
        passed_tests = 0
        
        # Test 1: POCs should be from different equipment
        if start_poc['equipment_id'] == end_poc['equipment_id']:
            error = ValidationError(
                test_id=self._get_test_id("CONNECTIVITY_DIFFERENT_EQUIPMENT"),
                severity=ValidationSeverity.MEDIUM,
                error_scope="CONNECTIVITY",
                error_type="SAME_EQUIPMENT",
                object_type="PATH",
                object_id=path['id'],
                object_guid=path.get('path_hash', ''),
                error_message="Path connects POCs from the same equipment",
                error_data={'equipment_id': start_poc['equipment_id']}
            )
            errors.append(error)
        else:
            passed_tests += 1
            
        # Test 2: Check if path has valid connections in the database
        has_valid_connection = self._check_equipment_connection_exists(
            start_poc['equipment_id'], end_poc['equipment_id']
        )
        
        if has_valid_connection:
            passed_tests += 1
        else:
            error = ValidationError(
                test_id=self._get_test_id("CONNECTIVITY_VALID_CONNECTION"),
                severity=ValidationSeverity.HIGH,
                error_scope="CONNECTIVITY",
                error_type="NO_VALID_CONNECTION",
                object_type="PATH",
                object_id=path['id'],
                object_guid=path.get('path_hash', ''),
                error_message="No valid connection found between equipment",
                error_data={
                    'start_equipment': start_poc['equipment_id'],
                    'end_equipment': end_poc['equipment_id']
                }
            )
            errors.append(error)
            
        return {
            'errors': errors,
            'total_tests': total_tests,
            'passed_tests': passed_tests
        }
        
    def _validate_utility_consistency(self, start_poc: Dict, end_poc: Dict, path: Dict) -> Dict:
        """Validate utility consistency along the path"""
        errors = []
        total_tests = 1
        passed_tests = 0
        
        start_utility = start_poc.get('utility_no')
        end_utility = end_poc.get('utility_no')
        
        if start_utility is None or end_utility is None:
            # Already handled in completeness validation
            return {'errors': [], 'total_tests': 0, 'passed_tests': 0}
            
        # Check if utility transition is valid
        is_valid_transition = self._is_valid_utility_transition(start_utility, end_utility)
        
        if is_valid_transition:
            passed_tests += 1
        else:
            error = ValidationError(
                test_id=self._get_test_id("UTILITY_CONSISTENCY"),
                severity=ValidationSeverity.HIGH,
                error_scope="FLOW",
                error_type="INVALID_UTILITY_TRANSITION",
                object_type="PATH",
                object_id=path['id'],
                object_guid=path.get('path_hash', ''),
                error_message=f"Invalid utility transition from {start_utility} to {end_utility}",
                error_data={
                    'start_utility': start_utility,
                    'end_utility': end_utility,
                    'transition_rules': self.utility_transitions.get(start_utility, {})
                }
            )
            errors.append(error)
            
        return {
            'errors': errors,
            'total_tests': total_tests,
            'passed_tests': passed_tests
        }
        
    def _validate_flow_direction(self, start_poc: Dict, end_poc: Dict, path: Dict) -> Dict:
        """Validate flow direction compatibility"""
        errors = []
        total_tests = 1
        passed_tests = 0
        
        start_flow = start_poc.get('flow')
        end_flow = end_poc.get('flow')
        
        if start_flow is None or end_flow is None:
            # Already handled in completeness validation
            return {'errors': [], 'total_tests': 0, 'passed_tests': 0}
            
        # Valid flow combinations: OUT -> IN, or bidirectional flows
        valid_combinations = [
            ('OUT', 'IN'),
            ('INOUT', 'IN'),
            ('OUT', 'INOUT'),
            ('INOUT', 'INOUT')
        ]
        
        flow_combination = (start_flow, end_flow)
        
        if flow_combination in valid_combinations:
            passed_tests += 1
        else:
            error = ValidationError(
                test_id=self._get_test_id("FLOW_DIRECTION"),
                severity=ValidationSeverity.MEDIUM,
                error_scope="FLOW",
                error_type="INVALID_FLOW_DIRECTION",
                object_type="PATH",
                object_id=path['id'],
                object_guid=path.get('path_hash', ''),
                error_message=f"Invalid flow direction combination: {start_flow} -> {end_flow}",
                error_data={
                    'start_flow': start_flow,
                    'end_flow': end_flow,
                    'valid_combinations': valid_combinations
                }
            )
            errors.append(error)
            
        return {
            'errors': errors,
            'total_tests': total_tests,
            'passed_tests': passed_tests
        }
        
    def _validate_equipment_compatibility(self, start_poc: Dict, end_poc: Dict, path: Dict) -> Dict:
        """Validate equipment compatibility"""
        errors = []
        total_tests = 1
        passed_tests = 0
        
        # Get equipment details
        start_equipment = self._get_equipment_by_id(start_poc['equipment_id'])
        end_equipment = self._get_equipment_by_id(end_poc['equipment_id'])
        
        if not start_equipment or not end_equipment:
            return {'errors': [], 'total_tests': 0, 'passed_tests': 0}
            
        # Check if equipment are from the same fab and phase (basic compatibility)
        if (start_equipment.get('fab') == end_equipment.get('fab') and
            start_equipment.get('phase_no') == end_equipment.get('phase_no')):
            passed_tests += 1
        else:
            error = ValidationError(
                test_id=self._get_test_id("EQUIPMENT_COMPATIBILITY"),
                severity=ValidationSeverity.LOW,
                error_scope="QA",
                error_type="EQUIPMENT_MISMATCH",
                object_type="PATH",
                object_id=path['id'],
                object_guid=path.get('path_hash', ''),
                error_message="Equipment from different fab or phase",
                error_data={
                    'start_fab': start_equipment.get('fab'),
                    'start_phase': start_equipment.get('phase_no'),
                    'end_fab': end_equipment.get('fab'),
                    'end_phase': end_equipment.get('phase_no')
                }
            )
            errors.append(error)
            
        return {
            'errors': errors,
            'total_tests': total_tests,
            'passed_tests': passed_tests
        }
        
    def _analyze_path_failure(self, start_poc: Dict, end_poc: Dict, path: Dict) -> str:
        """Analyze why a path failed"""
        reasons = []
        
        # Check if POCs are in different networks
        if start_poc.get('fab') != end_poc.get('fab'):
            reasons.append("POCs are in different buildings (fab)")
            
        if start_poc.get('phase_no') != end_poc.get('phase_no'):
            reasons.append("POCs are in different phases")
            
        # Check if equipment have connections defined
        has_connection = self._check_equipment_connection_exists(
            start_poc['equipment_id'], end_poc['equipment_id']
        )
        
        if not has_connection:
            reasons.append("No connection defined between equipment")
            
        # Check for utility incompatibility
        if not self._is_valid_utility_transition(
            start_poc.get('utility_no'), end_poc.get('utility_no')
        ):
            reasons.append("Incompatible utilities")
            
        return "; ".join(reasons) if reasons else "Unknown connectivity issue"
        
    def _load_validation_tests(self):
        """Load validation tests from database"""
        query = "SELECT * FROM tb_validation_tests WHERE is_active = 1"
        cursor = self.db.cursor()
        cursor.execute(query)
        tests = cursor.fetchall()
        
        for test in tests:
            self.validation_tests[test['code']] = test
            
        logger.info(f"Loaded {len(self.validation_tests)} validation tests")
        
    def _get_test_id(self, test_code: str) -> int:
        """Get test ID by code"""
        test = self.validation_tests.get(test_code)
        return test['id'] if test else 0
        
    def _initialize_utility_transitions(self) -> Dict[int, Set[int]]:
        """Initialize utility transition rules"""
        # Example utility transition rules - customize based on your system
        transitions = {
            1: {1, 2},      # Water can transition to water or water vapor
            2: {2, 1, 3},   # Water vapor can transition to water vapor, water, or condensate
            3: {3, 1},      # Condensate can transition to condensate or water
            4: {4, 5},      # CDA can transition to CDA or nitrogen
            5: {5, 4},      # Nitrogen can transition to nitrogen or CDA
            6: {6},         # Chemical A only to Chemical A
            7: {7},         # Chemical B only to Chemical B
        }
        
        return transitions
        
    def _is_valid_utility_transition(self, from_utility: int, to_utility: int) -> bool:
        """Check if utility transition is valid"""
        if from_utility is None or to_utility is None:
            return False
            
        valid_transitions = self.utility_transitions.get(from_utility, set())
        return to_utility in valid_transitions
        
    def _get_poc_by_node_id(self, node_id: int) -> Optional[Dict]:
        """Get POC by node ID"""
        query = """
            SELECT poc.*, eq.toolset, ts.fab, ts.model_no, ts.phase_no
            FROM tb_equipment_pocs poc
            JOIN tb_equipments eq ON poc.equipment_id = eq.id
            JOIN tb_toolsets ts ON eq.toolset = ts.code
            WHERE poc.node_id = ?
        """
        
        cursor = self.db.cursor()
        cursor.execute(query, (node_id,))
        return cursor.fetchone()
        
    def _get_equipment_by_id(self, equipment_id: int) -> Optional[Dict]:
        """Get equipment by ID"""
        query = """
            SELECT eq.*, ts.fab, ts.model_no, ts.phase_no
            FROM tb_equipments eq
            JOIN tb_toolsets ts ON eq.toolset = ts.code
            WHERE eq.id = ?
        """
        
        cursor = self.db.cursor()
        cursor.execute(query, (equipment_id,))
        return cursor.fetchone()
        
    def _check_equipment_connection_exists(self, from_equipment_id: int, to_equipment_id: int) -> bool:
        """Check if a connection exists between equipment"""
        query = """
            SELECT COUNT(*) as count
            FROM tb_equipment_connections
            WHERE (from_equipment_id = ? AND to_equipment_id = ?)
               OR (from_equipment_id = ? AND to_equipment_id = ?)
               AND is_valid = 1
        """
        
        cursor = self.db.cursor()
        cursor.execute(query, (from_equipment_id, to_equipment_id, to_equipment_id, from_equipment_id))
        result = cursor.fetchone()
        return result['count'] > 0
        
    def _get_successful_paths_for_run(self, run_id: str) -> List[Dict]:
        """Get successful paths for a run"""
        query = """
            SELECT pd.*, ap.id as attempt_id, ap.start_node_id, ap.end_node_id, ap.cost
            FROM tb_attempt_paths ap
            JOIN tb_path_definitions pd ON ap.path_definition_id = pd.id
            WHERE ap.run_id = ? AND ap.cost IS NOT NULL
        """
        
        cursor = self.db.cursor()
        cursor.execute(query, (run_id,))
        return cursor.fetchall()
        
    def _get_failed_paths_for_run(self, run_id: str) -> List[Dict]:
        """Get failed paths for a run"""
        query = """
            SELECT pd.*, ap.id as attempt_id, ap.start_node_id, ap.end_node_id
            FROM tb_attempt_paths ap
            JOIN tb_path_definitions pd ON ap.path_definition_id = pd.id
            WHERE ap.run_id = ? AND ap.cost IS NULL
        """
        
        cursor = self.db.cursor()
        cursor.execute(query, (run_id,))
        return cursor.fetchall()
        
    def _store_validation_errors(self, run_id: str, errors: List[ValidationError]):
        """Store validation errors in database"""
        if not errors:
            return
            
        query = """
            INSERT INTO tb_validation_errors (
                run_id, path_definition_id, validation_test_id, severity, error_scope,
                error_type, object_type, object_id, object_guid, object_fab,
                object_model_no, object_data_code, object_utility_no, object_markers,
                object_flow, object_is_loopback, error_message, error_data, notes
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        cursor = self.db.cursor()
        
        for error in errors:
            cursor.execute(query, (
                run_id,
                None,  # path_definition_id - would need to be passed separately
                error.test_id,
                error.severity.value,
                error.error_scope,
                error.error_type,
                error.object_type,
                error.object_id,
                error.object_guid,
                error.object_fab,
                error.object_model_no,
                error.object_data_code,
                error.object_utility_no,
                error.object_markers,
                error.object_flow,
                error.object_is_loopback,
                error.error_message,
                json.dumps(error.error_data) if error.error_data else None,
                error.notes
            ))
            
        self.db.commit()
        logger.info(f"Stored {len(errors)} validation errors for run {run_id}")
        
    def _count_errors_by_severity(self, errors: List[ValidationError]) -> Dict[ValidationSeverity, int]:
        """Count errors by severity"""
        counts = {}
        for error in errors:
            counts[error.severity] = counts.get(error.severity, 0) + 1
        return counts
        
    def get_validation_summary_for_run(self, run_id: str) -> Dict:
        """Get validation summary for a run"""
        query = """
            SELECT 
                severity,
                error_scope,
                error_type,
                COUNT(*) as count
            FROM tb_validation_errors
            WHERE run_id = ?
            GROUP BY severity, error_scope, error_type
            ORDER BY severity, error_scope, error_type
        """
        
        cursor = self.db.cursor()
        cursor.execute(query, (run_id,))
        summary_data = cursor.fetchall()
        
        # Get total counts
        total_query = """
            SELECT 
                COUNT(*) as total_errors,
                COUNT(DISTINCT object_id) as affected_objects
            FROM tb_validation_errors
            WHERE run_id = ?
        """
        
        cursor.execute(total_query, (run_id,))
        totals = cursor.fetchone()
        
        return {
            'run_id': run_id,
            'total_errors': totals['total_errors'],
            'affected_objects': totals['affected_objects'],
            'error_breakdown': summary_data,
            'severity_summary': self._get_severity_summary(run_id)
        }
        
    def _get_severity_summary(self, run_id: str) -> Dict:
        """Get summary by severity"""
        query = """
            SELECT 
                severity,
                COUNT(*) as count
            FROM tb_validation_errors
            WHERE run_id = ?
            GROUP BY severity
        """
        
        cursor = self.db.cursor()
        cursor.execute(query, (run_id,))
        results = cursor.fetchall()
        
        severity_counts = {severity.value: 0 for severity in ValidationSeverity}
        for result in results:
            severity_counts[result['severity']] = result['count']
            
        return severity_counts
        
    def get_critical_errors_for_run(self, run_id: str) -> List[Dict]:
        """Get critical errors for a run"""
        query = """
            SELECT *
            FROM tb_validation_errors
            WHERE run_id = ? AND severity = 'CRITICAL'
            ORDER BY created_at DESC
        """
        
        cursor = self.db.cursor()
        cursor.execute(query, (run_id,))
        return cursor.fetchall()
        
    def generate_validation_report(self, run_id: str) -> Dict:
        """Generate comprehensive validation report"""
        summary = self.get_validation_summary_for_run(run_id)
        critical_errors = self.get_critical_errors_for_run(run_id)
        
        # Calculate quality score
        total_errors = summary['total_errors']
        critical_count = summary['severity_summary'].get('CRITICAL', 0)
        high_count = summary['severity_summary'].get('HIGH', 0)
        
        # Quality score (0-100, lower is better for errors)
        quality_score = max(0, 100 - (critical_count * 10 + high_count * 5 + total_errors))
        
        return {
            'run_id': run_id,
            'summary': summary,
            'critical_errors': critical_errors,
            'quality_score': quality_score,
            'recommendations': self._generate_validation_recommendations(summary),
            'next_steps': self._generate_next_steps(summary, critical_errors)
        }
        
    def _generate_validation_recommendations(self, summary: Dict) -> List[str]:
        """Generate recommendations based on validation results"""
        recommendations = []
        
        critical_count = summary['severity_summary'].get('CRITICAL', 0)
        high_count = summary['severity_summary'].get('HIGH', 0)
        total_errors = summary['total_errors']
        
        if critical_count > 0:
            recommendations.append(f"Address {critical_count} critical errors immediately before proceeding")
            
        if high_count > 0:
            recommendations.append(f"Review and fix {high_count} high-severity errors")
            
        if total_errors > 100:
            recommendations.append("High error count suggests systematic issues - review data quality and validation rules")
            
        # Analyze error patterns
        error_types = {}
        for error in summary['error_breakdown']:
            error_type = error['error_type']
            error_types[error_type] = error_types.get(error_type, 0) + error['count']
            
        # Most common error type
        if error_types:
            most_common = max(error_types.items(), key=lambda x: x[1])
            if most_common[1] > 10:
                recommendations.append(f"Focus on fixing '{most_common[0]}' errors (most common)")
                
        return recommendations
        
    def _generate_next_steps(self, summary: Dict, critical_errors: List[Dict]) -> List[str]:
        """Generate next steps based on validation results"""
        next_steps = []
        
        if critical_errors:
            next_steps.append("1. Fix critical errors before proceeding with analysis")
            next_steps.append("2. Re-run validation after fixes")
            
        if summary['total_errors'] == 0:
            next_steps.append("1. Validation passed - proceed with path analysis")
            next_steps.append("2. Generate coverage reports")
            next_steps.append("3. Export results for review")
        else:
            next_steps.append("1. Review error details and patterns")
            next_steps.append("2. Update data quality or validation rules as needed")
            next_steps.append("3. Re-run sampling if necessary")
            
        return next_steps
        
    def create_default_validation_tests(self):
        """Create default validation tests in the database"""
        default_tests = [
            {
                'code': 'POC_COMPLETENESS',
                'name': 'POC Field Completeness',
                'scope': 'QA',
                'severity': 'HIGH',
                'test_type': 'STRUCTURAL',
                'description': 'Validates that POCs have all required fields populated'
            },
            {
                'code': 'CONNECTIVITY_POC_EXISTS',
                'name': 'POC Existence Check',
                'scope': 'CONNECTIVITY',
                'severity': 'CRITICAL',
                'test_type': 'STRUCTURAL',
                'description': 'Validates that POCs exist for given node IDs'
            },
            {
                'code': 'CONNECTIVITY_DIFFERENT_EQUIPMENT',
                'name': 'Different Equipment Validation',
                'scope': 'CONNECTIVITY',
                'severity': 'MEDIUM',
                'test_type': 'LOGICAL',
                'description': 'Validates that paths connect different equipment'
            },
            {
                'code': 'CONNECTIVITY_VALID_CONNECTION',
                'name': 'Valid Connection Check',
                'scope': 'CONNECTIVITY',
                'severity': 'HIGH',
                'test_type': 'STRUCTURAL',
                'description': 'Validates that equipment have defined connections'
            },
            {
                'code': 'CONNECTIVITY_PATH_FAILED',
                'name': 'Path Failure Analysis',
                'scope': 'CONNECTIVITY',
                'severity': 'HIGH',
                'test_type': 'PERFORMANCE',
                'description': 'Analyzes why paths failed to find routes'
            },
            {
                'code': 'UTILITY_CONSISTENCY',
                'name': 'Utility Consistency Check',
                'scope': 'FLOW',
                'severity': 'HIGH',
                'test_type': 'LOGICAL',
                'description': 'Validates utility transitions along paths'
            },
            {
                'code': 'FLOW_DIRECTION',
                'name': 'Flow Direction Validation',
                'scope': 'FLOW',
                'severity': 'MEDIUM',
                'test_type': 'LOGICAL',
                'description': 'Validates flow direction compatibility'
            },
            {
                'code': 'EQUIPMENT_COMPATIBILITY',
                'name': 'Equipment Compatibility Check',
                'scope': 'QA',
                'severity': 'LOW',
                'test_type': 'LOGICAL',
                'description': 'Validates equipment compatibility (fab, phase)'
            }
        ]
        
        cursor = self.db.cursor()
        
        for test in default_tests:
            # Check if test already exists
            cursor.execute("SELECT id FROM tb_validation_tests WHERE code = ?", (test['code'],))
            if cursor.fetchone():
                continue
                
            # Insert new test
            query = """
                INSERT INTO tb_validation_tests (
                    code, name, scope, severity, test_type, is_active, description
                ) VALUES (?, ?, ?, ?, ?, 1, ?)
            """
            
            cursor.execute(query, (
                test['code'],
                test['name'],
                test['scope'],
                test['severity'],
                test['test_type'],
                test['description']
            ))
            
        self.db.commit()
        logger.info(f"Created {len(default_tests)} default validation tests")
        
        # Reload tests
        self._load_validation_tests()
        
    def add_custom_validation_test(self, code: str, name: str, scope: str, 
                                 severity: str, test_type: str, description: str):
        """Add a custom validation test"""
        query = """
            INSERT INTO tb_validation_tests (
                code, name, scope, severity, test_type, is_active, description
            ) VALUES (?, ?, ?, ?, ?, 1, ?)
        """
        
        cursor = self.db.cursor()
        cursor.execute(query, (code, name, scope, severity, test_type, description))
        self.db.commit()
        
        # Reload tests
        self._load_validation_tests()
        
        logger.info(f"Added custom validation test: {code}")
        
    def update_utility_transitions(self, transitions: Dict[int, Set[int]]):
        """Update utility transition rules"""
        self.utility_transitions = transitions
        logger.info("Updated utility transition rules")
        
    def validate_single_poc_pair(self, from_poc_id: int, to_poc_id: int) -> ValidationResult:
        """
        Validate a single POC pair without requiring a full run.
        
        Args:
            from_poc_id: Starting POC ID
            to_poc_id: Ending POC ID
            
        Returns:
            Validation results for the POC pair
        """
        # Create a mock path for validation
        mock_path = {
            'id': 0,
            'start_node_id': from_poc_id,
            'end_node_id': to_poc_id,
            'path_hash': f"mock_{from_poc_id}_{to_poc_id}"
        }
        
        # Get POC details
        start_poc = self._get_poc_by_node_id(from_poc_id)
        end_poc = self._get_poc_by_node_id(to_poc_id)
        
        if not start_poc or not end_poc:
            error = ValidationError(
                test_id=self._get_test_id("CONNECTIVITY_POC_EXISTS"),
                severity=ValidationSeverity.CRITICAL,
                error_scope="CONNECTIVITY",
                error_type="POC_NOT_FOUND",
                object_type="POC_PAIR",
                object_id=0,
                object_guid="",
                error_message=f"POC not found: {from_poc_id} or {to_poc_id}"
            )
            return ValidationResult(1, 0, 1, 1, 0, 0, 0, [error], 0.0)
            
        # Use the existing validation logic
        return self.validate_path("test_run", mock_path)
        
    def get_validation_statistics(self) -> Dict:
        """Get overall validation statistics across all runs"""
        query = """
            SELECT 
                COUNT(DISTINCT run_id) as total_runs,
                COUNT(*) as total_errors,
                AVG(CASE WHEN severity = 'CRITICAL' THEN 1.0 ELSE 0.0 END) as critical_rate,
                AVG(CASE WHEN severity = 'HIGH' THEN 1.0 ELSE 0.0 END) as high_rate
            FROM tb_validation_errors
        """
        
        cursor = self.db.cursor()
        cursor.execute(query)
        stats = cursor.fetchone()
        
        # Get error trends by date
        trend_query = """
            SELECT 
                DATE(created_at) as error_date,
                COUNT(*) as error_count,
                COUNT(DISTINCT run_id) as runs_count
            FROM tb_validation_errors
            WHERE created_at >= DATE('now', '-30 days')
            GROUP BY DATE(created_at)
            ORDER BY error_date
        """
        
        cursor.execute(trend_query)
        trends = cursor.fetchall()
        
        return {
            'overall_stats': stats,
            'recent_trends': trends,
            'active_tests': len(self.validation_tests)
        }