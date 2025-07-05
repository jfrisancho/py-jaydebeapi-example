import logging
import json
from typing import Optional, List, Dict, Any, Set, Tuple
from dataclasses import dataclass
from datetime import datetime
from enum import Enum


class ValidationSeverity(Enum):
    LOW = 'LOW'
    MEDIUM = 'MEDIUM'
    HIGH = 'HIGH'
    CRITICAL = 'CRITICAL'


class ValidationScope(Enum):
    CONNECTIVITY = 'CONNECTIVITY'
    FLOW = 'FLOW'
    MATERIAL = 'MATERIAL'
    QA = 'QA'
    SCENARIO = 'SCENARIO'


class TestType(Enum):
    STRUCTURAL = 'STRUCTURAL'
    LOGICAL = 'LOGICAL'
    PERFORMANCE = 'PERFORMANCE'
    COMPLIANCE = 'COMPLIANCE'


@dataclass
class ValidationTest:
    """Represents a validation test definition."""
    id: int
    code: str
    name: str
    scope: ValidationScope
    severity: ValidationSeverity
    test_type: TestType
    is_active: bool
    description: str


@dataclass
class ValidationError:
    """Represents a validation error found during testing."""
    id: Optional[int]
    run_id: str
    path_definition_id: Optional[int]
    validation_test_id: Optional[int]
    severity: ValidationSeverity
    error_scope: str
    error_type: str
    object_type: str
    object_id: int
    object_guid: str
    error_message: str
    error_data: Optional[Dict[str, Any]]
    created_at: datetime


@dataclass
class ValidationResults:
    """Summary of validation results for a run."""
    total_errors: int
    total_reviews: int
    critical_errors: int
    errors_by_severity: Dict[str, int]
    errors_by_scope: Dict[str, int]
    errors_by_type: Dict[str, int]
    paths_with_errors: int
    validation_success_rate: float


class ValidationManager:
    """Manages comprehensive path validation and quality assurance."""
    
    def __init__(self, db_connection):
        self.db = db_connection
        self.logger = logging.getLogger(__name__)
        self._validation_tests = {}  # Cache for validation tests
        self._utility_flow_rules = {}  # Cache for utility flow rules
        
    def validate_run_paths(self, run_id: str) -> ValidationResults:
        """Validate all paths found in a run."""
        self.logger.info(f'Starting validation for run {run_id}')
        
        # Load validation tests
        self._load_validation_tests()
        
        # Get all paths for the run
        paths = self._get_run_paths(run_id)
        
        total_errors = 0
        total_reviews = 0
        critical_errors = 0
        errors_by_severity = {}
        errors_by_scope = {}
        errors_by_type = {}
        paths_with_errors = set()
        
        for path in paths:
            path_errors = self._validate_single_path(run_id, path)
            
            if path_errors:
                paths_with_errors.add(path['path_definition_id'])
                
                for error in path_errors:
                    total_errors += 1
                    
                    if error.severity == ValidationSeverity.CRITICAL:
                        critical_errors += 1
                    
                    # Count by severity
                    severity_key = error.severity.value
                    errors_by_severity[severity_key] = errors_by_severity.get(severity_key, 0) + 1
                    
                    # Count by scope
                    errors_by_scope[error.error_scope] = errors_by_scope.get(error.error_scope, 0) + 1
                    
                    # Count by type
                    errors_by_type[error.error_type] = errors_by_type.get(error.error_type, 0) + 1
        
        # Calculate success rate
        total_paths = len(paths)
        paths_without_errors = total_paths - len(paths_with_errors)
        success_rate = paths_without_errors / total_paths if total_paths > 0 else 1.0
        
        # Count review flags
        total_reviews = self._count_review_flags(run_id)
        
        results = ValidationResults(
            total_errors=total_errors,
            total_reviews=total_reviews,
            critical_errors=critical_errors,
            errors_by_severity=errors_by_severity,
            errors_by_scope=errors_by_scope,
            errors_by_type=errors_by_type,
            paths_with_errors=len(paths_with_errors),
            validation_success_rate=success_rate
        )
        
        self.logger.info(f'Validation completed: {total_errors} errors, {critical_errors} critical')
        return results
    
    def _validate_single_path(self, run_id: str, path: Dict[str, Any]) -> List[ValidationError]:
        """Validate a single path and return any errors found."""
        errors = []
        path_def_id = path['path_definition_id']
        
        # Get detailed path information
        path_details = self._get_path_details(path_def_id)
        if not path_details:
            return errors
        
        # Run connectivity validation tests
        errors.extend(self._validate_connectivity(run_id, path, path_details))
        
        # Run utility consistency validation tests
        errors.extend(self._validate_utility_consistency(run_id, path, path_details))
        
        # Run structural validation tests
        errors.extend(self._validate_path_structure(run_id, path, path_details))
        
        # Run performance validation tests
        errors.extend(self._validate_path_performance(run_id, path, path_details))
        
        return errors
    
    def _validate_connectivity(self, run_id: str, path: Dict[str, Any], 
                             path_details: Dict[str, Any]) -> List[ValidationError]:
        """Validate connectivity requirements for the path."""
        errors = []
        path_context = path_details.get('path_context', {})
        
        # Test CON_001: Validate PoC completeness
        start_poc_id = path_context.get('start_poc_id')
        end_poc_id = path_context.get('end_poc_id')
        
        if start_poc_id:
            start_poc = self._get_poc_details(start_poc_id)
            if not self._validate_poc_completeness(start_poc):
                errors.append(self._create_validation_error(
                    run_id=run_id,
                    path_def_id=path['path_definition_id'],
                    test_code='CON_001',
                    severity=ValidationSeverity.HIGH,
                    error_scope='CONNECTIVITY',
                    error_type='INCOMPLETE_POC',
                    object_type='POC',
                    object_id=start_poc['node_id'],
                    object_guid=f'poc_{start_poc_id}',
                    message='Start PoC missing required attributes',
                    error_data={'poc_id': start_poc_id, 'missing_attrs': self._get_missing_attributes(start_poc)}
                ))
        
        if end_poc_id:
            end_poc = self._get_poc_details(end_poc_id)
            if not self._validate_poc_completeness(end_poc):
                errors.append(self._create_validation_error(
                    run_id=run_id,
                    path_def_id=path['path_definition_id'],
                    test_code='CON_001',
                    severity=ValidationSeverity.HIGH,
                    error_scope='CONNECTIVITY',
                    error_type='INCOMPLETE_POC',
                    object_type='POC',
                    object_id=end_poc['node_id'],
                    object_guid=f'poc_{end_poc_id}',
                    message='End PoC missing required attributes',
                    error_data={'poc_id': end_poc_id, 'missing_attrs': self._get_missing_attributes(end_poc)}
                ))
        
        # Test CON_002: Validate path continuity
        path_nodes = path_context.get('nodes_sequence', [])
        path_links = path_context.get('links_sequence', [])
        
        if not self._validate_path_continuity(path_nodes, path_links):
            errors.append(self._create_validation_error(
                run_id=run_id,
                path_def_id=path['path_definition_id'],
                test_code='CON_002',
                severity=ValidationSeverity.CRITICAL,
                error_scope='CONNECTIVITY',
                error_type='PATH_DISCONTINUITY',
                object_type='PATH',
                object_id=path['path_definition_id'],
                object_guid=path_details['path_hash'],
                message='Path has connectivity gaps',
                error_data={'nodes': path_nodes, 'links': path_links}
            ))
        
        # Test CON_003: Validate bidirectional links
        invalid_links = self._validate_bidirectional_links(path_links)
        for link_id in invalid_links:
            errors.append(self._create_validation_error(
                run_id=run_id,
                path_def_id=path['path_definition_id'],
                test_code='CON_003',
                severity=ValidationSeverity.MEDIUM,
                error_scope='CONNECTIVITY',
                error_type='INVALID_DIRECTION',
                object_type='LINK',
                object_id=link_id,
                object_guid=f'link_{link_id}',
                message='Link direction inconsistent with path traversal',
                error_data={'link_id': link_id}
            ))
        
        return errors
    
    def _validate_utility_consistency(self, run_id: str, path: Dict[str, Any],
                                    path_details: Dict[str, Any]) -> List[ValidationError]:
        """Validate utility consistency along the path."""
        errors = []
        path_context = path_details.get('path_context', {})
        
        # Get utility sequence along the path
        utility_sequence = self._get_path_utility_sequence(path_context.get('nodes_sequence', []))
        
        # Test UTY_001: Validate utility transitions
        invalid_transitions = self._validate_utility_transitions(utility_sequence)
        for transition in invalid_transitions:
            errors.append(self._create_validation_error(
                run_id=run_id,
                path_def_id=path['path_definition_id'],
                test_code='UTY_001',
                severity=ValidationSeverity.HIGH,
                error_scope='UTILITY',
                error_type='INVALID_TRANSITION',
                object_type='NODE',
                object_id=transition['node_id'],
                object_guid=f'node_{transition["node_id"]}',
                message=f'Invalid utility transition: {transition["from_utility"]} -> {transition["to_utility"]}',
                error_data=transition
            ))
        
        # Test UTY_002: Validate PoC utility consistency
        start_poc_id = path_context.get('start_poc_id')
        end_poc_id = path_context.get('end_poc_id')
        
        if start_poc_id and end_poc_id:
            start_poc = self._get_poc_details(start_poc_id)
            end_poc = self._get_poc_details(end_poc_id)
            
            if not self._validate_poc_utility_consistency(start_poc, end_poc, utility_sequence):
                errors.append(self._create_validation_error(
                    run_id=run_id,
                    path_def_id=path['path_definition_id'],
                    test_code='UTY_002',
                    severity=ValidationSeverity.MEDIUM,
                    error_scope='UTILITY',
                    error_type='POC_UTILITY_MISMATCH',
                    object_type='PATH',
                    object_id=path['path_definition_id'],
                    object_guid=path_details['path_hash'],
                    message='PoC utilities inconsistent with path utilities',
                    error_data={
                        'start_poc_utility': start_poc.get('utility_no'),
                        'end_poc_utility': end_poc.get('utility_no'),
                        'path_utilities': list(set(utility_sequence))
                    }
                ))
        
        return errors
    
    def _validate_path_structure(self, run_id: str, path: Dict[str, Any],
                                path_details: Dict[str, Any]) -> List[ValidationError]:
        """Validate structural aspects of the path."""
        errors = []
        
        # Test STR_001: Validate path length reasonableness
        total_length = path_details.get('total_length_mm', 0)
        if total_length > 1000000:  # > 1km seems unreasonable for equipment connections
            errors.append(self._create_validation_error(
                run_id=run_id,
                path_def_id=path['path_definition_id'],
                test_code='STR_001',
                severity=ValidationSeverity.MEDIUM,
                error_scope='STRUCTURAL',
                error_type='EXCESSIVE_LENGTH',
                object_type='PATH',
                object_id=path['path_definition_id'],
                object_guid=path_details['path_hash'],
                message=f'Path length ({total_length/1000:.1f}m) exceeds reasonable limits',
                error_data={'length_mm': total_length}
            ))
        
        # Test STR_002: Validate node count reasonableness
        node_count = path_details.get('node_count', 0)
        if node_count > 100:  # More than 100 nodes seems excessive
            errors.append(self._create_validation_error(
                run_id=run_id,
                path_def_id=path['path_definition_id'],
                test_code='STR_002',
                severity=ValidationSeverity.LOW,
                error_scope='STRUCTURAL',
                error_type='EXCESSIVE_COMPLEXITY',
                object_type='PATH',
                object_id=path['path_definition_id'],
                object_guid=path_details['path_hash'],
                message=f'Path has excessive complexity ({node_count} nodes)',
                error_data={'node_count': node_count}
            ))
        
        # Test STR_003: Validate minimum path length
        if total_length < 100:  # Less than 10cm seems too short
            errors.append(self._create_validation_error(
                run_id=run_id,
                path_def_id=path['path_definition_id'],
                test_code='STR_003',
                severity=ValidationSeverity.LOW,
                error_scope='STRUCTURAL',
                error_type='INSUFFICIENT_LENGTH',
                object_type='PATH',
                object_id=path['path_definition_id'],
                object_guid=path_details['path_hash'],
                message=f'Path length ({total_length}mm) seems too short',
                error_data={'length_mm': total_length}
            ))
        
        return errors
    
    def _validate_path_performance(self, run_id: str, path: Dict[str, Any],
                                 path_details: Dict[str, Any]) -> List[ValidationError]:
        """Validate performance aspects of the path."""
        errors = []
        
        # Test PER_001: Validate path cost reasonableness
        total_cost = path.get('cost', 0)
        total_length = path_details.get('total_length_mm', 1)
        cost_per_mm = total_cost / total_length if total_length > 0 else 0
        
        if cost_per_mm > 10:  # Cost per mm seems excessive
            errors.append(self._create_validation_error(
                run_id=run_id,
                path_def_id=path['path_definition_id'],
                test_code='PER_001',
                severity=ValidationSeverity.LOW,
                error_scope='PERFORMANCE',
                error_type='HIGH_COST_RATIO',
                object_type='PATH',
                object_id=path['path_definition_id'],
                object_guid=path_details['path_hash'],
                message=f'Path has high cost-to-length ratio ({cost_per_mm:.2f})',
                error_data={'cost': total_cost, 'length_mm': total_length, 'ratio': cost_per_mm}
            ))
        
        return errors
    
    def _validate_poc_completeness(self, poc: Dict[str, Any]) -> bool:
        """Validate that a PoC has all required attributes."""
        required_attrs = ['utility_no', 'markers', 'reference']
        
        for attr in required_attrs:
            if not poc.get(attr):
                return False
        
        return True
    
    def _get_missing_attributes(self, poc: Dict[str, Any]) -> List[str]:
        """Get list of missing required attributes for a PoC."""
        required_attrs = ['utility_no', 'markers', 'reference']
        missing = []
        
        for attr in required_attrs:
            if not poc.get(attr):
                missing.append(attr)
        
        return missing
    
    def _validate_path_continuity(self, path_nodes: List[int], path_links: List[int]) -> bool:
        """Validate that the path is continuous (all links connect properly)."""
        if len(path_nodes) < 2 or len(path_links) != len(path_nodes) - 1:
            return False
        
        # Get link connectivity data
        link_connections = self._get_link_connections(path_links)
        
        # Check that each consecutive pair of nodes is connected by the corresponding link
        for i in range(len(path_links)):
            current_node = path_nodes[i]
            next_node = path_nodes[i + 1]
            link_id = path_links[i]
            
            link_data = link_connections.get(link_id)
            if not link_data:
                return False
            
            # Check if link connects the two nodes
            if not ((link_data['start_node_id'] == current_node and link_data['end_node_id'] == next_node) or
                    (link_data['end_node_id'] == current_node and link_data['start_node_id'] == next_node and 
                     link_data['bidirected'] == 'Y')):
                return False
        
        return True
    
    def _validate_bidirectional_links(self, path_links: List[int]) -> List[int]:
        """Validate bidirectional link usage and return invalid link IDs."""
        invalid_links = []
        
        # Get link data
        link_data = self._get_link_connections(path_links)
        
        # For now, just flag unidirectional links that might cause issues
        for link_id, data in link_data.items():
            if data['bidirected'] != 'Y':
                # Could add more sophisticated validation here
                pass
        
        return invalid_links
    
    def _get_path_utility_sequence(self, path_nodes: List[int]) -> List[Optional[int]]:
        """Get the utility sequence along the path nodes."""
        if not path_nodes:
            return []
        
        placeholders = ','.join(['%s'] * len(path_nodes))
        query = f'''
            SELECT id, utility_no 
            FROM nw_nodes 
            WHERE id IN ({placeholders})
            ORDER BY FIELD(id, {placeholders})
        '''
        
        with self.db.cursor() as cursor:
            cursor.execute(query, path_nodes + path_nodes)
            results = cursor.fetchall()
            
        # Create ordered sequence
        node_utilities = {row['id']: row['utility_no'] for row in results}
        return [node_utilities.get(node_id) for node_id in path_nodes]
    
    def _validate_utility_transitions(self, utility_sequence: List[Optional[int]]) -> List[Dict[str, Any]]:
        """Validate utility transitions along the path."""
        invalid_transitions = []
        
        # Define valid utility transitions (simplified for this iteration)
        # In a full implementation, this would come from a utility flow rules table
        valid_transitions = {
            # Water utilities
            (1, 1): True,  # Water to water
            (1, 2): True,  # Water to steam
            (2, 1): True,  # Steam to condensate/water
            (2, 2): True,  # Steam to steam
            
            # Gas utilities
            (10, 10): True,  # N2 to N2
            (11, 11): True,  # CDA to CDA
            
            # Special cases
            (None, None): True,  # Unknown to unknown
        }
        
        for i in range(len(utility_sequence) - 1):
            current_utility = utility_sequence[i]
            next_utility = utility_sequence[i + 1]
            
            # Skip if either utility is unknown
            if current_utility is None or next_utility is None:
                continue
            
            transition = (current_utility, next_utility)
            if transition not in valid_transitions:
                invalid_transitions.append({
                    'node_index': i + 1,
                    'node_id': None,  # Would need to track node IDs
                    'from_utility': current_utility,
                    'to_utility': next_utility
                })
        
        return invalid_transitions
    
    def _validate_poc_utility_consistency(self, start_poc: Dict[str, Any], 
                                        end_poc: Dict[str, Any],
                                        utility_sequence: List[Optional[int]]) -> bool:
        """Validate that PoC utilities are consistent with path utilities."""
        start_utility = start_poc.get('utility_no')
        end_utility = end_poc.get('utility_no')
        
        # Get unique utilities in the path
        path_utilities = set(u for u in utility_sequence if u is not None)
        
        # Check if PoC utilities are represented in the path
        if start_utility and start_utility not in path_utilities:
            return False
        
        if end_utility and end_utility not in path_utilities:
            return False
        
        return True
    
    def _get_run_paths(self, run_id: str) -> List[Dict[str, Any]]:
        """Get all paths for a run that have been successfully found."""
        query = '''
            SELECT ap.path_definition_id, ap.start_node_id, ap.end_node_id, 
                   ap.cost, ap.picked_at, ap.tested_at
            FROM tb_attempt_paths ap
            WHERE ap.run_id = %s AND ap.path_definition_id IS NOT NULL
        '''
        
        with self.db.cursor() as cursor:
            cursor.execute(query, (run_id,))
            return cursor.fetchall()
    
    def _get_path_details(self, path_def_id: int) -> Optional[Dict[str, Any]]:
        """Get detailed path information."""
        query = '''
            SELECT path_hash, node_count, link_count, total_length_mm,
                   path_context, data_codes_scope, utilities_scope, references_scope
            FROM tb_path_definitions
            WHERE id = %s
        '''
        
        with self.db.cursor() as cursor:
            cursor.execute(query, (path_def_id,))
            result = cursor.fetchone()
            
            if result:
                # Parse JSON fields
                result['path_context'] = json.loads(result['path_context']) if result['path_context'] else {}
                result['data_codes'] = json.loads(result['data_codes_scope']) if result['data_codes_scope'] else []
                result['utilities'] = json.loads(result['utilities_scope']) if result['utilities_scope'] else []
                result['references'] = json.loads(result['references_scope']) if result['references_scope'] else []
                
            return result
    
    def _get_poc_details(self, poc_id: int) -> Dict[str, Any]:
        """Get detailed PoC information."""
        query = '''
            SELECT id, equipment_id, node_id, utility_no, markers, reference, 
                   flow, is_loopback, is_used
            FROM tb_equipment_pocs
            WHERE id = %s
        '''
        
        with self.db.cursor() as cursor:
            cursor.execute(query, (poc_id,))
            return cursor.fetchone() or {}
    
    def _get_link_connections(self, link_ids: List[int]) -> Dict[int, Dict[str, Any]]:
        """Get connection information for links."""
        if not link_ids:
            return {}
        
        placeholders = ','.join(['%s'] * len(link_ids))
        query = f'''
            SELECT id, start_node_id, end_node_id, bidirected, cost
            FROM nw_links
            WHERE id IN ({placeholders})
        '''
        
        with self.db.cursor() as cursor:
            cursor.execute(query, link_ids)
            return {row['id']: row for row in cursor.fetchall()}
    
    def _load_validation_tests(self):
        """Load validation test definitions from database."""
        if self._validation_tests:
            return  # Already loaded
        
        query = '''
            SELECT id, code, name, scope, severity, test_type, is_active, description
            FROM tb_validation_tests
            WHERE is_active = 1
        '''
        
        with self.db.cursor() as cursor:
            cursor.execute(query)
            tests = cursor.fetchall()
            
            for test in tests:
                self._validation_tests[test['code']] = ValidationTest(
                    id=test['id'],
                    code=test['code'],
                    name=test['name'],
                    scope=ValidationScope(test['scope']),
                    severity=ValidationSeverity(test['severity']),
                    test_type=TestType(test['test_type']),
                    is_active=test['is_active'],
                    description=test['description']
                )
    
    def _create_validation_error(self, run_id: str, path_def_id: Optional[int],
                               test_code: str, severity: ValidationSeverity,
                               error_scope: str, error_type: str, object_type: str,
                               object_id: int, object_guid: str, message: str,
                               error_data: Optional[Dict[str, Any]] = None) -> ValidationError:
        """Create and store a validation error."""
        
        # Get additional object information for context
        object_info = self._get_object_context(object_type, object_id)
        
        error = ValidationError(
            id=None,
            run_id=run_id,
            path_definition_id=path_def_id,
            validation_test_id=self._validation_tests.get(test_code, {}).get('id') if test_code in self._validation_tests else None,
            severity=severity,
            error_scope=error_scope,
            error_type=error_type,
            object_type=object_type,
            object_id=object_id,
            object_guid=object_guid,
            error_message=message,
            error_data=error_data,
            created_at=datetime.now()
        )
        
        # Store error in database
        error.id = self._store_validation_error(error, object_info)
        
        return error
    
    def _store_validation_error(self, error: ValidationError, object_info: Dict[str, Any]) -> int:
        """Store validation error in database."""
        query = '''
            INSERT INTO tb_validation_errors (
                run_id, path_definition_id, validation_test_id, severity, error_scope, error_type,
                object_type, object_id, object_guid, object_fab, object_model_no, object_data_code,
                object_e2e_group_no, object_markers, object_utility_no, object_item_no, object_type_no,
                object_material_no, object_flow, object_is_loopback, object_cost,
                error_message, error_data, created_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
        '''
        
        with self.db.cursor() as cursor:
            cursor.execute(query, (
                error.run_id, error.path_definition_id, error.validation_test_id,
                error.severity.value, error.error_scope, error.error_type,
                error.object_type, error.object_id, error.object_guid,
                object_info.get('fab'), object_info.get('model_no'), object_info.get('data_code'),
                object_info.get('e2e_group_no'), object_info.get('markers'), object_info.get('utility_no'),
                object_info.get('item_no'), object_info.get('type_no'), object_info.get('material_no'),
                object_info.get('flow'), object_info.get('is_loopback'), object_info.get('cost'),
                error.error_message, json.dumps(error.error_data) if error.error_data else None,
                error.created_at
            ))
            self.db.commit()
            
            # Get inserted ID
            cursor.execute('SELECT LAST_INSERT_ID()')
            return cursor.fetchone()['LAST_INSERT_ID()']
    
    def _get_object_context(self, object_type: str, object_id: int) -> Dict[str, Any]:
        """Get additional context information for an object."""
        context = {}
        
        if object_type == 'NODE':
            query = '''
                SELECT data_code, utility_no, e2e_group_no, markers, item_no, nwo_type_no
                FROM nw_nodes WHERE id = %s
            '''
            with self.db.cursor() as cursor:
                cursor.execute(query, (object_id,))
                result = cursor.fetchone()
                if result:
                    context.update({
                        'data_code': result['data_code'],
                        'utility_no': result['utility_no'],
                        'e2e_group_no': result['e2e_group_no'],
                        'markers': result['markers'],
                        'item_no': result['item_no'],
                        'type_no': result['nwo_type_no']
                    })
        
        elif object_type == 'LINK':
            query = '''
                SELECT cost, nwo_type_no FROM nw_links WHERE id = %s
            '''
            with self.db.cursor() as cursor:
                cursor.execute(query, (object_id,))
                result = cursor.fetchone()
                if result:
                    context.update({
                        'cost': result['cost'],
                        'type_no': result['nwo_type_no']
                    })
        
        elif object_type == 'POC':
            query = '''
                SELECT ep.utility_no, ep.markers, ep.reference, ep.flow, ep.is_loopback,
                       t.fab, t.model_no
                FROM tb_equipment_pocs ep
                JOIN tb_equipments e ON ep.equipment_id = e.id
                JOIN tb_toolsets t ON e.toolset = t.code
                WHERE ep.node_id = %s
            '''
            with self.db.cursor() as cursor:
                cursor.execute(query, (object_id,))
                result = cursor.fetchone()
                if result:
                    context.update({
                        'utility_no': result['utility_no'],
                        'markers': result['markers'],
                        'flow': result['flow'],
                        'is_loopback': result['is_loopback'],
                        'fab': result['fab'],
                        'model_no': result['model_no']
                    })
        
        return context
    
    def _count_review_flags(self, run_id: str) -> int:
        """Count review flags for a run."""
        query = '''
            SELECT COUNT(*) as flag_count
            FROM tb_review_flags
            WHERE run_id = %s
        '''
        
        with self.db.cursor() as cursor:
            cursor.execute(query, (run_id,))
            result = cursor.fetchone()
            return result['flag_count'] if result else 0
    
    def flag_for_review(self, run_id: str, flag_type: str, severity: str, reason: str,
                       object_type: str, object_id: int, object_guid: str,
                       notes: Optional[str] = None, assigned_to: Optional[str] = None) -> int:
        """Flag an object for manual review."""
        
        # Get object context
        object_info = self._get_object_context(object_type, object_id)
        
        query = '''
            INSERT INTO tb_review_flags (
                run_id, flag_type, severity, reason, object_type, object_id, object_guid,
                object_fab, object_model_no, object_data_code, object_e2e_group_no,
                object_markers, object_utility_no, object_item_no, object_type_no,
                object_material_no, object_flow, object_is_loopback, object_cost,
                status, assigned_to, created_at, notes
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
        '''
        
        with self.db.cursor() as cursor:
            cursor.execute(query, (
                run_id, flag_type, severity, reason, object_type, object_id, object_guid,
                object_info.get('fab'), object_info.get('model_no'), object_info.get('data_code'),
                object_info.get('e2e_group_no'), object_info.get('markers'), object_info.get('utility_no'),
                object_info.get('item_no'), object_info.get('type_no'), object_info.get('material_no'),
                object_info.get('flow'), object_info.get('is_loopback'), object_info.get('cost'),
                'OPEN', assigned_to, datetime.now(), notes
            ))
            self.db.commit()
            
            # Get inserted ID
            cursor.execute('SELECT LAST_INSERT_ID()')
            return cursor.fetchone()['LAST_INSERT_ID()']
    
    def get_validation_errors(self, run_id: str, severity: Optional[ValidationSeverity] = None,
                            limit: int = 100) -> List[Dict[str, Any]]:
        """Get validation errors for a run with optional filtering."""
        query = '''
            SELECT ve.*, vt.name as test_name, vt.description as test_description
            FROM tb_validation_errors ve
            LEFT JOIN tb_validation_tests vt ON ve.validation_test_id = vt.id
            WHERE ve.run_id = %s
        '''
        
        params = [run_id]
        if severity:
            query += ' AND ve.severity = %s'
            params.append(severity.value)
        
        query += ' ORDER BY ve.created_at DESC LIMIT %s'
        params.append(limit)
        
        with self.db.cursor() as cursor:
            cursor.execute(query, params)
            errors = cursor.fetchall()
            
            # Parse JSON error_data fields
            for error in errors:
                if error['error_data']:
                    error['error_data'] = json.loads(error['error_data'])
            
            return errors
    
    def get_review_flags(self, run_id: str, status: Optional[str] = None,
                        limit: int = 100) -> List[Dict[str, Any]]:
        """Get review flags for a run with optional filtering."""
        query = '''
            SELECT * FROM tb_review_flags
            WHERE run_id = %s
        '''
        
        params = [run_id]
        if status:
            query += ' AND status = %s'
            params.append(status)
        
        query += ' ORDER BY created_at DESC LIMIT %s'
        params.append(limit)
        
        with self.db.cursor() as cursor:
            cursor.execute(query, params)
            flags = cursor.fetchall()
            
            # Parse JSON fields
            for flag in flags:
                if flag['path_context']:
                    flag['path_context'] = json.loads(flag['path_context'])
                if flag['flag_data']:
                    flag['flag_data'] = json.loads(flag['flag_data'])
            
            return flags
    
    def resolve_review_flag(self, flag_id: int, resolution_notes: str, resolved_by: str):
        """Mark a review flag as resolved."""
        query = '''
            UPDATE tb_review_flags 
            SET status = 'RESOLVED', resolved_at = %s, resolution_notes = %s
            WHERE id = %s
        '''
        
        with self.db.cursor() as cursor:
            cursor.execute(query, (datetime.now(), resolution_notes, flag_id))
            self.db.commit()
    
    def dismiss_review_flag(self, flag_id: int, resolution_notes: str):
        """Dismiss a review flag without action."""
        query = '''
            UPDATE tb_review_flags 
            SET status = 'DISMISSED', resolved_at = %s, resolution_notes = %s
            WHERE id = %s
        '''
        
        with self.db.cursor() as cursor:
            cursor.execute(query, (datetime.now(), resolution_notes, flag_id))
            self.db.commit()
    
    def get_validation_summary(self, run_id: str) -> Dict[str, Any]:
        """Get comprehensive validation summary for a run."""
        # Get error statistics
        error_query = '''
            SELECT 
                COUNT(*) as total_errors,
                SUM(CASE WHEN severity = 'CRITICAL' THEN 1 ELSE 0 END) as critical_errors,
                SUM(CASE WHEN severity = 'HIGH' THEN 1 ELSE 0 END) as high_errors,
                SUM(CASE WHEN severity = 'MEDIUM' THEN 1 ELSE 0 END) as medium_errors,
                SUM(CASE WHEN severity = 'LOW' THEN 1 ELSE 0 END) as low_errors,
                COUNT(DISTINCT path_definition_id) as paths_with_errors,
                COUNT(DISTINCT error_type) as unique_error_types
            FROM tb_validation_errors
            WHERE run_id = %s
        '''
        
        # Get flag statistics
        flag_query = '''
            SELECT 
                COUNT(*) as total_flags,
                SUM(CASE WHEN status = 'OPEN' THEN 1 ELSE 0 END) as open_flags,
                SUM(CASE WHEN status = 'RESOLVED' THEN 1 ELSE 0 END) as resolved_flags,
                SUM(CASE WHEN status = 'DISMISSED' THEN 1 ELSE 0 END) as dismissed_flags
            FROM tb_review_flags
            WHERE run_id = %s
        '''
        
        # Get path statistics
        path_query = '''
            SELECT COUNT(DISTINCT path_definition_id) as total_paths_validated
            FROM tb_attempt_paths
            WHERE run_id = %s AND path_definition_id IS NOT NULL
        '''
        
        with self.db.cursor() as cursor:
            # Get error stats
            cursor.execute(error_query, (run_id,))
            error_stats = cursor.fetchone() or {}
            
            # Get flag stats
            cursor.execute(flag_query, (run_id,))
            flag_stats = cursor.fetchone() or {}
            
            # Get path stats
            cursor.execute(path_query, (run_id,))
            path_stats = cursor.fetchone() or {}
        
        total_paths = path_stats.get('total_paths_validated', 0)
        paths_with_errors = error_stats.get('paths_with_errors', 0)
        success_rate = (total_paths - paths_with_errors) / total_paths if total_paths > 0 else 1.0
        
        return {
            'total_errors': error_stats.get('total_errors', 0),
            'critical_errors': error_stats.get('critical_errors', 0),
            'high_errors': error_stats.get('high_errors', 0),
            'medium_errors': error_stats.get('medium_errors', 0),
            'low_errors': error_stats.get('low_errors', 0),
            'unique_error_types': error_stats.get('unique_error_types', 0),
            'total_flags': flag_stats.get('total_flags', 0),
            'open_flags': flag_stats.get('open_flags', 0),
            'resolved_flags': flag_stats.get('resolved_flags', 0),
            'dismissed_flags': flag_stats.get('dismissed_flags', 0),
            'total_paths_validated': total_paths,
            'paths_with_errors': paths_with_errors,
            'validation_success_rate': success_rate
        }
    
    def add_validation_test(self, code: str, name: str, scope: ValidationScope,
                          severity: ValidationSeverity, test_type: TestType,
                          description: str) -> int:
        """Add a new validation test definition."""
        query = '''
            INSERT INTO tb_validation_tests (
                code, name, scope, severity, test_type, is_active, description
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        '''
        
        with self.db.cursor() as cursor:
            cursor.execute(query, (
                code, name, scope.value, severity.value, test_type.value, True, description
            ))
            self.db.commit()
            
            # Get inserted ID
            cursor.execute('SELECT LAST_INSERT_ID()')
            test_id = cursor.fetchone()['LAST_INSERT_ID()']
            
            # Clear test cache to reload
            self._validation_tests.clear()
            
            return test_id
    
    def disable_validation_test(self, test_code: str):
        """Disable a validation test."""
        query = '''
            UPDATE tb_validation_tests 
            SET is_active = 0 
            WHERE code = %s
        '''
        
        with self.db.cursor() as cursor:
            cursor.execute(query, (test_code,))
            self.db.commit()
            
            # Clear test cache to reload
            self._validation_tests.clear()
    
    def clear_run_validation_data(self, run_id: str):
        """Clear all validation data for a run (useful for re-validation)."""
        # Clear validation errors
        error_query = 'DELETE FROM tb_validation_errors WHERE run_id = %s'
        
        # Clear review flags
        flag_query = 'DELETE FROM tb_review_flags WHERE run_id = %s'
        
        with self.db.cursor() as cursor:
            cursor.execute(error_query, (run_id,))
            cursor.execute(flag_query, (run_id,))
            self.db.commit()
            
        self.logger.info(f'Cleared validation data for run {run_id}')
