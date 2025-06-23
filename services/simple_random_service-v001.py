"""
Simplified random path generation service.
"""

import random
import hashlib
from typing import List, Dict, Tuple, Optional, Set
from datetime import datetime

from models import RunConfig, PathResult, PathDefinition, ValidationError, ReviewFlag, CriticalError
from enums import Method, ObjectType, ErrorType, Severity, SourceType, FlagType, CriticalErrorType
from db import Database


class SimpleRandomService:
    """Simple random path generation: fab -> toolset -> equipment -> PoC."""
    
    def __init__(self, db: Database, fab: str = ""):
        self.db = db
        self.fab = fab
        self.used_node_pairs: Set[Tuple[int, int]] = set()
    
    def generate_random_path(self, config: RunConfig) -> PathResult:
        """Generate random path: fab -> toolset -> equipment -> PoC."""
        
        # Step 1: Select fab
        selected_fab = config.fab if config.fab else self._select_random_fab()
        if not selected_fab:
            return PathResult(
                path_found=False,
                errors=[ValidationError(
                    id=None, run_id=config.run_id, path_definition_id=None,
                    validation_test_id=None, severity=Severity.ERROR,
                    error_scope="FAB_SELECTION", error_type=ErrorType.MISSING_NODE,
                    object_type=ObjectType.NODE, building_code="",
                    notes="No active fabs found"
                )]
            )
        
        # Step 2: Select toolset
        toolset = self._select_toolset(selected_fab, config.toolset)
        if not toolset:
            return PathResult(
                path_found=False,
                errors=[ValidationError(
                    id=None, run_id=config.run_id, path_definition_id=None,
                    validation_test_id=None, severity=Severity.ERROR,
                    error_scope="TOOLSET_SELECTION", error_type=ErrorType.MISSING_NODE,
                    object_type=ObjectType.NODE, building_code=selected_fab,
                    notes=f"No toolsets found for fab {selected_fab}"
                )]
            )
        
        # Step 3: Select equipment pair
        equipment_pair = self._select_equipment_pair(toolset)
        if not equipment_pair:
            return PathResult(
                path_found=False,
                errors=[ValidationError(
                    id=None, run_id=config.run_id, path_definition_id=None,
                    validation_test_id=None, severity=Severity.ERROR,
                    error_scope="EQUIPMENT_SELECTION", error_type=ErrorType.EQUIPMENT_ERROR,
                    object_type=ObjectType.EQUIPMENT, building_code=selected_fab,
                    notes="Need at least 2 equipment pieces"
                )]
            )
        
        # Step 4: Select PoCs
        start_poc = self._select_poc(equipment_pair[0])
        end_poc = self._select_poc(equipment_pair[1])
        
        if not start_poc or not end_poc:
            return PathResult(
                path_found=False,
                errors=[ValidationError(
                    id=None, run_id=config.run_id, path_definition_id=None,
                    validation_test_id=None, severity=Severity.ERROR,
                    error_scope="POC_SELECTION", error_type=ErrorType.POC_ERROR,
                    object_type=ObjectType.POC, building_code=selected_fab,
                    notes="Could not select PoCs"
                )]
            )
        
        # Step 5: Critical validation
        critical_errors = self._check_critical_errors(start_poc, end_poc, selected_fab)
        if critical_errors:
            return PathResult(path_found=False, critical_errors=critical_errors)
        
        # Step 6: Check if already attempted
        node_pair = (min(start_poc['node_id'], end_poc['node_id']), 
                    max(start_poc['node_id'], end_poc['node_id']))
        if node_pair in self.used_node_pairs:
            return PathResult(
                path_found=False,
                errors=[ValidationError(
                    id=None, run_id=config.run_id, path_definition_id=None,
                    validation_test_id=None, severity=Severity.WARNING,
                    error_scope="BIAS_MITIGATION", error_type=ErrorType.PATH_NOT_FOUND,
                    object_type=ObjectType.NODE, building_code=selected_fab,
                    notes="Node pair already attempted"
                )]
            )
        
        # Step 7: Find path
        path_data = self._find_path(start_poc['node_id'], end_poc['node_id'], selected_fab)
        if not path_data:
            return PathResult(
                path_found=False,
                review_flags=[ReviewFlag(
                    id=None, run_id=config.run_id, flag_type=FlagType.MANUAL_REVIEW,
                    created_at=datetime.now(), severity=Severity.MEDIUM,
                    reason="No path found", object_type=ObjectType.NODE,
                    start_node_id=start_poc['node_id'], end_node_id=end_poc['node_id'],
                    building_code=selected_fab
                )]
            )
        
        # Step 8: Create path definition
        path_def = self._create_path_definition(
            path_data, toolset, equipment_pair, start_poc, end_poc, selected_fab
        )
        
        # Track used pair
        self.used_node_pairs.add(node_pair)
        
        return PathResult(
            path_found=True,
            path_definition=path_def,
            coverage_contribution=len(path_data.get('nodes', [])) + len(path_data.get('links', []))
        )
    
    def _select_random_fab(self) -> Optional[str]:
        """Select random fab."""
        sql = "SELECT DISTINCT fab FROM tb_toolsets WHERE is_active = TRUE ORDER BY RAND() LIMIT 1"
        try:
            result = self.db.query(sql)
            return result[0][0] if result else None
        except Exception:
            return None
    
    def _select_toolset(self, fab: str, toolset_code: str = "") -> Optional[Dict]:
        """Select toolset."""
        if toolset_code and toolset_code != "ALL":
            # Find specific toolset across phases
            sql = """
            SELECT code, fab, phase FROM tb_toolsets 
            WHERE fab = ? AND code = ? AND is_active = TRUE 
            ORDER BY RAND() LIMIT 1
            """
            params = [fab, toolset_code]
        else:
            # Random toolset
            sql = """
            SELECT code, fab, phase FROM tb_toolsets 
            WHERE fab = ? AND is_active = TRUE 
            ORDER BY RAND() LIMIT 1
            """
            params = [fab]
        
        try:
            result = self.db.query(sql, params)
            if result:
                return {'code': result[0][0], 'fab': result[0][1], 'phase': result[0][2]}
            return None
        except Exception:
            return None
    
    def _select_equipment_pair(self, toolset: Dict) -> Optional[Tuple[Dict, Dict]]:
        """Select two equipment from toolset."""
        sql = """
        SELECT id, name, guid, node_id, kind FROM tb_equipment
        WHERE fab = ? AND toolset_code = ? AND phase = ? AND is_active = TRUE
        ORDER BY RAND()
        """
        
        try:
            results = self.db.query(sql, [toolset['fab'], toolset['code'], toolset['phase']])
            if len(results) < 2:
                return None
            
            eq1 = {'id': results[0][0], 'name': results[0][1], 'guid': results[0][2], 
                   'node_id': results[0][3], 'kind': results[0][4]}
            eq2 = {'id': results[1][0], 'name': results[1][1], 'guid': results[1][2], 
                   'node_id': results[1][3], 'kind': results[1][4]}
            
            return (eq1, eq2)
        except Exception:
            return None
    
    def _select_poc(self, equipment: Dict) -> Optional[Dict]:
        """Select PoC from equipment."""
        sql = """
        SELECT id, code, node_id, utility_code, flow_direction, is_used
        FROM tb_equipment_pocs
        WHERE equipment_id = ? AND is_active = TRUE
        ORDER BY RAND() LIMIT 1
        """
        
        try:
            result = self.db.query(sql, [equipment['id']])
            if result:
                return {
                    'id': result[0][0], 'code': result[0][1], 'node_id': result[0][2],
                    'utility_code': result[0][3], 'flow_direction': result[0][4], 
                    'is_used': result[0][5]
                }
            return None
        except Exception:
            return None
    
    def _check_critical_errors(self, start_poc: Dict, end_poc: Dict, fab: str) -> List[CriticalError]:
        """Check for critical errors."""
        errors = []
        
        for poc in [start_poc, end_poc]:
            # Check if PoC node exists
            if not self._node_exists(poc['node_id'], fab):
                errors.append(CriticalError(
                    id=None, error_type=CriticalErrorType.MISSING_POC_NODE.value,
                    building_code=fab, toolset_code="", phase="",
                    poc_code=poc['code'], node_id=poc['node_id'],
                    error_reason=f"PoC node {poc['node_id']} not in nw_nodes"
                ))
            
            # Check if used PoC has utility
            if poc['is_used'] and not poc['utility_code']:
                errors.append(CriticalError(
                    id=None, error_type=CriticalErrorType.POC_NO_UTILITY.value,
                    building_code=fab, toolset_code="", phase="",
                    poc_code=poc['code'], node_id=poc['node_id'],
                    error_reason=f"Used PoC {poc['code']} has no utility"
                ))
        
        return errors
    
    def _node_exists(self, node_id: int, fab: str) -> bool:
        """Check if node exists."""
        sql = "SELECT 1 FROM nw_nodes WHERE id = ? AND fab = ? LIMIT 1"
        try:
            result = self.db.query(sql, [node_id, fab])
            return len(result) > 0
        except Exception:
            return False
    
    def _find_path(self, start_node: int, end_node: int, fab: str) -> Optional[Dict]:
        """Find path between nodes."""
        try:
            # Call your pathfinding function
            sql = "SELECT find_shortest_path(?, ?, ?) as path_data"
            result = self.db.query(sql, [start_node, end_node, fab])
            
            if result and result[0][0]:
                import json
                return json.loads(result[0][0]) if isinstance(result[0][0], str) else result[0][0]
            return None
        except Exception:
            return None
    
    def _create_path_definition(self, path_data: Dict, toolset: Dict, 
                              equipment_pair: Tuple[Dict, Dict], start_poc: Dict, 
                              end_poc: Dict, fab: str) -> PathDefinition:
        """Create path definition."""
        nodes = path_data.get('nodes', [])
        links = path_data.get('links', [])
        
        path_context = {
            'nodes': nodes,
            'links': links,
            'start_node_id': start_poc['node_id'],
            'end_node_id': end_poc['node_id'],
            'toolset_code': toolset['code'],
            'equipment_ids': [equipment_pair[0]['id'], equipment_pair[1]['id']]
        }
        
        # Simple hash
        hash_input = f"{start_poc['node_id']}-{end_poc['node_id']}-{'-'.join(map(str, nodes))}"
        path_hash = hashlib.sha256(hash_input.encode()).hexdigest()
        
        # Get utilities
        utilities = []
        for poc in [start_poc, end_poc]:
            if poc['utility_code'] and poc['utility_code'] not in utilities:
                utilities.append(poc['utility_code'])
        
        return PathDefinition(
            id=None, path_hash=path_hash, source_type=SourceType.RANDOM,
            building_code=fab, category=equipment_pair[0]['kind'] or "UNKNOWN",
            scope="CONNECTIVITY", node_count=len(nodes), link_count=len(links),
            total_length_mm=path_data.get('total_length_mm', 0.0),
            coverage=len(nodes) + len(links), utilities=utilities,
            path_context=path_context
        )


class SimplePopulationService:
    """Simple equipment table population on first run."""
    
    def __init__(self, db: Database):
        self.db = db
    
    def populate_on_first_run(self, fab: str):
        """Populate equipment tables if empty."""
        # Check if already populated
        if self._is_populated(fab):
            return
        
        print(f"First run detected. Populating equipment tables for fab {fab}...")
        
        # Simple population strategy - create minimal test data
        self._create_sample_toolsets(fab)
        self._create_sample_equipment(fab)
        
        print(f"Equipment tables populated for fab {fab}")
    
    def _is_populated(self, fab: str) -> bool:
        """Check if equipment tables have data for fab."""
        sql = "SELECT COUNT(*) FROM tb_toolsets WHERE fab = ?"
        try:
            result = self.db.query(sql, [fab])
            return result[0][0] > 0 if result else False
        except Exception:
            return False
    
    def _create_sample_toolsets(self, fab: str):
        """Create sample toolsets."""
        sample_toolsets = [
            {'code': 'TOOLSET_001', 'fab': fab, 'phase': 'PHASE1'},
            {'code': 'TOOLSET_002', 'fab': fab, 'phase': 'PHASE1'},
            {'code': 'TOOLSET_001', 'fab': fab, 'phase': 'PHASE2'},
        ]
        
        sql = "INSERT INTO tb_toolsets (code, fab, phase) VALUES (?, ?, ?)"
        
        for toolset in sample_toolsets:
            try:
                self.db.update(sql, [toolset['code'], toolset['fab'], toolset['phase']])
            except Exception as e:
                print(f"Error creating toolset: {e}")
    
    def _create_sample_equipment(self, fab: str):
        """Create sample equipment with PoCs."""
        # Get toolsets
        sql = "SELECT code, phase FROM tb_toolsets WHERE fab = ?"
        toolsets = self.db.query(sql, [fab])
        
        if not toolsets:
            return
        
        # Create sample equipment for each toolset
        for toolset_code, phase in toolsets:
            for i in range(2):  # 2 equipment per toolset
                # Create equipment
                eq_sql = """
                INSERT INTO tb_equipment 
                (toolset_code, fab, phase, name, guid, node_id, kind) 
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """
                
                equipment_name = f"Equipment_{toolset_code}_{phase}_{i+1}"
                equipment_guid = f"GUID_{fab}_{toolset_code}_{phase}_{i+1}"
                virtual_node_id = 1000000 + hash(equipment_guid) % 100000  # Sample virtual node
                kind = ['PRODUCTION', 'PROCESSING', 'SUPPLY'][i % 3]
                
                try:
                    self.db.update(eq_sql, [
                        toolset_code, fab, phase, equipment_name, 
                        equipment_guid, virtual_node_id, kind
                    ])
                    
                    # Get equipment ID
                    eq_id_result = self.db.query("SELECT LAST_INSERT_ID()")
                    if eq_id_result:
                        equipment_id = eq_id_result[0][0]
                        self._create_sample_pocs(equipment_id, virtual_node_id)
                        
                except Exception as e:
                    print(f"Error creating equipment: {e}")
    
    def _create_sample_pocs(self, equipment_id: int, base_node_id: int):
        """Create sample PoCs for equipment."""
        sample_pocs = [
            {'code': 'POC01', 'node_id': base_node_id + 1, 'utility': 'N2', 'flow': 'IN'},
            {'code': 'POC02', 'node_id': base_node_id + 2, 'utility': 'CDA', 'flow': 'OUT'},
        ]
        
        sql = """
        INSERT INTO tb_equipment_pocs 
        (equipment_id, code, node_id, utility_code, flow_direction, is_used) 
        VALUES (?, ?, ?, ?, ?, ?)
        """
        
        for poc in sample_pocs:
            try:
                self.db.update(sql, [
                    equipment_id, poc['code'], poc['node_id'], 
                    poc['utility'], poc['flow'], True
                ])
            except Exception as e:
                print(f"Error creating PoC: {e}")