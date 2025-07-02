"""
Path storage and retrieval service.
Handles path definitions, attempt tracking, and path data management.
"""

import json
import logging
from typing import List, Dict, Optional, Tuple, Any
from dataclasses import dataclass, asdict
from datetime import datetime
import hashlib

logger = logging.getLogger(__name__)


@dataclass
class PathDefinition:
    """Represents a path definition"""
    path_hash: str
    source_type: str
    scope: str
    target_fab: Optional[str]
    target_model_no: Optional[int]
    target_phase_no: Optional[int]
    target_toolset_no: Optional[int]
    target_data_codes: Optional[str]
    target_utilities: Optional[str]
    target_references: Optional[str]
    forbidden_node_ids: Optional[str]
    node_count: int
    link_count: int
    total_length_mm: float
    coverage: float
    data_codes_scope: Optional[str]
    utilities_scope: Optional[str]
    references_scope: Optional[str]
    path_context: Optional[str]


@dataclass
class AttemptPath:
    """Represents a path attempt"""
    run_id: str
    path_definition_id: int
    start_node_id: int
    end_node_id: int
    cost: Optional[float]
    notes: Optional[str] = None


@dataclass
class PathContext:
    """Represents path context with nodes and links"""
    nodes: List[Dict]
    links: List[Dict]
    sequence: List[int]
    total_length: float
    utilities_found: List[str]
    references_found: List[str]
    data_codes_found: List[int]


class PathService:
    """Service for path storage and retrieval operations"""
    
    def __init__(self, db_connection):
        self.db = db_connection
        
    def store_path_definition(self, path_def: PathDefinition) -> int:
        """
        Store a path definition and return its ID.
        
        Args:
            path_def: Path definition to store
            
        Returns:
            ID of the stored path definition
        """
        # Check if path already exists
        existing_id = self.get_path_definition_id_by_hash(path_def.path_hash)
        if existing_id:
            logger.debug(f"Path definition already exists with ID: {existing_id}")
            return existing_id
            
        query = """
            INSERT INTO tb_path_definitions (
                path_hash, source_type, scope, target_fab, target_model_no,
                target_phase_no, target_toolset_no, target_data_codes,
                target_utilities, target_references, forbidden_node_ids,
                node_count, link_count, total_length_mm, coverage,
                data_codes_scope, utilities_scope, references_scope, path_context
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        cursor = self.db.cursor()
        cursor.execute(query, (
            path_def.path_hash,
            path_def.source_type,
            path_def.scope,
            path_def.target_fab,
            path_def.target_model_no,
            path_def.target_phase_no,
            path_def.target_toolset_no,
            path_def.target_data_codes,
            path_def.target_utilities,
            path_def.target_references,
            path_def.forbidden_node_ids,
            path_def.node_count,
            path_def.link_count,
            path_def.total_length_mm,
            path_def.coverage,
            path_def.data_codes_scope,
            path_def.utilities_scope,
            path_def.references_scope,
            path_def.path_context
        ))
        
        path_id = cursor.lastrowid
        self.db.commit()
        
        logger.debug(f"Stored path definition with ID: {path_id}")
        return path_id
        
    def store_attempt_path(self, attempt: AttemptPath) -> int:
        """
        Store a path attempt and return its ID.
        
        Args:
            attempt: Path attempt to store
            
        Returns:
            ID of the stored attempt
        """
        query = """
            INSERT INTO tb_attempt_paths (
                run_id, path_definition_id, start_node_id, end_node_id, cost, notes
            ) VALUES (?, ?, ?, ?, ?, ?)
        """
        
        cursor = self.db.cursor()
        cursor.execute(query, (
            attempt.run_id,
            attempt.path_definition_id,
            attempt.start_node_id,
            attempt.end_node_id,
            attempt.cost,
            attempt.notes
        ))
        
        attempt_id = cursor.lastrowid
        self.db.commit()
        
        logger.debug(f"Stored attempt path with ID: {attempt_id}")
        return attempt_id
        
    def update_attempt_path_tested(self, attempt_id: int, cost: Optional[float], notes: Optional[str] = None):
        """
        Update attempt path with test results.
        
        Args:
            attempt_id: ID of the attempt to update
            cost: Path cost (None if no path found)
            notes: Additional notes
        """
        query = """
            UPDATE tb_attempt_paths 
            SET cost = ?, tested_at = CURRENT_TIMESTAMP, notes = ?
            WHERE id = ?
        """
        
        cursor = self.db.cursor()
        cursor.execute(query, (cost, notes, attempt_id))
        self.db.commit()
        
        logger.debug(f"Updated attempt path {attempt_id} with cost: {cost}")
        
    def get_path_definition_by_hash(self, path_hash: str) -> Optional[Dict]:
        """
        Get path definition by hash.
        
        Args:
            path_hash: Hash of the path
            
        Returns:
            Path definition dictionary or None
        """
        query = """
            SELECT * FROM tb_path_definitions WHERE path_hash = ?
        """
        
        cursor = self.db.cursor()
        cursor.execute(query, (path_hash,))
        return cursor.fetchone()
        
    def get_path_definition_id_by_hash(self, path_hash: str) -> Optional[int]:
        """
        Get path definition ID by hash.
        
        Args:
            path_hash: Hash of the path
            
        Returns:
            Path definition ID or None
        """
        query = """
            SELECT id FROM tb_path_definitions WHERE path_hash = ?
        """
        
        cursor = self.db.cursor()
        cursor.execute(query, (path_hash,))
        result = cursor.fetchone()
        return result['id'] if result else None
        
    def get_paths_for_run(self, run_id: str) -> List[Dict]:
        """
        Get all paths for a specific run.
        
        Args:
            run_id: Run identifier
            
        Returns:
            List of path dictionaries
        """
        query = """
            SELECT pd.*, ap.start_node_id, ap.end_node_id, ap.cost, ap.notes as attempt_notes
            FROM tb_path_definitions pd
            JOIN tb_attempt_paths ap ON pd.id = ap.path_definition_id
            WHERE ap.run_id = ?
            ORDER BY ap.picked_at
        """
        
        cursor = self.db.cursor()
        cursor.execute(query, (run_id,))
        return cursor.fetchall()
        
    def get_successful_paths_for_run(self, run_id: str) -> List[Dict]:
        """
        Get successful paths (with cost) for a specific run.
        
        Args:
            run_id: Run identifier
            
        Returns:
            List of successful path dictionaries
        """
        query = """
            SELECT pd.*, ap.start_node_id, ap.end_node_id, ap.cost, ap.notes as attempt_notes
            FROM tb_path_definitions pd
            JOIN tb_attempt_paths ap ON pd.id = ap.path_definition_id
            WHERE ap.run_id = ? AND ap.cost IS NOT NULL
            ORDER BY ap.cost
        """
        
        cursor = self.db.cursor()
        cursor.execute(query, (run_id,))
        return cursor.fetchall()
        
    def get_failed_paths_for_run(self, run_id: str) -> List[Dict]:
        """
        Get failed paths (no cost) for a specific run.
        
        Args:
            run_id: Run identifier
            
        Returns:
            List of failed path dictionaries
        """
        query = """
            SELECT pd.*, ap.start_node_id, ap.end_node_id, ap.notes as attempt_notes
            FROM tb_path_definitions pd
            JOIN tb_attempt_paths ap ON pd.id = ap.path_definition_id
            WHERE ap.run_id = ? AND ap.cost IS NULL
            ORDER BY ap.picked_at
        """
        
        cursor = self.db.cursor()
        cursor.execute(query, (run_id,))
        return cursor.fetchall()
        
    def create_path_definition_from_pocs(self, from_poc_id: int, to_poc_id: int, 
                                       source_type: str = "RANDOM",
                                       scope: str = "CONNECTIVITY") -> PathDefinition:
        """
        Create a path definition from POC pair.
        
        Args:
            from_poc_id: Starting POC ID
            to_poc_id: Ending POC ID
            source_type: Type of source (RANDOM, SCENARIO)
            scope: Scope of path (CONNECTIVITY, FLOW, MATERIAL)
            
        Returns:
            Path definition object
        """
        # Get POC details
        from_poc = self._get_poc_details(from_poc_id)
        to_poc = self._get_poc_details(to_poc_id)
        
        if not from_poc or not to_poc:
            raise ValueError(f"POC not found: {from_poc_id} or {to_poc_id}")
            
        # Create path hash
        path_hash = self._generate_path_hash(from_poc_id, to_poc_id)
        
        # Get equipment details for context
        from_equipment = self._get_equipment_details(from_poc['equipment_id'])
        to_equipment = self._get_equipment_details(to_poc['equipment_id'])
        
        # Create path context
        path_context = PathContext(
            nodes=[from_poc['node_id'], to_poc['node_id']],
            links=[],
            sequence=[from_poc['node_id'], to_poc['node_id']],
            total_length=0.0,
            utilities_found=[from_poc.get('utility_no'), to_poc.get('utility_no')],
            references_found=[from_poc.get('reference'), to_poc.get('reference')],
            data_codes_found=[from_equipment.get('data_code'), to_equipment.get('data_code')]
        )
        
        return PathDefinition(
            path_hash=path_hash,
            source_type=source_type,
            scope=scope,
            target_fab=from_equipment.get('fab'),
            target_model_no=from_equipment.get('model_no'),
            target_phase_no=from_equipment.get('phase_no'),
            target_toolset_no=from_equipment.get('toolset'),
            target_data_codes=json.dumps([from_equipment.get('data_code'), to_equipment.get('data_code')]),
            target_utilities=json.dumps([from_poc.get('utility_no'), to_poc.get('utility_no')]),
            target_references=json.dumps([from_poc.get('reference'), to_poc.get('reference')]),
            forbidden_node_ids=None,
            node_count=2,
            link_count=0,
            total_length_mm=0.0,
            coverage=0.0,
            data_codes_scope=json.dumps([from_equipment.get('data_code'), to_equipment.get('data_code')]),
            utilities_scope=json.dumps([from_poc.get('utility_no'), to_poc.get('utility_no')]),
            references_scope=json.dumps([from_poc.get('reference'), to_poc.get('reference')]),
            path_context=json.dumps(asdict(path_context))
        )
        
    def update_path_definition_with_path_data(self, path_def_id: int, path_data: Dict):
        """
        Update path definition with actual path data after pathfinding.
        
        Args:
            path_def_id: Path definition ID
            path_data: Dictionary containing path information
        """
        query = """
            UPDATE tb_path_definitions 
            SET node_count = ?, link_count = ?, total_length_mm = ?, 
                path_context = ?
            WHERE id = ?
        """
        
        cursor = self.db.cursor()
        cursor.execute(query, (
            path_data.get('node_count', 0),
            path_data.get('link_count', 0),
            path_data.get('total_length_mm', 0.0),
            json.dumps(path_data.get('path_context', {})),
            path_def_id
        ))
        self.db.commit()
        
        logger.debug(f"Updated path definition {path_def_id} with path data")
        
    def get_path_statistics_for_run(self, run_id: str) -> Dict:
        """
        Get path statistics for a run.
        
        Args:
            run_id: Run identifier
            
        Returns:
            Dictionary with path statistics
        """
        query = """
            SELECT 
                COUNT(*) as total_attempts,
                COUNT(CASE WHEN ap.cost IS NOT NULL THEN 1 END) as successful_paths,
                COUNT(CASE WHEN ap.cost IS NULL THEN 1 END) as failed_paths,
                AVG(CASE WHEN ap.cost IS NOT NULL THEN ap.cost END) as avg_cost,
                MIN(CASE WHEN ap.cost IS NOT NULL THEN ap.cost END) as min_cost,
                MAX(CASE WHEN ap.cost IS NOT NULL THEN ap.cost END) as max_cost,
                AVG(pd.node_count) as avg_node_count,
                AVG(pd.link_count) as avg_link_count,
                AVG(pd.total_length_mm) as avg_length
            FROM tb_attempt_paths ap
            JOIN tb_path_definitions pd ON ap.path_definition_id = pd.id
            WHERE ap.run_id = ?
        """
        
        cursor = self.db.cursor()
        cursor.execute(query, (run_id,))
        result = cursor.fetchone()
        
        if result:
            return {
                'total_attempts': result['total_attempts'] or 0,
                'successful_paths': result['successful_paths'] or 0,
                'failed_paths': result['failed_paths'] or 0,
                'success_rate': (result['successful_paths'] or 0) / max(result['total_attempts'] or 1, 1),
                'avg_cost': result['avg_cost'],
                'min_cost': result['min_cost'],
                'max_cost': result['max_cost'],
                'avg_node_count': result['avg_node_count'],
                'avg_link_count': result['avg_link_count'],
                'avg_length_mm': result['avg_length']
            }
        else:
            return {
                'total_attempts': 0,
                'successful_paths': 0,
                'failed_paths': 0,
                'success_rate': 0.0,
                'avg_cost': None,
                'min_cost': None,
                'max_cost': None,
                'avg_node_count': None,
                'avg_link_count': None,
                'avg_length_mm': None
            }
    
    def get_unique_paths_for_run(self, run_id: str) -> List[Dict]:
        """
        Get unique successful paths for a run (deduplicated by hash).
        
        Args:
            run_id: Run identifier
            
        Returns:
            List of unique path dictionaries
        """
        query = """
            SELECT DISTINCT pd.*, 
                   MIN(ap.cost) as best_cost,
                   COUNT(ap.id) as attempt_count
            FROM tb_path_definitions pd
            JOIN tb_attempt_paths ap ON pd.id = ap.path_definition_id
            WHERE ap.run_id = ? AND ap.cost IS NOT NULL
            GROUP BY pd.path_hash
            ORDER BY best_cost
        """
        
        cursor = self.db.cursor()
        cursor.execute(query, (run_id,))
        return cursor.fetchall()
        
    def _get_poc_details(self, poc_id: int) -> Optional[Dict]:
        """Get POC details by ID"""
        query = """
            SELECT poc.*, eq.toolset, ts.fab, ts.model_no, ts.phase_no
            FROM tb_equipment_pocs poc
            JOIN tb_equipments eq ON poc.equipment_id = eq.id
            JOIN tb_toolsets ts ON eq.toolset = ts.code
            WHERE poc.id = ?
        """
        
        cursor = self.db.cursor()
        cursor.execute(query, (poc_id,))
        return cursor.fetchone()
        
    def _get_equipment_details(self, equipment_id: int) -> Optional[Dict]:
        """Get equipment details by ID"""
        query = """
            SELECT eq.*, ts.fab, ts.model_no, ts.phase_no
            FROM tb_equipments eq
            JOIN tb_toolsets ts ON eq.toolset = ts.code
            WHERE eq.id = ?
        """
        
        cursor = self.db.cursor()
        cursor.execute(query, (equipment_id,))
        return cursor.fetchone()
        
    def _generate_path_hash(self, from_poc_id: int, to_poc_id: int) -> str:
        """Generate a unique hash for a POC pair"""
        # Create deterministic hash (order independent)
        data = f"{min(from_poc_id, to_poc_id)}-{max(from_poc_id, to_poc_id)}"
        return hashlib.md5(data.encode()).hexdigest()
        
    def delete_paths_for_run(self, run_id: str) -> int:
        """
        Delete all paths for a specific run.
        
        Args:
            run_id: Run identifier
            
        Returns:
            Number of deleted paths
        """
        # First get count
        cursor = self.db.cursor()
        cursor.execute("SELECT COUNT(*) as count FROM tb_attempt_paths WHERE run_id = ?", (run_id,))
        count = cursor.fetchone()['count']
        
        # Delete attempt paths (path definitions may be shared, so keep them)
        cursor.execute("DELETE FROM tb_attempt_paths WHERE run_id = ?", (run_id,))
        self.db.commit()
        
        logger.info(f"Deleted {count} attempt paths for run {run_id}")
        return count
        
    def cleanup_orphaned_path_definitions(self) -> int:
        """
        Clean up path definitions that have no associated attempts.
        
        Returns:
            Number of cleaned up definitions
        """
        query = """
            DELETE FROM tb_path_definitions 
            WHERE id NOT IN (SELECT DISTINCT path_definition_id FROM tb_attempt_paths)
        """
        
        cursor = self.db.cursor()
        cursor.execute(query)
        deleted_count = cursor.rowcount
        self.db.commit()
        
        logger.info(f"Cleaned up {deleted_count} orphaned path definitions")
        return deleted_count