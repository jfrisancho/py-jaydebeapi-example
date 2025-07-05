import json
import hashlib
import logging
from typing import Optional, List, Dict, Any, Set
from dataclasses import dataclass
from datetime import datetime


@dataclass
class PathDefinition:
    """Represents a unique path definition."""
    id: Optional[int]
    path_hash: str
    source_type: str
    scope: str
    target_fab: Optional[str]
    target_model_no: Optional[int]
    target_phase_no: Optional[int]
    target_toolset_no: Optional[str]
    target_data_codes: Optional[str]
    target_utilities: Optional[str]
    target_references: Optional[str]
    forbidden_node_ids: Optional[str]
    node_count: int
    link_count: int
    total_length_mm: float
    coverage: float
    data_codes_scope: str
    utilities_scope: str
    references_scope: str
    path_context: str
    created_at: datetime


@dataclass
class PathStatistics:
    """Statistics for paths in a run."""
    total_paths: int
    unique_paths: int
    total_nodes: int
    total_links: int
    avg_path_nodes: float
    avg_path_links: float
    avg_path_length: float
    min_path_length: float
    max_path_length: float
    coverage_contribution: float


class PathManager:
    """Manages path storage, retrieval, and analysis."""
    
    def __init__(self, db_connection):
        self.db = db_connection
        self.logger = logging.getLogger(__name__)
        self._path_cache = {}  # Cache for path definitions by hash
        
    def store_path(self, run_id: str, path_result: 'PathResult') -> int:
        """Store a path result and return the path definition ID."""
        try:
            # Generate path hash for deduplication
            path_hash = self._generate_path_hash(path_result)
            
            # Check if path definition already exists
            path_def_id = self._get_or_create_path_definition(path_result, path_hash)
            
            # Update attempt record with path definition
            self._update_attempt_with_path(run_id, path_result, path_def_id)
            
            # Generate automatic tags for the path
            self._generate_automatic_tags(run_id, path_def_id, path_result)
            
            self.logger.debug(f'Stored path {path_hash} with definition ID {path_def_id}')
            return path_def_id
            
        except Exception as e:
            self.logger.error(f'Error storing path: {str(e)}')
            raise
    
    def _generate_path_hash(self, path_result: 'PathResult') -> str:
        """Generate a unique hash for the path based on nodes and links."""
        # Create a canonical representation of the path
        path_data = {
            'nodes': sorted(path_result.path_nodes),
            'links': sorted(path_result.path_links),
            'start_node': path_result.start_node_id,
            'end_node': path_result.end_node_id
        }
        
        # Generate hash
        path_string = json.dumps(path_data, sort_keys=True)
        return hashlib.sha256(path_string.encode('utf-8')).hexdigest()
    
    def _get_or_create_path_definition(self, path_result: 'PathResult', path_hash: str) -> int:
        """Get existing path definition or create a new one."""
        # Check cache first
        if path_hash in self._path_cache:
            return self._path_cache[path_hash]
        
        # Check database
        existing_def = self._get_path_definition_by_hash(path_hash)
        if existing_def:
            self._path_cache[path_hash] = existing_def['id']
            return existing_def['id']
        
        # Create new path definition
        path_def_id = self._create_path_definition(path_result, path_hash)
        self._path_cache[path_hash] = path_def_id
        return path_def_id
    
    def _get_path_definition_by_hash(self, path_hash: str) -> Optional[Dict[str, Any]]:
        """Retrieve path definition by hash."""
        query = '''
            SELECT id, path_hash, source_type, scope, target_fab, target_model_no,
                   target_phase_no, target_toolset_no, node_count, link_count,
                   total_length_mm, coverage, created_at
            FROM tb_path_definitions
            WHERE path_hash = %s
        '''
        
        with self.db.cursor() as cursor:
            cursor.execute(query, (path_hash,))
            return cursor.fetchone()
    
    def _create_path_definition(self, path_result: 'PathResult', path_hash: str) -> int:
        """Create a new path definition record."""
        # Prepare path context
        path_context = {
            'nodes_sequence': path_result.path_nodes,
            'links_sequence': path_result.path_links,
            'start_poc_id': path_result.start_poc_id,
            'end_poc_id': path_result.end_poc_id,
            'start_equipment_id': path_result.start_equipment_id,
            'end_equipment_id': path_result.end_equipment_id
        }
        
        # Prepare scope data
        data_codes_scope = json.dumps(path_result.data_codes)
        utilities_scope = json.dumps(path_result.utilities)
        references_scope = json.dumps(path_result.references)
        path_context_json = json.dumps(path_context)
        
        # Calculate coverage (will be updated by coverage manager)
        coverage = 0.0  # Initial value
        
        query = '''
            INSERT INTO tb_path_definitions (
                path_hash, source_type, scope, target_fab, target_model_no,
                target_phase_no, target_toolset_no, target_data_codes, target_utilities,
                target_references, forbidden_node_ids, node_count, link_count,
                total_length_mm, coverage, data_codes_scope, utilities_scope,
                references_scope, path_context, created_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
        '''
        
        values = (
            path_hash, 'RANDOM', 'CONNECTIVITY', None, None, None,
            path_result.toolset_code, ','.join(map(str, path_result.data_codes)),
            ','.join(map(str, path_result.utilities)), ','.join(path_result.references),
            None, len(path_result.path_nodes), len(path_result.path_links),
            path_result.total_length_mm, coverage, data_codes_scope,
            utilities_scope, references_scope, path_context_json, datetime.now()
        )
        
        with self.db.cursor() as cursor:
            cursor.execute(query, values)
            self.db.commit()
            
            # Get the inserted ID
            cursor.execute('SELECT LAST_INSERT_ID()')
            path_def_id = cursor.fetchone()['LAST_INSERT_ID()']
            
        return path_def_id
    
    def _update_attempt_with_path(self, run_id: str, path_result: 'PathResult', path_def_id: int):
        """Update the attempt record with the found path information."""
        query = '''
            UPDATE tb_attempt_paths 
            SET path_definition_id = %s, cost = %s, tested_at = %s
            WHERE run_id = %s AND start_node_id = %s AND end_node_id = %s
            ORDER BY picked_at DESC LIMIT 1
        '''
        
        with self.db.cursor() as cursor:
            cursor.execute(query, (
                path_def_id, path_result.total_cost, datetime.now(),
                run_id, path_result.start_node_id, path_result.end_node_id
            ))
            self.db.commit()
    
    def _generate_automatic_tags(self, run_id: str, path_def_id: int, path_result: 'PathResult'):
        """Generate automatic tags for the path."""
        tags_to_create = []
        
        # Toolset tag
        if path_result.toolset_code:
            tags_to_create.append({
                'tag_type': 'FAB',
                'tag_code': path_result.toolset_code[:8],  # Truncate if needed
                'tag': f'TOOLSET_{path_result.toolset_code}',
                'source': 'SYSTEM',
                'confidence': 1.0
            })
        
        # Data code tags
        for data_code in path_result.data_codes[:5]:  # Limit to 5 most common
            tags_to_create.append({
                'tag_type': 'DAT',
                'tag_code': str(data_code),
                'tag': f'DATA_{data_code}',
                'source': 'SYSTEM',
                'confidence': 1.0
            })
        
        # Utility tags
        for utility in path_result.utilities[:5]:  # Limit to 5 most common
            tags_to_create.append({
                'tag_type': 'UTY',
                'tag_code': str(utility),
                'tag': f'UTILITY_{utility}',
                'source': 'SYSTEM',
                'confidence': 1.0
            })
        
        # Reference tags
        for reference in path_result.references[:5]:  # Limit to 5 most common
            tags_to_create.append({
                'tag_type': 'CAT',
                'tag_code': reference,
                'tag': f'REF_{reference}',
                'source': 'SYSTEM',
                'confidence': 1.0
            })
        
        # Path length category tags
        length_mm = path_result.total_length_mm
        if length_mm < 1000:  # < 1m
            length_category = 'SHORT'
        elif length_mm < 10000:  # < 10m
            length_category = 'MEDIUM'
        elif length_mm < 100000:  # < 100m
            length_category = 'LONG'
        else:  # >= 100m
            length_category = 'VERY_LONG'
        
        tags_to_create.append({
            'tag_type': 'QA',
            'tag_code': length_category,
            'tag': f'LENGTH_{length_category}',
            'source': 'SYSTEM',
            'confidence': 1.0
        })
        
        # Node count category tags
        node_count = len(path_result.path_nodes)
        if node_count < 5:
            complexity = 'SIMPLE'
        elif node_count < 20:
            complexity = 'MODERATE'
        elif node_count < 50:
            complexity = 'COMPLEX'
        else:
            complexity = 'VERY_COMPLEX'
        
        tags_to_create.append({
            'tag_type': 'QA',
            'tag_code': complexity,
            'tag': f'COMPLEXITY_{complexity}',
            'source': 'SYSTEM',
            'confidence': 1.0
        })
        
        # Insert all tags
        self._insert_path_tags(run_id, path_def_id, tags_to_create)
    
    def _insert_path_tags(self, run_id: str, path_def_id: int, tags: List[Dict[str, Any]]):
        """Insert path tags into the database."""
        if not tags:
            return
        
        query = '''
            INSERT INTO tb_path_tags (
                run_id, path_definition_id, tag_type, tag_code, tag,
                source, confidence, created_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s
            )
        '''
        
        with self.db.cursor() as cursor:
            for tag in tags:
                cursor.execute(query, (
                    run_id, path_def_id, tag['tag_type'], tag['tag_code'],
                    tag['tag'], tag['source'], tag['confidence'], datetime.now()
                ))
            self.db.commit()
    
    def get_path_definition(self, path_def_id: int) -> Optional[PathDefinition]:
        """Retrieve a path definition by ID."""
        query = '''
            SELECT id, path_hash, source_type, scope, target_fab, target_model_no,
                   target_phase_no, target_toolset_no, target_data_codes, target_utilities,
                   target_references, forbidden_node_ids, node_count, link_count,
                   total_length_mm, coverage, data_codes_scope, utilities_scope,
                   references_scope, path_context, created_at
            FROM tb_path_definitions
            WHERE id = %s
        '''
        
        with self.db.cursor() as cursor:
            cursor.execute(query, (path_def_id,))
            row = cursor.fetchone()
            
            if row:
                return PathDefinition(**row)
            return None
    
    def get_run_paths(self, run_id: str, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """Get all paths for a specific run."""
        query = '''
            SELECT pd.id, pd.path_hash, pd.node_count, pd.link_count, pd.total_length_mm,
                   pd.coverage, ap.start_node_id, ap.end_node_id, ap.cost, ap.picked_at, ap.tested_at
            FROM tb_path_definitions pd
            JOIN tb_attempt_paths ap ON pd.id = ap.path_definition_id
            WHERE ap.run_id = %s AND ap.path_definition_id IS NOT NULL
            ORDER BY ap.picked_at DESC
        '''
        
        params = [run_id]
        if limit:
            query += ' LIMIT %s'
            params.append(limit)
        
        with self.db.cursor() as cursor:
            cursor.execute(query, params)
            return cursor.fetchall()
    
    def get_unique_path_count(self, run_id: str) -> int:
        """Get count of unique paths found in a run."""
        query = '''
            SELECT COUNT(DISTINCT pd.path_hash) as unique_count
            FROM tb_path_definitions pd
            JOIN tb_attempt_paths ap ON pd.id = ap.path_definition_id
            WHERE ap.run_id = %s
        '''
        
        with self.db.cursor() as cursor:
            cursor.execute(query, (run_id,))
            result = cursor.fetchone()
            return result['unique_count'] if result else 0
    
    def get_path_statistics(self, run_id: str) -> Dict[str, Any]:
        """Get comprehensive path statistics for a run."""
        query = '''
            SELECT 
                COUNT(*) as total_paths,
                COUNT(DISTINCT pd.path_hash) as unique_paths,
                SUM(pd.node_count) as total_nodes,
                SUM(pd.link_count) as total_links,
                AVG(pd.node_count) as avg_path_nodes,
                AVG(pd.link_count) as avg_path_links,
                AVG(pd.total_length_mm) as avg_path_length,
                MIN(pd.total_length_mm) as min_path_length,
                MAX(pd.total_length_mm) as max_path_length,
                SUM(pd.coverage) as total_coverage_contribution
            FROM tb_path_definitions pd
            JOIN tb_attempt_paths ap ON pd.id = ap.path_definition_id
            WHERE ap.run_id = %s AND ap.path_definition_id IS NOT NULL
        '''
        
        with self.db.cursor() as cursor:
            cursor.execute(query, (run_id,))
            stats = cursor.fetchone()
            
            if not stats or stats['total_paths'] == 0:
                return {
                    'total_paths': 0,
                    'unique_paths': 0,
                    'total_nodes': 0,
                    'total_links': 0,
                    'avg_path_nodes': 0.0,
                    'avg_path_links': 0.0,
                    'avg_path_length': 0.0,
                    'min_path_length': 0.0,
                    'max_path_length': 0.0,
                    'coverage_contribution': 0.0
                }
            
            return {
                'total_paths': stats['total_paths'],
                'unique_paths': stats['unique_paths'],
                'total_nodes': stats['total_nodes'] or 0,
                'total_links': stats['total_links'] or 0,
                'avg_path_nodes': float(stats['avg_path_nodes'] or 0),
                'avg_path_links': float(stats['avg_path_links'] or 0),
                'avg_path_length': float(stats['avg_path_length'] or 0),
                'min_path_length': float(stats['min_path_length'] or 0),
                'max_path_length': float(stats['max_path_length'] or 0),
                'coverage_contribution': float(stats['total_coverage_contribution'] or 0)
            }
    
    def get_path_tags(self, path_def_id: int) -> List[Dict[str, Any]]:
        """Get all tags for a specific path definition."""
        query = '''
            SELECT tag_type, tag_code, tag, source, confidence, created_at, created_by, notes
            FROM tb_path_tags
            WHERE path_definition_id = %s
            ORDER BY created_at DESC
        '''
        
        with self.db.cursor() as cursor:
            cursor.execute(query, (path_def_id,))
            return cursor.fetchall()
    
    def add_manual_tag(self, run_id: str, path_def_id: int, tag_type: str, 
                      tag_code: str, tag: str, created_by: str, notes: Optional[str] = None):
        """Add a manual tag to a path."""
        query = '''
            INSERT INTO tb_path_tags (
                run_id, path_definition_id, tag_type, tag_code, tag,
                source, confidence, created_at, created_by, notes
            ) VALUES (
                %s, %s, %s, %s, %s, 'USER', 1.0, %s, %s, %s
            )
        '''
        
        with self.db.cursor() as cursor:
            cursor.execute(query, (
                run_id, path_def_id, tag_type, tag_code, tag,
                datetime.now(), created_by, notes
            ))
            self.db.commit()
    
    def search_paths_by_tags(self, tag_type: Optional[str] = None, 
                           tag_code: Optional[str] = None,
                           limit: int = 100) -> List[Dict[str, Any]]:
        """Search paths by their tags."""
        query = '''
            SELECT DISTINCT pd.id, pd.path_hash, pd.node_count, pd.link_count, 
                   pd.total_length_mm, pd.created_at
            FROM tb_path_definitions pd
            JOIN tb_path_tags pt ON pd.id = pt.path_definition_id
            WHERE 1=1
        '''
        
        params = []
        if tag_type:
            query += ' AND pt.tag_type = %s'
            params.append(tag_type)
        
        if tag_code:
            query += ' AND pt.tag_code = %s'
            params.append(tag_code)
        
        query += ' ORDER BY pd.created_at DESC LIMIT %s'
        params.append(limit)
        
        with self.db.cursor() as cursor:
            cursor.execute(query, params)
            return cursor.fetchall()
    
    def update_path_coverage(self, path_def_id: int, coverage: float):
        """Update the coverage value for a path definition."""
        query = '''
            UPDATE tb_path_definitions 
            SET coverage = %s 
            WHERE id = %s
        '''
        
        with self.db.cursor() as cursor:
            cursor.execute(query, (coverage, path_def_id))
            self.db.commit()
    
    def get_path_context(self, path_def_id: int) -> Optional[Dict[str, Any]]:
        """Get the full path context for a path definition."""
        query = '''
            SELECT path_context, data_codes_scope, utilities_scope, references_scope
            FROM tb_path_definitions
            WHERE id = %s
        '''
        
        with self.db.cursor() as cursor:
            cursor.execute(query, (path_def_id,))
            result = cursor.fetchone()
            
            if result:
                return {
                    'path_context': json.loads(result['path_context']) if result['path_context'] else {},
                    'data_codes': json.loads(result['data_codes_scope']) if result['data_codes_scope'] else [],
                    'utilities': json.loads(result['utilities_scope']) if result['utilities_scope'] else [],
                    'references': json.loads(result['references_scope']) if result['references_scope'] else []
                }
            return None
    
    def clear_cache(self):
        """Clear the internal path definition cache."""
        self._path_cache.clear()
