"""
Service for managing path definitions and attempts.
"""

import json
from datetime import datetime
from typing import Optional, List

from models import PathDefinition, PathResult, AttemptPath
from db import Database


class PathService:
    """Service for managing path definitions and storage."""
    
    def __init__(self, db: Database):
        self.db = db
    
    def store_path_attempt(self, run_id: str, path_result: PathResult) -> Optional[int]:
        """Store a path attempt in the database."""
        if not path_result.path_found or not path_result.path_definition:
            return None
        
        path_def = path_result.path_definition
        
        # First, store or get the path definition
        path_def_id = self._store_path_definition(path_def)
        if not path_def_id:
            return None
        
        # Store the attempt path
        attempt_id = self._store_attempt_path(run_id, path_def_id, path_def)
        
        return attempt_id
    
    def _store_path_definition(self, path_def: PathDefinition) -> Optional[int]:
        """Store or retrieve existing path definition."""
        
        # Check if path definition already exists
        existing_id = self._get_existing_path_definition(path_def.path_hash)
        if existing_id:
            return existing_id
        
        # Store new path definition
        sql = """
        INSERT INTO tb_path_definitions (
            path_hash, fab, category, scope, node_count, link_count,
            total_length_mm, coverage, utilities, path_context, scenario_context
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        try:
            # Serialize utilities and context as JSON
            utilities_json = json.dumps(path_def.utilities)
            path_context_json = json.dumps(path_def.path_context)
            scenario_context_json = json.dumps(path_def.scenario_context) if path_def.scenario_context else None
            
            rows_affected = self.db.update(sql, [
                path_def.path_hash,
                path_def.fab,
                path_def.category,
                path_def.scope,
                path_def.node_count,
                path_def.link_count,
                path_def.total_length_mm,
                path_def.coverage,
                utilities_json,
                path_context_json,
                scenario_context_json
            ])
            
            if rows_affected > 0:
                # Get the generated ID
                id_result = self.db.query("SELECT LAST_INSERT_ID()")
                return id_result[0][0] if id_result else None
            
            return None
            
        except Exception as e:
            print(f"Error storing path definition: {e}")
            return None
    
    def _get_existing_path_definition(self, path_hash: str) -> Optional[int]:
        """Check if a path definition already exists and return its ID."""
        sql = "SELECT id FROM tb_path_definitions WHERE path_hash = ?"
        
        try:
            result = self.db.query(sql, [path_hash])
            return result[0][0] if result else None
        except Exception as e:
            print(f"Error checking existing path definition: {e}")
            return None
    
    def _store_attempt_path(self, run_id: str, path_def_id: int, path_def: PathDefinition) -> Optional[int]:
        """Store an attempt path record."""
        sql = """
        INSERT INTO tb_attempt_paths (
            run_id, path_definition_id, start_node_id, end_node_id,
            fab, category, utility, picked_at, notes
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        try:
            # Extract start/end nodes from path context
            start_node = path_def.path_context.get('start_node_id')
            end_node = path_def.path_context.get('end_node_id')
            
            # Get primary utility (first one if multiple)
            utility = path_def.utilities[0] if path_def.utilities else 'UNKNOWN'
            
            rows_affected = self.db.update(sql, [
                run_id,
                path_def_id,
                start_node,
                end_node,
                path_def.fab,
                path_def.category,
                utility,
                datetime.now(),
                f"Path found with {path_def.node_count} nodes, {path_def.link_count} links"
            ])
            
            if rows_affected > 0:
                # Get the generated ID
                id_result = self.db.query("SELECT LAST_INSERT_ID()")
                return id_result[0][0] if id_result else None
            
            return None
            
        except Exception as e:
            print(f"Error storing attempt path: {e}")
            return None
    
    def get_path_definition(self, path_def_id: int) -> Optional[PathDefinition]:
        """Retrieve a path definition by ID."""
        sql = """
        SELECT id, path_hash, fab, category, scope, node_count, link_count,
               total_length_mm, coverage, utilities, path_context, scenario_context
        FROM tb_path_definitions 
        WHERE id = ?
        """
        
        try:
            result = self.db.query(sql, [path_def_id])
            if not result:
                return None
            
            row = result[0]
            utilities = json.loads(row[9]) if row[9] else []
            path_context = json.loads(row[10]) if row[10] else {}
            scenario_context = json.loads(row[11]) if row[11] else None
            
            return PathDefinition(
                id=row[0],
                path_hash=row[1],
                fab=row[2],
                category=row[3],
                scope=row[4],
                node_count=row[5],
                link_count=row[6],
                total_length_mm=row[7],
                coverage=row[8],
                utilities=utilities,
                path_context=path_context,
                scenario_context=scenario_context
            )
            
        except Exception as e:
            print(f"Error retrieving path definition {path_def_id}: {e}")
            return None
    
    def get_run_attempts(self, run_id: str) -> List[AttemptPath]:
        """Get all attempt paths for a run."""
        sql = """
        SELECT id, run_id, path_definition_id, start_node_id, end_node_id,
               fab, category, utility, picked_at, notes
        FROM tb_attempt_paths 
        WHERE run_id = ?
        ORDER BY picked_at
        """
        
        try:
            results = self.db.query(sql, [run_id])
            attempts = []
            
            for row in results:
                attempt = AttemptPath(
                    id=row[0],
                    run_id=row[1],
                    path_definition_id=row[2],
                    start_node_id=row[3],
                    end_node_id=row[4],
                    fab=row[5],
                    category=row[6],
                    utility=row[7],
                    picked_at=row[8],
                    notes=row[9]
                )
                attempts.append(attempt)
            
            return attempts
            
        except Exception as e:
            print(f"Error retrieving attempt paths for run {run_id}: {e}")
            return []
    
    def get_path_statistics(self, run_id: str) -> dict:
        """Get path statistics for a run."""
        sql = """
        SELECT 
            COUNT(*) as total_attempts,
            COUNT(DISTINCT ap.path_definition_id) as unique_paths,
            AVG(pd.node_count) as avg_nodes,
            AVG(pd.link_count) as avg_links,
            AVG(pd.total_length_mm) as avg_length,
            SUM(pd.coverage) as total_coverage
        FROM tb_attempt_paths ap
        JOIN tb_path_definitions pd ON ap.path_definition_id = pd.id
        WHERE ap.run_id = ?
        """
        
        try:
            result = self.db.query(sql, [run_id])
            if not result:
                return {}
            
            row = result[0]
            return {
                'total_attempts': row[0] or 0,
                'unique_paths': row[1] or 0,
                'avg_nodes': float(row[2]) if row[2] else 0.0,
                'avg_links': float(row[3]) if row[3] else 0.0,
                'avg_length_mm': float(row[4]) if row[4] else 0.0,
                'total_coverage': float(row[5]) if row[5] else 0.0
            }
            
        except Exception as e:
            print(f"Error getting path statistics for run {run_id}: {e}")
            return {}
    
    def store_path_tags(self, path_def_id: int, run_id: str, tags: List[dict]):
        """Store tags for a path definition."""
        sql = """
        INSERT INTO tb_path_tags (
            path_definition_id, run_id, path_hash, tag_type, tag_code, 
            notes, tag, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        try:
            # Get path hash
            path_def = self.get_path_definition(path_def_id)
            path_hash = path_def.path_hash if path_def else None
            
            for tag in tags:
                self.db.update(sql, [
                    path_def_id,
                    run_id,
                    path_hash,
                    tag.get('tag_type', 'QA'),
                    tag.get('tag_code', 'UNKNOWN'),
                    tag.get('notes'),
                    tag.get('tag'),
                    datetime.now()
                ])
                
        except Exception as e:
            print(f"Error storing path tags: {e}")