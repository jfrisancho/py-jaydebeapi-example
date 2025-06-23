"""
Service for managing path definitions and attempts.
"""

import json
from datetime import datetime
from typing import Optional, List

from models import PathDefinition, PathResult, AttemptPath, ScenarioExecution
from enums import SourceType
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
        
        # Store the attempt path (for random paths only)
        if path_def.source_type == SourceType.RANDOM:
            attempt_id = self._store_attempt_path(run_id, path_def_id, path_def)
            return attempt_id
        else:
            # For scenario paths, store scenario execution
            scenario_id = path_def.scenario_id
            if scenario_id:
                execution_id = self._store_scenario_execution(run_id, scenario_id, path_def_id, path_def)
                return execution_id
        
        return path_def_id
    
    def _store_path_definition(self, path_def: PathDefinition) -> Optional[int]:
        """Store or retrieve existing path definition."""
        
        # Check if path definition already exists
        existing_id = self._get_existing_path_definition(path_def.path_hash)
        if existing_id:
            return existing_id
        
        # Store new path definition
        sql = """
        INSERT INTO tb_path_definitions (
            path_hash, source_type, building_code, category, scope, node_count, link_count,
            total_length_mm, coverage, utilities, path_context, scenario_id, scenario_context
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        try:
            # Serialize utilities and context as JSON
            utilities_json = json.dumps(path_def.utilities)
            path_context_json = json.dumps(path_def.path_context)
            scenario_context_json = json.dumps(path_def.scenario_context) if path_def.scenario_context else None
            
            rows_affected = self.db.update(sql, [
                path_def.path_hash,
                path_def.source_type.value,
                path_def.building_code,
                path_def.category,
                path_def.scope,
                path_def.node_count,
                path_def.link_count,
                path_def.total_length_mm,
                path_def.coverage,
                utilities_json,
                path_context_json,
                path_def.scenario_id,
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
        """Store an attempt path record for random paths."""
        sql = """
        INSERT INTO tb_attempt_paths (
            run_id, path_definition_id, start_node_id, end_node_id,
            building_code, category, utility, toolset, picked_at, notes
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        try:
            # Extract start/end nodes from path context
            start_node = path_def.path_context.get('start_node_id')
            end_node = path_def.path_context.get('end_node_id')
            
            # Get primary utility (first one if multiple)
            utility = path_def.utilities[0] if path_def.utilities else 'UNKNOWN'
            
            # Get toolset from path context
            toolset = path_def.path_context.get('toolset_code', '')
            
            rows_affected = self.db.update(sql, [
                run_id,
                path_def_id,
                start_node,
                end_node,
                path_def.building_code,
                path_def.category,
                utility,
                toolset,
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
    
    def _store_scenario_execution(self, run_id: str, scenario_id: int, path_def_id: int, 
                                 path_def: PathDefinition) -> Optional[int]:
        """Store a scenario execution record."""
        sql = """
        INSERT INTO tb_scenario_executions (
            run_id, scenario_id, path_definition_id, execution_status,
            actual_nodes, actual_links, actual_coverage,
            validation_passed, executed_at, notes
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        try:
            rows_affected = self.db.update(sql, [
                run_id,
                scenario_id,
                path_def_id,
                'SUCCESS',  # Status - could be determined by validation results
                path_def.node_count,
                path_def.link_count,
                path_def.coverage,
                True,  # Validation passed - could be determined by validation service
                datetime.now(),
                f"Scenario executed with {path_def.node_count} nodes, {path_def.link_count} links"
            ])
            
            if rows_affected > 0:
                # Get the generated ID
                id_result = self.db.query("SELECT LAST_INSERT_ID()")
                return id_result[0][0] if id_result else None
            
            return None
            
        except Exception as e:
            print(f"Error storing scenario execution: {e}")
            return None
    
    def get_path_definition(self, path_def_id: int) -> Optional[PathDefinition]:
        """Retrieve a path definition by ID."""
        sql = """
        SELECT id, path_hash, source_type, building_code, category, scope, node_count, link_count,
               total_length_mm, coverage, utilities, path_context, scenario_id, scenario_context
        FROM tb_path_definitions 
        WHERE id = ?
        """
        
        try:
            result = self.db.query(sql, [path_def_id])
            if not result:
                return None
            
            row = result[0]
            utilities = json.loads(row[10]) if row[10] else []
            path_context = json.loads(row[11]) if row[11] else {}
            scenario_context = json.loads(row[13]) if row[13] else None
            
            return PathDefinition(
                id=row[0],
                path_hash=row[1],
                source_type=SourceType(row[2]),
                building_code=row[3],
                category=row[4],
                scope=row[5],
                node_count=row[6],
                link_count=row[7],
                total_length_mm=row[8],
                coverage=row[9],
                utilities=utilities,
                path_context=path_context,
                scenario_id=row[12],
                scenario_context=scenario_context
            )
            
        except Exception as e:
            print(f"Error retrieving path definition {path_def_id}: {e}")
            return None
    
    def get_run_attempts(self, run_id: str) -> List[AttemptPath]:
        """Get all attempt paths for a run."""
        sql = """
        SELECT id, run_id, path_definition_id, start_node_id, end_node_id,
               building_code, category, utility, toolset, picked_at, notes
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
                    building_code=row[5],
                    category=row[6],
                    utility=row[7],
                    toolset=row[8],
                    picked_at=row[9],
                    notes=row[10]
                )
                attempts.append(attempt)
            
            return attempts
            
        except Exception as e:
            print(f"Error retrieving attempt paths for run {run_id}: {e}")
            return []
    
    def get_scenario_executions(self, run_id: str) -> List[ScenarioExecution]:
        """Get all scenario executions for a run."""
        sql = """
        SELECT id, run_id, scenario_id, path_definition_id, execution_status,
               execution_time_ms, actual_nodes, actual_links, actual_coverage,
               validation_passed, validation_errors, executed_at, notes
        FROM tb_scenario_executions 
        WHERE run_id = ?
        ORDER BY executed_at
        """
        
        try:
            results = self.db.query(sql, [run_id])
            executions = []
            
            for row in results:
                validation_errors = json.loads(row[10]) if row[10] else None
                
                execution = ScenarioExecution(
                    id=row[0],
                    run_id=row[1],
                    scenario_id=row[2],
                    path_definition_id=row[3],
                    execution_status=row[4],
                    execution_time_ms=row[5],
                    actual_nodes=row[6],
                    actual_links=row[7],
                    actual_coverage=row[8],
                    validation_passed=row[9],
                    validation_errors=validation_errors,
                    executed_at=row[11],
                    notes=row[12]
                )
                executions.append(execution)
            
            return executions
            
        except Exception as e:
            print(f"Error retrieving scenario executions for run {run_id}: {e}")
            return []
    
    def get_path_statistics(self, run_id: str) -> dict:
        """Get path statistics for a run."""
        # Get statistics for random paths
        random_sql = """
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
        
        # Get statistics for scenario executions
        scenario_sql = """
        SELECT 
            COUNT(*) as total_executions,
            COUNT(CASE WHEN execution_status = 'SUCCESS' THEN 1 END) as successful_executions,
            AVG(actual_nodes) as avg_nodes,
            AVG(actual_links) as avg_links,
            AVG(actual_coverage) as avg_coverage,
            AVG(execution_time_ms) as avg_execution_time_ms
        FROM tb_scenario_executions
        WHERE run_id = ?
        """
        
        try:
            stats = {}
            
            # Get random path stats
            random_result = self.db.query(random_sql, [run_id])
            if random_result and random_result[0][0] > 0:
                row = random_result[0]
                stats.update({
                    'random_attempts': row[0] or 0,
                    'unique_random_paths': row[1] or 0,
                    'avg_random_nodes': float(row[2]) if row[2] else 0.0,
                    'avg_random_links': float(row[3]) if row[3] else 0.0,
                    'avg_random_length_mm': float(row[4]) if row[4] else 0.0,
                    'total_random_coverage': float(row[5]) if row[5] else 0.0
                })
            
            # Get scenario execution stats
            scenario_result = self.db.query(scenario_sql, [run_id])
            if scenario_result and scenario_result[0][0] > 0:
                row = scenario_result[0]
                stats.update({
                    'scenario_executions': row[0] or 0,
                    'successful_scenarios': row[1] or 0,
                    'avg_scenario_nodes': float(row[2]) if row[2] else 0.0,
                    'avg_scenario_links': float(row[3]) if row[3] else 0.0,
                    'avg_scenario_coverage': float(row[4]) if row[4] else 0.0,
                    'avg_execution_time_ms': float(row[5]) if row[5] else 0.0
                })
            
            # Calculate combined stats
            total_attempts = stats.get('random_attempts', 0) + stats.get('scenario_executions', 0)
            total_found = stats.get('unique_random_paths', 0) + stats.get('successful_scenarios', 0)
            
            stats.update({
                'total_attempts': total_attempts,
                'total_paths_found': total_found,
                'success_rate': (total_found / total_attempts * 100) if total_attempts > 0 else 0.0
            })
            
            return stats
            
        except Exception as e:
            print(f"Error getting path statistics for run {run_id}: {e}")
            return {}
    
    def store_path_tags(self, path_def_id: int, run_id: str, tags: List[dict]):
        """Store tags for a path definition."""
        sql = """
        INSERT INTO tb_path_tags (
            path_definition_id, run_id, path_hash, tag_type, tag_code, 
            tag_value, source, confidence, created_at, notes
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                    tag.get('tag_value'),
                    tag.get('source', 'SYSTEM'),
                    tag.get('confidence', 1.0),
                    datetime.now(),
                    tag.get('notes')
                ])
                
        except Exception as e:
            print(f"Error storing path tags: {e}")
