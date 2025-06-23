"""
Service for managing path definitions and attempts.
Updated for simplified schema and new field names.
"""

import json
from datetime import datetime
from typing import Optional, List

from models import PathDefinition, PathResult, AttemptPath, ScenarioExecution, RunConfig # Added RunConfig for context
from enums import SourceType
from db import Database


class PathService:
    """Service for managing path definitions and storage."""
    
    def __init__(self, db: Database):
        self.db = db
    
    def store_path_attempt(self, run_id: str, path_result: PathResult, run_config: RunConfig) -> Optional[int]:
        """Store a path attempt in the database. run_config added for context if needed."""
        if not path_result.path_found or not path_result.path_definition:
            return None
        
        path_def = path_result.path_definition
        
        # First, store or get the path definition
        path_def_id = self._store_path_definition(path_def)
        if not path_def_id:
            return None
        
        # Update path_definition with the ID if it was newly created or retrieved
        path_def.id = path_def_id

        # Store the attempt path (for random paths only)
        if path_def.source_type == SourceType.RANDOM:
            # AttemptPath specific context
            attempt_notes = f"Path found with {path_def.node_count} nodes, {path_def.link_count} links"
            start_node = path_def.path_context.get('start_node_id')
            end_node = path_def.path_context.get('end_node_id')
            utility = path_def.utilities[0] if path_def.utilities else 'UNKNOWN'
            toolset = path_def.path_context.get('toolset_code', run_config.toolset if run_config else '') # fallback to run_config

            attempt = AttemptPath(
                id=None,
                run_id=run_id,
                path_definition_id=path_def_id,
                start_node_id=start_node,
                end_node_id=end_node,
                building_code=path_def.building_code, # This is fab
                category=path_def.category,
                utility=utility,
                toolset=toolset,
                picked_at=datetime.now(), # Or a more specific time from path_result if available
                notes=attempt_notes
            )
            attempt_id = self._store_attempt_path_object(attempt)
            return attempt_id
        else: # SCENARIO paths
            scenario_id = path_def.scenario_id
            if scenario_id:
                # ScenarioExecution specific context
                # execution_status, validation_passed etc. should ideally come from scenario_result processing
                scenario_exec = ScenarioExecution(
                    id=None,
                    run_id=run_id,
                    scenario_id=scenario_id,
                    path_definition_id=path_def_id,
                    execution_status='SUCCESS', # Placeholder - should be determined by validation/execution
                    execution_time_ms=path_def.path_context.get('execution_time_ms'), # If available
                    actual_nodes=path_def.node_count,
                    actual_links=path_def.link_count,
                    actual_coverage=path_def.coverage,
                    validation_passed=True, # Placeholder
                    validation_errors=[],   # Placeholder
                    executed_at=datetime.now(), # Or a more specific time
                    notes=f"Scenario executed: {path_def.node_count} nodes, {path_def.link_count} links"
                )
                execution_id = self._store_scenario_execution_object(scenario_exec)
                return execution_id
        
        return path_def_id # Fallback if neither attempt nor scenario execution stored.
    
    def _store_path_definition(self, path_def: PathDefinition) -> Optional[int]:
        """Store or retrieve existing path definition."""
        
        existing_id = self._get_existing_path_definition(path_def.path_hash)
        if existing_id:
            path_def.id = existing_id # Ensure the model has the ID
            return existing_id
        
        sql = """
        INSERT INTO tb_path_definitions (
            path_hash, source_type, building_code, category, scope, node_count, link_count,
            total_length_mm, coverage, utilities, path_context, scenario_id, scenario_context
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        try:
            utilities_json = json.dumps(path_def.utilities)
            path_context_json = json.dumps(path_def.path_context)
            scenario_context_json = json.dumps(path_def.scenario_context) if path_def.scenario_context else None
            
            # building_code corresponds to fab
            params = [
                path_def.path_hash,
                path_def.source_type.value,
                path_def.building_code, # This is fab
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
            ]
            rows_affected = self.db.update(sql, params)
            
            if rows_affected > 0:
                id_result = self.db.query("SELECT LAST_INSERT_ID()")
                new_id = id_result[0][0] if id_result and id_result[0] else None
                if new_id:
                    path_def.id = new_id # Ensure the model has the ID
                return new_id
            
            return None
            
        except Exception as e:
            print(f"Error storing path definition ({path_def.path_hash}): {e}")
            return None
    
    def _get_existing_path_definition(self, path_hash: str) -> Optional[int]:
        """Check if a path definition already exists and return its ID."""
        sql = "SELECT id FROM tb_path_definitions WHERE path_hash = ?"
        try:
            result = self.db.query(sql, [path_hash])
            return result[0][0] if result and result[0] else None
        except Exception as e:
            print(f"Error checking existing path definition for hash {path_hash}: {e}")
            return None

    def _store_attempt_path_object(self, attempt: AttemptPath) -> Optional[int]:
        """Store an AttemptPath object."""
        sql = """
        INSERT INTO tb_attempt_paths (
            run_id, path_definition_id, start_node_id, end_node_id,
            building_code, category, utility, toolset, picked_at, notes
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        try:
            params = [
                attempt.run_id,
                attempt.path_definition_id,
                attempt.start_node_id,
                attempt.end_node_id,
                attempt.building_code, # This is fab
                attempt.category,
                attempt.utility,
                attempt.toolset,
                attempt.picked_at,
                attempt.notes
            ]
            rows_affected = self.db.update(sql, params)
            if rows_affected > 0:
                id_result = self.db.query("SELECT LAST_INSERT_ID()")
                return id_result[0][0] if id_result and id_result[0] else None
            return None
        except Exception as e:
            print(f"Error storing attempt path for run {attempt.run_id}, def_id {attempt.path_definition_id}: {e}")
            return None

    def _store_scenario_execution_object(self, scenario_exec: ScenarioExecution) -> Optional[int]:
        """Store a ScenarioExecution object."""
        sql = """
        INSERT INTO tb_scenario_executions (
            run_id, scenario_id, path_definition_id, execution_status,
            execution_time_ms, actual_nodes, actual_links, actual_coverage,
            validation_passed, validation_errors, executed_at, notes
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        try:
            validation_errors_json = json.dumps(scenario_exec.validation_errors) if scenario_exec.validation_errors else None
            params = [
                scenario_exec.run_id,
                scenario_exec.scenario_id,
                scenario_exec.path_definition_id,
                scenario_exec.execution_status,
                scenario_exec.execution_time_ms,
                scenario_exec.actual_nodes,
                scenario_exec.actual_links,
                scenario_exec.actual_coverage,
                scenario_exec.validation_passed,
                validation_errors_json,
                scenario_exec.executed_at,
                scenario_exec.notes
            ]
            rows_affected = self.db.update(sql, params)
            if rows_affected > 0:
                id_result = self.db.query("SELECT LAST_INSERT_ID()")
                return id_result[0][0] if id_result and id_result[0] else None
            return None
        except Exception as e:
            print(f"Error storing scenario execution for run {scenario_exec.run_id}, scenario {scenario_exec.scenario_id}: {e}")
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
            if not result or not result[0]:
                return None
            
            row = result[0]
            # Gracefully handle JSON parsing
            utilities = json.loads(row[10]) if row[10] else []
            path_context = json.loads(row[11]) if row[11] else {}
            scenario_context = json.loads(row[13]) if row[13] and row[13] != 'null' else None

            return PathDefinition(
                id=row[0],
                path_hash=row[1],
                source_type=SourceType(row[2]),
                building_code=row[3], # This is fab
                category=row[4],
                scope=row[5],
                node_count=row[6],
                link_count=row[7],
                total_length_mm=float(row[8]) if row[8] is not None else 0.0,
                coverage=float(row[9]) if row[9] is not None else 0.0,
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
                    building_code=row[5], # This is fab
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
                validation_errors = json.loads(row[10]) if row[10] and row[10] != 'null' else None
                execution = ScenarioExecution(
                    id=row[0],
                    run_id=row[1],
                    scenario_id=row[2],
                    path_definition_id=row[3],
                    execution_status=row[4],
                    execution_time_ms=row[5],
                    actual_nodes=row[6],
                    actual_links=row[7],
                    actual_coverage=float(row[8]) if row[8] is not None else None,
                    validation_passed=bool(row[9]) if row[9] is not None else None,
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
        """Get path statistics for a run. To be used by RunService for summary."""
        # This function might need to be smarter or RunService._create_run_summary
        # needs to call this twice (once for random, once for scenario) or parse combined results.
        # For now, providing a combined attempt/success count. Detailed averages are tricky.
        
        # Count total attempts (random)
        random_attempts_sql = "SELECT COUNT(*) FROM tb_attempt_paths WHERE run_id = ?"
        # Count total scenario executions
        scenario_execs_sql = "SELECT COUNT(*) FROM tb_scenario_executions WHERE run_id = ?"
        
        # Count successful random paths (those with a path_definition_id)
        # This assumes path_definition_id implies success for random.
        successful_random_sql = """
        SELECT COUNT(DISTINCT ap.path_definition_id) 
        FROM tb_attempt_paths ap
        WHERE ap.run_id = ?
        """
        # Count successful scenario executions
        successful_scenarios_sql = """
        SELECT COUNT(*) 
        FROM tb_scenario_executions 
        WHERE run_id = ? AND execution_status = 'SUCCESS' AND path_definition_id IS NOT NULL
        """
        
        # Get average node/link counts for successful random paths
        avg_random_stats_sql = """
        SELECT AVG(pd.node_count), AVG(pd.link_count), AVG(pd.total_length_mm)
        FROM tb_path_definitions pd
        JOIN tb_attempt_paths ap ON pd.id = ap.path_definition_id
        WHERE ap.run_id = ?
        """
        # Get average node/link counts for successful scenario paths
        avg_scenario_stats_sql = """
        SELECT AVG(pd.node_count), AVG(pd.link_count), AVG(pd.total_length_mm)
        FROM tb_path_definitions pd
        JOIN tb_scenario_executions se ON pd.id = se.path_definition_id
        WHERE se.run_id = ? AND se.execution_status = 'SUCCESS'
        """

        stats = {
            'total_attempts': 0, # Combined Random attempts + Scenario Executions
            'total_paths_found': 0, # Combined successful Random paths + successful Scenario paths
            'unique_paths': 0, # Unique path definitions found across both random and scenario for this run
            'avg_nodes': 0.0,
            'avg_links': 0.0,
            'avg_length_mm': 0.0,
            # Specifics for RunSummary
            'random_attempts': 0,
            'successful_random_paths': 0,
            'scenario_executions': 0,
            'successful_scenarios': 0,
        }
        
        try:
            # Random path counts
            res_ra = self.db.query(random_attempts_sql, [run_id])
            stats['random_attempts'] = res_ra[0][0] if res_ra and res_ra[0] else 0
            
            res_sr = self.db.query(successful_random_sql, [run_id])
            stats['successful_random_paths'] = res_sr[0][0] if res_sr and res_sr[0] else 0

            # Scenario execution counts
            res_se = self.db.query(scenario_execs_sql, [run_id])
            stats['scenario_executions'] = res_se[0][0] if res_se and res_se[0] else 0
            
            res_ss = self.db.query(successful_scenarios_sql, [run_id])
            stats['successful_scenarios'] = res_ss[0][0] if res_ss and res_ss[0] else 0

            stats['total_attempts'] = stats['random_attempts'] + stats['scenario_executions']
            stats['total_paths_found'] = stats['successful_random_paths'] + stats['successful_scenarios']
            
            # Unique paths for the run (combining both sources)
            unique_paths_sql = """
            SELECT COUNT(DISTINCT id) FROM tb_path_definitions WHERE id IN (
                SELECT path_definition_id FROM tb_attempt_paths WHERE run_id = ?
                UNION
                SELECT path_definition_id FROM tb_scenario_executions WHERE run_id = ? AND path_definition_id IS NOT NULL
            )
            """
            res_up = self.db.query(unique_paths_sql, [run_id, run_id])
            stats['unique_paths'] = res_up[0][0] if res_up and res_up[0] else 0
            
            # Averages (simplified: weighted average if both present, else the one available)
            total_node_sum = 0
            total_link_sum = 0
            total_length_sum = 0
            count_for_avg = 0

            res_avg_random = self.db.query(avg_random_stats_sql, [run_id])
            if res_avg_random and res_avg_random[0] and res_avg_random[0][0] is not None:
                avg_r_nodes, avg_r_links, avg_r_len = res_avg_random[0]
                num_r_paths = stats['successful_random_paths']
                total_node_sum += (avg_r_nodes or 0) * num_r_paths
                total_link_sum += (avg_r_links or 0) * num_r_paths
                total_length_sum += (avg_r_len or 0) * num_r_paths
                count_for_avg += num_r_paths
            
            res_avg_scenario = self.db.query(avg_scenario_stats_sql, [run_id])
            if res_avg_scenario and res_avg_scenario[0] and res_avg_scenario[0][0] is not None:
                avg_s_nodes, avg_s_links, avg_s_len = res_avg_scenario[0]
                num_s_paths = stats['successful_scenarios']
                total_node_sum += (avg_s_nodes or 0) * num_s_paths
                total_link_sum += (avg_s_links or 0) * num_s_paths
                total_length_sum += (avg_s_len or 0) * num_s_paths
                count_for_avg += num_s_paths

            if count_for_avg > 0:
                stats['avg_nodes'] = total_node_sum / count_for_avg
                stats['avg_links'] = total_link_sum / count_for_avg
                stats['avg_length_mm'] = total_length_sum / count_for_avg
            
            stats['success_rate'] = (stats['total_paths_found'] / stats['total_attempts'] * 100) if stats['total_attempts'] > 0 else 0.0
            
            return stats
            
        except Exception as e:
            print(f"Error getting path statistics for run {run_id}: {e}")
            return stats # Return partially filled stats or default
    
    def store_path_tags(self, path_def_id: int, run_id: str, tags: List[dict]):
        """Store tags for a path definition."""
        # tb_path_tags: id, path_definition_id, run_id, path_hash, tag_type, tag_code, 
        #               tag_value, source, confidence, created_at, created_by, notes
        sql = """
        INSERT INTO tb_path_tags (
            path_definition_id, run_id, path_hash, tag_type, tag_code, 
            tag_value, source, confidence, created_at, created_by, notes
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        try:
            path_def = self.get_path_definition(path_def_id)
            path_hash = path_def.path_hash if path_def else None
            
            if not path_hash:
                print(f"Error storing path tags: Could not retrieve path_hash for path_definition_id {path_def_id}")
                return

            for tag_dict in tags: # Renamed 'tag' to 'tag_dict' to avoid conflict
                params = [
                    path_def_id,
                    run_id,
                    path_hash,
                    tag_dict.get('tag_type', 'QA'), # From TagType enum
                    tag_dict.get('tag_code', 'UNKNOWN'),
                    tag_dict.get('tag_value'),
                    tag_dict.get('source', 'SYSTEM'),
                    tag_dict.get('confidence', 1.0),
                    tag_dict.get('created_at', datetime.now()),
                    tag_dict.get('created_by'), # Optional: user or system component
                    tag_dict.get('notes')
                ]
                self.db.update(sql, params)
                
        except Exception as e:
            print(f"Error storing path tags for path_definition_id {path_def_id}, run_id {run_id}: {e}")