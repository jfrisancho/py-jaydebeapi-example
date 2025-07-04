"""
Main execution orchestration service for path discovery and validation.
Handles both random sampling and scenario-based approaches.
"""

import uuid
import logging
from datetime import datetime, date
from typing import Optional, Dict, Any, List, Tuple
from dataclasses import dataclass
from enum import Enum

from random_service import RandomService
from path_service import PathService
from coverage_service import CoverageService
from validation_service import ValidationService


class ApproachType(Enum):
    RANDOM = "RANDOM"
    SCENARIO = "SCENARIO"


class MethodType(Enum):
    SIMPLE = "SIMPLE"
    STRATIFIED = "STRATIFIED"
    PREDEFINED = "PREDEFINED"
    SYNTHETIC = "SYNTHETIC"
    FILE = "FILE"


class ExecutionMode(Enum):
    DEFAULT = "DEFAULT"
    INTERACTIVE = "INTERACTIVE"
    UNATTENDED = "UNATTENDED"


class RunStatus(Enum):
    RUNNING = "RUNNING"
    DONE = "DONE"
    FAILED = "FAILED"


@dataclass
class RunConfig:
    """Configuration for a run execution."""
    coverage_target: float
    approach: ApproachType = ApproachType.RANDOM
    method: MethodType = MethodType.SIMPLE
    execution_mode: ExecutionMode = ExecutionMode.DEFAULT
    
    # Filtering parameters
    fab: Optional[str] = None
    toolset: Optional[str] = None
    phase_no: Optional[int] = None
    model_no: Optional[int] = None
    
    # Scenario-specific parameters
    scenario_code: Optional[str] = None
    scenario_file: Optional[str] = None
    scenario_type: Optional[str] = None
    
    # Additional parameters
    max_attempts: int = 10000
    timeout_seconds: int = 3600
    tag: Optional[str] = None


class RunService:
    """Main orchestration service for path discovery runs."""
    
    def __init__(self, db_connection):
        self.db = db_connection
        self.logger = logging.getLogger(__name__)
        
        # Initialize dependent services
        self.random_service = RandomService(db_connection)
        self.path_service = PathService(db_connection)
        self.coverage_service = CoverageService(db_connection)
        self.validation_service = ValidationService(db_connection)
    
    def execute_run(self, config: RunConfig) -> str:
        """
        Execute a complete run based on configuration.
        Returns the run ID.
        """
        run_id = str(uuid.uuid4())
        
        try:
            # Create run record
            self._create_run_record(run_id, config)
            
            if config.approach == ApproachType.RANDOM:
                result = self._execute_random_run(run_id, config)
            else:
                result = self._execute_scenario_run(run_id, config)
            
            # Update run status and metrics
            self._update_run_completion(run_id, result)
            
            # Generate summary
            self._generate_run_summary(run_id)
            
            self.logger.info(f"Run {run_id} completed successfully")
            return run_id
            
        except Exception as e:
            self.logger.error(f"Run {run_id} failed: {str(e)}")
            self._mark_run_failed(run_id, str(e))
            raise
    
    def _create_run_record(self, run_id: str, config: RunConfig) -> None:
        """Create initial run record in database."""
        tag = config.tag or self._generate_run_tag(config)
        
        query = """
        INSERT INTO tb_runs (
            id, date, approach, method, coverage_target, fab, toolset, 
            phase_no, model_no, scenario_code, scenario_file, scenario_type,
            total_coverage, total_nodes, total_links, tag, status, 
            execution_mode, run_at
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
            0.0, 0, 0, %s, %s, %s, %s
        )
        """
        
        with self.db.cursor() as cursor:
            cursor.execute(query, (
                run_id, date.today(), config.approach.value, config.method.value,
                config.coverage_target, config.fab, config.toolset,
                config.phase_no, config.model_no, config.scenario_code,
                config.scenario_file, config.scenario_type,
                tag, RunStatus.RUNNING.value, config.execution_mode.value,
                datetime.now()
            ))
        self.db.commit()
    
    def _execute_random_run(self, run_id: str, config: RunConfig) -> Dict[str, Any]:
        """Execute random sampling run."""
        self.logger.info(f"Starting random run {run_id} with target coverage {config.coverage_target}")
        
        # Initialize coverage tracker
        self.coverage_service.initialize_coverage(run_id, config)
        
        attempts = 0
        paths_found = 0
        start_time = datetime.now()
        
        while attempts < config.max_attempts:
            # Check timeout
            if (datetime.now() - start_time).seconds > config.timeout_seconds:
                self.logger.warning(f"Run {run_id} timed out after {config.timeout_seconds} seconds")
                break
            
            attempts += 1
            
            try:
                # Generate random PoC pair
                poc_pair = self.random_service.generate_random_poc_pair(config)
                
                if poc_pair is None:
                    continue
                
                from_poc, to_poc = poc_pair
                
                # Record attempt
                self._record_attempt(run_id, from_poc, to_poc, attempts)
                
                # Find path
                path_result = self.path_service.find_path(from_poc, to_poc, config)
                
                if path_result:
                    paths_found += 1
                    path_definition_id = self.path_service.store_path_definition(
                        run_id, path_result, config
                    )
                    
                    # Update coverage
                    coverage_updated = self.coverage_service.update_coverage(
                        run_id, path_result
                    )
                    
                    if coverage_updated:
                        current_coverage = self.coverage_service.get_current_coverage(run_id)
                        self.logger.info(f"Coverage updated: {current_coverage:.4f}")
                        
                        # Check if target reached
                        if current_coverage >= config.coverage_target:
                            self.logger.info(f"Target coverage {config.coverage_target} reached!")
                            break
                
                else:
                    # Check if PoCs should be connected but aren't
                    self._check_disconnected_pocs(run_id, from_poc, to_poc)
                
                # Progress logging
                if attempts % 100 == 0:
                    current_coverage = self.coverage_service.get_current_coverage(run_id)
                    self.logger.info(f"Attempt {attempts}: {paths_found} paths found, coverage: {current_coverage:.4f}")
            
            except Exception as e:
                self.logger.error(f"Error in attempt {attempts}: {str(e)}")
                continue
        
        # Final coverage check
        final_coverage = self.coverage_service.get_current_coverage(run_id)
        
        return {
            'attempts': attempts,
            'paths_found': paths_found,
            'final_coverage': final_coverage,
            'target_reached': final_coverage >= config.coverage_target
        }
    
    def _execute_scenario_run(self, run_id: str, config: RunConfig) -> Dict[str, Any]:
        """Execute scenario-based run."""
        # This would be implemented for scenario-based testing
        # For now, we focus on random sampling
        raise NotImplementedError("Scenario runs not implemented yet")
    
    def _record_attempt(self, run_id: str, from_poc: Dict, to_poc: Dict, attempt_no: int) -> None:
        """Record a path finding attempt."""
        query = """
        INSERT INTO tb_attempt_paths (
            run_id, path_definition_id, start_node_id, end_node_id, 
            cost, picked_at, notes
        ) VALUES (%s, NULL, %s, %s, NULL, %s, %s)
        """
        
        with self.db.cursor() as cursor:
            cursor.execute(query, (
                run_id, from_poc['node_id'], to_poc['node_id'],
                datetime.now(), f"Attempt #{attempt_no}"
            ))
        self.db.commit()
    
    def _check_disconnected_pocs(self, run_id: str, from_poc: Dict, to_poc: Dict) -> None:
        """Check if disconnected PoCs should be flagged for review."""
        # Check if both PoCs are marked as used
        if from_poc.get('is_used') and to_poc.get('is_used'):
            # Same utility type - might need connection
            if (from_poc.get('utility_no') == to_poc.get('utility_no') and 
                from_poc.get('utility_no') is not None):
                
                self._create_review_flag(
                    run_id, from_poc, to_poc,
                    "CONNECTIVITY_ISSUE",
                    "Used PoCs with same utility have no path"
                )
    
    def _create_review_flag(self, run_id: str, from_poc: Dict, to_poc: Dict, 
                           flag_type: str, reason: str) -> None:
        """Create a review flag for manual inspection."""
        query = """
        INSERT INTO tb_review_flags (
            run_id, flag_type, severity, reason, object_type, object_id,
            object_guid, created_at, notes
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        with self.db.cursor() as cursor:
            cursor.execute(query, (
                run_id, flag_type, "MEDIUM", reason, "POC",
                from_poc['id'], from_poc.get('equipment_guid', ''),
                datetime.now(),
                f"PoC pair: {from_poc['node_id']} -> {to_poc['node_id']}"
            ))
        self.db.commit()
    
    def _update_run_completion(self, run_id: str, result: Dict[str, Any]) -> None:
        """Update run record with completion data."""
        query = """
        UPDATE tb_runs SET
            total_coverage = %s,
            total_nodes = %s,
            total_links = %s,
            status = %s,
            ended_at = %s
        WHERE id = %s
        """
        
        coverage = result.get('final_coverage', 0.0)
        status = RunStatus.DONE.value if result.get('target_reached', False) else RunStatus.DONE.value
        
        with self.db.cursor() as cursor:
            cursor.execute(query, (
                coverage, 0, 0,  # Will be updated by coverage service
                status, datetime.now(), run_id
            ))
        self.db.commit()
    
    def _mark_run_failed(self, run_id: str, error_message: str) -> None:
        """Mark run as failed with error message."""
        query = """
        UPDATE tb_runs SET
            status = %s,
            ended_at = %s
        WHERE id = %s
        """
        
        with self.db.cursor() as cursor:
            cursor.execute(query, (
                RunStatus.FAILED.value, datetime.now(), run_id
            ))
        self.db.commit()
    
    def _generate_run_summary(self, run_id: str) -> None:
        """Generate comprehensive run summary."""
        # Get run details
        run_query = "SELECT * FROM tb_runs WHERE id = %s"
        
        with self.db.cursor() as cursor:
            cursor.execute(run_query, (run_id,))
            run_data = cursor.fetchone()
        
        if not run_data:
            return
        
        # Calculate metrics
        metrics = self._calculate_run_metrics(run_id)
        
        # Insert summary
        summary_query = """
        INSERT INTO tb_run_summaries (
            run_id, total_attempts, total_paths_found, unique_paths,
            total_errors, total_reviews, target_coverage, achieved_coverage,
            coverage_efficiency, total_nodes, total_links, success_rate,
            completion_status, execution_time_seconds, started_at, ended_at
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """
        
        with self.db.cursor() as cursor:
            cursor.execute(summary_query, (
                run_id, metrics['total_attempts'], metrics['total_paths_found'],
                metrics['unique_paths'], metrics['total_errors'], metrics['total_reviews'],
                metrics['target_coverage'], metrics['achieved_coverage'],
                metrics['coverage_efficiency'], metrics['total_nodes'], metrics['total_links'],
                metrics['success_rate'], metrics['completion_status'],
                metrics['execution_time_seconds'], metrics['started_at'], metrics['ended_at']
            ))
        self.db.commit()
    
    def _calculate_run_metrics(self, run_id: str) -> Dict[str, Any]:
        """Calculate comprehensive metrics for run summary."""
        metrics = {}
        
        with self.db.cursor() as cursor:
            # Basic counts
            cursor.execute("SELECT COUNT(*) FROM tb_attempt_paths WHERE run_id = %s", (run_id,))
            metrics['total_attempts'] = cursor.fetchone()[0]
            
            cursor.execute("""
                SELECT COUNT(*) FROM tb_attempt_paths ap
                JOIN tb_path_definitions pd ON ap.path_definition_id = pd.id
                WHERE ap.run_id = %s
            """, (run_id,))
            metrics['total_paths_found'] = cursor.fetchone()[0]
            
            cursor.execute("""
                SELECT COUNT(DISTINCT pd.path_hash) FROM tb_attempt_paths ap
                JOIN tb_path_definitions pd ON ap.path_definition_id = pd.id
                WHERE ap.run_id = %s
            """, (run_id,))
            metrics['unique_paths'] = cursor.fetchone()[0]
            
            # Error and review counts
            cursor.execute("SELECT COUNT(*) FROM tb_validation_errors WHERE run_id = %s", (run_id,))
            metrics['total_errors'] = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM tb_review_flags WHERE run_id = %s", (run_id,))
            metrics['total_reviews'] = cursor.fetchone()[0]
            
            # Run details
            cursor.execute("""
                SELECT coverage_target, total_coverage, run_at, ended_at
                FROM tb_runs WHERE id = %s
            """, (run_id,))
            run_data = cursor.fetchone()
            
            if run_data:
                metrics['target_coverage'] = run_data[0]
                metrics['achieved_coverage'] = run_data[1]
                metrics['started_at'] = run_data[2]
                metrics['ended_at'] = run_data[3]
                
                # Calculate efficiency
                if metrics['target_coverage'] > 0:
                    metrics['coverage_efficiency'] = metrics['achieved_coverage'] / metrics['target_coverage']
                else:
                    metrics['coverage_efficiency'] = 0.0
                
                # Calculate execution time
                if metrics['ended_at'] and metrics['started_at']:
                    delta = metrics['ended_at'] - metrics['started_at']
                    metrics['execution_time_seconds'] = delta.total_seconds()
                else:
                    metrics['execution_time_seconds'] = 0
        
        # Additional metrics
        metrics['total_nodes'] = self.coverage_service.get_total_nodes_covered(run_id)
        metrics['total_links'] = self.coverage_service.get_total_links_covered(run_id)
        
        # Success rate
        if metrics['total_attempts'] > 0:
            metrics['success_rate'] = (metrics['total_paths_found'] / metrics['total_attempts']) * 100
        else:
            metrics['success_rate'] = 0.0
        
        # Completion status
        if metrics['achieved_coverage'] >= metrics['target_coverage']:
            metrics['completion_status'] = "COMPLETED"
        elif metrics['total_paths_found'] > 0:
            metrics['completion_status'] = "PARTIAL"
        else:
            metrics['completion_status'] = "FAILED"
        
        return metrics
    
    def _generate_run_tag(self, config: RunConfig) -> str:
        """Generate auto-tag for run identification."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        if config.approach == ApproachType.RANDOM:
            parts = ["RND", timestamp]
            if config.fab:
                parts.append(f"FAB_{config.fab}")
            if config.model_no:
                parts.append(f"M{config.model_no}")
            if config.phase_no:
                parts.append(f"P{config.phase_no}")
            parts.append(f"COV_{int(config.coverage_target * 100)}")
        else:
            parts = ["SCN", timestamp]
            if config.scenario_code:
                parts.append(config.scenario_code)
        
        return "_".join(parts)
    
    def validate_run_paths(self, run_id: str) -> Dict[str, Any]:
        """Validate all paths found in a run."""
        self.logger.info(f"Starting validation for run {run_id}")
        
        # Get all path definitions for this run
        query = """
        SELECT DISTINCT pd.id, pd.path_hash, pd.path_context
        FROM tb_path_definitions pd
        JOIN tb_attempt_paths ap ON pd.id = ap.path_definition_id
        WHERE ap.run_id = %s
        """
        
        validation_results = {
            'total_paths': 0,
            'validation_errors': 0,
            'connectivity_errors': 0,
            'utility_errors': 0,
            'paths_validated': []
        }
        
        with self.db.cursor() as cursor:
            cursor.execute(query, (run_id,))
            path_definitions = cursor.fetchall()
        
        for path_def in path_definitions:
            path_definition_id, path_hash, path_context = path_def
            validation_results['total_paths'] += 1
            
            try:
                # Validate path
                path_validation = self.validation_service.validate_path_definition(
                    run_id, path_definition_id, path_context
                )
                
                validation_results['paths_validated'].append({
                    'path_definition_id': path_definition_id,
                    'path_hash': path_hash,
                    'validation_result': path_validation
                })
                
                # Count errors by type
                for error in path_validation.get('errors', []):
                    validation_results['validation_errors'] += 1
                    if 'connectivity' in error.get('error_type', '').lower():
                        validation_results['connectivity_errors'] += 1
                    elif 'utility' in error.get('error_type', '').lower():
                        validation_results['utility_errors'] += 1
                
            except Exception as e:
                self.logger.error(f"Error validating path {path_definition_id}: {str(e)}")
                validation_results['validation_errors'] += 1
        
        self.logger.info(f"Validation completed for run {run_id}: {validation_results['validation_errors']} errors found")
        return validation_results
