import uuid
import logging
from datetime import datetime
from typing import Optional, Dict, Any, List
from dataclasses import dataclass
from enum import Enum

from .random_manager import RandomManager
from .path_manager import PathManager
from .coverage_manager import CoverageManager
from .validation_manager import ValidationManager


class RunStatus(Enum):
    RUNNING = 'RUNNING'
    DONE = 'DONE'
    FAILED = 'FAILED'


class ExecutionMode(Enum):
    DEFAULT = 'DEFAULT'
    INTERACTIVE = 'INTERACTIVE'
    UNATTENDED = 'UNATTENDED'


class Approach(Enum):
    RANDOM = 'RANDOM'
    SCENARIO = 'SCENARIO'


class Method(Enum):
    SIMPLE = 'SIMPLE'
    STRATIFIED = 'STRATIFIED'
    PREDEFINED = 'PREDEFINED'
    SYNTHETIC = 'SYNTHETIC'
    FILE = 'FILE'


@dataclass
class RunSummary:
    """Summary of run execution results."""
    run_id: str
    total_attempts: int
    total_paths_found: int
    unique_paths: int
    total_errors: int
    total_reviews: int
    critical_errors: int
    target_coverage: Optional[float]
    achieved_coverage: Optional[float]
    coverage_efficiency: Optional[float]
    total_nodes: int
    total_links: int
    avg_path_nodes: float
    avg_path_links: float
    avg_path_length: float
    success_rate: float
    completion_status: str
    execution_time_seconds: float
    started_at: datetime
    ended_at: Optional[datetime]


class RunManager:
    """Main orchestration class for managing analysis runs."""
    
    def __init__(self, db_connection):
        self.db = db_connection
        self.logger = logging.getLogger(__name__)
        
        # Initialize managers
        self.random_manager = RandomManager(db_connection)
        self.path_manager = PathManager(db_connection)
        self.coverage_manager = CoverageManager(db_connection)
        self.validation_manager = ValidationManager(db_connection)
        
    def execute_run(self, config: 'RunConfig') -> RunSummary:
        """Execute a complete analysis run."""
        self.logger.info(f'Starting run {config.run_id} with approach {config.approach.value}')
        
        try:
            # Initialize run in database
            self._initialize_run(config)
            
            # Execute based on approach
            if config.approach == Approach.RANDOM:
                summary = self._execute_random_run(config)
            elif config.approach == Approach.SCENARIO:
                summary = self._execute_scenario_run(config)
            else:
                raise ValueError(f'Unsupported approach: {config.approach}')
            
            # Update run status
            self._update_run_status(config.run_id, RunStatus.DONE)
            
            # Generate and store summary
            self._store_run_summary(summary)
            
            self.logger.info(f'Run {config.run_id} completed successfully')
            return summary
            
        except Exception as e:
            self.logger.error(f'Run {config.run_id} failed: {str(e)}')
            self._update_run_status(config.run_id, RunStatus.FAILED)
            raise
    
    def _initialize_run(self, config: 'RunConfig'):
        """Initialize run record in database."""
        run_data = {
            'id': config.run_id,
            'date': config.started_at.date(),
            'approach': config.approach.value,
            'method': config.method.value,
            'tag': config.tag,
            'status': RunStatus.RUNNING.value,
            'execution_mode': config.execution_mode.value,
            'run_at': config.started_at
        }
        
        # Add random-specific fields
        if config.random_config:
            run_data.update({
                'coverage_target': config.random_config.coverage_target,
                'fab': config.random_config.fab,
                'toolset': config.random_config.toolset,
                'phase_no': config.random_config.phase_no,
                'model_no': config.random_config.model_no,
                'total_coverage': 0.0,
                'total_nodes': 0,
                'total_links': 0
            })
        
        # Add scenario-specific fields
        if config.scenario_config:
            scenario_type = None
            if config.scenario_config.scenario_code:
                if config.scenario_config.scenario_code.startswith('PRE'):
                    scenario_type = 'PREDEFINED'
                elif config.scenario_config.scenario_code.startswith('SYN'):
                    scenario_type = 'SYNTHETIC'
            
            run_data.update({
                'scenario_code': config.scenario_config.scenario_code,
                'scenario_file': config.scenario_config.scenario_file,
                'scenario_type': scenario_type,
                'total_coverage': 0.0,
                'total_nodes': 0,
                'total_links': 0
            })
        
        # Insert run record
        columns = ', '.join(run_data.keys())
        placeholders = ', '.join(['%s'] * len(run_data))
        query = f'INSERT INTO tb_runs ({columns}) VALUES ({placeholders})'
        
        with self.db.cursor() as cursor:
            cursor.execute(query, list(run_data.values()))
            self.db.commit()
    
    def _execute_random_run(self, config: 'RunConfig') -> RunSummary:
        """Execute a random sampling run."""
        self.logger.info(f'Executing random run with target coverage: {config.random_config.coverage_target}')
        
        start_time = datetime.now()
        
        # Initialize coverage tracking
        self.coverage_manager.initialize_coverage(
            config.run_id,
            config.random_config.fab,
            config.random_config.phase_no,
            config.random_config.model_no,
            config.random_config.toolset
        )
        
        # Generate random paths until coverage target is reached
        total_attempts = 0
        total_paths_found = 0
        
        while not self.coverage_manager.is_target_reached(config.run_id, config.random_config.coverage_target):
            if config.verbose_mode:
                current_coverage = self.coverage_manager.get_current_coverage(config.run_id)
                self.logger.info(f'Current coverage: {current_coverage:.4f}, Target: {config.random_config.coverage_target}')
            
            # Generate random path attempt
            path_result = self.random_manager.generate_random_path(
                config.run_id,
                config.random_config
            )
            
            total_attempts += 1
            
            if path_result:
                # Store path and update coverage
                path_id = self.path_manager.store_path(config.run_id, path_result)
                self.coverage_manager.update_coverage(config.run_id, path_result)
                total_paths_found += 1
                
                if config.verbose_mode:
                    self.logger.info(f'Path found: {path_result.start_node_id} -> {path_result.end_node_id}')
            else:
                # Check if unused PoCs should be flagged for review
                self._check_unused_pocs(config.run_id)
        
        # Validate all found paths
        self.logger.info('Starting path validation...')
        validation_results = self.validation_manager.validate_run_paths(config.run_id)
        
        end_time = datetime.now()
        execution_time = (end_time - start_time).total_seconds()
        
        # Calculate final metrics
        final_coverage = self.coverage_manager.get_current_coverage(config.run_id)
        unique_paths = self.path_manager.get_unique_path_count(config.run_id)
        
        # Get path statistics
        path_stats = self.path_manager.get_path_statistics(config.run_id)
        
        return RunSummary(
            run_id=config.run_id,
            total_attempts=total_attempts,
            total_paths_found=total_paths_found,
            unique_paths=unique_paths,
            total_errors=validation_results.total_errors,
            total_reviews=validation_results.total_reviews,
            critical_errors=validation_results.critical_errors,
            target_coverage=config.random_config.coverage_target,
            achieved_coverage=final_coverage,
            coverage_efficiency=final_coverage / config.random_config.coverage_target if config.random_config.coverage_target else None,
            total_nodes=path_stats.get('total_nodes', 0),
            total_links=path_stats.get('total_links', 0),
            avg_path_nodes=path_stats.get('avg_path_nodes', 0.0),
            avg_path_links=path_stats.get('avg_path_links', 0.0),
            avg_path_length=path_stats.get('avg_path_length', 0.0),
            success_rate=total_paths_found / total_attempts if total_attempts > 0 else 0.0,
            completion_status='COMPLETED' if final_coverage >= config.random_config.coverage_target else 'PARTIAL',
            execution_time_seconds=execution_time,
            started_at=start_time,
            ended_at=end_time
        )
    
    def _execute_scenario_run(self, config: 'RunConfig') -> RunSummary:
        """Execute a scenario-based run."""
        # TODO: Implement scenario execution
        # This would involve loading predefined scenarios and executing them
        raise NotImplementedError('Scenario execution not implemented yet')
    
    def _check_unused_pocs(self, run_id: str):
        """Check for unused PoCs that should be flagged for review."""
        # Query for used PoCs that don't have paths
        query = '''
            SELECT ep.id, ep.equipment_id, ep.node_id, ep.utility_no, ep.markers, ep.reference
            FROM tb_equipment_pocs ep
            WHERE ep.is_used = TRUE
            AND ep.node_id NOT IN (
                SELECT DISTINCT start_node_id FROM tb_attempt_paths WHERE run_id = %s
                UNION
                SELECT DISTINCT end_node_id FROM tb_attempt_paths WHERE run_id = %s
            )
            LIMIT 100
        '''
        
        with self.db.cursor() as cursor:
            cursor.execute(query, (run_id, run_id))
            unused_pocs = cursor.fetchall()
            
            for poc in unused_pocs:
                # Flag for review
                self.validation_manager.flag_for_review(
                    run_id=run_id,
                    flag_type='MANUAL_REVIEW',
                    severity='MEDIUM',
                    reason='Used PoC with no discoverable paths',
                    object_type='POC',
                    object_id=poc['node_id'],
                    object_guid=f'poc_{poc["id"]}',
                    notes=f'PoC {poc["id"]} marked as used but no paths found'
                )
    
    def _update_run_status(self, run_id: str, status: RunStatus):
        """Update run status in database."""
        query = '''
            UPDATE tb_runs 
            SET status = %s, ended_at = %s 
            WHERE id = %s
        '''
        
        with self.db.cursor() as cursor:
            cursor.execute(query, (status.value, datetime.now(), run_id))
            self.db.commit()
    
    def _store_run_summary(self, summary: RunSummary):
        """Store run summary in database."""
        query = '''
            INSERT INTO tb_run_summaries (
                run_id, total_attempts, total_paths_found, unique_paths,
                total_errors, total_reviews, critical_errors,
                target_coverage, achieved_coverage, coverage_efficiency,
                total_nodes, total_links, avg_path_nodes, avg_path_links, avg_path_length,
                success_rate, completion_status, execution_time_seconds,
                started_at, ended_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
        '''
        
        with self.db.cursor() as cursor:
            cursor.execute(query, (
                summary.run_id, summary.total_attempts, summary.total_paths_found, summary.unique_paths,
                summary.total_errors, summary.total_reviews, summary.critical_errors,
                summary.target_coverage, summary.achieved_coverage, summary.coverage_efficiency,
                summary.total_nodes, summary.total_links, summary.avg_path_nodes, summary.avg_path_links, summary.avg_path_length,
                summary.success_rate, summary.completion_status, summary.execution_time_seconds,
                summary.started_at, summary.ended_at
            ))
            self.db.commit()
    
    def get_run_status(self, run_id: str) -> Optional[Dict[str, Any]]:
        """Get current run status and metrics."""
        query = '''
            SELECT r.*, rs.achieved_coverage, rs.total_attempts, rs.total_paths_found
            FROM tb_runs r
            LEFT JOIN tb_run_summaries rs ON r.id = rs.run_id
            WHERE r.id = %s
        '''
        
        with self.db.cursor() as cursor:
            cursor.execute(query, (run_id,))
            return cursor.fetchone()
    
    def list_runs(self, limit: int = 50, status: Optional[RunStatus] = None) -> List[Dict[str, Any]]:
        """List recent runs with optional status filter."""
        query = '''
            SELECT r.*, rs.achieved_coverage, rs.total_attempts, rs.total_paths_found
            FROM tb_runs r
            LEFT JOIN tb_run_summaries rs ON r.id = rs.run_id
        '''
        
        params = []
        if status:
            query += ' WHERE r.status = %s'
            params.append(status.value)
        
        query += ' ORDER BY r.run_at DESC LIMIT %s'
        params.append(limit)
        
        with self.db.cursor() as cursor:
            cursor.execute(query, params)
            return cursor.fetchall()
