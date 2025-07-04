"""
Main execution orchestration for random sampling analysis.
"""
import logging
import uuid
from datetime import datetime
from typing import Optional, Dict, Any, List
from dataclasses import dataclass, asdict
from enum import Enum
import sqlite3
import json

from .random_manager import RandomManager
from .path_manager import PathManager
from .coverage_manager import CoverageManager
from .validation_manager import ValidationManager


class ExecutionMode(Enum):
    DEFAULT = "DEFAULT"
    INTERACTIVE = "INTERACTIVE"
    UNATTENDED = "UNATTENDED"


class Approach(Enum):
    RANDOM = "RANDOM"
    SCENARIO = "SCENARIO"


class Method(Enum):
    SIMPLE = "SIMPLE"
    STRATIFIED = "STRATIFIED"
    PREDEFINED = "PREDEFINED"
    SYNTHETIC = "SYNTHETIC"
    FILE = "FILE"


@dataclass
class RandomRunConfig:
    coverage_target: Optional[float] = None
    
    # Filtering parameters
    fab: Optional[str] = None
    phase_no: Optional[int] = None
    model_no: Optional[int] = None
    toolset: Optional[str] = None
    
    @property
    def coverage_tag(self) -> str:
        return f'{self.coverage_target*100:.0f}P' if self.coverage_target else ''
    
    @property
    def tag(self) -> str:
        tag = self.coverage_tag
        
        if self.fab:
            tag += f'_{self.fab}'
        
        if self.phase_no:
            tag += f'_P{self.phase_no}'
        
        if self.model_no:
            tag += f'_M{self.model_no}'
        
        if self.toolset:
            tag += f'_{self.toolset}'
        
        return tag if tag else ''


@dataclass
class ScenarioRunConfig:    
    scenario_code: Optional[str] = None
    scenario_file: Optional[str] = None
        
    @property
    def tag(self) -> str:
        if self.scenario_code:
            return f'_SC_{self.scenario_code}'
        elif self.scenario_file:
            import hashlib
            return f'_SF_{hashlib.sha256(self.scenario_file.encode("utf-8")).hexdigest()[:8]}'
        else:
            return ''


@dataclass
class RunConfig:
    """Configuration for a single analysis run."""
    run_id: str
    approach: Approach
    method: Method
    started_at: datetime
    tag: str
    random_config: Optional[RandomRunConfig] = None
    scenario_config: Optional[ScenarioRunConfig] = None
    execution_mode: ExecutionMode = ExecutionMode.DEFAULT
    verbose_mode: bool = False
    
    @property
    def coverage_tag(self) -> str:
        return self.random_config.coverage_tag if self.random_config else ''
    
    @classmethod
    def generate_tag(cls, approach: Approach, method: Method, 
                    random_config: Optional[RandomRunConfig] = None,
                    scenario_config: Optional[ScenarioRunConfig] = None,
                    date: Optional[datetime] = None) -> str:
        """Generate tag using the specified format."""
        if date is None:
            date = datetime.now()
        
        tag = f'{date.strftime("%Y%m%d")}_{approach.value}_{method.value}'
        
        if approach == Approach.SCENARIO and scenario_config:
            tag += scenario_config.tag
        elif approach == Approach.RANDOM and random_config:
            tag += random_config.tag
        
        return tag


class RunManager:
    """Main orchestration class for running analysis."""
    
    def __init__(self, db_connection: sqlite3.Connection):
        self.db = db_connection
        self.logger = logging.getLogger(__name__)
        
        # Initialize managers
        self.random_manager = RandomManager(db_connection)
        self.path_manager = PathManager(db_connection)
        self.coverage_manager = CoverageManager(db_connection)
        self.validation_manager = ValidationManager(db_connection)
        
    def run_random_analysis(self, config: RandomRunConfig, 
                          execution_mode: ExecutionMode = ExecutionMode.DEFAULT,
                          verbose: bool = False) -> str:
        """Execute a random sampling analysis."""
        
        # Generate run configuration
        run_config = RunConfig(
            run_id=str(uuid.uuid4()),
            approach=Approach.RANDOM,
            method=Method.SIMPLE,  # Default for random
            started_at=datetime.now(),
            tag=RunConfig.generate_tag(Approach.RANDOM, Method.SIMPLE, 
                                     random_config=config),
            random_config=config,
            execution_mode=execution_mode,
            verbose_mode=verbose
        )
        
        return self._execute_run(run_config)
    
    def run_scenario_analysis(self, config: ScenarioRunConfig,
                            execution_mode: ExecutionMode = ExecutionMode.DEFAULT,
                            verbose: bool = False) -> str:
        """Execute a scenario-based analysis."""
        
        run_config = RunConfig(
            run_id=str(uuid.uuid4()),
            approach=Approach.SCENARIO,
            method=Method.PREDEFINED,  # Default for scenarios
            started_at=datetime.now(),
            tag=RunConfig.generate_tag(Approach.SCENARIO, Method.PREDEFINED,
                                     scenario_config=config),
            scenario_config=config,
            execution_mode=execution_mode,
            verbose_mode=verbose
        )
        
        return self._execute_run(run_config)
    
    def _execute_run(self, config: RunConfig) -> str:
        """Execute a run with the given configuration."""
        try:
            # Create run record
            self._create_run_record(config)
            
            if config.approach == Approach.RANDOM:
                self._execute_random_run(config)
            else:
                self._execute_scenario_run(config)
            
            # Update run status
            self._update_run_status(config.run_id, "DONE")
            
            # Generate summary
            self._generate_run_summary(config.run_id)
            
            self.logger.info(f"Run {config.run_id} completed successfully")
            return config.run_id
            
        except Exception as e:
            self.logger.error(f"Run {config.run_id} failed: {str(e)}")
            self._update_run_status(config.run_id, "FAILED")
            raise
    
    def _create_run_record(self, config: RunConfig) -> None:
        """Create initial run record in database."""
        
        # Get total nodes and links for coverage calculation
        total_nodes, total_links = self.coverage_manager.get_total_nodes_links(
            config.random_config.fab if config.random_config else None,
            config.random_config.model_no if config.random_config else None,
            config.random_config.phase_no if config.random_config else None,
            config.random_config.toolset if config.random_config else None
        )
        
        insert_sql = """
        INSERT INTO tb_runs (
            id, date, approach, method, coverage_target, fab, toolset, 
            phase_no, model_no, scenario_code, scenario_file, scenario_type,
            total_coverage, total_nodes, total_links, tag, status, 
            execution_mode, run_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        params = [
            config.run_id,
            config.started_at.date(),
            config.approach.value,
            config.method.value,
            config.random_config.coverage_target if config.random_config else None,
            config.random_config.fab if config.random_config else None,
            config.random_config.toolset if config.random_config else None,
            config.random_config.phase_no if config.random_config else None,
            config.random_config.model_no if config.random_config else None,
            config.scenario_config.scenario_code if config.scenario_config else None,
            config.scenario_config.scenario_file if config.scenario_config else None,
            self._detect_scenario_type(config.scenario_config) if config.scenario_config else None,
            0.0,  # Initial coverage
            total_nodes,
            total_links,
            config.tag,
            "RUNNING",
            config.execution_mode.value,
            config.started_at
        ]
        
        self.db.execute(insert_sql, params)
        self.db.commit()
    
    def _execute_random_run(self, config: RunConfig) -> None:
        """Execute random sampling analysis."""
        
        self.logger.info(f"Starting random analysis for run {config.run_id}")
        
        # Initialize coverage tracking
        self.coverage_manager.initialize_coverage(
            config.run_id,
            config.random_config.fab,
            config.random_config.model_no,
            config.random_config.phase_no,
            config.random_config.toolset
        )
        
        attempts = 0
        max_attempts = 10000  # Prevent infinite loops
        target_coverage = config.random_config.coverage_target
        
        while attempts < max_attempts:
            # Check current coverage
            current_coverage = self.coverage_manager.get_current_coverage(config.run_id)
            
            if current_coverage >= target_coverage:
                self.logger.info(f"Target coverage {target_coverage:.2%} achieved!")
                break
            
            # Generate random PoC pair
            poc_pair = self.random_manager.generate_random_poc_pair(
                config.random_config.fab,
                config.random_config.model_no,
                config.random_config.phase_no,
                config.random_config.toolset
            )
            
            if not poc_pair:
                attempts += 1
                continue
            
            # Record attempt
            attempt_id = self._record_attempt(config.run_id, poc_pair)
            
            # Try to find path
            path_result = self.path_manager.find_path(
                poc_pair['from_node_id'],
                poc_pair['to_node_id']
            )
            
            if path_result:
                # Store path and update coverage
                path_def_id = self.path_manager.store_path(
                    config.run_id,
                    path_result,
                    source_type="RANDOM",
                    target_fab=config.random_config.fab,
                    target_model_no=config.random_config.model_no,
                    target_phase_no=config.random_config.phase_no,
                    target_toolset_no=config.random_config.toolset
                )
                
                # Update coverage
                self.coverage_manager.update_coverage(
                    config.run_id,
                    path_result['nodes'],
                    path_result['links']
                )
                
                # Update attempt record
                self._update_attempt_result(attempt_id, path_def_id, True)
                
                if config.verbose_mode:
                    self.logger.info(f"Path found: {len(path_result['nodes'])} nodes, "
                                   f"coverage: {current_coverage:.2%}")
            else:
                # No path found - check if PoCs should be reviewed
                self._check_unused_pocs(config.run_id, poc_pair)
                self._update_attempt_result(attempt_id, None, False)
            
            attempts += 1
        
        if attempts >= max_attempts:
            self.logger.warning(f"Maximum attempts ({max_attempts}) reached")
        
        # Run validation on all found paths
        self._run_validation(config.run_id)
    
    def _execute_scenario_run(self, config: RunConfig) -> None:
        """Execute scenario-based analysis."""
        # Implementation for scenario runs - not detailed in this version
        pass
    
    def _record_attempt(self, run_id: str, poc_pair: Dict[str, Any]) -> int:
        """Record a path finding attempt."""
        
        insert_sql = """
        INSERT INTO tb_attempt_paths (
            run_id, path_definition_id, start_node_id, end_node_id, 
            cost, picked_at
        ) VALUES (?, ?, ?, ?, ?, ?)
        """
        
        cursor = self.db.execute(insert_sql, [
            run_id,
            None,  # Will be updated if path is found
            poc_pair['from_node_id'],
            poc_pair['to_node_id'],
            poc_pair.get('cost', 0.0),
            datetime.now()
        ])
        
        self.db.commit()
        return cursor.lastrowid
    
    def _update_attempt_result(self, attempt_id: int, path_def_id: Optional[int], 
                             success: bool) -> None:
        """Update attempt result."""
        
        update_sql = """
        UPDATE tb_attempt_paths 
        SET path_definition_id = ?, tested_at = ?, notes = ?
        WHERE id = ?
        """
        
        notes = "Path found" if success else "No path found"
        
        self.db.execute(update_sql, [
            path_def_id,
            datetime.now(),
            notes,
            attempt_id
        ])
        self.db.commit()
    
    def _check_unused_pocs(self, run_id: str, poc_pair: Dict[str, Any]) -> None:
        """Check if PoCs that couldn't connect should be flagged for review."""
        
        # Check if both PoCs are marked as used
        check_sql = """
        SELECT p.id, p.is_used, p.utility_no, p.markers, p.reference
        FROM tb_equipment_pocs p
        WHERE p.node_id IN (?, ?)
        """
        
        cursor = self.db.execute(check_sql, [
            poc_pair['from_node_id'],
            poc_pair['to_node_id']
        ])
        
        pocs = cursor.fetchall()
        
        for poc in pocs:
            if poc[1]:  # is_used is True
                # Flag for review
                self._create_review_flag(
                    run_id,
                    flag_type="CONNECTIVITY_ISSUE",
                    severity="MEDIUM",
                    reason="Used PoC without connectivity path",
                    object_type="POC",
                    object_id=poc[0],
                    object_guid=f"POC_{poc[0]}"
                )
    
    def _run_validation(self, run_id: str) -> None:
        """Run validation on all paths found in the run."""
        
        self.logger.info(f"Running validation for run {run_id}")
        
        # Get all path definitions for this run
        paths_sql = """
        SELECT DISTINCT pd.id, pd.path_context, pd.utilities_scope, pd.data_codes_scope
        FROM tb_path_definitions pd
        JOIN tb_attempt_paths ap ON pd.id = ap.path_definition_id
        WHERE ap.run_id = ?
        """
        
        cursor = self.db.execute(paths_sql, [run_id])
        paths = cursor.fetchall()
        
        for path_row in paths:
            path_def_id = path_row[0]
            path_context = json.loads(path_row[1]) if path_row[1] else {}
            utilities_scope = json.loads(path_row[2]) if path_row[2] else []
            data_codes_scope = json.loads(path_row[3]) if path_row[3] else []
            
            # Run validation
            self.validation_manager.validate_path(
                run_id,
                path_def_id,
                path_context,
                utilities_scope,
                data_codes_scope
            )
    
    def _create_review_flag(self, run_id: str, flag_type: str, severity: str,
                          reason: str, object_type: str, object_id: int,
                          object_guid: str, **kwargs) -> None:
        """Create a review flag."""
        
        insert_sql = """
        INSERT INTO tb_review_flags (
            run_id, flag_type, severity, reason, object_type, object_id,
            object_guid, created_at, status
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        self.db.execute(insert_sql, [
            run_id, flag_type, severity, reason, object_type, object_id,
            object_guid, datetime.now(), "OPEN"
        ])
        self.db.commit()
    
    def _update_run_status(self, run_id: str, status: str) -> None:
        """Update run status."""
        
        update_sql = """
        UPDATE tb_runs 
        SET status = ?, ended_at = ?
        WHERE id = ?
        """
        
        self.db.execute(update_sql, [status, datetime.now(), run_id])
        self.db.commit()
    
    def _generate_run_summary(self, run_id: str) -> None:
        """Generate run summary statistics."""
        
        # Get run metrics
        metrics_sql = """
        SELECT 
            r.total_nodes,
            r.total_links,
            r.coverage_target,
            r.run_at,
            r.ended_at,
            COUNT(DISTINCT ap.id) as total_attempts,
            COUNT(DISTINCT CASE WHEN ap.path_definition_id IS NOT NULL THEN ap.id END) as paths_found,
            COUNT(DISTINCT ap.path_definition_id) as unique_paths
        FROM tb_runs r
        LEFT JOIN tb_attempt_paths ap ON r.id = ap.run_id
        WHERE r.id = ?
        GROUP BY r.id
        """
        
        cursor = self.db.execute(metrics_sql, [run_id])
        metrics = cursor.fetchone()
        
        if not metrics:
            return
        
        # Calculate coverage
        current_coverage = self.coverage_manager.get_current_coverage(run_id)
        
        # Get error counts
        error_sql = """
        SELECT 
            COUNT(*) as total_errors,
            COUNT(CASE WHEN severity = 'CRITICAL' THEN 1 END) as critical_errors
        FROM tb_validation_errors
        WHERE run_id = ?
        """
        
        cursor = self.db.execute(error_sql, [run_id])
        error_counts = cursor.fetchone()
        
        # Get review flag count
        review_sql = "SELECT COUNT(*) FROM tb_review_flags WHERE run_id = ?"
        cursor = self.db.execute(review_sql, [run_id])
        review_count = cursor.fetchone()[0]
        
        # Calculate timing
        start_time = datetime.fromisoformat(metrics[3])
        end_time = datetime.fromisoformat(metrics[4]) if metrics[4] else datetime.now()
        execution_time = (end_time - start_time).total_seconds()
        
        # Insert summary
        summary_sql = """
        INSERT INTO tb_run_summaries (
            run_id, total_attempts, total_paths_found, unique_paths,
            total_errors, total_reviews, critical_errors,
            target_coverage, achieved_coverage, coverage_efficiency,
            total_nodes, total_links, success_rate, completion_status,
            execution_time_seconds, started_at, ended_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        success_rate = (metrics[7] / metrics[6] * 100) if metrics[6] > 0 else 0
        coverage_efficiency = (current_coverage / metrics[2] * 100) if metrics[2] > 0 else 0
        
        self.db.execute(summary_sql, [
            run_id,
            metrics[6],  # total_attempts
            metrics[7],  # total_paths_found
            metrics[8],  # unique_paths
            error_counts[0],  # total_errors
            review_count,  # total_reviews
            error_counts[1],  # critical_errors
            metrics[2],  # target_coverage
            current_coverage,  # achieved_coverage
            coverage_efficiency,  # coverage_efficiency
            metrics[0],  # total_nodes
            metrics[1],  # total_links
            success_rate,  # success_rate
            "COMPLETED",  # completion_status
            execution_time,  # execution_time_seconds
            start_time,  # started_at
            end_time   # ended_at
        ])
        
        self.db.commit()
    
    def _detect_scenario_type(self, scenario_config: Optional[ScenarioRunConfig]) -> Optional[str]:
        """Detect scenario type from scenario code."""
        if not scenario_config or not scenario_config.scenario_code:
            return None
        
        code = scenario_config.scenario_code.upper()
        if code.startswith('PRE'):
            return 'PREDEFINED'
        elif code.startswith('SYN'):
            return 'SYNTHETIC'
        else:
            return 'PREDEFINED'  # Default
    
    def get_run_status(self, run_id: str) -> Optional[Dict[str, Any]]:
        """Get current run status and metrics."""
        
        status_sql = """
        SELECT r.*, rs.*
        FROM tb_runs r
        LEFT JOIN tb_run_summaries rs ON r.id = rs.run_id
        WHERE r.id = ?
        """
        
        cursor = self.db.execute(status_sql, [run_id])
        result = cursor.fetchone()
        
        if not result:
            return None
        
        # Convert to dictionary
        columns = [desc[0] for desc in cursor.description]
        return dict(zip(columns, result))
