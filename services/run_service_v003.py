"""
Service for managing analysis run execution.
Updated for simplified schema with new field names.
"""

import time
from datetime import datetime
from typing import List, Optional

from models import RunConfig, RunResult, CoverageStats
from enums import RunStatus, Approach, Method, ExecutionMode
from services.path_service import PathService
from services.coverage_service import CoverageService
from services.random_service import RandomService
from services.validation_service import ValidationService
from services.scenario_service import ScenarioService
from db import Database


class RunService:
    """Service for orchestrating analysis runs."""
    
    def __init__(self, db: Database):
        self.db = db
        self.validation_service = ValidationService(db)
    
    def execute_run(self, config: RunConfig, path_service: PathService, 
                   coverage_service: CoverageService, verbose: bool = False) -> RunResult:
        """Execute a complete analysis run."""
        start_time = time.time()
        
        # Initialize run in database
        self._create_run_record(config)
        
        try:
            if config.approach == Approach.RANDOM:
                result = self._execute_random_run(config, path_service, coverage_service, verbose)
            elif config.approach == Approach.SCENARIO:
                result = self._execute_scenario_run(config, path_service, coverage_service, verbose)
            else:
                raise ValueError(f"Unsupported approach: {config.approach}")
            
            # Update run status
            end_time = time.time()
            result.ended_at = datetime.now()
            result.duration = end_time - start_time
            result.status = RunStatus.DONE
            
            self._update_run_record(result)
            self._create_run_summary(result)
            
            if verbose:
                print(f"Run completed successfully in {result.duration:.2f}s")
            
            return result
            
        except Exception as e:
            # Handle failure
            end_time = time.time()
            result = RunResult(
                run_id=config.run_id,
                approach=config.approach,
                method=config.method,
                coverage_target=config.coverage_target,
                total_coverage=0.0,
                total_nodes=0,
                total_links=0,
                building_code=config.building_code,
                tag=config.tag,
                status=RunStatus.FAILED,
                started_at=config.started_at,
                ended_at=datetime.now(),
                duration=end_time - start_time,
                execution_mode=config.execution_mode,
                verbose_mode=config.verbose_mode,
                scenario_code=config.scenario_code,
                scenario_file=config.scenario_file,
                errors=[str(e)]
            )
            
            self._update_run_record(result)
            
            if verbose:
                print(f"Run failed after {result.duration:.2f}s: {e}")
            
            raise
    
    def _execute_random_run(self, config: RunConfig, path_service: PathService,
                          coverage_service: CoverageService, verbose: bool = False) -> RunResult:
        """Execute a random sampling run."""
        if verbose:
            print(f"Executing {config.method.value} random sampling...")
        
        # Initialize services
        random_service = RandomService(self.db, config.building_code)
        
        # Initialize coverage tracking
        coverage_stats = coverage_service.initialize_coverage(config.building_code)
        
        result = RunResult(
            run_id=config.run_id,
            approach=config.approach,
            method=config.method,
            coverage_target=config.coverage_target,
            total_coverage=0.0,
            total_nodes=coverage_stats.total_nodes,
            total_links=coverage_stats.total_links,
            building_code=config.building_code,
            tag=config.tag,
            status=RunStatus.RUNNING,
            started_at=config.started_at,
            ended_at=None,
            duration=0.0,
            execution_mode=config.execution_mode,
            verbose_mode=config.verbose_mode
        )
        
        attempts = 0
        max_attempts = 10000  # Prevent infinite loops
        
        while (result.total_coverage < config.coverage_target and 
               attempts < max_attempts):
            
            attempts += 1
            
            if verbose and attempts % 100 == 0:
                print(f"Attempt {attempts}, coverage: {result.total_coverage:.1%}")
            
            # Generate random path attempt
            path_result = random_service.generate_random_path(config)
            result.paths_attempted += 1
            
            if path_result.path_found:
                result.paths_found += 1
                
                # Update coverage
                new_coverage = coverage_service.update_coverage(
                    path_result.path_definition,
                    coverage_stats
                )
                result.total_coverage = new_coverage.coverage_percentage
                
                # Store path
                path_service.store_path_attempt(config.run_id, path_result)
                
                # Validate path
                validation_errors = self.validation_service.validate_path(
                    config.run_id, path_result.path_definition
                )
                result.errors.extend([str(e) for e in validation_errors])
                
            else:
                # Handle path not found
                result.errors.extend([str(e) for e in path_result.errors])
                result.review_flags.extend([str(f) for f in path_result.review_flags])
        
        if attempts >= max_attempts:
            result.errors.append(f"Maximum attempts ({max_attempts}) reached")
        
        if verbose:
            print(f"Random sampling complete: {result.paths_found}/{result.paths_attempted} paths found")
        
        return result
    
    def _execute_scenario_run(self, config: RunConfig, path_service: PathService,
                            coverage_service: CoverageService, verbose: bool = False) -> RunResult:
        """Execute a scenario-based run."""
        if verbose:
            print(f"Executing {config.method.value} scenario analysis...")
        
        # Initialize scenario service
        scenario_service = ScenarioService(self.db)
        
        # Load scenarios based on config
        scenarios = []
        if config.scenario_code:
            scenario = scenario_service.get_scenario_by_code(config.scenario_code)
            if scenario:
                scenarios = [scenario]
        elif config.scenario_file:
            scenarios = scenario_service.load_scenarios_from_file(config.scenario_file)
        
        if not scenarios:
            raise ValueError("No scenarios found to execute")
        
        # Initialize coverage tracking (scenarios don't use traditional coverage)
        coverage_stats = coverage_service.initialize_coverage("SCENARIO")  # Dummy building
        
        result = RunResult(
            run_id=config.run_id,
            approach=config.approach,
            method=config.method,
            coverage_target=config.coverage_target,
            total_coverage=0.0,
            total_nodes=0,
            total_links=0,
            building_code="",  # Scenarios are building-agnostic
            tag=config.tag,
            status=RunStatus.RUNNING,
            started_at=config.started_at,
            ended_at=None,
            duration=0.0,
            execution_mode=config.execution_mode,
            verbose_mode=config.verbose_mode,
            scenario_code=config.scenario_code,
            scenario_file=config.scenario_file
        )
        
        # Execute scenarios
        for scenario in scenarios:
            if verbose:
                print(f"Executing scenario: {scenario.name}")
            
            result.scenario_tests += 1
            
            # Execute scenario
            scenario_result = scenario_service.execute_scenario(config.run_id, scenario)
            
            if scenario_result.path_found:
                result.paths_found += 1
                
                # Store scenario execution
                path_service.store_path_attempt(config.run_id, scenario_result)
                
                # Validate scenario result
                validation_errors = self.validation_service.validate_path(
                    config.run_id, scenario_result.path_definition
                )
                result.errors.extend([str(e) for e in validation_errors])
                
                # Update totals
                if scenario_result.path_definition:
                    result.total_nodes += scenario_result.path_definition.node_count
                    result.total_links += scenario_result.path_definition.link_count
                
            else:
                # Handle scenario failure
                result.errors.extend([str(e) for e in scenario_result.errors])
                result.review_flags.extend([str(f) for f in scenario_result.review_flags])
        
        # Calculate scenario-specific coverage (percentage of successful scenarios)
        result.total_coverage = result.paths_found / result.scenario_tests if result.scenario_tests > 0 else 0.0
        
        if verbose:
            print(f"Scenario execution complete: {result.paths_found}/{result.scenario_tests} scenarios successful")
        
        return result
    
    def _create_run_record(self, config: RunConfig):
        """Create initial run record in database with updated schema."""
        sql = """
        INSERT INTO tb_runs (
            id, date, approach, method, coverage_target,
            total_coverage, total_nodes, total_links,
            fab, toolset, phase, scenario_code, scenario_file, scenario_type,
            tag, status, execution_mode, verbose_mode, started_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        # Determine scenario type from method for SCENARIO approach
        scenario_type = None
        if config.approach == Approach.SCENARIO:
            scenario_type = config.method.value
        
        self.db.update(sql, [
            config.run_id,
            config.started_at.date(),
            config.approach.value,
            config.method.value,
            config.coverage_target,
            0.0,  # initial coverage
            0,    # will be updated
            0,    # will be updated
            config.building_code or None,  # fab field
            config.toolset or None,
            config.phase or None,  # phase field (A, B, C, D)
            config.scenario_code or None,
            config.scenario_file or None,
            scenario_type,
            config.tag,
            RunStatus.RUNNING.value,
            config.execution_mode.value,
            config.verbose_mode,
            config.started_at
        ])
    
    def _update_run_record(self, result: RunResult):
        """Update run record with final results."""
        sql = """
        UPDATE tb_runs SET
            total_coverage = ?,
            total_nodes = ?,
            total_links = ?,
            status = ?,
            ended_at = ?,
            updated_at = CURRENT_TIMESTAMP
        WHERE id = ?
        """
        
        self.db.update(sql, [
            result.total_coverage,
            result.total_nodes,
            result.total_links,
            result.status.value,
            result.ended_at,
            result.run_id
        ])
    
    def _create_run_summary(self, result: RunResult):
        """Create aggregated run summary."""
        sql = """
        INSERT INTO tb_run_summaries (
            run_id, total_attempts, total_paths_found, unique_paths,
            total_scenario_tests, scenario_success_rate, total_errors, total_review_flags,
            target_coverage, achieved_coverage, coverage_efficiency,
            total_nodes, total_links, avg_path_nodes, avg_path_links,
            success_rate, completion_status, execution_time_seconds,
            started_at, ended_at, summarized_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        # Calculate metrics
        success_rate = (result.paths_found / result.paths_attempted * 100 
                       if result.paths_attempted > 0 else 0)
        
        scenario_success_rate = (result.paths_found / result.scenario_tests * 100
                               if result.scenario_tests > 0 else None)
        
        coverage_efficiency = (result.total_coverage / result.coverage_target * 100
                             if result.coverage_target > 0 else None)
        
        # Get path statistics from path service
        path_service = PathService(self.db)
        path_stats = path_service.get_path_statistics(result.run_id)
        
        avg_path_nodes = path_stats.get('avg_nodes', 0.0)
        avg_path_links = path_stats.get('avg_links', 0.0)
        unique_paths = path_stats.get('unique_paths', result.paths_found)
        
        # Determine completion status
        from enums import CompletionStatus
        if result.status == RunStatus.DONE:
            if result.approach == Approach.RANDOM:
                completion_status = (CompletionStatus.COMPLETED if result.total_coverage >= result.coverage_target 
                                   else CompletionStatus.PARTIAL)
            else:
                completion_status = (CompletionStatus.COMPLETED if result.paths_found == result.scenario_tests
                                   else CompletionStatus.PARTIAL)
        else:
            completion_status = CompletionStatus.FAILED
        
        self.db.update(sql, [
            result.run_id,
            result.paths_attempted,
            result.paths_found,
            unique_paths,
            result.scenario_tests,
            scenario_success_rate,
            len(result.errors),
            len(result.review_flags),
            result.coverage_target if result.approach == Approach.RANDOM else None,
            result.total_coverage,
            coverage_efficiency,
            result.total_nodes,
            result.total_links,
            avg_path_nodes,
            avg_path_links,
            success_rate,
            completion_status.value,
            result.duration,
            result.started_at,
            result.ended_at,
            datetime.now()
        ])


class ScenarioService:
    """Service for scenario execution - placeholder implementation."""
    
    def __init__(self, db: Database):
        self.db = db
    
    def get_scenario_by_code(self, scenario_code: str):
        """Get scenario by code from database."""
        # TODO: Implement scenario loading from database
        from models import Scenario
        from enums import ScenarioType
        
        # Placeholder implementation
        return Scenario(
            id=1,
            code=scenario_code,
            name=f"Scenario {scenario_code}",
            scenario_type=ScenarioType.PREDEFINED if scenario_code.startswith("PRE") else ScenarioType.SYNTHETIC
        )
    
    def load_scenarios_from_file(self, scenario_file: str):
        """Load scenarios from file."""
        # TODO: Implement file-based scenario loading
        return []
    
    def execute_scenario(self, run_id: str, scenario):
        """Execute a scenario and return path result."""
        # TODO: Implement actual scenario execution
        from models import PathResult, PathDefinition
        from enums import SourceType
        
        # Placeholder implementation - return empty result
        return PathResult(path_found=False)