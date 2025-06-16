"""
Service for managing analysis run execution.
"""

import time
from datetime import datetime
from typing import List, Optional

from models import RunConfig, RunResult, CoverageStats
from enums import RunStatus, Approach, Method
from services.path_service import PathService
from services.coverage_service import CoverageService
from services.random_service import RandomService
from services.validation_service import ValidationService
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
                fab=config.fab,
                tag=config.tag,
                status=RunStatus.FAILED,
                started_at=config.started_at,
                ended_at=datetime.now(),
                duration=end_time - start_time,
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
        random_service = RandomService(self.db, config.fab)
        
        # Initialize coverage tracking
        coverage_stats = coverage_service.initialize_coverage(config.fab)
        
        result = RunResult(
            run_id=config.run_id,
            approach=config.approach,
            method=config.method,
            coverage_target=config.coverage_target,
            total_coverage=0.0,
            total_nodes=coverage_stats.total_nodes,
            total_links=coverage_stats.total_links,
            fab=config.fab,
            tag=config.tag,
            status=RunStatus.RUNNING,
            started_at=config.started_at,
            ended_at=None,
            duration=0.0
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
        # TODO: Implement scenario execution logic
        raise NotImplementedError("Scenario execution not yet implemented")
    
    def _create_run_record(self, config: RunConfig):
        """Create initial run record in database."""
        sql = """
        INSERT INTO tb_runs (
            id, date, approach, method, coverage_target,
            total_coverage, total_nodes, total_links,
            fab, tag, status, started_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        self.db.update(sql, [
            config.run_id,
            config.started_at.date(),
            config.approach.value,
            config.method.value,
            config.coverage_target,
            0.0,  # initial coverage
            0,    # will be updated
            0,    # will be updated
            config.fab,
            config.tag,
            RunStatus.RUNNING.value,
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
            ended_at = ?
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
        INSERT INTO run_summaries (
            run_id, total_attempts, total_paths_found,
            total_scenario_tests, total_errors, total_review_flags,
            total_nodes, total_links, avg_path_nodes, avg_path_links,
            success_rate, startedd_at, endedd_at, summarized_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        success_rate = (result.paths_found / result.paths_attempted * 100 
                       if result.paths_attempted > 0 else 0)
        
        self.db.update(sql, [
            result.run_id,
            result.paths_attempted,
            result.paths_found,
            0,  # scenario tests (not implemented yet)
            len(result.errors),
            len(result.review_flags),
            result.total_nodes,
            result.total_links,
            0.0,  # avg path nodes (calculate from path_definitions)
            0.0,  # avg path links (calculate from path_definitions)
            success_rate,
            result.started_at,
            result.ended_at,
            datetime.now()
        ])