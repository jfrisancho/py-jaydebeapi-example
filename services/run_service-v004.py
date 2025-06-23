"""
Service for managing analysis run execution.
Updated for simplified schema with new field names.
"""

import time
from datetime import datetime
from typing import List, Optional

from models import RunConfig, RunResult, CoverageStats, PathDefinition, PathResult as ModelPathResult, Scenario
from enums import RunStatus, Approach, Method, ExecutionMode, CompletionStatus, ScenarioType
from services.path_service import PathService
from services.coverage_service import CoverageService
from services.random_service import RandomService # Assuming this is the correct random service
from services.validation_service import ValidationService
# from services.scenario_service import ScenarioService # Defined as inner class for now
from db import Database


class ScenarioService: # Placeholder implementation, move to its own file if it grows
    """Service for scenario execution."""
    def __init__(self, db: Database):
        self.db = db

    def get_scenario_by_code(self, scenario_code: str) -> Optional[Scenario]:
        sql = "SELECT id, code, name, description, scenario_type, file_path, expected_coverage, expected_nodes, expected_links, expected_paths, expected_valid, expected_criticality, created_by, is_active, created_at FROM tb_scenarios WHERE code = ? AND is_active = TRUE"
        try:
            result = self.db.query(sql, [scenario_code])
            if result and result[0]:
                row = result[0]
                return Scenario(
                    id=row[0], code=row[1], name=row[2], description=row[3],
                    scenario_type=ScenarioType(row[4]), file_path=row[5],
                    expected_coverage=row[6], expected_nodes=row[7], expected_links=row[8],
                    expected_paths=row[9], expected_valid=bool(row[10]),
                    expected_criticality=row[11], created_by=row[12], is_active=bool(row[13]),
                    created_at=row[14]
                )
            return None
        except Exception as e:
            print(f"Error getting scenario by code {scenario_code}: {e}")
            return None

    def load_scenarios_from_file(self, scenario_file_path: str) -> List[Scenario]:
        # Placeholder: Implement JSON/CSV/etc. file parsing to load Scenario objects
        print(f"Placeholder: Loading scenarios from file {scenario_file_path} (not implemented)")
        return []

    def execute_scenario(self, run_id: str, scenario: Scenario, run_config: RunConfig) -> ModelPathResult:
        # Placeholder: This would involve complex logic to interpret the scenario,
        # find or generate paths based on its definition, and create a PathDefinition.
        print(f"Placeholder: Executing scenario {scenario.code} (not implemented)")
        # Simulate path finding based on scenario (e.g. 90% success)
        if random.random() < 0.9:
            # Simulate a found path
            import hashlib, random # For placeholder
            nodes = [random.randint(1,100) for _ in range(random.randint(5,15))]
            links = [random.randint(101,200) for _ in range(len(nodes)-1)]
            path_hash = hashlib.md5(f"{scenario.code}_{nodes}_{links}".encode()).hexdigest()
            
            path_def = PathDefinition(
                id=None, # Will be set by PathService
                path_hash=path_hash,
                source_type=SourceType.SCENARIO,
                building_code=None, # Scenarios might be cross-building or abstract
                category=scenario.expected_criticality or "SCENARIO_DEFAULT",
                scope="SCENARIO_SPECIFIC",
                node_count=len(nodes),
                link_count=len(links),
                total_length_mm=sum(random.uniform(10,100) for _ in links),
                coverage=random.uniform(0.01, 0.05), # Placeholder coverage contribution
                utilities=scenario.path_context.get('utilities', ['GENERAL']) if scenario.path_context else ['GENERAL'],
                path_context={'nodes': nodes, 'links': links, 'scenario_name': scenario.name},
                scenario_id=scenario.id,
                scenario_context=scenario.path_context or {'details': scenario.description}
            )
            return ModelPathResult(path_found=True, path_definition=path_def)
        else:
            return ModelPathResult(path_found=False, errors=[]) # Add mock ValidationErrors if needed


class RunService:
    """Service for orchestrating analysis runs."""
    
    def __init__(self, db: Database):
        self.db = db
        self.validation_service = ValidationService(db)
        self.scenario_service = ScenarioService(db) # Initialize scenario service
    
    def execute_run(self, config: RunConfig, path_service: PathService, 
                   coverage_service: CoverageService, verbose: bool = False) -> RunResult:
        """Execute a complete analysis run."""
        start_time = time.time()
        self._create_run_record(config) # Create DB record for the run
        
        run_result: RunResult # Initialize for use in try/except/finally
        
        try:
            if config.approach == Approach.RANDOM:
                run_result = self._execute_random_run(config, path_service, coverage_service, verbose)
            elif config.approach == Approach.SCENARIO:
                run_result = self._execute_scenario_run(config, path_service, coverage_service, verbose)
            else:
                raise ValueError(f"Unsupported approach: {config.approach}")
            
            # Update run status to DONE
            run_result.status = RunStatus.DONE
            
        except Exception as e:
            print(f"Run {config.run_id} failed with exception: {e}")
            # Ensure basic RunResult structure for failure case
            run_result = RunResult(
                run_id=config.run_id, approach=config.approach, method=config.method,
                coverage_target=config.coverage_target, total_coverage=0.0,
                total_nodes=0, total_links=0, building_code=config.building_code,
                tag=config.tag, status=RunStatus.FAILED, started_at=config.started_at,
                ended_at=datetime.now(), duration=time.time() - start_time,
                execution_mode=config.execution_mode, verbose_mode=config.verbose_mode,
                scenario_code=config.scenario_code, scenario_file=config.scenario_file,
                errors=[str(e)] # Store the main error
            )
            # Re-raise if you want main CLI to handle it, or just log here.
            # For now, we'll let it proceed to _update_run_record and _create_run_summary.
        
        # Finalize run record and summary regardless of success/failure
        end_time = time.time()
        run_result.ended_at = datetime.now()
        run_result.duration = end_time - start_time
        
        self._update_run_record(run_result)
        self._create_run_summary(run_result) # Summary created even for failed runs
            
        if verbose:
            print(f"Run {config.run_id} finished with status {run_result.status.value} in {run_result.duration:.2f}s")
        
        return run_result

    def _execute_random_run(self, config: RunConfig, path_service: PathService,
                          coverage_service: CoverageService, verbose: bool = False) -> RunResult:
        if verbose: print(f"Executing {config.method.value} random sampling for fab {config.building_code}...")
        
        random_path_service = RandomService(self.db, config.building_code) # building_code is fab
        current_coverage_stats = coverage_service.initialize_coverage(config.building_code)
        
        result = RunResult(
            run_id=config.run_id, approach=config.approach, method=config.method,
            coverage_target=config.coverage_target, total_coverage=current_coverage_stats.coverage_percentage,
            total_nodes=current_coverage_stats.total_nodes, total_links=current_coverage_stats.total_links,
            building_code=config.building_code, tag=config.tag, status=RunStatus.RUNNING,
            started_at=config.started_at, ended_at=None, duration=0.0,
            execution_mode=config.execution_mode, verbose_mode=config.verbose_mode,
            scenario_code=config.scenario_code, scenario_file=config.scenario_file
        )
        
        attempts = 0
        max_attempts = 1000 # Safety break, make configurable
        
        while result.total_coverage < config.coverage_target and attempts < max_attempts:
            attempts += 1
            if verbose and attempts % 50 == 0:
                print(f"Attempt {attempts}, Current Coverage: {result.total_coverage:.2%}")

            path_attempt_result = random_path_service.generate_random_path(config)
            result.paths_attempted += 1
            
            if path_attempt_result.path_found and path_attempt_result.path_definition:
                # Store path attempt which also stores/retrieves path_definition and sets its ID
                path_service.store_path_attempt(config.run_id, path_attempt_result, config)
                
                # Path definition ID should now be set by store_path_attempt
                if path_attempt_result.path_definition.id:
                    result.paths_found += 1
                    # Update coverage based on the (potentially new) path
                    current_coverage_stats = coverage_service.update_coverage(path_attempt_result.path_definition, current_coverage_stats)
                    result.total_coverage = current_coverage_stats.coverage_percentage
                    
                    # Validate path (now that it has an ID)
                    validation_errors = self.validation_service.validate_path(config.run_id, path_attempt_result.path_definition)
                    if validation_errors:
                         result.errors.extend([f"PathDefID {path_attempt_result.path_definition.id}: {ve.error_type.value} - {ve.error_message}" for ve in validation_errors])
                else:
                    result.errors.append("Failed to store path definition, cannot validate or count as found.")

            # Aggregate errors/flags from path_attempt_result even if path not found
            # (e.g. bias mitigation flags, selection errors)
            result.errors.extend([f"Attempt Error: {ve.error_type.value} - {ve.error_message or ve.notes}" for ve in path_attempt_result.errors])
            result.review_flags.extend([f"Attempt Flag: {rf.reason}" for rf in path_attempt_result.review_flags])

        if attempts >= max_attempts and result.total_coverage < config.coverage_target:
            result.errors.append(f"Max attempts ({max_attempts}) reached before achieving target coverage.")
        if verbose: print(f"Random sampling complete: {result.paths_found}/{result.paths_attempted} paths found. Final Coverage: {result.total_coverage:.2%}")
        
        return result

    def _execute_scenario_run(self, config: RunConfig, path_service: PathService,
                            coverage_service: CoverageService, verbose: bool = False) -> RunResult:
        if verbose: print(f"Executing {config.method.value} scenario analysis...")
        
        scenarios_to_run: List[Scenario] = []
        if config.scenario_code:
            scenario = self.scenario_service.get_scenario_by_code(config.scenario_code)
            if scenario: scenarios_to_run.append(scenario)
        elif config.scenario_file:
            scenarios_to_run.extend(self.scenario_service.load_scenarios_from_file(config.scenario_file))
        
        if not scenarios_to_run:
            raise ValueError("No scenarios found to execute based on config.")

        # Coverage for scenarios is often about scenario pass rate, not node/link coverage.
        # Initialize with dummy building_code for CoverageService if it's not used for scenario coverage.
        # Or, pass None or a specific marker if CoverageService handles it.
        # For tb_runs, total_nodes/links might be sum of items in successful scenario paths.
        # coverage_service.initialize_coverage("SCENARIO") # If needed for some metric
        
        result = RunResult(
            run_id=config.run_id, approach=config.approach, method=config.method,
            coverage_target=1.0, # For scenarios, target is 100% scenario success typically
            total_coverage=0.0,  # Will be success_rate of scenarios
            total_nodes=0, total_links=0, # Sum of nodes/links from successful scenarios
            building_code="", tag=config.tag, status=RunStatus.RUNNING, # Scenarios can be cross-fab
            started_at=config.started_at, ended_at=None, duration=0.0,
            execution_mode=config.execution_mode, verbose_mode=config.verbose_mode,
            scenario_code=config.scenario_code, scenario_file=config.scenario_file
        )

        for scenario in scenarios_to_run:
            if verbose: print(f"Executing scenario: {scenario.code} ({scenario.name})")
            result.scenario_tests += 1
            
            scenario_attempt_result = self.scenario_service.execute_scenario(config.run_id, scenario, config)
            
            if scenario_attempt_result.path_found and scenario_attempt_result.path_definition:
                path_service.store_path_attempt(config.run_id, scenario_attempt_result, config)

                if scenario_attempt_result.path_definition.id:
                    result.paths_found += 1
                    # Aggregate nodes/links from this successful scenario path
                    result.total_nodes += scenario_attempt_result.path_definition.node_count
                    result.total_links += scenario_attempt_result.path_definition.link_count

                    validation_errors = self.validation_service.validate_path(config.run_id, scenario_attempt_result.path_definition)
                    if validation_errors:
                        result.errors.extend([f"Scenario {scenario.code}, PathDefID {scenario_attempt_result.path_definition.id}: {ve.error_type.value} - {ve.error_message}" for ve in validation_errors])
                else:
                     result.errors.append(f"Scenario {scenario.code}: Failed to store path definition.")
            else: # Scenario execution failed to find/create a path
                 result.errors.extend([f"Scenario {scenario.code} Error: {ve.error_type.value} - {ve.error_message or ve.notes}" for ve in scenario_attempt_result.errors])

            result.review_flags.extend([f"Scenario {scenario.code} Flag: {rf.reason}" for rf in scenario_attempt_result.review_flags])
        
        result.total_coverage = (result.paths_found / result.scenario_tests) if result.scenario_tests > 0 else 0.0
        if verbose: print(f"Scenario execution complete: {result.paths_found}/{result.scenario_tests} scenarios successful. Success Rate: {result.total_coverage:.2%}")
        
        return result

    def _create_run_record(self, config: RunConfig):
        """Create initial run record in tb_runs."""
        # tb_runs: id, date, approach, method, coverage_target, total_coverage, total_nodes, total_links,
        #          fab, toolset, phase, scenario_code, scenario_file, scenario_type,
        #          tag, status, execution_mode, verbose_mode, started_at
        sql = """
        INSERT INTO tb_runs (
            id, date, approach, method, coverage_target, total_coverage, total_nodes, total_links,
            fab, toolset, phase, scenario_code, scenario_file, scenario_type,
            tag, status, execution_mode, verbose_mode, started_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        scenario_type_val = None
        if config.approach == Approach.SCENARIO:
            # Method for SCENARIO (PREDEFINED, SYNTHETIC) can map to scenario_type
            if config.method in [Method.PREDEFINED, Method.SYNTHETIC]:
                 scenario_type_val = ScenarioType(config.method.value).value # Ensure ScenarioType enum covers these
            # Or detect from scenario_code prefix if method is more generic
            elif config.scenario_code:
                if config.scenario_code.startswith("PRE"): scenario_type_val = ScenarioType.PREDEFINED.value
                elif config.scenario_code.startswith("SYN"): scenario_type_val = ScenarioType.SYNTHETIC.value
            elif config.scenario_file:
                 scenario_type_val = ScenarioType.FILE.value # Assuming a FILE type

        params = [
            config.run_id, config.started_at.date(), config.approach.value, config.method.value,
            config.coverage_target, 0.0, 0, 0, # Initial values, updated later
            config.building_code or None, config.toolset or None, config.phase or None, # fab, toolset, phase
            config.scenario_code or None, config.scenario_file or None, scenario_type_val,
            config.tag, RunStatus.RUNNING.value,
            config.execution_mode.value, config.verbose_mode, config.started_at
        ]
        self.db.update(sql, params)

    def _update_run_record(self, result: RunResult):
        """Update tb_runs with final results."""
        sql = """
        UPDATE tb_runs SET
            total_coverage = ?, total_nodes = ?, total_links = ?,
            status = ?, ended_at = ?,
            updated_at = CURRENT_TIMESTAMP 
        WHERE id = ? 
        """
        # Note: fab, toolset, phase, scenario_code, scenario_file, scenario_type, tag, execution_mode, verbose_mode
        # are set at creation and not typically updated here.
        params = [
            result.total_coverage, result.total_nodes, result.total_links,
            result.status.value, result.ended_at, result.run_id
        ]
        self.db.update(sql, params)

    def _create_run_summary(self, result: RunResult):
        """Create aggregated run summary in tb_run_summaries."""
        # tb_run_summaries: run_id, total_attempts, total_paths_found, unique_paths,
        # total_scenario_tests, scenario_success_rate, total_errors, total_review_flags, critical_errors,
        # target_coverage, achieved_coverage, coverage_efficiency,
        # total_nodes, total_links, avg_path_nodes, avg_path_links, avg_path_length,
        # success_rate, completion_status, execution_time_seconds,
        # started_at, ended_at, summarized_at
        sql = """
        INSERT INTO tb_run_summaries (
            run_id, total_attempts, total_paths_found, unique_paths,
            total_scenario_tests, scenario_success_rate, total_errors, total_review_flags, critical_errors,
            target_coverage, achieved_coverage, coverage_efficiency,
            total_nodes, total_links, avg_path_nodes, avg_path_links, avg_path_length,
            success_rate, completion_status, execution_time_seconds,
            started_at, ended_at, summarized_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON DUPLICATE KEY UPDATE summarized_at = VALUES(summarized_at) /* Handle re-summarization if needed */
        """
        
        path_stats = PathService(self.db).get_path_statistics(result.run_id)

        # Determine completion status
        comp_status = CompletionStatus.FAILED
        if result.status == RunStatus.DONE:
            if result.approach == Approach.RANDOM:
                comp_status = CompletionStatus.COMPLETED if result.total_coverage >= result.coverage_target else CompletionStatus.PARTIAL
            elif result.approach == Approach.SCENARIO:
                comp_status = CompletionStatus.COMPLETED if result.paths_found == result.scenario_tests and result.scenario_tests > 0 else CompletionStatus.PARTIAL
        
        scenario_success_rate = (result.paths_found / result.scenario_tests * 100) if result.approach == Approach.SCENARIO and result.scenario_tests > 0 else None
        
        # For RANDOM approach, target_coverage is from config. For SCENARIO, it could be 1.0 (100% success)
        # achieved_coverage is result.total_coverage (which is pass rate for SCENARIO)
        target_cov = result.coverage_target if result.approach == Approach.RANDOM else (1.0 if result.scenario_tests > 0 else None)
        achieved_cov = result.total_coverage
        coverage_eff = (achieved_cov / target_cov * 100) if target_cov and target_cov > 0 else None

        params = [
            result.run_id,
            path_stats.get('total_attempts', result.paths_attempted), # from path_stats or result
            path_stats.get('total_paths_found', result.paths_found),   # from path_stats or result
            path_stats.get('unique_paths', 0), # from path_stats
            result.scenario_tests,
            scenario_success_rate,
            len(result.errors),
            len(result.review_flags),
            len(result.critical_errors), # Assuming critical_errors list in RunResult stores messages
            target_cov,
            achieved_cov,
            coverage_eff,
            result.total_nodes, # Overall total nodes covered or involved
            result.total_links, # Overall total links
            path_stats.get('avg_nodes'),
            path_stats.get('avg_links'),
            path_stats.get('avg_length_mm'),
            path_stats.get('success_rate'), # Overall success rate from path_stats
            comp_status.value,
            result.duration,
            result.started_at,
            result.ended_at,
            datetime.now()
        ]
        self.db.update(sql, params)