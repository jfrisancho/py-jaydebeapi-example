#!/usr/bin/env python3
"""
Main CLI application entry point for path analysis system.
Final version with simplified fab-based implementation and updated phase handling.
"""

import argparse
import sys
import uuid
from datetime import datetime
from typing import Optional, List

from enums import Approach, Method, RunStatus, ExecutionMode, Phase
from models import RunConfig, RunResult
from services.run_service import RunService
from services.path_service import PathService
from services.coverage_service import CoverageService
from services.simple_random_service import SimplePopulationService
from db import Database


def create_parser() -> argparse.ArgumentParser:
    """Create command line argument parser."""
    parser = argparse.ArgumentParser(
        description="Path Analysis CLI Tool",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  Default (quick random test with default settings):
    python main.py
    python main.py -v                              # with verbose output
    python main.py --fab M16                       # specify fab
    python main.py --coverage-target 0.3 -v       # custom coverage

  Interactive mode (for exploration/training):
    python main.py --interactive
    python main.py -i

  Random approach (specific tests):
    python main.py -a RANDOM --fab M16 --toolset "TS001"
    python main.py -a RANDOM --method STRATIFIED --coverage-target 0.25

  Scenario approach (predefined paths by code or file):
    python main.py -a SCENARIO --scenario-code "PRE001"    # predefined scenario
    python main.py -a SCENARIO --scenario-code "SYN001"    # synthetic scenario
    python main.py -a SCENARIO --scenario-file "scenarios.json"

  Silent unattended mode (for scripts/automation):
    python main.py --fab M16 --unattended
    python main.py -a SCENARIO --scenario-code "PRE001" --unattended
        """
    )
    
    parser.add_argument(
        '--approach', '-a',
        type=str,
        choices=['RANDOM', 'SCENARIO'],
        default='RANDOM',
        help='Analysis approach (default: RANDOM)'
    )
    
    parser.add_argument(
        '--method',
        type=str,
        choices=['SIMPLE', 'STRATIFIED'],
        help='Analysis method for random - ignored for SCENARIO approach'
    )
    
    parser.add_argument(
        '--coverage-target',
        type=float,
        default=0.2,
        help='Coverage target as decimal for RANDOM approach (default: 0.2 for 20%%) - ignored for SCENARIO approach'
    )
    
    parser.add_argument(
        '--fab',
        type=str,
        choices=['M15', 'M15X', 'M16'],
        help='Fab identifier for RANDOM approach (M16, M15, M15X) - ignored for SCENARIO approach'
    )
    
    parser.add_argument(
        '--toolset',
        type=str,
        help='Toolset identifier for RANDOM approach (e.g., TS001) - ignored for SCENARIO approach'
    )
    
    parser.add_argument(
        '--phase',
        type=str,
        choices=['PHASE1', 'PHASE2', 'PHASE3', 'PHASE4', 'A', 'B', 'C', 'D', '1', '2', '3', '4'],
        help='Phase identifier for RANDOM approach (PHASE1/A/1, PHASE2/B/2, PHASE3/C/3, PHASE4/D/4) - ignored for SCENARIO approach'
    )
    
    # SCENARIO approach arguments
    parser.add_argument(
        '--scenario-code',
        type=str,
        help='Scenario code for SCENARIO approach (e.g., PRE001 for predefined, SYN001 for synthetic) - ignored for RANDOM approach'
    )
    
    parser.add_argument(
        '--scenario-file',
        type=str,
        help='Scenario file path for SCENARIO approach (e.g., scenarios.json) - ignored for RANDOM approach'
    )
    
    parser.add_argument(
        '--interactive', '-i',
        action='store_true',
        help='Run in interactive mode for exploration and training'
    )
    
    parser.add_argument(
        '--unattended', '-u',
        action='store_true',
        help='Silent unattended mode - minimal output, no summary (for scripts/automation)'
    )
    
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable verbose output (ignored if --unattended is used) - ignored for unattended mode'
    )
    
    return parser


def fetch_user_choice(prompt: str, choices: List[str], default_idx: int = 0) -> str:
    """Get user choice from a list of options."""
    print(f"\n{prompt}")
    for i, choice in enumerate(choices, 1):
        marker = " (default)" if i - 1 == default_idx else ""
        print(f"  {i}. {choice}{marker}")
    
    while True:
        try:
            user_input = input(f"\nEnter choice (1-{len(choices)}) [default: {default_idx + 1}]: ").strip()
            
            if not user_input:
                return choices[default_idx]
            
            choice_idx = int(user_input) - 1
            if 0 <= choice_idx < len(choices):
                return choices[choice_idx]
            else:
                print(f"Invalid choice. Please enter a number between 1 and {len(choices)}.")
        except ValueError:
            print("Invalid input. Please enter a number.")
        except KeyboardInterrupt:
            print("\nOperation cancelled by user.")
            sys.exit(130)


def fetch_float_input(prompt: str, default: float, min_val: float = 0.0, max_val: float = 1.0) -> float:
    """Get float input from user with validation."""
    while True:
        try:
            user_input = input(f"{prompt} [default: {default}]: ").strip()
            
            if not user_input:
                return default
            
            value = float(user_input)
            if min_val <= value <= max_val:
                return value
            else:
                print(f"Value must be between {min_val} and {max_val}.")
        except ValueError:
            print("Invalid input. Please enter a decimal number (e.g., 0.2 for 20%).")
        except KeyboardInterrupt:
            print("\nOperation cancelled by user.")
            sys.exit(130)


def fetch_string_input(prompt: str, default: str = "", required: bool = False, available_options: List[str] = None) -> str:
    """Get string input from user."""
    while True:
        try:
            if available_options:
                print(f"\n{prompt}")
                print("Available options:")
                for option in available_options:
                    print(f"  - {option}")
                user_input = input(f"Enter value [default: {default}]: " if default else "Enter value: ").strip()
            else:
                user_input = input(f"{prompt} [default: {default}]: " if default else f"{prompt}: ").strip()
            
            if not user_input:
                if default:
                    return default
                elif not required:
                    return ""
                else:
                    print("This field is required.")
                    continue
            
            return user_input
        except KeyboardInterrupt:
            print("\nOperation cancelled by user.")
            sys.exit(130)


def fetch_available_fabs(db: Database) -> List[str]:
    """Get available fabs from database."""
    try:
        sql = "SELECT DISTINCT fab FROM tb_toolsets WHERE is_active = TRUE ORDER BY fab"
        results = db.query(sql)
        fabs = [row[0] for row in results] if results else []
        
        # Add defaults if none found
        if not fabs:
            fabs = ["M16", "M15", "M15X"]
        
        return fabs
    except Exception:
        return ["M16", "M15", "M15X"]  # Default fallback


def fetch_available_scenarios(db: Database) -> List[str]:
    """Get available scenarios from database."""
    try:
        sql = "SELECT code FROM tb_scenarios WHERE is_active = TRUE ORDER BY code"
        results = db.query(sql)
        scenarios = [row[0] for row in results] if results else []
        
        # Add some default options if none found
        if not scenarios:
            scenarios = ["PRE001", "PRE002", "SYN001"]
        
        return scenarios
    except Exception:
        return ["PRE001", "PRE002", "SYN001"]


def fetch_available_toolsets(db: Database, fab: str) -> List[str]:
    """Get available toolsets for a specific fab."""
    try:
        sql = "SELECT DISTINCT code FROM tb_toolsets WHERE fab = ? AND is_active = TRUE ORDER BY code"
        results = db.query(sql, [fab])
        toolsets = [row[0] for row in results] if results else []
        
        # Add "ALL" option
        options = ["ALL"]
        if toolsets:
            options.extend(toolsets)
        else:
            # Default toolsets if none found
            options.extend(["TS001", "TS002", "TS003"])
        
        return options
    except Exception:
        return ["ALL", "TS001", "TS002", "TS003"]


def fetch_available_phases(db: Database, fab: str) -> List[str]:
    """Get available phases for a specific fab."""
    try:
        sql = "SELECT DISTINCT phase FROM tb_toolsets WHERE fab = ? AND is_active = TRUE ORDER BY phase"
        results = db.query(sql, [fab])
        phases = [row[0] for row in results] if results else []
        
        # Convert system nomenclature (A, B, C, D) to human readable
        human_readable_phases = []
        for phase in phases:
            phase_enum = Phase.normalize(phase)
            if phase_enum:
                human_readable_phases.append(phase_enum.conceptual)  # PHASE1, PHASE2, etc.
        
        # Add default phases if none found
        if not human_readable_phases:
            human_readable_phases = ["PHASE1", "PHASE2", "PHASE3", "PHASE4"]
        
        return human_readable_phases
    except Exception:
        return ["PHASE1", "PHASE2", "PHASE3", "PHASE4"]


def validate_approach_specific_args(approach: Approach, args) -> None:
    """Validate that approach-specific arguments are provided correctly."""
    if approach == Approach.SCENARIO:
        # SCENARIO approach doesn't use fab/toolset/coverage-target/method/phase
        if args.fab:
            print(f"Warning: --fab is ignored for SCENARIO approach")
        if args.toolset:
            print(f"Warning: --toolset is ignored for SCENARIO approach")
        if args.phase:
            print(f"Warning: --phase is ignored for SCENARIO approach")
        if args.method:
            print(f"Warning: --method is ignored for SCENARIO approach (determined by scenario code)")
        if args.coverage_target != 0.2:  # Only warn if explicitly set
            print(f"Warning: --coverage-target is ignored for SCENARIO approach (scenarios have predefined coverage)")
        
        # SCENARIO approach needs exactly one scenario identifier
        scenario_args = [args.scenario_code, args.scenario_file]
        if not any(scenario_args):
            raise ValueError("SCENARIO approach requires either --scenario-code or --scenario-file")
        
        # Only one scenario identifier should be provided
        provided_args = [arg for arg in scenario_args if arg]
        if len(provided_args) > 1:
            raise ValueError("SCENARIO approach: only one of --scenario-code or --scenario-file should be provided")
    
    elif approach == Approach.RANDOM:
        # RANDOM approach doesn't use scenario arguments
        scenario_args = [args.scenario_code, args.scenario_file]
        if any(scenario_args):
            print(f"Warning: scenario arguments (--scenario-code, --scenario-file) are ignored for RANDOM approach")


def validate_method_for_approach(approach: Approach, method: Optional[str]) -> Method:
    """Validate and return the appropriate method for the given approach."""
    if approach == Approach.RANDOM:
        if method is None:
            return Method.SIMPLE
        if method.upper() not in ['SIMPLE', 'STRATIFIED']:
            raise ValueError(f"Invalid method '{method}' for RANDOM approach. Use SIMPLE or STRATIFIED.")
        return Method(method.upper())
    
    elif approach == Approach.SCENARIO:
        # For SCENARIO approach, method is determined by the scenario code/type
        # We'll default to PREDEFINED for now, but this could be auto-detected from the code
        return Method.PREDEFINED
    
    else:
        raise ValueError(f"Unknown approach: {approach}")


def detect_scenario_method_from_code(scenario_code: str) -> Method:
    """Detect scenario method from the scenario code pattern."""
    if scenario_code.startswith("PRE"):
        return Method.PREDEFINED
    elif scenario_code.startswith("SYN"):
        return Method.SYNTHETIC
    else:
        # Default to PREDEFINED for unknown patterns
        return Method.PREDEFINED


def validate_method(approach: Approach, method: str) -> Method:
    """Validate and return the appropriate method for the given approach."""
    if approach == Approach.RANDOM:
        if method.upper() not in ['SIMPLE', 'STRATIFIED']:
            raise ValueError(f"Invalid method '{method}' for RANDOM approach. Use SIMPLE or STRATIFIED.")
        return Method(method.upper())
    
    elif approach == Approach.SCENARIO:
        if method.upper() not in ['PREDEFINED', 'SYNTHETIC']:
            raise ValueError(f"Invalid method '{method}' for SCENARIO approach. Use PREDEFINED or SYNTHETIC.")
        return Method(method.upper())
    
    else:
        raise ValueError(f"Unknown approach: {approach}")


def normalize_phase(phase_str: str) -> str:
    """Normalize phase string to system nomenclature (A, B, C, D)."""
    if not phase_str:
        return ""
    
    phase_enum = Phase.normalize(phase_str)
    return phase_enum.nominal if phase_enum else phase_str  # Return system nomenclature (A, B, C, D)


def print_configuration_summary(config: RunConfig):
    """Print configuration summary."""
    print(f"\n{'='*60}")
    print(f"CONFIGURATION SUMMARY")
    print(f"{'='*60}")
    print(f"Run ID: {config.run_id}")
    print(f"Approach: {config.approach.value}")
    
    if config.approach == Approach.RANDOM:
        print(f"Method: {config.method.value}")
        print(f"Coverage Target: {config.coverage_target:.1%}")
        print(f"Fab: {config.building_code}")
        if config.phase:
            # Convert system nomenclature back to human readable for display
            phase_enum = Phase.normalize(config.phase)
            display_phase = phase_enum.conceptual if phase_enum else config.phase
            print(f"Phase: {display_phase} ({config.phase})")
        if config.toolset:
            print(f"Toolset: {config.toolset}")
    elif config.approach == Approach.SCENARIO:
        print(f"Type: {config.method.value} (auto-detected)")
        print(f"Coverage: Predefined by scenario")
        if config.scenario_code:
            print(f"Scenario Code: {config.scenario_code}")
        if config.scenario_file:
            print(f"Scenario File: {config.scenario_file}")
    
    print(f"Execution Mode: {config.execution_mode.value}")
    print(f"Verbose Mode: {config.verbose_mode}")
    print(f"Tag: {config.tag}")
    print(f"Started: {config.started_at.strftime('%Y-%m-%d %H:%M:%S')}")


def print_run_summary(result: RunResult, verbose: bool = False):
    """Print run summary to console."""
    print(f"\n{'='*60}")
    print(f"RUN SUMMARY")
    print(f"{'='*60}")
    print(f"Run ID: {result.run_id}")
    print(f"Approach: {result.approach.value}")
    print(f"Method: {result.method.value}")
    print(f"Fab: {result.building_code}")
    print(f"Tag: {result.tag}")
    print(f"Status: {result.status.value}")
    print(f"Duration: {result.duration:.2f}s")
    
    if result.approach == Approach.RANDOM:
        print(f"\nCOVERAGE RESULTS:")
        print(f"Target Coverage: {result.coverage_target:.1%}")
        print(f"Achieved Coverage: {result.total_coverage:.1%}")
        print(f"Paths Attempted: {result.paths_attempted}")
        print(f"Paths Found: {result.paths_found}")
    else:
        print(f"\nSCENARIO RESULTS:")
        print(f"Scenarios Executed: {result.scenario_tests}")
        print(f"Scenarios Successful: {result.paths_found}")
        print(f"Success Rate: {result.total_coverage:.1%}")
    
    print(f"Total Nodes: {result.total_nodes:,}")
    print(f"Total Links: {result.total_links:,}")
    
    if verbose and result.errors:
        print(f"\nERRORS ({len(result.errors)}):")
        for error in result.errors[:10]:  # Show first 10 errors
            print(f"  - {error}")
        if len(result.errors) > 10:
            print(f"  ... and {len(result.errors) - 10} more errors")
    
    if verbose and result.review_flags:
        print(f"\nREVIEW FLAGS ({len(result.review_flags)}):")
        for flag in result.review_flags[:5]:  # Show first 5 flags
            print(f"  - {flag}")
        if len(result.review_flags) > 5:
            print(f"  ... and {len(result.review_flags) - 5} more flags")
    
    if verbose and result.critical_errors:
        print(f"\nCRITICAL ERRORS ({len(result.critical_errors)}):")
        for error in result.critical_errors[:5]:  # Show first 5 critical errors
            print(f"  - {error}")
        if len(result.critical_errors) > 5:
            print(f"  ... and {len(result.critical_errors) - 5} more critical errors")


def execute_run_with_config(config: RunConfig, verbose: bool = False) -> RunResult:
    """Execute a run with the given configuration."""
    # Initialize database and services
    db = Database()
    try:
        # Auto-populate equipment tables on first run
        if config.approach == Approach.RANDOM and config.building_code:
            population_service = SimplePopulationService(db)
            population_service.populate_on_first_run(config.building_code)
        
        run_service = RunService(db)
        path_service = PathService(db)
        coverage_service = CoverageService(db)
        
        result = run_service.execute_run(config, path_service, coverage_service, verbose=verbose)
        return result
        
    finally:
        db.close()


def unattended_mode(args) -> None:
    """Run the application in unattended mode (silent, for scripts/automation)."""
    try:
        # Use the same configuration logic as default mode
        approach = Approach(args.approach)
        method = validate_method_for_approach(approach, args.method)
        
        # Validate approach-specific arguments (but suppress warnings in unattended mode)
        if approach == Approach.SCENARIO:
            scenario_args = [args.scenario_code, args.scenario_file]
            if not any(scenario_args):
                print("error")
                sys.exit(1)
            provided_args = [arg for arg in scenario_args if arg]
            if len(provided_args) > 1:
                print("error")
                sys.exit(1)
        
        # Handle approach-specific configuration
        building_code = ""
        toolset = ""
        phase = ""
        scenario_code = ""
        scenario_file = ""
        
        if approach == Approach.RANDOM:
            building_code = args.fab or ""
            toolset = args.toolset or ""
            phase = normalize_phase(args.phase or "")
            
            # Get default fab if not provided (silently)
            if not building_code:
                db = Database()
                try:
                    available_fabs = fetch_available_fabs(db)
                    if available_fabs:
                        building_code = available_fabs[0]
                    else:
                        building_code = "M16"  # Default fallback
                except Exception:
                    building_code = "M16"
                finally:
                    db.close()
        
        elif approach == Approach.SCENARIO:
            scenario_code = args.scenario_code or ""
            scenario_file = args.scenario_file or ""
            
            # Auto-detect method from scenario code if provided
            if scenario_code:
                method = detect_scenario_method_from_code(scenario_code)
        
        # Generate configuration
        coverage_target = args.coverage_target if approach == Approach.RANDOM else 0.0
        config = RunConfig.create_with_auto_tag(
            run_id=str(uuid.uuid4()),
            approach=approach,
            method=method,
            coverage_target=coverage_target,
            building_code=building_code,
            toolset=toolset,
            phase=phase,
            scenario_code=scenario_code,
            scenario_file=scenario_file,
            execution_mode=ExecutionMode.UNATTENDED,
            verbose_mode=False,
            started_at=datetime.now()
        )
        
        # Execute the run (always silent and non-verbose in unattended mode)
        result = execute_run_with_config(config, verbose=False)
        
        # Silent output - just status
        if result.status == RunStatus.DONE:
            print("completed")
            sys.exit(0)
        else:
            print("failed")
            sys.exit(1)
            
    except Exception:
        # Silent error handling - no details, just "error"
        print("error")
        sys.exit(1)


def default_mode(args) -> None:
    """Run the application in default mode (quick test with optional parameters)."""
    try:
        # Configuration logic
        approach = Approach(args.approach)
        method = validate_method_for_approach(approach, args.method)
        
        # Validate approach-specific arguments
        validate_approach_specific_args(approach, args)
        
        # Handle approach-specific configuration
        building_code = ""
        toolset = ""
        phase = ""
        scenario_code = ""
        scenario_file = ""
        
        if approach == Approach.RANDOM:
            building_code = args.fab or ""
            toolset = args.toolset or ""
            phase = normalize_phase(args.phase or "")
            
            # Get default fab if not provided
            if not building_code:
                db = Database()
                try:
                    available_fabs = fetch_available_fabs(db)
                    if available_fabs:
                        building_code = available_fabs[0]
                        if args.verbose:
                            print(f"No fab specified, using default: {building_code}")
                    else:
                        building_code = "M16"
                        if args.verbose:
                            print(f"No fabs found in database, using: {building_code}")
                except Exception as e:
                    building_code = "M16"
                    if args.verbose:
                        print(f"Could not retrieve fabs from database, using: {building_code}")
                finally:
                    db.close()
        
        elif approach == Approach.SCENARIO:
            scenario_code = args.scenario_code or ""
            scenario_file = args.scenario_file or ""
            
            # Auto-detect method from scenario code if provided
            if scenario_code:
                method = detect_scenario_method_from_code(scenario_code)
        
        # Generate configuration
        coverage_target = args.coverage_target if approach == Approach.RANDOM else 0.0
        config = RunConfig.create_with_auto_tag(
            run_id=str(uuid.uuid4()),
            approach=approach,
            method=method,
            coverage_target=coverage_target,
            building_code=building_code,
            toolset=toolset,
            phase=phase,
            scenario_code=scenario_code,
            scenario_file=scenario_file,
            execution_mode=ExecutionMode.DEFAULT,
            verbose_mode=args.verbose,
            started_at=datetime.now()
        )
        
        # Output based on verbosity
        if args.verbose:
            # Verbose mode - full configuration
            print_configuration_summary(config)
            print(f"\nStarting analysis run...")
        else:
            # Standard mode - brief message
            if approach == Approach.RANDOM:
                fab_display = f" for {building_code}" if building_code else ""
                print(f"Starting {approach.value} {method.value} analysis{fab_display} (target: {coverage_target:.1%})")
            else:
                scenario_display = scenario_code or scenario_file or "default"
                print(f"Starting {approach.value} analysis with {scenario_display}")
        
        # Execute the run
        result = execute_run_with_config(config, args.verbose)
        
        # Print results
        print_run_summary(result, verbose=args.verbose)
        
        # Exit with appropriate code
        if result.status == RunStatus.DONE:
            if not args.verbose:
                print(f"✅ Analysis completed successfully! Run ID: {result.run_id}")
            sys.exit(0)
        else:
            if not args.verbose:
                print(f"❌ Analysis failed. Run ID: {result.run_id}")
            sys.exit(1)
            
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        if args.verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)


def interactive_mode():
    """Run the application in interactive mode."""
    print("=" * 60)
    print("PATH ANALYSIS CLI TOOL")
    print("=" * 60)
    print("Welcome! This tool will guide you through setting up a path analysis run.")
    
    # Initialize database connection for options lookup
    db = Database()
    
    try:
        # 1. Select approach
        approach_str = fetch_user_choice(
            "Select analysis approach:",
            ["RANDOM", "SCENARIO"],
            default_idx=0
        )
        approach = Approach(approach_str)
        
        # 2. Select method based on approach
        if approach == Approach.RANDOM:
            method_choices = ["SIMPLE", "STRATIFIED"]
            method_help = "SIMPLE: Basic random sampling\nSTRATIFIED: Stratified random sampling (advanced)"
        else:
            method_choices = ["PREDEFINED", "SYNTHETIC"]
            method_help = "PREDEFINED: Use existing scenarios\nSYNTHETIC: Generate synthetic scenarios"
        
        print(f"\n{method_help}")
        method_str = fetch_user_choice(
            f"Select method for {approach_str} approach:",
            method_choices,
            default_idx=0
        )
        method = validate_method(approach, method_str)
        
        # 3. Get coverage target (only for RANDOM approach)
        coverage_target = 0.0  # Default for SCENARIO
        if approach == Approach.RANDOM:
            coverage_target = fetch_float_input(
                "\nEnter coverage target (as decimal, e.g., 0.2 for 20%)",
                default=0.2,
                min_val=0.01,
                max_val=1.0
            )
        else:
            print("\nSCENARIO approach uses predefined coverage from scenarios - no target needed.")
        
        # 4. Approach-specific configuration
        building_code = ""
        toolset = ""
        phase = ""
        scenario_code = ""
        scenario_file = ""
        
        if approach == Approach.RANDOM:
            # RANDOM approach: get fab, phase, and toolset
            available_fabs = fetch_available_fabs(db)
            building_code = fetch_string_input(
                "\nEnter fab identifier",
                required=True,
                available_options=available_fabs
            )
            
            available_phases = fetch_available_phases(db, building_code)
            if len(available_phases) > 1:
                phase_choice = fetch_string_input(
                    "\nEnter phase (optional)",
                    default="",
                    required=False,
                    available_options=available_phases
                )
                phase = normalize_phase(phase_choice)  # Convert to system nomenclature
            
            available_toolsets = fetch_available_toolsets(db, building_code)
            print("\nToolset selection (optional):")
            print("  - Leave empty to use all toolsets")
            print("  - Enter 'ALL' to explicitly use all toolsets")
            print("  - Enter specific toolset ID to limit analysis")
            
            toolset = fetch_string_input(
                "Enter toolset ID",
                default="",
                required=False,
                available_options=available_toolsets
            )
            
        elif approach == Approach.SCENARIO:
            # SCENARIO approach: get scenario configuration
            print("\nScenario configuration:")
            print("  - Specify a scenario code:")
            print("    • PRE### for predefined scenarios")
            print("    • SYN### for synthetic scenarios")
            print("  - Or provide a scenario file (e.g., scenarios.json)")
            
            available_scenarios = fetch_available_scenarios(db)
            scenario_code = fetch_string_input(
                "\nEnter scenario code (e.g., PRE001, SYN001)",
                default="",
                required=False,
                available_options=available_scenarios
            )
            
            if not scenario_code:
                scenario_file = fetch_string_input(
                    "Enter scenario file path (optional)",
                    default="",
                    required=False
                )
            
            # Ensure at least one scenario parameter is provided
            if not any([scenario_code, scenario_file]):
                print("At least one scenario parameter is required for SCENARIO approach.")
                return
            
            # Auto-detect method from scenario code if provided
            if scenario_code:
                method = detect_scenario_method_from_code(scenario_code)
                print(f"Auto-detected scenario type: {method.value}")
        
        # 5. Generate tag and create configuration
        config = RunConfig.create_with_auto_tag(
            run_id=str(uuid.uuid4()),
            approach=approach,
            method=method,
            coverage_target=coverage_target,
            building_code=building_code,
            toolset=toolset,
            phase=phase,
            scenario_code=scenario_code,
            scenario_file=scenario_file,
            execution_mode=ExecutionMode.INTERACTIVE,
            verbose_mode=False,  # Will be set below
            started_at=datetime.now()
        )
        
        # 6. Verbose option
        verbose_choice = fetch_user_choice(
            "\nEnable verbose output?",
            ["No", "Yes"],
            default_idx=0
        )
        verbose = verbose_choice == "Yes"
        config.verbose_mode = verbose
        
        # Show configuration and confirm
        print_configuration_summary(config)
        
        confirm = fetch_user_choice(
            "\nProceed with this configuration?",
            ["Yes", "No"],
            default_idx=0
        )
        
        if confirm == "No":
            print("Operation cancelled.")
            return
        
        # Execute the run
        print(f"\nStarting analysis run...")
        print(f"This may take several minutes depending on coverage target and network size.")
        
        result = execute_run_with_config(config, verbose)
        
        # Print results
        print_run_summary(result, verbose=verbose)
        
        # Exit with appropriate code
        if result.status == RunStatus.DONE:
            print(f"\n✅ Analysis completed successfully!")
            print(f"📊 Results stored with run ID: {result.run_id}")
            sys.exit(0)
        else:
            print(f"\n❌ Analysis failed. Check logs for details.")
            sys.exit(1)
            
    except Exception as e:
        print(f"\nError: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        db.close()


def main():
    """Main CLI entry point."""
    parser = create_parser()
    args = parser.parse_args()
    
    # Determine mode based on arguments
    try:
        if args.interactive:
            # Interactive mode for exploration/training
            interactive_mode()
        elif args.unattended:
            # Unattended mode for scripts/automation (always silent)
            unattended_mode(args)
        else:
            # Default mode (quick test) - handles normal operations
            default_mode(args)
    except KeyboardInterrupt:
        print("\nOperation cancelled by user.")
        sys.exit(130)


if __name__ == "__main__":
    main()
