#!/usr/bin/env python3
"""
Main CLI application entry point for path analysis system.
"""

import argparse
import sys
import uuid
from datetime import datetime
from typing import Optional, List

from enums import Approach, Method, RunStatus
from models import RunConfig, RunResult
from services.run_service import RunService
from services.path_service import PathService
from services.coverage_service import CoverageService
from db import Database


def create_parser() -> argparse.ArgumentParser:
    """Create command line argument parser."""
    parser = argparse.ArgumentParser(
        description="Path Analysis CLI Tool",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  Interactive mode:
    python main.py

  RANDOM approach (minimal - uses default 0.2 coverage):
    python main.py --approach RANDOM --method SIMPLE --coverage-target 0.2
    python main.py --approach RANDOM --coverage-target 0.2 --fab M16

  RANDOM approach (with optional parameters):
    python main.py --approach RANDOM --method SIMPLE --coverage-target 0.2 --fab M16 --toolset "6DXXXXXX" --verbose

  SCENARIO approach (minimal - uses default 0.2 coverage):
    python main.py --approach SCENARIO --method PREDEFINED --coverage-target 0.2
    python main.py --approach SCENARIO --method SYNTHETIC --coverage-target 0.1 --scenario-file "scenarios.json"
    python main.py --approach SCENARIO --coverage-target 0.2 --scenario-name "test-scenario-01"

  Silent unattended mode (automation/scripting):
    python main.py --approach RANDOM --coverage-target 0.2 --fab M16 --unattended
    python main.py --approach SCENARIO --coverage-target 0.2 --unattended
        """
    )
    
    parser.add_argument(
        '--approach',
        type=str,
        choices=['RANDOM', 'SCENARIO'],
        help='Analysis approach (RANDOM or SCENARIO)'
    )
    
    parser.add_argument(
        '--method',
        type=str,
        help='Analysis method (SIMPLE/STRATIFIED for RANDOM, PREDEFINED/SYNTHETIC for SCENARIO)'
    )
    
    parser.add_argument(
        '--coverage-target',
        type=float,
        help='Coverage target as decimal (e.g., 0.2 for 20%%)'
    )
    
    parser.add_argument(
        '--fab',
        type=str,
        help='Fabrication identifier for RANDOM approach (e.g., M16, M15)'
    )
    
    parser.add_argument(
        '--toolset',
        type=str,
        help='Toolset identifier for RANDOM approach (optional, e.g., 6DXXXXXX)'
    )
    
    parser.add_argument(
        '--scenario-file',
        type=str,
        help='Scenario file path for SCENARIO approach (optional)'
    )
    
    parser.add_argument(
        '--scenario-name',
        type=str,
        help='Scenario name for SCENARIO approach (optional)'
    )
    
    parser.add_argument(
        '--verbose',
        '-v',
        action='store_true',
        help='Enable verbose output (ignored if --unattended is used)'
    )
    
    parser.add_argument(
        '--unattended',
        '-u',
        action='store_true',
        help='Silent unattended mode - minimal output, no summary'
    )
    
    return parser


def get_user_choice(prompt: str, choices: List[str], default_idx: int = 0) -> str:
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


def get_float_input(prompt: str, default: float, min_val: float = 0.0, max_val: float = 1.0) -> float:
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


def get_string_input(prompt: str, default: str = "", required: bool = False, available_options: List[str] = None) -> str:
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


def get_available_fabs(db: Database) -> List[str]:
    """Get available fabs from database."""
    try:
        sql = "SELECT DISTINCT fab FROM tb_runs ORDER BY fab"
        results = db.query(sql)
        fabs = [row[0] for row in results] if results else []
        
        # Add some common default fabs if none found
        if not fabs:
            fabs = ["M16", "M15", "M14", "M13"]
        
        return fabs
    except Exception as e:
        print(f"Warning: Could not retrieve fabs from database: {e}")
        return ["M16", "M15", "M14", "M13"]  # Default options


def get_available_scenarios(db: Database) -> List[str]:
    """Get available scenarios from database."""
    try:
        sql = "SELECT DISTINCT name FROM scenarios ORDER BY name"
        results = db.query(sql)
        scenarios = [row[0] for row in results] if results else []
        
        # Add some default options if none found
        if not scenarios:
            scenarios = ["test-scenario-01", "test-scenario-02", "validation-suite"]
        
        return scenarios
    except Exception as e:
        print(f"Warning: Could not retrieve scenarios from database: {e}")
        return ["test-scenario-01", "test-scenario-02", "validation-suite"]
    """Get available toolsets for a specific fab."""
    try:
        sql = "SELECT DISTINCT toolset_id FROM toolsets WHERE fab = ? ORDER BY toolset_id"
        results = db.query(sql, [fab])
        toolsets = [row[0] for row in results] if results else []
        
        # Add "ALL" option and empty option
        options = ["ALL", ""]
        if toolsets:
            options.extend(toolsets)
        else:
            # Default toolsets if none found
            options.extend(["TOOLSET_001", "TOOLSET_002", "TOOLSET_003"])
        
        return options
    except Exception as e:
        print(f"Warning: Could not retrieve toolsets from database: {e}")
        return ["ALL", "", "TOOLSET_001", "TOOLSET_002", "TOOLSET_003"]


def get_available_toolsets(db: Database, fab: str) -> List[str]:
    """Validate and return the appropriate method for the given approach."""
    if approach == Approach.RANDOM:
        if method is None:
            return Method.SIMPLE
        if method.upper() not in ['SIMPLE', 'STRATIFIED']:
            raise ValueError(f"Invalid method '{method}' for RANDOM approach. Use SIMPLE or STRATIFIED.")
        return Method(method.upper())
    
    elif approach == Approach.SCENARIO:
        if method is None:
            return Method.PREDEFINED
        if method.upper() not in ['PREDEFINED', 'SYNTHETIC']:
            raise ValueError(f"Invalid method '{method}' for SCENARIO approach. Use PREDEFINED or SYNTHETIC.")
        return Method(method.upper())
    
    else:
        raise ValueError(f"Unknown approach: {approach}")


def validate_approach_specific_args(approach: Approach, args) -> None:
    """Validate that approach-specific arguments are provided correctly."""
    if approach == Approach.RANDOM:
        # RANDOM approach can work without fab/toolset (will use defaults)
        pass
    elif approach == Approach.SCENARIO:
        # SCENARIO approach doesn't use fab/toolset
        if args.fab:
            print(f"Warning: --fab is ignored for SCENARIO approach")
        if args.toolset:
            print(f"Warning: --toolset is ignored for SCENARIO approach")


def validate_method_for_approach(approach: Approach, method: Optional[str]) -> Method:
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


def validate_method(approach: Approach, method: str) -> Method:
    """Print configuration summary."""
    print(f"\n{'='*60}")
    print(f"CONFIGURATION SUMMARY")
    print(f"{'='*60}")
    print(f"Run ID: {config.run_id}")
    print(f"Approach: {config.approach.value}")
    print(f"Method: {config.method.value}")
    print(f"Coverage Target: {config.coverage_target:.1%}")
    print(f"Fab: {config.fab}")
    if config.toolset:
        print(f"Toolset: {config.toolset}")
    print(f"Tag: {config.tag}")
    print(f"Started: {config.started_at.strftime('%Y-%m-%d %H:%M:%S')}")


def print_configuration_summary(config: RunConfig, scenario_file: str = "", scenario_name: str = ""):
    """Print configuration summary."""
    print(f"\n{'='*60}")
    print(f"CONFIGURATION SUMMARY")
    print(f"{'='*60}")
    print(f"Run ID: {config.run_id}")
    print(f"Approach: {config.approach.value}")
    print(f"Method: {config.method.value}")
    print(f"Coverage Target: {config.coverage_target:.1%}")
    
    if config.approach == Approach.RANDOM:
        print(f"Fab: {config.fab}")
        if config.toolset:
            print(f"Toolset: {config.toolset}")
    elif config.approach == Approach.SCENARIO:
        if scenario_file:
            print(f"Scenario File: {scenario_file}")
        if scenario_name:
            print(f"Scenario Name: {scenario_name}")
    
    print(f"Tag: {config.tag}")
    print(f"Started: {config.started_at.strftime('%Y-%m-%d %H:%M:%S')}")
    """Print run summary to console."""
    print(f"\n{'='*60}")
    print(f"RUN SUMMARY")
    print(f"{'='*60}")
    print(f"Run ID: {result.run_id}")
    print(f"Approach: {result.approach.value}")
    print(f"Method: {result.method.value}")
    print(f"Fab: {result.fab}")
    print(f"Tag: {result.tag}")
    print(f"Status: {result.status.value}")
    print(f"Duration: {result.duration:.2f}s")
    print(f"\nCOVERAGE RESULTS:")
    print(f"Target Coverage: {result.coverage_target:.1%}")
    print(f"Achieved Coverage: {result.total_coverage:.1%}")
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


def print_run_summary(result: RunResult, verbose: bool = False):
    """Execute a run with the given configuration."""
    # Initialize database and services
    db = Database()
    try:
        run_service = RunService(db)
        path_service = PathService(db)
        coverage_service = CoverageService(db)
        
        # The toolset is now part of the config object
        result = run_service.execute_run(config, path_service, coverage_service, verbose=verbose)
        return result
        
    finally:
        db.close()


def execute_run_with_config(config: RunConfig, scenario_file: str = "", scenario_name: str = "", verbose: bool = False) -> RunResult:
    """Execute a run with the given configuration."""
    # Initialize database and services
    db = Database()
    try:
        run_service = RunService(db)
        path_service = PathService(db)
        coverage_service = CoverageService(db)
        
        # Pass scenario parameters if this is a SCENARIO approach
        if config.approach == Approach.SCENARIO:
            # TODO: Pass scenario_file and scenario_name to the run service
            # For now, these will be stored in the config or passed separately
            pass
        
        result = run_service.execute_run(config, path_service, coverage_service, verbose=verbose)
        return result
        
    finally:
        db.close()
    """Run the application in unattended mode using command line arguments."""
    try:
        # Validate required arguments
        if not args.approach:
            raise ValueError("--approach is required for unattended mode")
        if not args.coverage_target:
            raise ValueError("--coverage-target is required for unattended mode")
        
        # Validate inputs
        approach = Approach(args.approach)
        method = validate_method_for_approach(approach, args.method)
        
        if not (0.0 < args.coverage_target <= 1.0):
            raise ValueError("Coverage target must be between 0.0 and 1.0")
        
        # Handle optional fab (use default if not provided)
        fab = args.fab or ""
        toolset = args.toolset or ""
        
        # If fab is not provided, try to get a default from database
        if not fab:
            db = Database()
            try:
                available_fabs = get_available_fabs(db)
                if available_fabs:
                    fab = available_fabs[0]  # Use first available fab as default
                    if args.verbose and not args.unattended:
                        print(f"No fab specified, using default: {fab}")
                else:
                    fab = "DEFAULT"  # Fallback if no fabs in database
                    if args.verbose and not args.unattended:
                        print(f"No fabs found in database, using: {fab}")
            except Exception as e:
                fab = "DEFAULT"
                if args.verbose and not args.unattended:
                    print(f"Could not retrieve fabs from database, using: {fab}")
            finally:
                db.close()
        
        # Generate tag and create run configuration
        config = RunConfig.create_with_auto_tag(
            run_id=str(uuid.uuid4()),
            approach=approach,
            method=method,
            coverage_target=args.coverage_target,
            fab=fab,
            toolset=toolset,
            started_at=datetime.now()
        )
        
        # Output based on mode
        if args.unattended:
            # Silent mode - minimal output
            pass  # No startup message
        elif args.verbose:
            # Verbose mode - full configuration
            print_configuration_summary(config)
            print(f"\nStarting analysis run...")
        else:
            # Standard mode - brief message
            fab_display = f" for {fab}" if fab and fab != "DEFAULT" else ""
            print(f"Starting {approach.value} {method.value} analysis{fab_display} (target: {args.coverage_target:.1%})")
        
        # Execute the run (verbose is ignored if unattended)
        verbose_mode = args.verbose and not args.unattended
        result = execute_run_with_config(config, verbose_mode)
        
        # Output based on mode
        if args.unattended:
            # Silent mode - just print "completed" and run ID
            if result.status == RunStatus.DONE:
                print("completed")
                sys.exit(0)
            else:
                print("failed")
                sys.exit(1)
        else:
            # Standard/verbose mode - print full summary
            print_run_summary(result, verbose=verbose_mode)
            
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
        if args.unattended:
            # Silent mode - just print "error"
            print("error")
            sys.exit(1)
        else:
            # Standard mode - print error details
            print(f"Error: {e}", file=sys.stderr)
            if args.verbose:
                import traceback
                traceback.print_exc()
            sys.exit(1)


def unattended_mode(args) -> None:
    """Run the application in unattended mode using command line arguments."""
    try:
        # Validate required arguments
        if not args.approach:
            raise ValueError("--approach is required for unattended mode")
        if not args.coverage_target:
            raise ValueError("--coverage-target is required for unattended mode")
        
        # Validate inputs
        approach = Approach(args.approach)
        method = validate_method_for_approach(approach, args.method)
        
        if not (0.0 < args.coverage_target <= 1.0):
            raise ValueError("Coverage target must be between 0.0 and 1.0")
        
        # Validate approach-specific arguments
        validate_approach_specific_args(approach, args)
        
        # Handle approach-specific configuration
        fab = ""
        toolset = ""
        scenario_file = ""
        scenario_name = ""
        
        if approach == Approach.RANDOM:
            # Handle RANDOM approach configuration
            fab = args.fab or ""
            toolset = args.toolset or ""
            
            # If fab is not provided, try to get a default from database
            if not fab:
                db = Database()
                try:
                    available_fabs = get_available_fabs(db)
                    if available_fabs:
                        fab = available_fabs[0]  # Use first available fab as default
                        if args.verbose and not args.unattended:
                            print(f"No fab specified, using default: {fab}")
                    else:
                        fab = "DEFAULT"  # Fallback if no fabs in database
                        if args.verbose and not args.unattended:
                            print(f"No fabs found in database, using: {fab}")
                except Exception as e:
                    fab = "DEFAULT"
                    if args.verbose and not args.unattended:
                        print(f"Could not retrieve fabs from database, using: {fab}")
                finally:
                    db.close()
        
        elif approach == Approach.SCENARIO:
            # Handle SCENARIO approach configuration
            scenario_file = args.scenario_file or ""
            scenario_name = args.scenario_name or ""
            
            # For SCENARIO, we might want to validate that at least one scenario parameter is provided
            # or use defaults from database
            if not scenario_file and not scenario_name:
                if args.verbose and not args.unattended:
                    print("No scenario file or name specified, will use default scenario selection")
        
        # Generate tag and create run configuration
        config = RunConfig.create_with_auto_tag(
            run_id=str(uuid.uuid4()),
            approach=approach,
            method=method,
            coverage_target=args.coverage_target,
            fab=fab,
            toolset=toolset,
            started_at=datetime.now()
        )
        
        # Output based on mode
        if args.unattended:
            # Silent mode - minimal output
            pass  # No startup message
        elif args.verbose:
            # Verbose mode - full configuration
            print_configuration_summary(config, scenario_file, scenario_name)
            print(f"\nStarting analysis run...")
        else:
            # Standard mode - brief message
            if approach == Approach.RANDOM:
                fab_display = f" for {fab}" if fab and fab != "DEFAULT" else ""
                print(f"Starting {approach.value} {method.value} analysis{fab_display} (target: {args.coverage_target:.1%})")
            else:
                scenario_display = f" with {scenario_name}" if scenario_name else ""
                print(f"Starting {approach.value} {method.value} analysis{scenario_display} (target: {args.coverage_target:.1%})")
        
        # Execute the run (verbose is ignored if unattended)
        verbose_mode = args.verbose and not args.unattended
        result = execute_run_with_config(config, scenario_file, scenario_name, verbose_mode)
        
        # Output based on mode
        if args.unattended:
            # Silent mode - just print "completed" and run ID
            if result.status == RunStatus.DONE:
                print("completed")
                sys.exit(0)
            else:
                print("failed")
                sys.exit(1)
        else:
            # Standard/verbose mode - print full summary
            print_run_summary(result, verbose=verbose_mode)
            
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
        if args.unattended:
            # Silent mode - just print "error"
            print("error")
            sys.exit(1)
        else:
            # Standard mode - print error details
            print(f"Error: {e}", file=sys.stderr)
            if args.verbose:
                import traceback
                traceback.print_exc()
            sys.exit(1)
    """Run the application in interactive mode."""
    print("=" * 60)
    print("PATH ANALYSIS CLI TOOL")
    print("=" * 60)
    print("Welcome! This tool will guide you through setting up a path analysis run.")
    
    # Initialize database connection for options lookup
    db = Database()
    
    try:
        # 1. Select approach
        approach_str = get_user_choice(
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
        method_str = get_user_choice(
            f"Select method for {approach_str} approach:",
            method_choices,
            default_idx=0
        )
        method = validate_method(approach, method_str)
        
        # 3. Get coverage target
        coverage_target = get_float_input(
            "\nEnter coverage target (as decimal, e.g., 0.2 for 20%)",
            default=0.2,
            min_val=0.01,
            max_val=1.0
        )
        
        # 4. Select fab
        available_fabs = get_available_fabs(db)
        fab = get_string_input(
            "\nEnter fabrication identifier (fab)",
            required=True,
            available_options=available_fabs
        )
        
        # 5. Select toolset (optional)
        available_toolsets = get_available_toolsets(db, fab)
        print("\nToolset selection (optional):")
        print("  - Leave empty to use all toolsets")
        print("  - Enter 'ALL' to explicitly use all toolsets")
        print("  - Enter specific toolset ID to limit analysis")
        
        toolset = get_string_input(
            "Enter toolset ID",
            default="",
            required=False,
            available_options=available_toolsets
        )
        
        # 6. Generate tag and create configuration
        config = RunConfig.create_with_auto_tag(
            run_id=str(uuid.uuid4()),
            approach=approach,
            method=method,
            coverage_target=coverage_target,
            fab=fab,
            toolset=toolset,
            started_at=datetime.now()
        )
        verbose_choice = get_user_choice(
            "Enable verbose output?",
            ["No", "Yes"],
            default_idx=0
        )
        verbose = verbose_choice == "Yes"
        
        # 7. Verbose option
        
        # Show configuration and confirm
        print_configuration_summary(config)
        
        confirm = get_user_choice(
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
        approach_str = get_user_choice(
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
        method_str = get_user_choice(
            f"Select method for {approach_str} approach:",
            method_choices,
            default_idx=0
        )
        method = validate_method(approach, method_str)
        
        # 3. Get coverage target
        coverage_target = get_float_input(
            "\nEnter coverage target (as decimal, e.g., 0.2 for 20%)",
            default=0.2,
            min_val=0.01,
            max_val=1.0
        )
        
        # 4. Approach-specific configuration
        fab = ""
        toolset = ""
        scenario_file = ""
        scenario_name = ""
        
        if approach == Approach.RANDOM:
            # RANDOM approach: get fab and toolset
            available_fabs = get_available_fabs(db)
            fab = get_string_input(
                "\nEnter fabrication identifier (fab)",
                required=True,
                available_options=available_fabs
            )
            
            available_toolsets = get_available_toolsets(db, fab)
            print("\nToolset selection (optional):")
            print("  - Leave empty to use all toolsets")
            print("  - Enter 'ALL' to explicitly use all toolsets")
            print("  - Enter specific toolset ID to limit analysis")
            
            toolset = get_string_input(
                "Enter toolset ID",
                default="",
                required=False,
                available_options=available_toolsets
            )
            
        elif approach == Approach.SCENARIO:
            # SCENARIO approach: get scenario configuration
            print("\nScenario configuration:")
            print("  - You can specify a scenario file, scenario name, or both")
            print("  - Leave both empty to use default scenario selection")
            
            scenario_file = get_string_input(
                "\nEnter scenario file path (optional)",
                default="",
                required=False
            )
            
            if not scenario_file:
                available_scenarios = get_available_scenarios(db)
                scenario_name = get_string_input(
                    "Enter scenario name (optional)",
                    default="",
                    required=False,
                    available_options=available_scenarios
                )
        
        # 5. Generate tag and create configuration
        config = RunConfig.create_with_auto_tag(
            run_id=str(uuid.uuid4()),
            approach=approach,
            method=method,
            coverage_target=coverage_target,
            fab=fab,
            toolset=toolset,
            started_at=datetime.now()
        )
        
        # 6. Verbose option
        verbose_choice = get_user_choice(
            "\nEnable verbose output?",
            ["No", "Yes"],
            default_idx=0
        )
        verbose = verbose_choice == "Yes"
        
        # Show configuration and confirm
        print_configuration_summary(config, scenario_file, scenario_name)
        
        confirm = get_user_choice(
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
        
        result = execute_run_with_config(config, scenario_file, scenario_name, verbose)
        
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
    """Main CLI entry point."""
    parser = create_parser()
    
    # If no arguments provided, run in interactive mode
    if len(sys.argv) == 1:
        try:
            interactive_mode()
        except KeyboardInterrupt:
            print("\n\nOperation cancelled by user.")
            sys.exit(130)
    else:
        # Parse arguments and run in unattended mode
        args = parser.parse_args()
        try:
            unattended_mode(args)
        except KeyboardInterrupt:
            print("\nOperation cancelled by user.")
            sys.exit(130)


if __name__ == "__main__":
    main()
