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

  Unattended mode (minimal):
    python main.py --approach RANDOM --method SIMPLE --coverage-target 0.2
    python main.py --approach SCENARIO --method PREDEFINED --coverage-target 0.15

  Unattended mode (with optional parameters):
    python main.py --approach RANDOM --method SIMPLE --coverage-target 0.2 --fab M16
    python main.py --approach RANDOM --method SIMPLE --coverage-target 0.3 --fab M15 --toolset "6DXXXXXX" --verbose
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
        help='Fabrication identifier (e.g., M16, M15)'
    )
    
    parser.add_argument(
        '--toolset',
        type=str,
        help='Toolset identifier (optional, e.g., 6DXXXXXX)'
    )
    
    parser.add_argument(
        '--verbose',
        '-v',
        action='store_true',
        help='Enable verbose output'
    )
    
    return parser
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


def get_available_toolsets(db: Database, fab: str) -> List[str]:
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


def get_user_choice(prompt: str, choices: List[str], default_idx: int = 0) -> str:
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


def validate_method_for_approach(approach: Approach, method: Optional[str]) -> Method:
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


def validate_method(approach: Approach, method: str) -> Method: 
    """Generate tag using the specified format."""
    date = datetime.now()
    coverage_target_tag = f'{coverage_target*100:.0f}P'
    
    # Base tag
    tag = f"{date.strftime('%Y%m%d')}_{approach.value}_{method.value}_{coverage_target_tag}"
    
    # Add fab if not empty
    if fab:
        tag += f"_{fab}"
    
    # Add toolset if not empty and not "ALL"
    if toolset and toolset != "ALL":
        tag += f"_{toolset}"
    
    return tag


def print_configuration_summary(config: RunConfig, toolset: str = ""):
    """Print configuration summary."""
    print(f"\n{'='*60}")
    print(f"CONFIGURATION SUMMARY")
    print(f"{'='*60}")
    print(f"Run ID: {config.run_id}")
    print(f"Approach: {config.approach.value}")
    print(f"Method: {config.method.value}")
    print(f"Coverage Target: {config.coverage_target:.1%}")
    print(f"Fab: {config.fab}")
    if toolset:
        print(f"Toolset: {toolset}")
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


def generate_tag(approach: Approach, method: Method, coverage_target: float,
                fab: str = "", toolset: str = "") -> str:
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
        
        # 6. Generate tag
        tag = generate_tag(approach, method, coverage_target, fab, toolset)
        
        # 7. Verbose option
        verbose_choice = get_user_choice(
            "Enable verbose output?",
            ["No", "Yes"],
            default_idx=0
        )
        verbose = verbose_choice == "Yes"
        
        # Create run configuration
        config = RunConfig(
            run_id=str(uuid.uuid4()),
            approach=approach,
            method=method,
            coverage_target=coverage_target,
            fab=fab,
            tag=tag,
            started_at=datetime.now()
        )
        
        # Show configuration and confirm
        print_configuration_summary(config, toolset)
        
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
        
        run_service = RunService(db)
        path_service = PathService(db)
        coverage_service = CoverageService(db)
        
        # Store toolset in config for use in services
        # Note: You may need to modify RunConfig model to include toolset
        # For now, we'll pass it through the services if needed
        
        result = run_service.execute_run(config, path_service, coverage_service, verbose=verbose)
        
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


def execute_run_with_config(config: RunConfig, toolset: str = "", verbose: bool = False) -> RunResult:
    """Execute a run with the given configuration."""
    # Initialize database and services
    db = Database()
    try:
        run_service = RunService(db)
        path_service = PathService(db)
        coverage_service = CoverageService(db)
        
        # Store toolset in config for use in services if needed
        # Note: You may need to modify RunConfig model to include toolset
        # For now, we'll pass it through the services if needed
        
        result = run_service.execute_run(config, path_service, coverage_service, verbose=verbose)
        return result
        
    finally:
        db.close()


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
                    if args.verbose:
                        print(f"No fab specified, using default: {fab}")
                else:
                    fab = "DEFAULT"  # Fallback if no fabs in database
                    if args.verbose:
                        print(f"No fabs found in database, using: {fab}")
            except Exception as e:
                fab = "DEFAULT"
                if args.verbose:
                    print(f"Could not retrieve fabs from database, using: {fab}")
            finally:
                db.close()
        
        # Generate tag
        tag = generate_tag(approach, method, args.coverage_target, fab, toolset)
        
        # Create run configuration
        config = RunConfig(
            run_id=str(uuid.uuid4()),
            approach=approach,
            method=method,
            coverage_target=args.coverage_target,
            fab=fab,
            tag=tag,
            started_at=datetime.now()
        )
        
        if args.verbose:
            print_configuration_summary(config, toolset)
            print(f"\nStarting analysis run...")
        else:
            fab_display = f" for {fab}" if fab and fab != "DEFAULT" else ""
            print(f"Starting {approach.value} {method.value} analysis{fab_display} (target: {args.coverage_target:.1%})")
        
        # Execute the run
        result = execute_run_with_config(config, toolset, args.verbose)
        
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
    """Main CLI entry point."""
    # Check if any command line arguments were provided
    if len(sys.argv) > 1:
        print("Command line arguments detected, but this version only supports interactive mode.")
        print("Please run with: python main.py")
        sys.exit(1)
    
    try:
        interactive_mode()
    except KeyboardInterrupt:
        print("\n\nOperation cancelled by user.")
        sys.exit(130)


if __name__ == "__main__":
    main()