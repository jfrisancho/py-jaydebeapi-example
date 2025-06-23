#!/usr/bin/env python3
"""
Main CLI application entry point for path analysis system.
Final version with simplified fab-based implementation and updated phase handling.
"""

import argparse
import sys
import uuid
from datetime import datetime
from typing import Optional, List, Union # Added Union

from enums import Approach, Method, RunStatus, ExecutionMode, Phase, ScenarioType # Added ScenarioType
from models import RunConfig, RunResult
from services.run_service import RunService
from services.path_service import PathService
from services.coverage_service import CoverageService
from services.simple_random_service import SimplePopulationService # Assumed this populates nw_nodes/links
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
    python main.py --fab M16 --phase PHASE1        # specify fab and phase

  Interactive mode (for exploration/training):
    python main.py --interactive
    python main.py -i

  Random approach (specific tests):
    python main.py -a RANDOM --fab M16 --toolset "TS001" --phase A
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
        choices=[e.value for e in Approach], # Use enum values
        default=Approach.RANDOM.value,
        help='Analysis approach (default: RANDOM)'
    )
    
    parser.add_argument(
        '--method',
        type=str,
        # Choices depend on approach, validated later. Provide all for help string.
        choices=[e.value for e in Method],
        help='Analysis method (RANDOM: SIMPLE, STRATIFIED; SCENARIO: PREDEFINED, SYNTHETIC)'
    )
    
    parser.add_argument(
        '--coverage-target',
        type=float,
        default=0.2,
        help='Coverage target as decimal for RANDOM approach (default: 0.2 for 20%%) - ignored for SCENARIO approach'
    )
    
    parser.add_argument(
        '--fab', # Corresponds to building_code
        type=str,
        # Dynamic choices based on DB if possible, or common examples
        help='Fab identifier for RANDOM approach (e.g., M16, M15, M15X) - ignored for SCENARIO approach'
    )
    
    parser.add_argument(
        '--toolset',
        type=str,
        help='Toolset identifier for RANDOM approach (e.g., TS001, or "ALL") - ignored for SCENARIO approach'
    )
    
    parser.add_argument(
        '--phase',
        type=str,
        # User can input PHASE1, 1, A, etc. Will be normalized.
        help='Phase identifier for RANDOM approach (e.g., PHASE1, A, 1) - ignored for SCENARIO approach'
    )
    
    parser.add_argument(
        '--scenario-code',
        type=str,
        help='Scenario code for SCENARIO approach (e.g., PRE001, SYN001) - ignored for RANDOM approach'
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
        help='Enable verbose output (ignored if --unattended is used)'
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
            if not user_input: return choices[default_idx]
            choice_idx = int(user_input) - 1
            if 0 <= choice_idx < len(choices): return choices[choice_idx]
            else: print(f"Invalid choice. Please enter a number between 1 and {len(choices)}.")
        except ValueError: print("Invalid input. Please enter a number.")
        except KeyboardInterrupt: print("\nOperation cancelled by user."); sys.exit(130)


def fetch_float_input(prompt: str, default: float, min_val: float = 0.0, max_val: float = 1.0) -> float:
    """Get float input from user with validation."""
    while True:
        try:
            user_input = input(f"{prompt} [default: {default}]: ").strip()
            if not user_input: return default
            value = float(user_input)
            if min_val <= value <= max_val: return value
            else: print(f"Value must be between {min_val} and {max_val}.")
        except ValueError: print("Invalid input. Please enter a decimal number (e.g., 0.2 for 20%).")
        except KeyboardInterrupt: print("\nOperation cancelled by user."); sys.exit(130)


def fetch_string_input(prompt: str, default: str = "", required: bool = False, available_options: Optional[List[str]] = None) -> str:
    """Get string input from user, optionally from a list of available options."""
    full_prompt = prompt
    if default: full_prompt += f" [default: {default}]"
    full_prompt += ": "

    while True:
        try:
            if available_options:
                print(f"\n{prompt}")
                # Create a temporary list for display and selection if options are numerous
                display_options = available_options
                if len(available_options) > 10 and not required and (default=="" or default not in available_options): # Add "None" for optional with many choices
                    display_options = ["None"] + available_options
                
                for i, option in enumerate(display_options, 1):
                     print(f"  {i}. {option}")
                
                input_prompt = f"Enter choice (1-{len(display_options)}) or type value"
                if default and default in display_options:
                    default_display_idx = display_options.index(default) + 1
                    input_prompt += f" [default: {default_display_idx} ({default})]: "
                elif default:
                     input_prompt += f" [default: {default} (type it)]: "
                else:
                    input_prompt += ": "
                user_input = input(input_prompt).strip()

                if not user_input and default: return default
                if not user_input and not required: return ""
                
                # Check if user entered a number corresponding to an option
                if user_input.isdigit():
                    choice_idx = int(user_input) -1
                    if 0 <= choice_idx < len(display_options):
                        selected_option = display_options[choice_idx]
                        return "" if selected_option == "None" else selected_option
                
                # If not a number or invalid number, treat as direct string input
                # Validate against original available_options if it's a typed input
                if user_input in available_options: return user_input
                if not available_options and user_input: return user_input # Allow any string if no options given
                if not user_input and required: print("This field is required."); continue
                if user_input and available_options and user_input not in available_options:
                    print(f"Invalid input. '{user_input}' is not in the available options. Please choose from the list or type an exact match.")
                    continue # Re-prompt
                if not user_input and not default and not required: return ""

            else: # Simple string input without listed options
                user_input = input(full_prompt).strip()
                if not user_input:
                    if default: return default
                    if not required: return ""
                    else: print("This field is required."); continue
                return user_input

        except ValueError: print("Invalid input.")
        except KeyboardInterrupt: print("\nOperation cancelled by user."); sys.exit(130)


def fetch_available_fabs(db: Database) -> List[str]:
    """Get available fabs from tb_toolsets."""
    try:
        results = db.query("SELECT DISTINCT fab FROM tb_toolsets WHERE is_active = TRUE ORDER BY fab")
        return [row[0] for row in results] if results else ["M16", "M15", "M15X"] # Default fallback
    except Exception: return ["M16", "M15", "M15X"]


def fetch_available_scenarios(db: Database) -> List[str]:
    """Get available scenario codes from tb_scenarios."""
    try:
        results = db.query("SELECT code FROM tb_scenarios WHERE is_active = TRUE ORDER BY code")
        return [row[0] for row in results] if results else ["PRE001", "SYN001"] # Default fallback
    except Exception: return ["PRE001", "SYN001"]


def fetch_available_toolsets(db: Database, fab: str) -> List[str]:
    """Get available toolsets for a specific fab from tb_toolsets."""
    options = ["ALL"] # "ALL" is always an option
    try:
        if fab: # Only query if fab is specified
            results = db.query("SELECT DISTINCT code FROM tb_toolsets WHERE fab = ? AND is_active = TRUE ORDER BY code", [fab])
            if results: options.extend([row[0] for row in results])
    except Exception: pass # Fallback to just "ALL" or add hardcoded defaults
    if len(options) == 1: options.extend(["TS001_Example", "TS002_Example"]) # Add if DB empty
    return options


def fetch_available_phases(db: Database, fab: str) -> List[str]: # Returns human-readable: PHASE1, PHASE2...
    """Get available phases for a specific fab from tb_toolsets, returns human-readable."""
    # Phases are A, B, C, D in DB. Convert to PHASE1 etc for user.
    human_readable_phases: List[str] = []
    default_phases = [p.conceptual for p in Phase.phases()] # ["PHASE1", "PHASE2", ...]
    try:
        if fab: # Only query if fab is specified
            results = db.query("SELECT DISTINCT phase FROM tb_toolsets WHERE fab = ? AND is_active = TRUE ORDER BY phase", [fab])
            if results:
                db_phases = {row[0] for row in results} # set of 'A', 'B' etc.
                for phase_enum_member in Phase: # Iterate through PHASE1, PHASE2...
                    if phase_enum_member.nominal in db_phases: # Check if 'A' is in db_phases
                        human_readable_phases.append(phase_enum_member.conceptual) # Add "PHASE1"
        return human_readable_phases if human_readable_phases else default_phases
    except Exception: return default_phases


def validate_approach_specific_args(approach_enum: Approach, args: argparse.Namespace, verbose: bool):
    """Validate arguments based on approach. Warn if ignored."""
    warnings = []
    if approach_enum == Approach.SCENARIO:
        ignored_for_scenario = {'fab': args.fab, 'toolset': args.toolset, 'phase': args.phase, 'method': args.method}
        if args.coverage_target != 0.2: ignored_for_scenario['coverage_target'] = args.coverage_target # Only if not default
        for arg_name, arg_val in ignored_for_scenario.items():
            if arg_val: warnings.append(f"--{arg_name.replace('_','-')} is ignored for SCENARIO approach.")
        
        if not (args.scenario_code or args.scenario_file):
            raise ValueError("SCENARIO approach requires either --scenario-code or --scenario-file.")
        if args.scenario_code and args.scenario_file:
            raise ValueError("SCENARIO approach: provide only one of --scenario-code or --scenario-file.")
    
    elif approach_enum == Approach.RANDOM:
        ignored_for_random = {'scenario_code': args.scenario_code, 'scenario_file': args.scenario_file}
        for arg_name, arg_val in ignored_for_random.items():
            if arg_val: warnings.append(f"--{arg_name.replace('_','-')} is ignored for RANDOM approach.")
        # Fab is usually required for RANDOM unless handled by a default elsewhere
        # if not args.fab: raise ValueError("RANDOM approach typically requires --fab.")

    if verbose and warnings:
        print("Argument Warnings:")
        for warn in warnings: print(f"  - {warn}")


def determine_method(approach_enum: Approach, args_method_str: Optional[str], scenario_code: Optional[str]) -> Method:
    """Determine the Method enum based on approach and arguments."""
    if approach_enum == Approach.RANDOM:
        if args_method_str:
            try: return Method(args_method_str.upper())
            except ValueError: raise ValueError(f"Invalid method '{args_method_str}' for RANDOM. Must be SIMPLE or STRATIFIED.")
        return Method.SIMPLE # Default for RANDOM
    
    elif approach_enum == Approach.SCENARIO:
        # For SCENARIO, method is often implicit (PREDEFINED/SYNTHETIC from code, or just "SCENARIO_METHOD")
        # Or could be explicitly set if tb_scenarios.scenario_type becomes the method source.
        if args_method_str: # If user explicitly provides method for scenario
             try: return Method(args_method_str.upper())
             except ValueError: raise ValueError(f"Invalid method '{args_method_str}' for SCENARIO. Must be PREDEFINED or SYNTHETIC.")

        if scenario_code: # Auto-detect from scenario_code
            if scenario_code.upper().startswith("PRE"): return Method.PREDEFINED
            if scenario_code.upper().startswith("SYN"): return Method.SYNTHETIC
        return Method.PREDEFINED # Default for SCENARIO if not otherwise determinable
    
    raise ValueError(f"Cannot determine method for approach {approach_enum.value}")


def normalize_phase_input(phase_input_str: Optional[str]) -> str: # Returns system nominal (A,B,C,D) or ""
    """Normalize phase input string to system nominal (A, B, C, D) or empty string."""
    if not phase_input_str: return ""
    phase_enum = Phase.normalize(phase_input_str)
    return phase_enum.nominal if phase_enum else ""


def print_configuration_summary(config: RunConfig):
    print(f"\n{'='*60}\nCONFIGURATION SUMMARY\n{'='*60}")
    print(f"Run ID: {config.run_id}\nApproach: {config.approach.value}")
    
    if config.approach == Approach.RANDOM:
        print(f"Method: {config.method.value}\nCoverage Target: {config.coverage_target:.1%}")
        print(f"Fab: {config.building_code if config.building_code else 'Not Specified (using default)'}")
        if config.phase: # config.phase is 'A', 'B', etc.
            phase_enum = Phase.normalize(config.phase)
            display_phase = phase_enum.conceptual if phase_enum else config.phase # Display PHASE1
            print(f"Phase: {display_phase} (System: {config.phase})")
        if config.toolset: print(f"Toolset: {config.toolset}")
    
    elif config.approach == Approach.SCENARIO:
        print(f"Method (Scenario Type): {config.method.value}") # Method now reflects PREDEFINED/SYNTHETIC
        if config.scenario_code: print(f"Scenario Code: {config.scenario_code}")
        if config.scenario_file: print(f"Scenario File: {config.scenario_file}")
    
    print(f"Execution Mode: {config.execution_mode.value}\nVerbose Mode: {config.verbose_mode}")
    print(f"Tag: {config.tag}\nStarted: {config.started_at.strftime('%Y-%m-%d %H:%M:%S')}")


def print_run_summary(result: RunResult, verbose: bool = False):
    print(f"\n{'='*60}\nRUN SUMMARY\n{'='*60}")
    print(f"Run ID: {result.run_id}\nApproach: {result.approach.value}\nMethod: {result.method.value}")
    if result.building_code: print(f"Fab: {result.building_code}")
    print(f"Tag: {result.tag}\nStatus: {result.status.value}\nDuration: {result.duration:.2f}s")

    if result.approach == Approach.RANDOM:
        print(f"\nCOVERAGE RESULTS (RANDOM):")
        print(f"  Target Coverage: {result.coverage_target:.1%}")
        print(f"  Achieved Coverage: {result.total_coverage:.1%}")
        print(f"  Paths Attempted: {result.paths_attempted}\n  Paths Found: {result.paths_found}")
        print(f"  Total Nodes in Fab: {result.total_nodes:,}\n  Total Links in Fab: {result.total_links:,}")
    else: # SCENARIO
        print(f"\nSCENARIO RESULTS:")
        print(f"  Scenarios Executed: {result.scenario_tests}\n  Scenarios Successful: {result.paths_found}")
        print(f"  Success Rate: {result.total_coverage:.1%}") # total_coverage is success rate for scenarios
        print(f"  Total Nodes in Paths: {result.total_nodes:,}\n  Total Links in Paths: {result.total_links:,}")

    if verbose:
        for error_type, errors_list_str in [
            ("ERRORS", result.errors), ("REVIEW FLAGS", result.review_flags), ("CRITICAL ERRORS", result.critical_errors)
        ]:
            if errors_list_str:
                print(f"\n{error_type} ({len(errors_list_str)}):")
                for item_str in errors_list_str[:5]: print(f"  - {item_str}")
                if len(errors_list_str) > 5: print(f"  ... and {len(errors_list_str) - 5} more.")
    print(f"{'='*60}")


def execute_run_with_config(config: RunConfig, db: Database) -> RunResult:
    """Execute a run with the given configuration. DB passed as arg."""
    # Initialize services
    if config.approach == Approach.RANDOM and config.building_code:
        # SimplePopulationService might populate nw_nodes/links, not tb_toolsets/tb_equipments
        # This is okay if they serve different purposes.
        population_service = SimplePopulationService(db)
        population_service.populate_on_first_run(config.building_code) # For nw_ tables if needed
    
    run_service = RunService(db)
    path_service = PathService(db)
    coverage_service = CoverageService(db)
    
    return run_service.execute_run(config, path_service, coverage_service, verbose=config.verbose_mode)


def main_flow(args: argparse.Namespace, execution_mode: ExecutionMode):
    """Core logic for default and unattended modes."""
    db = Database()
    try:
        approach_enum = Approach(args.approach.upper())
        validate_approach_specific_args(approach_enum, args, verbose=(execution_mode != ExecutionMode.UNATTENDED and args.verbose))
        
        # Determine fab (building_code)
        fab_code = args.fab or ""
        if approach_enum == Approach.RANDOM and not fab_code:
            available_fabs = fetch_available_fabs(db)
            if available_fabs: fab_code = available_fabs[0]
            else: fab_code = "M16" # Fallback if DB empty or no specific fab
            if execution_mode != ExecutionMode.UNATTENDED and args.verbose:
                print(f"No fab specified for RANDOM run, using default: {fab_code}")
        
        # Determine method
        method_enum = determine_method(approach_enum, args.method, args.scenario_code)
        
        # Normalize phase input for RANDOM runs
        normalized_phase_str = normalize_phase_input(args.phase) if approach_enum == Approach.RANDOM else ""

        run_config = RunConfig.create_with_auto_tag(
            run_id=str(uuid.uuid4()),
            approach=approach_enum,
            method=method_enum,
            coverage_target=args.coverage_target if approach_enum == Approach.RANDOM else 0.0,
            building_code=fab_code, # This is fab
            toolset=args.toolset or ("ALL" if approach_enum == Approach.RANDOM else ""),
            phase=normalized_phase_str,
            scenario_code=args.scenario_code or "",
            scenario_file=args.scenario_file or "",
            execution_mode=execution_mode,
            verbose_mode=(execution_mode != ExecutionMode.UNATTENDED and args.verbose),
            started_at=datetime.now()
        )

        if execution_mode == ExecutionMode.UNATTENDED:
            result = execute_run_with_config(run_config, db)
            print("completed" if result.status == RunStatus.DONE else "failed")
            sys.exit(0 if result.status == RunStatus.DONE else 1)
        
        # Default mode execution
        if args.verbose: print_configuration_summary(run_config)
        else:
            run_type_msg = f"{approach_enum.value} ({method_enum.value})"
            target_msg = f"for {fab_code}, target: {run_config.coverage_target:.1%}" if approach_enum == Approach.RANDOM else f"with scenario {run_config.scenario_code or run_config.scenario_file}"
            print(f"Starting {run_type_msg} analysis {target_msg}...")
        
        result = execute_run_with_config(run_config, db)
        print_run_summary(result, verbose=args.verbose)
        
        if result.status == RunStatus.DONE:
            if not args.verbose: print(f"✅ Analysis completed successfully! Run ID: {result.run_id}")
            sys.exit(0)
        else:
            if not args.verbose: print(f"❌ Analysis failed. Run ID: {result.run_id}")
            sys.exit(1)

    except ValueError as ve:
        print(f"Configuration Error: {ve}", file=sys.stderr)
        if args.verbose: import traceback; traceback.print_exc()
        sys.exit(2)
    except Exception as e:
        print(f"An unexpected error occurred: {e}", file=sys.stderr)
        if args.verbose: import traceback; traceback.print_exc()
        sys.exit(1)
    finally:
        db.close()


def interactive_mode():
    """Run the application in interactive mode."""
    print("=" * 60 + "\nPATH ANALYSIS CLI TOOL - INTERACTIVE MODE\n" + "=" * 60)
    print("Welcome! This tool will guide you through setting up a path analysis run.")
    
    db = Database()
    try:
        # 1. Approach
        approach_str = fetch_user_choice("Select analysis approach:", [e.value for e in Approach], 0)
        approach_enum = Approach(approach_str)

        # 2. Method (depends on approach)
        if approach_enum == Approach.RANDOM:
            method_choices = [Method.SIMPLE.value, Method.STRATIFIED.value]
            method_help = "SIMPLE: Basic random sampling.\nSTRATIFIED: Stratified random sampling (more advanced control)."
        else: # SCENARIO
            method_choices = [Method.PREDEFINED.value, Method.SYNTHETIC.value]
            method_help = "PREDEFINED: Use existing scenarios from DB.\nSYNTHETIC: Generate synthetic scenarios (if supported)."
        print(f"\n{method_help}")
        method_str = fetch_user_choice(f"Select method for {approach_str} approach:", method_choices, 0)
        method_enum = Method(method_str)

        # 3. Common / Approach-specific params
        fab_code, toolset_str, phase_str_input, scenario_code_str, scenario_file_str = "", "", "", "", ""
        coverage_target_val = 0.2 # Default

        if approach_enum == Approach.RANDOM:
            coverage_target_val = fetch_float_input("\nEnter coverage target (e.g., 0.2 for 20%)", default=0.2, min_val=0.01, max_val=1.0)
            
            available_fabs = fetch_available_fabs(db)
            fab_code = fetch_string_input("\nEnter Fab identifier (e.g. M16)", required=True, available_options=available_fabs, default=available_fabs[0] if available_fabs else "M16")
            
            available_phases = fetch_available_phases(db, fab_code) # Human-readable
            phase_str_input = fetch_string_input("\nEnter Phase (e.g. PHASE1, A, 1 - optional)", available_options=available_phases, default="", required=False)
            
            available_toolsets = fetch_available_toolsets(db, fab_code)
            toolset_str = fetch_string_input("\nEnter Toolset ID (e.g. TS001, or ALL - optional)", available_options=available_toolsets, default="ALL", required=False)
        
        else: # SCENARIO
            print("\nScenario Configuration:")
            scenario_choice = fetch_user_choice("Specify scenario by:", ["Code (from DB)", "File Path"], 0)
            if scenario_choice == "Code (from DB)":
                available_scenarios = fetch_available_scenarios(db)
                scenario_code_str = fetch_string_input("\nEnter Scenario Code (e.g. PRE001)", required=True, available_options=available_scenarios, default=available_scenarios[0] if available_scenarios else "PRE001")
                # Auto-adjust method based on scenario code if not explicitly set by advanced user
                if scenario_code_str.upper().startswith("PRE"): method_enum = Method.PREDEFINED
                elif scenario_code_str.upper().startswith("SYN"): method_enum = Method.SYNTHETIC
            else: # File Path
                scenario_file_str = fetch_string_input("\nEnter Scenario File Path (e.g. data/scenarios.json)", required=True, default="data/scenarios.json")
                # For file based, method might be generic or also PREDEFINED/SYNTHETIC if file implies type
                # For now, let's assume Method.PREDEFINED if it's a file of predefined paths.
                # Or, we could add a Method.FILE if that's distinct. Using PREDEFINED for now.
                method_enum = Method.PREDEFINED 

        # Normalize phase if provided
        normalized_phase_str = normalize_phase_input(phase_str_input)

        # 4. Verbose
        verbose_str = fetch_user_choice("\nEnable verbose output?", ["No", "Yes"], 0)
        verbose_bool = verbose_str == "Yes"

        # 5. Config
        run_config = RunConfig.create_with_auto_tag(
            run_id=str(uuid.uuid4()), approach=approach_enum, method=method_enum,
            coverage_target=coverage_target_val, building_code=fab_code,
            toolset=toolset_str, phase=normalized_phase_str,
            scenario_code=scenario_code_str, scenario_file=scenario_file_str,
            execution_mode=ExecutionMode.INTERACTIVE, verbose_mode=verbose_bool,
            started_at=datetime.now()
        )
        
        print_configuration_summary(run_config)
        confirm_str = fetch_user_choice("\nProceed with this configuration?", ["Yes", "No"], 0)
        if confirm_str == "No": print("Operation cancelled."); return

        print(f"\nStarting analysis run... This may take some time.")
        result = execute_run_with_config(run_config, db)
        print_run_summary(result, verbose=verbose_bool)
        
        sys.exit(0 if result.status == RunStatus.DONE else 1)

    except ValueError as ve: print(f"Configuration Error: {ve}", file=sys.stderr); sys.exit(2)
    except Exception as e: print(f"An unexpected error occurred: {e}", file=sys.stderr); sys.exit(1)
    finally: db.close()


def main():
    """Main CLI entry point."""
    parser = create_parser()
    args = parser.parse_args()
    
    try:
        if args.interactive: interactive_mode()
        elif args.unattended: main_flow(args, ExecutionMode.UNATTENDED)
        else: main_flow(args, ExecutionMode.DEFAULT)
    except KeyboardInterrupt: print("\nOperation cancelled by user."); sys.exit(130)

if __name__ == "__main__":
    main()