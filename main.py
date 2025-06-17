#!/usr/bin/env python3
"""
Main CLI application entry point for path analysis system.
"""

import argparse
import sys
import uuid
from datetime import datetime
from typing import Optional

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
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument(
        '--approach',
        type=str,
        choices=['RANDOM', 'SCENARIO'],
        default='RANDOM',
        help='Analysis approach (default: RANDOM)'
    )
    
    parser.add_argument(
        '--method',
        type=str,
        help='Analysis method (SIMPLE/STRATIFIED for RANDOM, PREDEFINED/SYNTHETIC for SCENARIO)'
    )
    
    parser.add_argument(
        '--coverage-target',
        type=float,
        default=0.2,
        help='Coverage target as decimal (default: 0.2 for 20%%)'
    )
    
    parser.add_argument(
        '--fab',
        type=str,
        required=True,
        help='Fabrication identifier (e.g., M16, M15)'
    )
    
    parser.add_argument(
        '--tag',
        type=str,
        default='default',
        help='Run tag for identification (default: default)'
    )
    
    parser.add_argument(
        '--verbose',
        '-v',
        action='store_true',
        help='Enable verbose output'
    )
    
    return parser


def validate_method(approach: Approach, method: Optional[str]) -> Method:
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


def main():
    """Main CLI entry point."""
    parser = create_parser()
    args = parser.parse_args()
    
    try:
        # Validate inputs
        approach = Approach(args.approach)
        method = validate_method(approach, args.method)
        
        if not (0.0 < args.coverage_target <= 1.0):
            raise ValueError("Coverage target must be between 0.0 and 1.0")
        
        # Create run configuration
        config = RunConfig(
            run_id=str(uuid.uuid4()),
            approach=approach,
            method=method,
            coverage_target=args.coverage_target,
            fab=args.fab,
            tag=args.tag,
            started_at=datetime.now()
        )
        
        if args.verbose:
            print(f"Starting run with configuration:")
            print(f"  ID: {config.run_id}")
            print(f"  Approach: {config.approach.value}")
            print(f"  Method: {config.method.value}")
            print(f"  Coverage Target: {config.coverage_target:.1%}")
            print(f"  Fab: {config.fab}")
            print(f"  Tag: {config.tag}")
        
        # Initialize database and services
        db = Database()
        try:
            run_service = RunService(db)
            path_service = PathService(db)
            coverage_service = CoverageService(db)
            
            # Execute the run
            result = run_service.execute_run(config, path_service, coverage_service, verbose=args.verbose)
            
            # Print results
            print_run_summary(result, verbose=args.verbose)
            
            # Exit with appropriate code
            if result.status == RunStatus.DONE:
                sys.exit(0)
            else:
                sys.exit(1)
                
        finally:
            db.close()
            
    except KeyboardInterrupt:
        print("\nOperation cancelled by user.")
        sys.exit(130)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        if args.verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()