"""
Example usage of the intelligent coverage strategy services.
Demonstrates how to handle the scenario where a specific toolset only covers 1% of the factory.
"""

import sqlite3
import uuid
from datetime import datetime
from random_service import RandomService, RandomGenerationConfig, ModelType, Phase
from coverage_service import CoverageService
from path_service import PathService
from validation_service import ValidationService

def example_scenario_small_toolset():
    """
    Example: 500,000 toolsets, one specific toolset covers only 1% of factory,
    but we want 20% coverage.
    """
    
    # Setup database connection (replace with your actual DB)
    db = sqlite3.connect(':memory:')
    db.row_factory = sqlite3.Row
    
    # Initialize services
    random_service = RandomService(db)
    coverage_service = CoverageService(db)
    path_service = PathService(db)
    validation_service = ValidationService(db)
    
    # Create a run ID
    run_id = str(uuid.uuid4())
    
    print("=== Scenario: Small Toolset Coverage Challenge ===")
    print(f"Run ID: {run_id}")
    print(f"Challenge: Toolset XXXXXXXX covers only 1% of factory")
    print(f"Requested: 20% coverage target")
    print()
    
    # Configuration for the problematic scenario
    config = RandomGenerationConfig(
        coverage_target=0.20,  # 20% target
        toolset="XXXXXXXX",    # Specific toolset that only covers 1%
        fab="M15",             # Optional: specific building
        model=ModelType.BIM,   # Optional: specific model
        phase=Phase.P1,        # Optional: specific phase
        allow_scope_expansion=True,  # Enable intelligent expansion
        coverage_strategy="adaptive"  # Let system choose best strategy
    )
    
    print("Original Configuration:")
    print(f"  Target Coverage: {config.coverage_target:.1%}")
    print(f"  Toolset: {config.toolset}")
    print(f"  Fab: {config.fab}")
    print(f"  Model: {config.model}")
    print(f"  Phase: {config.phase}")
    print(f"  Allow Expansion: {config.allow_scope_expansion}")
    print()
    
    # Generate coverage strategy report before execution
    print("=== Coverage Strategy Analysis ===")
    strategy_report = random_service.generate_coverage_strategy_report(config)
    
    print("Original Request:")
    print(f"  Target: {strategy_report['original_request']['coverage_target']:.1%}")
    print(f"  Scope: {strategy_report['original_request']['scope']}")
    print(f"  Potential: {strategy_report['original_request']['potential']:.1%}")
    print()
    
    print("Strategy Applied:")
    print(f"  Name: {strategy_report['strategy_applied']['name']}")
    print(f"  Adjusted Target: {strategy_report['strategy_applied']['adjusted_target']:.1%}")
    print(f"  Scope: {strategy_report['strategy_applied']['scope']}")
    print(f"  Potential: {strategy_report['strategy_applied']['potential']:.1%}")
    print()
    
    print("Improvements:")
    print(f"  Potential Increase: {strategy_report['improvements']['potential_increase']:.1%}")
    print(f"  Target Achievable: {strategy_report['improvements']['target_achievable']}")
    print(f"  Scope Expanded: {strategy_report['improvements']['scope_expansion']}")
    print()
    
    print("Recommendations:")
    for rec in strategy_report['recommendations']:
        print(f"  • {rec}")
    print()
    
    # Generate random paths with intelligent strategy
    print("=== Path Generation with Intelligent Strategy ===")
    try:
        results = random_service.generate_random_paths(config)
        
        print("Generation Results:")
        print(f"  Pairs Generated: {results['pairs_generated']}")
        print(f"  Attempts Made: {results['attempts_made']}")
        print(f"  Coverage Achieved: {results['coverage_achieved']:.1%}")
        print(f"  Original Target: {results['original_target_coverage']:.1%}")
        print(f"  Adjusted Target: {results['target_coverage']:.1%}")
        print(f"  Strategy Used: {results['coverage_strategy']}")
        print(f"  Scope Expanded: {results['scope_expanded']}")
        print(f"  Success: {results['success']}")
        
        if results['expanded_toolsets']:
            print(f"  Expanded Toolsets: {len(results['expanded_toolsets'])}")
            print(f"    {', '.join(results['expanded_toolsets'][:5])}...")
        
        print()
        
        # Coverage Analysis
        print("=== Coverage Analysis ===")
        coverage_analysis = results['coverage_analysis']
        print(f"  Total Factory POCs: {coverage_analysis['total_factory_pocs']:,}")
        print(f"  Scope POCs: {coverage_analysis['scope_pocs']:,}")
        print(f"  Scope Percentage: {coverage_analysis['scope_percentage']:.1%}")
        print(f"  Adjustment Reason: {coverage_analysis['adjustment_reason']}")
        print()
        
        # Initialize coverage tracking
        config_dict = {
            'fab': config.fab,
            'model_no': 1 if config.model == ModelType.BIM else 2,
            'phase_no': 1 if config.phase == Phase.P1 else 2,
            'expanded_toolsets': config.expanded_toolsets,
            'toolset': config.toolset if not config.expanded_toolsets else None,
            'coverage_strategy': config.coverage_strategy
        }
        
        coverage_init = coverage_service.initialize_coverage_tracking(run_id, config_dict)
        print(f"Coverage Tracking Initialized: {coverage_init['total_pocs']} POCs, {coverage_init['total_equipment']} equipment")
        
        # Generate coverage report
        coverage_report = coverage_service.generate_coverage_report(run_id, config_dict)
        
        print("\n=== Final Coverage Report ===")
        print("Overall Metrics:")
        metrics = coverage_report['overall_metrics']
        print(f"  POC Coverage: {metrics.poc_coverage_percentage:.1%}")
        print(f"  Equipment Coverage: {metrics.equipment_coverage_percentage:.1%}")
        print(f"  Connection Success Rate: {metrics.connection_success_rate:.1%}")
        
        print("\nInsights:")
        for insight in coverage_report['insights']:
            print(f"  • {insight}")
            
        print("\nRecommendations:")
        for rec in coverage_report['recommendations']:
            print(f"  • {rec}")
            
    except Exception as e:
        print(f"Error during generation: {e}")
        print("This is expected in a demo without actual database data")
    
    print("\n=== Alternative Strategies Demo ===")
    
    # Strategy 1: No expansion allowed
    print("Strategy 1: No Scope Expansion")
    config_no_expansion = RandomGenerationConfig(
        coverage_target=0.20,
        toolset="XXXXXXXX",
        allow_scope_expansion=False
    )
    
    try:
        strategy_report_no_exp = random_service.generate_coverage_strategy_report(config_no_expansion)
        print(f"  Adjusted Target: {strategy_report_no_exp['strategy_applied']['adjusted_target']:.1%}")
        print(f"  Strategy: {strategy_report_no_exp['strategy_applied']['name']}")
    except:
        print("  Would adjust target to realistic level within toolset")
    
    # Strategy 2: Intensive sampling
    print("\nStrategy 2: Intensive Sampling of Critical Toolset")
    config_intensive = RandomGenerationConfig(
        coverage_target=0.20,
        toolset="CRITICAL_TOOLSET",  # Assume this is critical
        allow_scope_expansion=True,
        coverage_strategy="intensive"
    )
    
    print("  Would sample 80% of the critical toolset intensively")
    print("  Suitable for high-importance, small-scope analysis")
    
    # Strategy 3: Grouped expansion
    print("\nStrategy 3: Grouped Toolset Expansion")
    config_grouped = RandomGenerationConfig(
        coverage_target=0.20,
        toolset="XXXXXXXX",
        allow_scope_expansion=True,
        coverage_strategy="grouped"
    )
    
    print("  Would find and group related toolsets")
    print("  Expands scope while maintaining process similarity")
    
    print("\n=== Summary ===")
    print("The intelligent coverage strategy addresses the '1% toolset, 20% target' problem by:")
    print("