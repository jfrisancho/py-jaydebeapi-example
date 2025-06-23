"""
Simplified main.py updates for fab-based system.
"""

# Key changes to main.py:

def fetch_available_fabs(db: Database) -> List[str]:
    """Get available fabs from database."""
    try:
        # Simple query - just get distinct fabs from toolsets
        sql = "SELECT DISTINCT fab FROM tb_toolsets WHERE is_active = TRUE ORDER BY fab"
        results = db.query(sql)
        fabs = [row[0] for row in results] if results else []
        
        # Add defaults if none found
        if not fabs:
            fabs = ["M16", "M15"]  # Only codes we actually use
        
        return fabs
    except Exception:
        return ["M16", "M15"]  # Default fallback


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
            options.extend(["TOOLSET_001", "TOOLSET_002"])
        
        return options
    except Exception:
        return ["ALL", "TOOLSET_001", "TOOLSET_002"]


def execute_run_with_config(config: RunConfig, verbose: bool = False) -> RunResult:
    """Execute a run with the given configuration."""
    # Initialize database and services
    db = Database()
    try:
        # NEW: Auto-populate on first run
        from services.simple_random_service import SimplePopulationService
        population_service = SimplePopulationService(db)
        population_service.populate_on_first_run(config.fab)
        
        # Use simplified services
        run_service = RunService(db)
        path_service = PathService(db)
        coverage_service = CoverageService(db)
        
        result = run_service.execute_run(config, path_service, coverage_service, verbose=verbose)
        return result
        
    finally:
        db.close()


# Update argument parser
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
    python main.py -a RANDOM --fab M16 --toolset "TOOLSET_001"
    python main.py -a RANDOM --method STRATIFIED --coverage-target 0.25

  Scenario approach (predefined paths by code or file):
    python main.py -a SCENARIO --scenario-code "PRE001"
    python main.py -a SCENARIO --scenario-file "scenarios.json"

  Silent unattended mode (for scripts/automation):
    python main.py --fab M16 --unattended
    python main.py -a SCENARIO --scenario-code "PRE001" --unattended
        """
    )
    
    # Change --building to --fab
    parser.add_argument(
        '--fab',
        type=str,
        choices=['M15', 'M16'],  # Only the codes we actually use
        help='Fab identifier for RANDOM approach (M16, M15) - ignored for SCENARIO approach'
    )
    
    # Keep other arguments the same...
    
    return parser


# Update interactive mode fab selection
def interactive_mode():
    """Run the application in interactive mode."""
    print("=" * 60)
    print("PATH ANALYSIS CLI TOOL")
    print("=" * 60)
    print("Welcome! This tool will guide you through setting up a path analysis run.")
    
    # Initialize database connection for options lookup
    db = Database()
    
    try:
        # ... approach and method selection same as before ...
        
        if approach == Approach.RANDOM:
            # RANDOM approach: get fab and toolset
            available_fabs = fetch_available_fabs(db)
            fab = fetch_string_input(
                "\nEnter fab identifier",
                required=True,
                available_options=available_fabs
            )
            
            available_toolsets = fetch_available_toolsets(db, fab)
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
            
            # No phase selection needed for simplicity
            phase = ""
            
        elif approach == Approach.SCENARIO:
            # ... scenario configuration same as before ...
            pass
        
        # Generate configuration with fab instead of building_code
        config = RunConfig.create_with_auto_tag(
            run_id=str(uuid.uuid4()),
            approach=approach,
            method=method,
            coverage_target=coverage_target,
            fab=fab,  # Changed from building_code
            toolset=toolset,
            phase=phase,
            scenario_code=scenario_code,
            scenario_file=scenario_file,
            execution_mode=ExecutionMode.INTERACTIVE,
            verbose_mode=False,
            started_at=datetime.now()
        )
        
        # Rest of interactive mode same...
        
    finally:
        db.close()


# Update models.py to use fab
@dataclass
class RunConfig:
    """Configuration for a single analysis run."""
    run_id: str
    approach: Approach
    method: Method
    coverage_target: float
    fab: str  # Changed from building_code
    tag: str
    started_at: datetime
    toolset: str = ""
    phase: str = ""
    scenario_code: str = ""
    scenario_file: str = ""
    execution_mode: ExecutionMode = ExecutionMode.DEFAULT
    verbose_mode: bool = False
    
    @staticmethod
    def generate_tag(approach: Approach, method: Method, coverage_target: float,
                    fab: str = "", toolset: str = "", phase: str = "",
                    scenario_code: str = "", date: datetime = None) -> str:
        """Generate tag using the specified format."""
        if date is None:
            date = datetime.now()
        
        if approach == Approach.SCENARIO:
            scenario_tag = scenario_code or "DEFAULT"
            tag = f"{date.strftime('%Y%m%d')}_{approach.value}_{method.value}_{scenario_tag}"
        else:
            coverage_target_tag = f'{coverage_target*100:.0f}P'
            tag = f"{date.strftime('%Y%m%d')}_{approach.value}_{method.value}_{coverage_target_tag}"
            
            if fab:
                tag += f"_{fab}"
            
            if phase:
                tag += f"_{phase}"
            
            if toolset and toolset != "ALL":
                tag += f"_{toolset}"
        
        return tag


# Update database schema creation to remove unnecessary fields
CREATE_TABLES_SQL = """
-- Minimal equipment tables
CREATE TABLE IF NOT EXISTS tb_toolsets (
    code VARCHAR(64) PRIMARY KEY,
    fab VARCHAR(10) NOT NULL,
    phase VARCHAR(8) NOT NULL,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_fab_toolset_phase (fab, code, phase)
);

CREATE TABLE IF NOT EXISTS tb_equipment (
    id INTEGER AUTO_INCREMENT PRIMARY KEY,
    toolset_code VARCHAR(64) NOT NULL,
    fab VARCHAR(10) NOT NULL,
    phase VARCHAR(8) NOT NULL,
    name VARCHAR(128) NOT NULL,
    guid VARCHAR(64) NOT NULL,
    node_id INTEGER NOT NULL,
    kind VARCHAR(32),
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_equipment_toolset (fab, toolset_code, phase),
    UNIQUE KEY uk_equipment_guid (guid)
);

CREATE TABLE IF NOT EXISTS tb_equipment_pocs (
    id INTEGER AUTO_INCREMENT PRIMARY KEY,
    equipment_id INTEGER NOT NULL,
    code VARCHAR(8) NOT NULL,
    node_id INTEGER NOT NULL,
    utility_code VARCHAR(32),
    flow_direction VARCHAR(8),
    is_used BOOLEAN DEFAULT FALSE,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (equipment_id) REFERENCES tb_equipment(id) ON DELETE CASCADE,
    INDEX idx_poc_equipment (equipment_id),
    INDEX idx_poc_node (node_id),
    UNIQUE KEY uk_equipment_poc_code (equipment_id, code),
    UNIQUE KEY uk_poc_node (node_id)
);

-- Update existing tables to use fab instead of building_code
ALTER TABLE tb_runs CHANGE COLUMN building_code fab VARCHAR(10);
ALTER TABLE tb_path_definitions CHANGE COLUMN building_code fab VARCHAR(10);
ALTER TABLE tb_attempt_paths CHANGE COLUMN building_code fab VARCHAR(10);
ALTER TABLE tb_validation_errors CHANGE COLUMN building_code fab VARCHAR(10);
ALTER TABLE tb_review_flags CHANGE COLUMN building_code fab VARCHAR(10);
"""

# Update run service to use simplified random service
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
                fab=config.fab,  # Changed from building_code
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
        """Execute a random sampling run using simplified service."""
        if verbose:
            print(f"Executing {config.method.value} random sampling...")
        
        # Use simplified random service
        from services.simple_random_service import SimpleRandomService
        random_service = SimpleRandomService(self.db, config.fab)
        
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
            fab=config.fab,  # Changed from building_code
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
                result.critical_errors.extend([str(e) for e in path_result.critical_errors])
        
        if attempts >= max_attempts:
            result.errors.append(f"Maximum attempts ({max_attempts}) reached")
        
        if verbose:
            print(f"Random sampling complete: {result.paths_found}/{result.paths_attempted} paths found")
        
        return result


# Simplified validation service updates
class ValidationService:
    """Simplified validation service."""
    
    def __init__(self, db: Database):
        self.db = db
    
    def validate_path(self, run_id: str, path_definition: PathDefinition) -> List[ValidationError]:
        """Perform basic validation on a path."""
        errors = []
        
        # Extract path data
        nodes = path_definition.path_context.get('nodes', [])
        links = path_definition.path_context.get('links', [])
        
        # Basic validation - check nodes exist
        for node_id in nodes:
            if not self._node_exists(node_id, path_definition.fab):
                errors.append(ValidationError(
                    id=None,
                    run_id=run_id,
                    path_definition_id=path_definition.id,
                    validation_test_id=None,
                    severity=Severity.ERROR,
                    error_scope="CONNECTIVITY",
                    error_type=ErrorType.MISSING_NODE,
                    object_type=ObjectType.NODE,
                    node_id=node_id,
                    fab=path_definition.fab,  # Changed from building_code
                    category=path_definition.category,
                    notes=f"Node {node_id} does not exist in fab {path_definition.fab}"
                ))
        
        # Basic validation - check links exist
        for link_id in links:
            if not self._link_exists(link_id, path_definition.fab):
                errors.append(ValidationError(
                    id=None,
                    run_id=run_id,
                    path_definition_id=path_definition.id,
                    validation_test_id=None,
                    severity=Severity.ERROR,
                    error_scope="CONNECTIVITY",
                    error_type=ErrorType.MISSING_LINK,
                    object_type=ObjectType.LINK,
                    link_id=link_id,
                    fab=path_definition.fab,  # Changed from building_code
                    category=path_definition.category,
                    notes=f"Link {link_id} does not exist in fab {path_definition.fab}"
                ))
        
        return errors
    
    def _node_exists(self, node_id: int, fab: str) -> bool:
        """Check if a node exists in the database."""
        sql = "SELECT 1 FROM nw_nodes WHERE id = ? AND fab = ? LIMIT 1"
        try:
            result = self.db.query(sql, [node_id, fab])
            return len(result) > 0
        except Exception:
            return False
    
    def _link_exists(self, link_id: int, fab: str) -> bool:
        """Check if a link exists in the database."""
        sql = "SELECT 1 FROM nw_links WHERE id = ? AND fab = ? LIMIT 1"
        try:
            result = self.db.query(sql, [link_id, fab])
            return len(result) > 0
        except Exception:
            return False


# Update coverage service to use fab
class CoverageService:
    """Simplified coverage tracking service."""
    
    def __init__(self, db):
        self.db = db
        self._covered_nodes: Set[int] = set()
        self._covered_links: Set[int] = set()
        self._total_nodes = 0
        self._total_links = 0
    
    def initialize_coverage(self, fab: str) -> CoverageStats:
        """Initialize coverage tracking for a specific fab."""
        self._covered_nodes.clear()
        self._covered_links.clear()
        
        # Get total node and link counts for the fab
        self._total_nodes, self._total_links = self._get_fab_totals(fab)
        
        return CoverageStats(
            nodes_covered=0,
            links_covered=0,
            total_nodes=self._total_nodes,
            total_links=self._total_links,
            coverage_percentage=0.0
        )
    
    def _get_fab_totals(self, fab: str) -> tuple:
        """Get total node and link counts for a fab."""
        node_sql = "SELECT COUNT(*) FROM nw_nodes WHERE fab = ?"
        link_sql = "SELECT COUNT(*) FROM nw_links WHERE fab = ?"
        
        try:
            node_result = self.db.query(node_sql, [fab])
            link_result = self.db.query(link_sql, [fab])
            
            total_nodes = node_result[0][0] if node_result else 0
            total_links = link_result[0][0] if link_result else 0
            
            return total_nodes, total_links
            
        except Exception as e:
            print(f"Error getting fab totals: {e}")
            return 0, 0
    
    # ... rest of coverage service methods same but using fab instead of building_code ...


# Update path service to use fab
class PathService:
    """Simplified path service."""
    
    def __init__(self, db: Database):
        self.db = db
    
    def store_path_attempt(self, run_id: str, path_result: PathResult) -> Optional[int]:
        """Store a path attempt in the database."""
        if not path_result.path_found or not path_result.path_definition:
            return None
        
        path_def = path_result.path_definition
        
        # Store path definition
        path_def_id = self._store_path_definition(path_def)
        if not path_def_id:
            return None
        
        # Store attempt path for random paths
        if path_def.source_type == SourceType.RANDOM:
            attempt_id = self._store_attempt_path(run_id, path_def_id, path_def)
            return attempt_id
        
        return path_def_id
    
    def _store_path_definition(self, path_def: PathDefinition) -> Optional[int]:
        """Store path definition."""
        # Check if already exists
        existing_id = self._get_existing_path_definition(path_def.path_hash)
        if existing_id:
            return existing_id
        
        sql = """
        INSERT INTO tb_path_definitions (
            path_hash, source_type, fab, category, scope, node_count, link_count,
            total_length_mm, coverage, utilities, path_context
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        try:
            import json
            utilities_json = json.dumps(path_def.utilities)
            path_context_json = json.dumps(path_def.path_context)
            
            rows_affected = self.db.update(sql, [
                path_def.path_hash,
                path_def.source_type.value,
                path_def.fab,  # Changed from building_code
                path_def.category,
                path_def.scope,
                path_def.node_count,
                path_def.link_count,
                path_def.total_length_mm,
                path_def.coverage,
                utilities_json,
                path_context_json
            ])
            
            if rows_affected > 0:
                id_result = self.db.query("SELECT LAST_INSERT_ID()")
                return id_result[0][0] if id_result else None
            
            return None
            
        except Exception as e:
            print(f"Error storing path definition: {e}")
            return None
    
    def _get_existing_path_definition(self, path_hash: str) -> Optional[int]:
        """Check if path definition exists."""
        sql = "SELECT id FROM tb_path_definitions WHERE path_hash = ?"
        try:
            result = self.db.query(sql, [path_hash])
            return result[0][0] if result else None
        except Exception:
            return None
    
    def _store_attempt_path(self, run_id: str, path_def_id: int, path_def: PathDefinition) -> Optional[int]:
        """Store attempt path record."""
        sql = """
        INSERT INTO tb_attempt_paths (
            run_id, path_definition_id, start_node_id, end_node_id,
            fab, category, utility, toolset, picked_at, notes
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        try:
            start_node = path_def.path_context.get('start_node_id')
            end_node = path_def.path_context.get('end_node_id')
            utility = path_def.utilities[0] if path_def.utilities else 'UNKNOWN'
            toolset = path_def.path_context.get('toolset_code', '')
            
            rows_affected = self.db.update(sql, [
                run_id, path_def_id, start_node, end_node,
                path_def.fab,  # Changed from building_code
                path_def.category, utility, toolset,
                datetime.now(),
                f"Path found with {path_def.node_count} nodes, {path_def.link_count} links"
            ])
            
            if rows_affected > 0:
                id_result = self.db.query("SELECT LAST_INSERT_ID()")
                return id_result[0][0] if id_result else None
            
            return None
            
        except Exception as e:
            print(f"Error storing attempt path: {e}")
            return None