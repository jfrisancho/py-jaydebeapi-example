"""
Enhanced data models for the path analysis system with equipment support.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional, Dict, Any
from enums import (
    Approach, Method, RunStatus, ObjectType, Severity, ErrorType, Building, Phase,
    ExecutionMode, ScenarioType, SourceType, FlagType, FlagStatus, CompletionStatus
)


@dataclass
class RunConfig:
    """Configuration for a single analysis run."""
    run_id: str
    approach: Approach
    method: Method
    coverage_target: float
    building_code: str
    tag: str
    started_at: datetime
    toolset: str = ""
    phase: str = ""  # Store as A, B, C, D (system nomenclature)
    scenario_code: str = ""
    scenario_file: str = ""
    execution_mode: ExecutionMode = ExecutionMode.DEFAULT
    verbose_mode: bool = False
    
    @classmethod
    def create_with_auto_tag(cls, run_id: str, approach: Approach, method: Method,
                           coverage_target: float, building_code: str = "", toolset: str = "",
                           phase: str = "", scenario_code: str = "", scenario_file: str = "",
                           execution_mode: ExecutionMode = ExecutionMode.DEFAULT,
                           verbose_mode: bool = False, started_at: datetime = None) -> 'RunConfig':
        """Create a RunConfig with automatically generated tag."""
        if started_at is None:
            started_at = datetime.now()
        
        tag = cls.generate_tag(approach, method, coverage_target, building_code, 
                              toolset, phase, scenario_code, started_at)
        
        return cls(
            run_id=run_id,
            approach=approach,
            method=method,
            coverage_target=coverage_target,
            building_code=building_code,
            tag=tag,
            started_at=started_at,
            toolset=toolset,
            phase=phase,
            scenario_code=scenario_code,
            scenario_file=scenario_file,
            execution_mode=execution_mode,
            verbose_mode=verbose_mode
        )
    
    @staticmethod
    def generate_tag(approach: Approach, method: Method, coverage_target: float,
                    building_code: str = "", toolset: str = "", phase: str = "",
                    scenario_code: str = "", date: datetime = None) -> str:
        """Generate tag using the specified format."""
        if date is None:
            date = datetime.now()
        
        if approach == Approach.SCENARIO:
            # For scenarios: YYYYMMDD_SCENARIO_METHOD_SCENARIOCODE
            scenario_tag = scenario_code or "DEFAULT"
            tag = f"{date.strftime('%Y%m%d')}_{approach.value}_{method.value}_{scenario_tag}"
        else:
            # For random: YYYYMMDD_APPROACH_METHOD_COVERAGEP_BUILDING[_PHASE][_TOOLSET]
            coverage_target_tag = f'{coverage_target*100:.0f}P'
            tag = f"{date.strftime('%Y%m%d')}_{approach.value}_{method.value}_{coverage_target_tag}"
            
            if building_code:
                tag += f"_{building_code}"
            
            if phase:
                tag += f"_{phase}"
            
            if toolset and toolset != "ALL":
                tag += f"_{toolset}"
        
        return tag


@dataclass
class Scenario:
    """Scenario definition."""
    id: Optional[int]
    code: str
    name: str
    description: Optional[str] = None
    scenario_type: ScenarioType = ScenarioType.PREDEFINED
    file_path: Optional[str] = None
    expected_coverage: Optional[float] = None
    expected_nodes: Optional[int] = None
    expected_links: Optional[int] = None
    expected_paths: Optional[int] = None
    expected_valid: bool = True
    expected_criticality: Optional[str] = None
    created_by: Optional[str] = None
    is_active: bool = True
    created_at: datetime = field(default_factory=datetime.now)


@dataclass
class Toolset:
    """Simplified toolset definition."""
    code: str                    # Primary key - unique toolset code
    fab: str                     # Building/fab identifier (M15, M16, etc.)
    phase: str                   # Phase as A, B, C, D (system nomenclature)
    name: str                    # Optional name
    description: Optional[str] = None
    is_active: bool = True
    equipment_list: List['Equipment'] = field(default_factory=list)
    
    @property
    def phase_enum(self) -> Optional[Phase]:
        """Get Phase enum for this toolset."""
        return Phase.normalize(self.phase)


@dataclass
class EquipmentPoC:
    """Equipment Point of Contact."""
    id: Optional[int]
    equipment_id: int
    code: str  # POC01, POC02, IN01, OUT01
    node_id: int  # Maps to nw_nodes.id
    utility_code: Optional[str] = None  # N2, CDA, PW, etc.
    flow_direction: Optional[str] = None  # IN, OUT, BIDIRECTIONAL
    is_used: bool = False
    priority: int = 0
    description: Optional[str] = None
    is_active: bool = True


@dataclass
class Equipment:
    """Simplified equipment definition."""
    id: Optional[int]
    toolset_code: str            # Simple FK to toolset code
    name: str
    guid: str
    node_id: int                 # Virtual equipment node
    kind: Optional[str] = None   # PRODUCTION, PROCESSING, SUPPLY, etc.
    description: Optional[str] = None
    is_active: bool = True
    pocs: List[EquipmentPoC] = field(default_factory=list)
    
    def get_available_pocs(self) -> List[EquipmentPoC]:
        """Get active PoCs that can be used for path generation."""
        return [poc for poc in self.pocs if poc.is_active]
    
    def get_used_pocs(self) -> List[EquipmentPoC]:
        """Get PoCs that are currently in use."""
        return [poc for poc in self.pocs if poc.is_active and poc.is_used]
    
    @property
    def fab(self) -> str:
        """Get fab from toolset code (requires database lookup in practice)."""
        # This is a simplified property - in practice would require toolset lookup
        return self.toolset_code.split('_')[0] if '_' in self.toolset_code else ''
    
    @property
    def phase(self) -> str:
        """Get phase from toolset (requires database lookup in practice)."""
        # This is a simplified property - in practice would require toolset lookup
        return ''


@dataclass
class PathDefinition:
    """Definition of a discovered or scenario-based path."""
    id: Optional[int]
    path_hash: str
    source_type: SourceType
    building_code: Optional[str]  # NULL for scenarios
    category: str
    scope: str
    node_count: int
    link_count: int
    total_length_mm: float
    coverage: float
    utilities: List[str]
    path_context: Dict[str, Any]  # Serialized nodes/links sequence
    scenario_id: Optional[int] = None
    scenario_context: Optional[Dict[str, Any]] = None


@dataclass
class AttemptPath:
    """Record of a random sampling attempt."""
    id: Optional[int]
    run_id: str
    path_definition_id: int
    start_node_id: int
    end_node_id: int
    building_code: Optional[str]
    category: Optional[str]
    utility: Optional[str]
    toolset: Optional[str]
    picked_at: datetime
    notes: Optional[str] = None


@dataclass
class ScenarioExecution:
    """Record of a scenario execution."""
    id: Optional[int]
    run_id: str
    scenario_id: int
    path_definition_id: Optional[int]
    execution_status: str  # SUCCESS, FAILED, ERROR
    execution_time_ms: Optional[int]
    actual_nodes: Optional[int]
    actual_links: Optional[int]
    actual_coverage: Optional[float]
    validation_passed: Optional[bool]
    validation_errors: Optional[List[str]] = None
    executed_at: datetime = field(default_factory=datetime.now)
    notes: Optional[str] = None


@dataclass
class CriticalError:
    """Critical error found during validation."""
    id: Optional[int]
    error_type: str  # POC_NO_UTILITY, MISSING_NODE, MISSING_EQUIPMENT_NODE
    building_code: str
    toolset_code: str
    phase: str
    equipment_name: Optional[str] = None
    poc_code: Optional[str] = None
    node_id: Optional[int] = None
    error_reason: str = ""
    detected_at: datetime = field(default_factory=datetime.now)


@dataclass
class ValidationError:
    """Node/link-level diagnostic information."""
    id: Optional[int]
    run_id: str
    path_definition_id: Optional[int]
    validation_test_id: Optional[int]
    severity: Severity
    error_scope: str
    error_type: ErrorType
    object_type: ObjectType
    node_id: Optional[int] = None
    link_id: Optional[int] = None
    scenario_id: Optional[int] = None
    building_code: Optional[str] = None
    category: Optional[str] = None
    utility: Optional[str] = None
    material: Optional[str] = None
    flow: Optional[str] = None
    item_name: Optional[str] = None
    error_message: Optional[str] = None
    error_data: Optional[Dict[str, Any]] = None
    created_at: datetime = field(default_factory=datetime.now)
    notes: Optional[str] = None


@dataclass
class ReviewFlag:
    """Manual or critical item flagged for review."""
    id: Optional[int]
    run_id: str
    flag_type: FlagType
    severity: Severity
    reason: str
    object_type: ObjectType
    start_node_id: Optional[int] = None
    end_node_id: Optional[int] = None
    link_id: Optional[int] = None
    path_definition_id: Optional[int] = None
    scenario_id: Optional[int] = None
    building_code: Optional[str] = None
    utility: Optional[str] = None
    material: Optional[str] = None
    flow: Optional[str] = None
    path_context: Optional[Dict[str, Any]] = None
    flag_data: Optional[Dict[str, Any]] = None
    status: FlagStatus = FlagStatus.OPEN
    assigned_to: Optional[str] = None
    resolved_at: Optional[datetime] = None
    resolution_notes: Optional[str] = None
    created_at: datetime = field(default_factory=datetime.now)
    notes: Optional[str] = None


@dataclass
class PathResult:
    """Result of a path discovery attempt."""
    path_found: bool
    path_definition: Optional[PathDefinition] = None
    coverage_contribution: float = 0.0
    errors: List[ValidationError] = field(default_factory=list)
    review_flags: List[ReviewFlag] = field(default_factory=list)
    critical_errors: List[CriticalError] = field(default_factory=list)


@dataclass
class RunResult:
    """Complete result of an analysis run."""
    run_id: str
    approach: Approach
    method: Method
    coverage_target: float
    total_coverage: float
    total_nodes: int
    total_links: int
    building_code: str
    tag: str
    status: RunStatus
    started_at: datetime
    ended_at: Optional[datetime]
    duration: float  # seconds
    execution_mode: ExecutionMode = ExecutionMode.DEFAULT
    verbose_mode: bool = False
    scenario_code: str = ""
    scenario_file: str = ""
    paths_attempted: int = 0
    paths_found: int = 0
    scenario_tests: int = 0
    errors: List[str] = field(default_factory=list)
    review_flags: List[str] = field(default_factory=list)
    critical_errors: List[str] = field(default_factory=list)


@dataclass
class RunSummary:
    """Aggregated run metrics."""
    run_id: str
    total_attempts: int
    total_paths_found: int
    unique_paths: int
    total_scenario_tests: int = 0
    scenario_success_rate: Optional[float] = None
    total_errors: int = 0
    total_review_flags: int = 0
    critical_errors: int = 0
    target_coverage: Optional[float] = None
    achieved_coverage: Optional[float] = None
    coverage_efficiency: Optional[float] = None
    total_nodes: int = 0
    total_links: int = 0
    avg_path_nodes: Optional[float] = None
    avg_path_links: Optional[float] = None
    avg_path_length: Optional[float] = None
    success_rate: Optional[float] = None
    completion_status: CompletionStatus = CompletionStatus.COMPLETED
    execution_time_seconds: Optional[float] = None
    started_at: datetime = field(default_factory=datetime.now)
    ended_at: Optional[datetime] = None
    summarized_at: datetime = field(default_factory=datetime.now)


@dataclass
class CoverageStats:
    """Coverage tracking statistics."""
    nodes_covered: int
    links_covered: int
    total_nodes: int
    total_links: int
    coverage_percentage: float
    
    @property
    def node_coverage(self) -> float:
        """Calculate node coverage percentage."""
        return self.nodes_covered / self.total_nodes if self.total_nodes > 0 else 0.0
    
    @property
    def link_coverage(self) -> float:
        """Calculate link coverage percentage."""
        return self.links_covered / self.total_links if self.total_links > 0 else 0.0


@dataclass
class BiasReduction:
    """Configuration for bias reduction in random sampling."""
    max_attempts_per_toolset: int = 5
    max_attempts_per_equipment: int = 3
    min_distance_between_nodes: int = 10
    utility_diversity_weight: float = 0.3
    category_diversity_weight: float = 0.2
    phase_diversity_weight: float = 0.2


@dataclass
class EquipmentSelectionResult:
    """Result of equipment selection for path generation."""
    equipment: Equipment
    start_poc: EquipmentPoC
    end_poc: EquipmentPoC
    selection_metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class DataSyncResult:
    """Result of equipment data synchronization."""
    sync_type: str
    table_name: str
    records_processed: int = 0
    records_inserted: int = 0
    records_updated: int = 0
    errors_found: int = 0
    critical_errors: List[CriticalError] = field(default_factory=list)
    started_at: datetime = field(default_factory=datetime.now)
    completed_at: Optional[datetime] = None
    status: str = "RUNNING"  # RUNNING, COMPLETED, FAILED
    notes: Optional[str] = None


@dataclass
class SystemConfig:
    """System configuration setting."""
    id: Optional[int]
    config_key: str
    config_value: str
    config_type: str  # STRING, INTEGER, FLOAT, BOOLEAN, JSON
    description: Optional[str] = None
    category: Optional[str] = None
    is_user_configurable: bool = True
    requires_restart: bool = False
    created_at: datetime = field(default_factory=datetime.now)
    updated_at: datetime = field(default_factory=datetime.now)
