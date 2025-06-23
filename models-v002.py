"""
Enhanced data models for the path analysis system with equipment support.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional, Dict, Any
from enums import Approach, Method, RunStatus, ObjectType, Severity, ErrorType


@dataclass
class RunConfig:
    """Configuration for a single analysis run."""
    run_id: str
    approach: Approach
    method: Method
    coverage_target: float
    fab: str
    tag: str
    started_at: datetime
    toolset: str = ""
    phase: str = ""  # PHASE1, PHASE2
    
    @classmethod
    def create_with_auto_tag(cls, run_id: str, approach: Approach, method: Method,
                           coverage_target: float, fab: str = "", toolset: str = "",
                           phase: str = "", started_at: datetime = None) -> 'RunConfig':
        """Create a RunConfig with automatically generated tag."""
        if started_at is None:
            started_at = datetime.now()
        
        tag = cls.generate_tag(approach, method, coverage_target, fab, toolset, phase, started_at)
        
        return cls(
            run_id=run_id,
            approach=approach,
            method=method,
            coverage_target=coverage_target,
            fab=fab,
            tag=tag,
            started_at=started_at,
            toolset=toolset,
            phase=phase
        )
    
    @staticmethod
    def generate_tag(approach: Approach, method: Method, coverage_target: float,
                    fab: str = "", toolset: str = "", phase: str = "", date: datetime = None) -> str:
        """Generate tag using the specified format."""
        if date is None:
            date = datetime.now()
        
        coverage_target_tag = f'{coverage_target*100:.0f}P'
        
        # Base tag: YYYYMMDD_APPROACH_METHOD_COVERAGEP
        tag = f"{date.strftime('%Y%m%d')}_{approach.value}_{method.value}_{coverage_target_tag}"
        
        # Add fab if not empty
        if fab:
            tag += f"_{fab}"
        
        # Add phase if not empty
        if phase:
            tag += f"_{phase}"
        
        # Add toolset if not empty and not "ALL"
        if toolset and toolset != "ALL":
            tag += f"_{toolset}"
        
        return tag


@dataclass
class Toolset:
    """Toolset definition with equipment."""
    code: str
    fab: str
    phase: str
    name: str
    description: Optional[str] = None
    is_active: bool = True
    equipment_list: List['Equipment'] = field(default_factory=list)


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
    """Equipment definition with points of contact."""
    id: Optional[int]
    toolset_code: str
    fab: str
    phase: str
    name: str
    guid: str
    node_id: int  # Virtual equipment node
    kind: Optional[str] = None  # PRODUCTION, PROCESSING, SUPPLY, etc.
    description: Optional[str] = None
    is_active: bool = True
    pocs: List[EquipmentPoC] = field(default_factory=list)
    
    def get_available_pocs(self) -> List[EquipmentPoC]:
        """Get active PoCs that can be used for path generation."""
        return [poc for poc in self.pocs if poc.is_active]
    
    def get_used_pocs(self) -> List[EquipmentPoC]:
        """Get PoCs that are currently in use."""
        return [poc for poc in self.pocs if poc.is_active and poc.is_used]


@dataclass
class PathDefinition:
    """Definition of a discovered or scenario-based path."""
    id: Optional[int]
    path_hash: str
    fab: str
    category: str
    scope: str
    node_count: int
    link_count: int
    total_length_mm: float
    coverage: float
    utilities: List[str]
    path_context: Dict[str, Any]  # Serialized nodes/links sequence
    scenario_context: Optional[Dict[str, Any]] = None
    source_type: str = "RANDOM"  # RANDOM, SCENARIO


@dataclass
class AttemptPath:
    """Record of a random sampling attempt."""
    id: Optional[int]
    run_id: str
    path_definition_id: int
    start_node_id: int
    end_node_id: int
    fab: str
    phase: str
    category: str
    utility: str
    toolset_code: str
    picked_at: datetime
    notes: Optional[str] = None


@dataclass
class CriticalError:
    """Critical error found during validation."""
    id: Optional[int]
    error_type: str  # POC_NO_UTILITY, MISSING_NODE, MISSING_EQUIPMENT_NODE
    fab: str
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
    equipment_id: Optional[int] = None
    poc_id: Optional[int] = None
    category: Optional[str] = None
    utility: Optional[str] = None
    material: Optional[str] = None
    flow: Optional[str] = None
    item_name: Optional[str] = None
    created_at: datetime = field(default_factory=datetime.now)
    notes: Optional[str] = None


@dataclass
class ReviewFlag:
    """Manual or critical item flagged for review."""
    id: Optional[int]
    run_id: str
    created_at: datetime
    severity: Severity
    reason: str
    object_type: ObjectType
    start_node_id: Optional[int] = None
    end_node_id: Optional[int] = None
    link_id: Optional[int] = None
    equipment_id: Optional[int] = None
    poc_id: Optional[int] = None
    utility: Optional[str] = None
    material: Optional[str] = None
    flow: Optional[str] = None
    path_context: Optional[Dict[str, Any]] = None
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
    fab: str
    tag: str
    status: RunStatus
    started_at: datetime
    ended_at: Optional[datetime]
    duration: float  # seconds
    paths_attempted: int = 0
    paths_found: int = 0
    errors: List[str] = field(default_factory=list)
    review_flags: List[str] = field(default_factory=list)
    critical_errors: List[str] = field(default_factory=list)


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
