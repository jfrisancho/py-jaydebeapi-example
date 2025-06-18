"""
Data models for the path analysis system.
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
    
    @classmethod
    def create_with_auto_tag(cls, run_id: str, approach: Approach, method: Method,
                           coverage_target: float, fab: str = "", toolset: str = "",
                           started_at: datetime = None) -> 'RunConfig':
        """Create a RunConfig with automatically generated tag."""
        if started_at is None:
            started_at = datetime.now()
        
        tag = cls.generate_tag(approach, method, coverage_target, fab, toolset, started_at)
        
        return cls(
            run_id=run_id,
            approach=approach,
            method=method,
            coverage_target=coverage_target,
            fab=fab,
            tag=tag,
            started_at=started_at,
            toolset=toolset
        )
    
    @staticmethod
    def generate_tag(approach: Approach, method: Method, coverage_target: float,
                    fab: str = "", toolset: str = "", date: datetime = None) -> str:
        """Generate tag using the specified format."""
        if date is None:
            date = datetime.now()
        
        coverage_target_tag = f'{coverage_target*100:.0f}P'
        
        # Base tag: YYYYMMDD_APPROACH_METHOD_COVERAGEP
        tag = f"{date.strftime('%Y%m%d')}_{approach.value}_{method.value}_{coverage_target_tag}"
        
        # Add fab if not empty
        if fab:
            tag += f"_{fab}"
        
        # Add toolset if not empty and not "ALL"
        if toolset and toolset != "ALL":
            tag += f"_{toolset}"
        
        return tag


@dataclass
class Equipment:
    """Equipment definition with points of contact."""
    id: str
    name: str
    toolset_id: str
    points_of_contact: List[int]  # Node IDs
    utility_codes: List[str]
    category: str


@dataclass
class Toolset:
    """Toolset containing multiple equipment pieces."""
    id: str
    name: str
    fab: str
    category: str
    equipment_list: List[Equipment]
    utility_codes: List[str]


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


@dataclass
class AttemptPath:
    """Record of a random sampling attempt."""
    id: Optional[int]
    run_id: str
    path_definition_id: int
    start_node_id: int
    end_node_id: int
    fab: str
    category: str
    utility: str
    picked_at: datetime
    notes: Optional[str] = None


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