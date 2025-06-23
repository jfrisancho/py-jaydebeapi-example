"""
Enhanced enumerations for the path analysis system with equipment support.
"""

from enum import Enum, auto


class Building(Enum):
    """Building enumeration"""
    M15 = 'M15'
    M15X = 'M15X'
    M16 = 'M16'


class Phase(Enum):
    """Phase enumeration"""
    PHASE1 = 'PHASE1'
    PHASE2 = 'PHASE2'
    # Legacy support
    A = 'PHASE1'  # Maps to PHASE1
    B = 'PHASE2'  # Maps to PHASE2
    
    @classmethod
    def normalize(cls, phase_str: str) -> 'Phase':
        """Normalize phase string to standard format."""
        phase_str = phase_str.upper()
        if phase_str in ['A', 'PHASE_A', 'PHASEA']:
            return cls.PHASE1
        elif phase_str in ['B', 'PHASE_B', 'PHASEB']:
            return cls.PHASE2
        elif phase_str in ['1', 'PHASE1', 'PHASE_1']:
            return cls.PHASE1
        elif phase_str in ['2', 'PHASE2', 'PHASE_2']:
            return cls.PHASE2
        else:
            return cls(phase_str)


class EquipmentKind(Enum):
    """Equipment kind enumeration"""
    PRODUCTION = 'PRODUCTION'
    PROCESSING = 'PROCESSING'
    SUPPLY = 'SUPPLY'
    EXHAUST = 'EXHAUST'
    STORAGE = 'STORAGE'
    TRANSPORT = 'TRANSPORT'
    UTILITY = 'UTILITY'
    CONTROL = 'CONTROL'


class FlowDirection(Enum):
    """Flow direction enumeration"""
    IN = 'IN'
    OUT = 'OUT'
    BIDIRECTIONAL = 'BIDIRECTIONAL'


class UtilityCode(Enum):
    """Common utility codes"""
    N2 = 'N2'          # Nitrogen
    CDA = 'CDA'        # Clean Dry Air
    PW = 'PW'          # Process Water
    DI = 'DI'          # Deionized Water
    VAC = 'VAC'        # Vacuum
    H2 = 'H2'          # Hydrogen
    AR = 'AR'          # Argon
    O2 = 'O2'          # Oxygen
    EXHAUST = 'EXHAUST'  # Exhaust
    POWER = 'POWER'    # Electrical Power
    SIGNAL = 'SIGNAL'  # Control Signal


class CriticalErrorType(Enum):
    """Critical error types for equipment validation"""
    POC_NO_UTILITY = 'POC_NO_UTILITY'
    MISSING_POC_NODE = 'MISSING_POC_NODE'
    MISSING_EQUIPMENT_NODE = 'MISSING_EQUIPMENT_NODE'
    DUPLICATE_POC_NODE = 'DUPLICATE_POC_NODE'
    INVALID_UTILITY_FLOW = 'INVALID_UTILITY_FLOW'
    ORPHANED_EQUIPMENT = 'ORPHANED_EQUIPMENT'
    INACTIVE_USED_POC = 'INACTIVE_USED_POC'


class SyncStatus(Enum):
    """Data synchronization status"""
    PENDING = 'PENDING'
    RUNNING = 'RUNNING'
    COMPLETED = 'COMPLETED'
    FAILED = 'FAILED'
    PARTIAL = 'PARTIAL'


class Approach(Enum):
    """Analysis approach types."""
    RANDOM = "RANDOM"
    SCENARIO = "SCENARIO"


class Method(Enum):
    """Analysis method types."""
    # For RANDOM approach
    SIMPLE = "SIMPLE"
    STRATIFIED = "STRATIFIED"
    
    # For SCENARIO approach
    PREDEFINED = "PREDEFINED"
    SYNTHETIC = "SYNTHETIC"


class RunStatus(Enum):
    """Run execution status."""
    RUNNING = "RUNNING"
    DONE = "DONE"
    FAILED = "FAILED"


class ObjectType(Enum):
    """Database object types."""
    NODE = "NODE"
    LINK = "LINK"
    EQUIPMENT = "EQUIPMENT"
    POC = "POC"
    PATH = "PATH"
    SCENARIO = "SCENARIO"


class Severity(Enum):
    """Error/issue severity levels."""
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"
    WARNING = "WARNING"
    ERROR = "ERROR"


class TagType(Enum):
    """Path tag classification types."""
    QA = "QA"           # Quality assurance
    RISK = "RISK"       # Risk assessment
    INS = "INS"         # Inspection
    CRIT = "CRIT"       # Criticality
    UTY = "UTY"         # Utility
    CAT = "CAT"         # Category
    DAT = "DAT"         # Data
    FAB = "FAB"         # Fabrication
    EQUIPMENT = "EQUIPMENT"  # Equipment-related
    POC = "POC"         # Point of Contact
    SCENARIO = "SCENARIO"    # Scenario-related


class ValidationScope(Enum):
    """Validation test scopes."""
    FLOW = "FLOW"
    CONNECTIVITY = "CONNECTIVITY"
    MATERIAL = "MATERIAL"
    QA = "QA"
    EQUIPMENT = "EQUIPMENT"
    POC = "POC"
    UTILITY = "UTILITY"
    SCENARIO = "SCENARIO"


class ErrorType(Enum):
    """Validation error types."""
    MISSING_FLOW = "MISSING_FLOW"
    WRONG_DIRECTION = "WRONG_DIRECTION"
    MISSING_NODE = "MISSING_NODE"
    MISSING_LINK = "MISSING_LINK"
    INVALID_MATERIAL = "INVALID_MATERIAL"
    CONNECTIVITY_BREAK = "CONNECTIVITY_BREAK"
    PATH_NOT_FOUND = "PATH_NOT_FOUND"
    UTILITY_MISMATCH = "UTILITY_MISMATCH"
    EQUIPMENT_ERROR = "EQUIPMENT_ERROR"
    POC_ERROR = "POC_ERROR"
    MISSING_UTILITY = "MISSING_UTILITY"
    INVALID_POC = "INVALID_POC"
    ORPHANED_NODE = "ORPHANED_NODE"
    SCENARIO_ERROR = "SCENARIO_ERROR"


class SelectionStrategy(Enum):
    """Equipment selection strategies for bias mitigation"""
    PURE_RANDOM = "PURE_RANDOM"
    WEIGHTED_RANDOM = "WEIGHTED_RANDOM"
    UTILITY_BALANCED = "UTILITY_BALANCED"
    COVERAGE_OPTIMIZED = "COVERAGE_OPTIMIZED"
    ERROR_FOCUSED = "ERROR_FOCUSED"


class ExecutionMode(Enum):
    """CLI execution modes"""
    DEFAULT = "DEFAULT"
    INTERACTIVE = "INTERACTIVE"
    UNATTENDED = "UNATTENDED"


class ScenarioType(Enum):
    """Scenario types"""
    PREDEFINED = "PREDEFINED"
    SYNTHETIC = "SYNTHETIC"


class SourceType(Enum):
    """Path source types"""
    RANDOM = "RANDOM"
    SCENARIO = "SCENARIO"


class FlagType(Enum):
    """Review flag types"""
    MANUAL_REVIEW = "MANUAL_REVIEW"
    CRITICAL_ERROR = "CRITICAL_ERROR"
    PERFORMANCE = "PERFORMANCE"
    ANOMALY = "ANOMALY"


class FlagStatus(Enum):
    """Review flag status"""
    OPEN = "OPEN"
    ACKNOWLEDGED = "ACKNOWLEDGED"
    RESOLVED = "RESOLVED"
    DISMISSED = "DISMISSED"


class CompletionStatus(Enum):
    """Run completion status"""
    COMPLETED = "COMPLETED"
    PARTIAL = "PARTIAL"
    FAILED = "FAILED"


class ConfigCategory(Enum):
    """System configuration categories"""
    DATABASE = "DATABASE"
    EXECUTION = "EXECUTION"
    VALIDATION = "VALIDATION"
    UI = "UI"


class ConfigType(Enum):
    """System configuration value types"""
    STRING = "STRING"
    INTEGER = "INTEGER"
    FLOAT = "FLOAT"
    BOOLEAN = "BOOLEAN"
    JSON = "JSON"
