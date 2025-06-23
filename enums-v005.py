"""
Enhanced enumerations for the path analysis system with equipment support.
"""

from enum import Enum, auto
from typing import Optional, List


class Building(Enum):
    """Building enumeration"""
    M15 = 'M15'
    M15X = 'M15X'
    M16 = 'M16'


class Phase(Enum):
    """Phase enumeration with 4 phases"""
    PHASE1 = 'A'  # Human readable → System nomenclature
    PHASE2 = 'B'
    PHASE3 = 'C'
    PHASE4 = 'D'
    
    @classmethod
    def normalize(cls, phase_str: Optional[str]) -> Optional['Phase']:
        """Normalize phase string to standard format."""
        if not phase_str:
            return None
            
        s = phase_str.upper().strip()
        
        # Human readable format
        phase_mappings = {
            'PHASE1': cls.PHASE1,
            'PHASE2': cls.PHASE2,
            'PHASE3': cls.PHASE3,
            'PHASE4': cls.PHASE4,
            # Numeric format
            '1': cls.PHASE1,
            '2': cls.PHASE2,
            '3': cls.PHASE3,
            '4': cls.PHASE4,
            # System nomenclature format
            'A': cls.PHASE1,
            'B': cls.PHASE2,
            'C': cls.PHASE3,
            'D': cls.PHASE4,
        }
        
        if s in phase_mappings:
            return phase_mappings[s]
        
        # Try to create from value directly (A, B, C, D)
        try:
            return cls(s)
        except ValueError:
            return None # Not a valid system nomenclature value
            
    @classmethod
    def phases(cls) -> List['Phase']:
        """Get all phases."""
        return [cls.PHASE1, cls.PHASE2, cls.PHASE3, cls.PHASE4]
    
    @property
    def cardinal(self) -> int:
        """Get numeric value for phase."""
        phase_numbers = {
            'PHASE1': 1, # Based on Enum name
            'PHASE2': 2,
            'PHASE3': 3,
            'PHASE4': 4
        }
        return phase_numbers.get(self.name, 0)
    
    @property
    def conceptual(self) -> str:
        """Get human readable name."""
        return self.name  # Returns PHASE1, PHASE2, etc.
    
    @property
    def nominal(self) -> str:
        """Get system nomenclature."""
        return self.value  # Returns A, B, C, D


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
    RANDOM = 'RANDOM'
    SCENARIO = 'SCENARIO'


class Method(Enum):
    """Analysis method types."""
    # For RANDOM approach
    SIMPLE = 'SIMPLE'
    STRATIFIED = 'STRATIFIED'
    
    # For SCENARIO approach
    PREDEFINED = 'PREDEFINED'
    SYNTHETIC = 'SYNTHETIC'
    # FILE method might be considered part of SCENARIO
    # or a separate top-level method if CLI structure changes


class RunStatus(Enum):
    """Run execution status."""
    RUNNING = 'RUNNING'
    DONE = 'DONE'
    FAILED = 'FAILED'


class ObjectType(Enum):
    """Database object types."""
    NODE = 'NODE'
    LINK = 'LINK'
    EQUIPMENT = 'EQUIPMENT'
    POC = 'POC'
    PATH = 'PATH'
    SCENARIO = 'SCENARIO'
    TOOLSET = 'TOOLSET' # Added for completeness


class Severity(Enum):
    """Error/issue severity levels."""
    LOW = 'LOW'
    MEDIUM = 'MEDIUM'
    HIGH = 'HIGH'
    CRITICAL = 'CRITICAL'
    WARNING = 'WARNING'  # Often used for non-blocking issues
    ERROR = 'ERROR'      # Often used for blocking issues that don't stop the run


class TagType(Enum):
    """Path tag classification types."""
    QA = 'QA'           # Quality assurance
    RISK = 'RISK'       # Risk assessment
    INS = 'INS'         # Inspection
    CRIT = 'CRIT'       # Criticality
    UTY = 'UTY'         # Utility
    CAT = 'CAT'         # Category
    DAT = 'DAT'         # Data
    FAB = 'FAB'         # Fabrication related
    EQUIPMENT = 'EQUIPMENT'  # Equipment-related
    POC = 'POC'         # Point of Contact related
    SCENARIO = 'SCENARIO'    # Scenario-related


class ValidationScope(Enum):
    """Validation test scopes."""
    FLOW = 'FLOW'
    CONNECTIVITY = 'CONNECTIVITY'
    MATERIAL = 'MATERIAL'
    QA = 'QA'                   # General quality assurance
    EQUIPMENT = 'EQUIPMENT'
    POC = 'POC'
    UTILITY = 'UTILITY'
    SCENARIO = 'SCENARIO'       # Scenario specific validation


class ErrorType(Enum):
    """Validation error types."""
    MISSING_FLOW = 'MISSING_FLOW'
    WRONG_DIRECTION = 'WRONG_DIRECTION'
    MISSING_NODE = 'MISSING_NODE'
    MISSING_LINK = 'MISSING_LINK'
    INVALID_MATERIAL = 'INVALID_MATERIAL'
    CONNECTIVITY_BREAK = 'CONNECTIVITY_BREAK'
    PATH_NOT_FOUND = 'PATH_NOT_FOUND'
    UTILITY_MISMATCH = 'UTILITY_MISMATCH'
    EQUIPMENT_ERROR = 'EQUIPMENT_ERROR'
    POC_ERROR = 'POC_ERROR'
    MISSING_UTILITY = 'MISSING_UTILITY'
    INVALID_POC = 'INVALID_POC'
    ORPHANED_NODE = 'ORPHANED_NODE'
    SCENARIO_ERROR = 'SCENARIO_ERROR'
    # Added based on schema / usage
    RANDOM_BUILDING_REQUIRED = 'RANDOM_BUILDING_REQUIRED'
    RANDOM_EQUIPMENT_REQUIRED = 'RANDOM_EQUIPMENT_REQUIRED'
    SCENARIO_ID_REQUIRED = 'SCENARIO_ID_REQUIRED'
    SCENARIO_CONTEXT_REQUIRED = 'SCENARIO_CONTEXT_REQUIRED'
    PATH_LENGTH = 'PATH_LENGTH'
    PATH_LOOPS = 'PATH_LOOPS'


class SelectionStrategy(Enum):
    """Equipment selection strategies for bias mitigation"""
    PURE_RANDOM = 'PURE_RANDOM'
    WEIGHTED_RANDOM = 'WEIGHTED_RANDOM'
    UTILITY_BALANCED = 'UTILITY_BALANCED'
    COVERAGE_OPTIMIZED = 'COVERAGE_OPTIMIZED'
    ERROR_FOCUSED = 'ERROR_FOCUSED'


class ExecutionMode(Enum):
    """CLI execution modes"""
    DEFAULT = 'DEFAULT'
    INTERACTIVE = 'INTERACTIVE'
    UNATTENDED = 'UNATTENDED'


class ScenarioType(Enum):
    """Scenario types"""
    PREDEFINED = 'PREDEFINED'
    SYNTHETIC = 'SYNTHETIC'
    FILE = 'FILE' # If file based scenarios are a distinct type


class SourceType(Enum):
    """Path source types"""
    RANDOM = 'RANDOM'
    SCENARIO = 'SCENARIO'


class FlagType(Enum):
    """Review flag types"""
    MANUAL_REVIEW = 'MANUAL_REVIEW'
    CRITICAL_ERROR = 'CRITICAL_ERROR'
    PERFORMANCE = 'PERFORMANCE'
    ANOMALY = 'ANOMALY'


class FlagStatus(Enum):
    """Review flag status"""
    OPEN = 'OPEN'
    ACKNOWLEDGED = 'ACKNOWLEDGED'
    RESOLVED = 'RESOLVED'
    DISMISSED = 'DISMISSED'


class CompletionStatus(Enum):
    """Run completion status"""
    COMPLETED = 'COMPLETED' # Target met or all scenarios successful
    PARTIAL = 'PARTIAL'     # Target not fully met, or some scenarios failed
    FAILED = 'FAILED'       # Run itself failed due to error


class ConfigCategory(Enum):
    """System configuration categories"""
    DATABASE = 'DATABASE'
    EXECUTION = 'EXECUTION'
    VALIDATION = 'VALIDATION'
    UI = 'UI'


class ConfigType(Enum):
    """System configuration value types"""
    STRING = 'STRING'
    INTEGER = 'INTEGER'
    FLOAT = 'FLOAT'
    BOOLEAN = 'BOOLEAN'
    JSON = 'JSON'
