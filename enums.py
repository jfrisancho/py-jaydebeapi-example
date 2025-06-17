"""
Enumerations for the path analysis system.
"""

from enum import Enum, auto


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


class ValidationScope(Enum):
    """Validation test scopes."""
    FLOW = "FLOW"
    CONNECTIVITY = "CONNECTIVITY"
    MATERIAL = "MATERIAL"
    QA = "QA"


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