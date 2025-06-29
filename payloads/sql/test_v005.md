```sql
-- CONNECTIVITY VALIDATION (Structural Integrity)
INSERT INTO tb_validation_tests (
    code, name, description, scope, severity, test_type,
    applies_to_random, applies_to_scenario, building_specific,
    test_config, is_active
) VALUES 
(
    'CONN_001',
    'Link Record Completeness',
    'Validates that all consecutive node pairs have proper link records in database',
    'CONNECTIVITY',
    'HIGH',
    'STRUCTURAL',
    1, 1, 0,
    '{"check_type": "link_records", "validate_link_attributes": true}',
    1
),
(
    'CONN_002',
    'Node Reference Integrity', 
    'Ensures all path nodes reference valid, active equipment records',
    'CONNECTIVITY',
    'CRITICAL',
    'STRUCTURAL',
    1, 1, 0,
    '{"check_type": "node_equipment_refs", "check_equipment_active": true}',
    1
),
(
    'CONN_003',
    'Equipment POC Integrity',
    'Validates that equipment POCs are properly defined and linked',
    'CONNECTIVITY',
    'MEDIUM',
    'STRUCTURAL', 
    1, 1, 0,
    '{"check_type": "poc_integrity", "require_poc_references": true}',
    1
);

-- DATA QUALITY VALIDATION (Attribute-level)
INSERT INTO tb_validation_tests (
    code, name, description, scope, severity, test_type,
    applies_to_random, applies_to_scenario, building_specific,
    test_config, is_active
) VALUES
(
    'QUAL_001',
    'Missing Critical Attributes',
    'Identifies nodes with missing critical attributes like utility, eq_poc_no',
    'QA',
    'MEDIUM',
    'COMPLIANCE',
    1, 1, 0,
    '{"check_type": "missing_attributes", "critical_fields": ["utility", "eq_poc_no"], "null_tolerance": 0}',
    1
),
(
    'QUAL_002',
    'Utility Pattern Consistency',
    'Validates utility patterns and detects anomalies in utility sequences', 
    'FLOW',
    'LOW',
    'LOGICAL',
    1, 1, 0,
    '{"check_type": "utility_patterns", "detect_interruptions": true, "max_consecutive_nulls": 2}',
    1
),
(
    'QUAL_003',
    'Equipment Type Consistency',
    'Ensures equipment types are consistent along the path context',
    'QA',
    'MEDIUM',
    'LOGICAL',
    1, 1, 0,
    '{"check_type": "equipment_consistency", "check_kind_transitions": true}',
    1
),
(
    'QUAL_004',
    'POC Reference Completeness',
    'Validates that eq_poc_no fields are populated and reference valid POCs',
    'QA',
    'HIGH',
    'COMPLIANCE',
    1, 1, 0,
    '{"check_type": "poc_references", "allow_null_poc": false, "validate_poc_format": true}',
    1
);
```