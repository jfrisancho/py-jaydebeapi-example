```sql
-- Updated Database Schema for Path Analysis CLI v2.0
-- Supports Building enum, enhanced scenario handling, and improved run tracking

-- Simplified Schema: No phase mapping table needed

-- Toolsets: Simple with unique code, phase stored as A/B/C/D
CREATE TABLE tb_toolsets (
    code VARCHAR(64) PRIMARY KEY,       -- Unique toolset code

    model_no INTEGER NOT NULL,             -- Represents the data model type (BIM, 5D)
    fab VARCHAR(10) NOT NULL,
    phase_no INTEGER NOT NULL,          -- Store as A, B, C, D (system nomenclature)
    
    name VARCHAR(128),                  -- Optional name
    description VARCHAR(512),           -- Optional description

    is_active BIT(1) NOT NULL,
    created_at TIMESTAMP DEFAULT now() NOT NULL
);

CREATE INDEX idx_toolsets_model_no ON tb_toolsets (model_no);
CREATE INDEX idx_toolsets_fab ON tb_toolsets (fab);
CREATE INDEX idx_toolsets_phase_no ON tb_toolsets (phase_no);
CREATE INDEX idx_toolsets_fab_phase_no ON tb_toolsets (fab, phase_no);
CREATE INDEX idx_toolsets_model_no_fab_phase_no ON tb_toolsets (model_no, fab, phase_no);

-- Equipment: Simple FK to toolset code
CREATE TABLE tb_equipments (
    id INTEGER AUTO_INCREMENT PRIMARY KEY,
    toolset VARCHAR(64) REFERENCES tb_toolsets(code) NOT NULL,  -- Simple FK to toolset code

    guid VARCHAR(64) UNIQUE NOT NULL,
    node_id INTEGER NOT NULL,           -- Virtual equipment node
    data_code INTEGER NOT NULL,
    category_no INTEGER NOT NULL,
    vertices INTEGER NOT NULL,

    kind VARCHAR(32),                   -- PRODUCTION, PROCESSING, SUPPLY, etc.
    
    name VARCHAR(128),                  -- Optional name
    description VARCHAR(512),           -- Optional description

    is_active BIT(1) NOT NULL,
    created_at TIMESTAMP DEFAULT now() NOT NULL
);

CREATE INDEX idx_equipments_toolset ON tb_equipments (toolset);

-- Equipment PoCs: Same as before
CREATE TABLE tb_equipment_pocs (
    id INTEGER AUTO_INCREMENT PRIMARY KEY,
    equipment_id INTEGER REFERENCES tb_equipment(id) ON DELETE CASCADE NOT NULL,

    node_id INTEGER NOT NULL,           -- Actual network node ID
    is_used BOOLEAN DEFAULT FALSE,
    
    
    markers VARCHAR(128)        -- Identifies PoC labels and associated metadata changes for this element
    utility_no INTEGER,         -- N2, CDA, PW, etc. - NULL if unused
    reference VARCHAR(8)        -- Identifies the first formatter element of the markers
    flow VARCHAR(8),            -- IN, OUT - NULL if unused
    is_loopback BIT(1) NOT NULL, -- If is there is a path connecting two or more PoCs in the same equipment.
    
    is_active BIT(1) NOT NULL,
    created_at TIMESTAMP DEFAULT now() NOT NULL
);

CREATE INDEX idx_pocs_equipment (equipment_id);
CREATE UNIQUE INDEX idx_pocs_node (node_id);
CREATE INDEX idx_pocs_equipment_poc_node_id (equipment_id, node_id);

CREATE TABLE tb_equipment_connections (
    id INTEGER AUTO_INCREMENT PRIMARY KEY,

    from_equipment_id INTEGER NOT NULL REFERENCES tb_equipments(id) ON DELETE CASCADE,
    to_equipment_id   INTEGER NOT NULL REFERENCES tb_equipments(id) ON DELETE CASCADE,

    from_poc_id INTEGER NOT NULL REFERENCES tb_equipment_pocs(id) ON DELETE CASCADE,
    to_poc_id   INTEGER NOT NULL REFERENCES tb_equipment_pocs(id) ON DELETE CASCADE,

    is_valid BIT(1) NOT NULL,  -- Mark whether the path is usable or blocked

    connection_type VARCHAR(16), -- Optional: STRAIGHT, BRANCHED, LOOPBACK, etc.
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_eq_conn_from_to ON tb_equipment_connections (from_equipment_id, to_equipment_id);
CREATE INDEX idx_eq_conn_path_id ON tb_equipment_connections (path_id);

-- 1. Runs: CLI execution metadata and coverage summary
CREATE TABLE tb_runs (
    id VARCHAR(36) PRIMARY KEY,
    
    date DATE NOT NULL,
    approach VARCHAR(20) NOT NULL,      -- RANDOM, SCENARIO
    method VARCHAR(20) NOT NULL,        -- SIMPLE, STRATIFIED, PREDEFINED, SYNTHETIC, FILE
    
    -- Random-specific fields
    coverage_target FLOAT NOT NULL,     -- Only relevant for RANDOM approach
    fab VARCHAR(64),                    -- Building identifier (M15, M15X, M16) - NULL for SCENARIO
    toolset VARCHAR(128),               -- Toolset identifier - NULL for SCENARIO
    phase_no INTEGER,                   -- Phase identifier - NULL for SCENARIO
    model_no INTEGER,                   -- Data model type identifier - NULL for SCENARIO
    
    -- Scenario-specific fields
    scenario_code VARCHAR(128),         -- Scenario code (PREXXXXXXX, SYNXXXXXXX) - NULL for RANDOM
    scenario_file VARCHAR(512),         -- Scenario file path - NULL for RANDOM
    scenario_type VARCHAR(20),          -- PREDEFINED, SYNTHETIC (auto-detected from code) - NULL for RANDOM

    total_coverage FLOAT NOT NULL,
    total_nodes INTEGER NOT NULL,
    total_links INTEGER NOT NULL,

    tag VARCHAR(256) NOT NULL,          -- Auto-generated tag
    status VARCHAR(20) NOT NULL,        -- RUNNING, DONE, FAILED
    
    -- Execution mode tracking
    execution_mode VARCHAR(20) DEFAULT 'DEFAULT',  -- DEFAULT, INTERACTIVE, UNATTENDED
    
    -- Metadata
    run_at TIMESTAMP DEFAULT now() NOT NULL NOT NULL,
    ended_at TIMESTAMP
);



-- 3. Scenarios: Predefined and synthetic scenarios
CREATE TABLE tb_scenarios (
    id INTEGER AUTO_INCREMENT PRIMARY KEY,
    
    code VARCHAR(128) UNIQUE NOT NULL,  -- PREXXXXXXX, SYNXXXXXXX, FILXXXXXXX
    name VARCHAR(128) NOT NULL,
    type VARCHAR(20) NOT NULL,          -- PREDEFINED, SYNTHETIC, FILE
    
    -- Scenario metadata
    expected_coverage FLOAT,            -- Expected coverage for this scenario
    expected_nodes INTEGER,             -- Expected number of nodes
    expected_links INTEGER,             -- Expected number of links
    expected_paths INTEGER,             -- Expected number of paths
    
    -- Validation settings
    expected_valid BIT(1) NOT NULL, -- Should this scenario pass validation
    expected_criticality VARCHAR(32),   -- Expected criticality level
 
    file_path VARCHAR(512),             -- Optional file path for file-based scenarios   
    description VARCHAR(512),
    
    -- Ownership and lifecycle
    created_by VARCHAR(64),
    created_at TIMESTAMP DEFAULT now() NOT NULL,
    updated_at TIMESTAMP DEFAULT now() NOT NULL ON UPDATE CURRENT_TIMESTAMP,
    is_active BIT(1) NOT NULL
);


-- 6. Path Definitions: Enhanced with scenario support
CREATE TABLE tb_path_definitions (
    id INTEGER AUTO_INCREMENT PRIMARY KEY,
    path_hash VARCHAR(128) UNIQUE NOT NULL,

    -- Path classification
    source_type VARCHAR(16),    -- RANDOM, SCENARIO    
    scope VARCHAR(32),          -- CONNECTIVITY, FLOW, MATERIAL
    
    target_fab VARCHAR(32),     -- NULL for scenarios
    target_model_no INTEGER,
    target_phase_no INTEGER,
    target_toolset_no INTEGER,
    
    target_data_codes VARCHAR(128),
    target_utilities VARCHAR(128),
    target_references VARCHAR(128),
    forbidden_node_ids VARCHAR(128),
    
    -- Path metrics
    node_count INTEGER NOT NULL,
    link_count INTEGER NOT NULL,
    total_length_mm NUMERIC(15,3) NOT NULL,
    coverage FLOAT NOT NULL,

    -- Path data
    data_codes_scope CLOB,
    utilities_scope CLOB,       -- JSON array of utility codes
    references_scope CLOB,
    path_context CLOB,          -- JSON object with nodes/links sequence
    
    -- Metadata
    created_at TIMESTAMP DEFAULT now() NOT NULL,
);

CREATE INDEX idx_path_definition_source_type_target_fab ON tb_path_definitions (source_type, target_fab);
    
CREATE TABLE tb_run_scenarios (
    id INTEGER AUTO_INCREMENT PRIMARY KEY,
    run_id VARCHAR(36) REFERENCES tb_runs(id) ON DELETE CASCADE NOT NULL,
    scenario_id INTEGER REFERENCES tb_scenarios(id) ON DELETE CASCADE NOT NULL,
    
    run_by VARCHAR(64),
    run_at TIMESTAMP DEFAULT now() NOT NULL
);
    
-- 7. Attempt Paths: Random sampling attempts
CREATE TABLE tb_attempt_paths (
    id INTEGER AUTO_INCREMENT PRIMARY KEY,
    run_id VARCHAR(36) REFERENCES tb_runs(id) ON DELETE CASCADE NOT NULL,
    path_definition_id INTEGER REFERENCES tb_path_definitions(id) ON DELETE CASCADE NOT NULL,

    start_node_id BIGINT NOT NULL,
    end_node_id BIGINT NOT NULL,
    cost DOUBLE,

    picked_at TIMESTAMP DEFAULT now() NOT NULL,
    tested_at TIMESTAMP,
    notes VARCHAR(512)
);

CREATE TABLE tb_scenario_paths (
    id INTEGER AUTO_INCREMENT PRIMARY KEY,
    run_scenario_id INTEGER REFERENCES tb_run_scenarios(id) ON DELETE CASCADE NOT NULL,
    path_definition_id INTEGER REFERENCES tb_path_definitions(id) ON DELETE CASCADE NOT NULL,

    was_tested BIT(1) NOT NULL,
    is_test_valid BIT(1),

    start_node_id BIGINT NOT NULL,
    end_node_id BIGINT NOT NULL,
    cost DOUBLE,

    picked_at TIMESTAMP DEFAULT now() NOT NULL,
    tested_at TIMESTAMP,
    notes VARCHAR(512)
);

CREATE INDEX idx_scenario_paths_run_id ON tb_scenario_paths (run_id);

-- 8. Scenario Executions: Scenario test executions
CREATE TABLE tb_scenario_executions (
    id INTEGER AUTO_INCREMENT PRIMARY KEY,
    run_id VARCHAR(36) REFERENCES tb_runs(id) ON DELETE CASCADE NOT NULL,
    scenario_id INTEGER REFERENCES tb_scenarios(id) ON DELETE CASCADE NOT NULL,
    path_definition_id INTEGER REFERENCES tb_path_definitions(id) ON DELETE CASCADE NOT NULL,   -- NULL if scenario failed to generate path

    execution_status VARCHAR(20) NOT NULL,  -- SUCCESS, FAILED, ERROR
    execution_time_ms INTEGER,
    
    -- Results
    actual_nodes INTEGER,
    actual_links INTEGER,
    actual_coverage FLOAT,
    
    -- Validation results
    validation_passed BOOLEAN,
    validation_errors TEXT,             -- JSON array of validation errors
    
    executed_at TIMESTAMP DEFAULT now() NOT NULL
);

CREATE INDEX idx_scenario_exec_run ON tb_scenario_executions (run_id);
CREATE INDEX idx_scenario_exec_scenario ON tb_scenario_executions (scenario_id);
CREATE INDEX idx_scenario_exec_path_definition ON tb_scenario_executions (path_definition_id);

-- 9. Path Tags: Enhanced with source tracking
CREATE TABLE tb_path_tags (
    id INTEGER AUTO_INCREMENT PRIMARY KEY,
    run_id VARCHAR(36) REFERENCES tb_runs(id) ON DELETE CASCADE NOT NULL,
    path_definition_id INTEGER REFERENCES tb_path_definitions(id) ON DELETE CASCADE NOT NULL,
    path_hash VARCHAR(128),

    tag_type VARCHAR(16) NOT NULL,      -- QA, RISK, INS, CRIT, UTY, CAT, DAT, FAB, SCENARIO
    tag_code VARCHAR(48) NOT NULL,
    tag VARCHAR(64),
    
    -- Tag metadata
    source VARCHAR(20),                 -- SYSTEM, USER, VALIDATION
    confidence FLOAT DEFAULT 1.0,      -- Confidence score for auto-generated tags
    
    created_at TIMESTAMP DEFAULT now() NOT NULL,
    created_by VARCHAR(64),
    notes VARCHAR(512)
);

CREATE INDEX idx_path_tags_definition ON tb_path_tags (path_definition_id),
CREATE INDEX idx_path_tags_type ON tb_path_tags (tag_type, tag_code)

-- 10. Validation Tests: Enhanced validation framework
CREATE TABLE tb_validation_tests (
    id INTEGER AUTO_INCREMENT PRIMARY KEY,

    code VARCHAR(32) UNIQUE NOT NULL,
    name VARCHAR(128) NOT NULL,

    scope VARCHAR(32) NOT NULL,         -- FLOW, CONNECTIVITY, MATERIAL, QA, SCENARIO
    severity VARCHAR(16) NOT NULL,      -- LOW, MEDIUM, HIGH, CRITICAL
    test_type VARCHAR(32),              -- STRUCTURAL, LOGICAL, PERFORMANCE, COMPLIANCE
    
    -- Applicability
  --  applies_to_random BIT(1) NOT NULL,
  --  applies_to_scenario BIT(1) NOT NULL,
    
    is_active BIT(1) NOT NULL,
    description VARCHAR(512)
);

CREATE TABLE tb_validation_outcomes (
    id INTEGER AUTO_INCREMENT PRIMARY KEY,
    validation_test_id INTEGER REFERENCES tb_validation_tests(id) ON DELETE CASCADE NOT NULL,
    
    tag_type VARCHAR(16) NOT NULL,      -- QA, RISK, INS, CRIT, UTY, CAT, DAT, FAB, SCENARIO
    tag_code VARCHAR(48) NOT NULL,
    tag VARCHAR(64),
);

-- 11. Validation Errors: Enhanced error tracking
CREATE TABLE tb_validation_errors (
    id INTEGER AUTO_INCREMENT PRIMARY KEY,
    run_id VARCHAR(36) REFERENCES tb_runs(id) ON DELETE CASCADE NOT NULL,
    path_definition_id INTEGER REFERENCES tb_path_definitions(id) ON DELETE CASCADE,
    validation_test_id REFERENCES tb_validation_tests(id),

    severity VARCHAR(16) NOT NULL,
    error_scope VARCHAR(64) NOT NULL,
    error_type VARCHAR(64) NOT NULL,
    
    -- Object references
    object_type VARCHAR(8) NOT NULL,
    object_id BIGINT NOT NULL,
    object_guid VARCHAR(64) NOT NULL,

    object_fab VARCHAR(32),
    object_model_no INTEGER,
    object_data_code INTEGER,
    object_e2e_group_no INTEGER,
    object_markers VARCHAR(128),
    object_utility_no INTEGER,
    object_item_no INTEGER,
    object_type_no INTEGER,

    object_material_no INTEGER,
    object_flow VARCHAR(8),
    object_is_loopback BIT(1) NOT NULL,
    object_cost DOUBLE,

    -- Error details
    error_message TEXT,
    error_data TEXT,                    -- JSON object with additional error data
    
    created_at TIMESTAMP DEFAULT now() NOT NULL,
    notes VARCHAR(512)
);
    
CREATE INDEX idx_validation_errors_run ON tb_validation_errors (run_id),
CREATE INDEX idx_validation_errors_severity ON tb_validation_errors (severity),
CREATE INDEX idx_validation_errors_type ON tb_validation_errors (error_type)

-- 12. Review Flags: Enhanced flagging system
CREATE TABLE tb_review_flags (
    id INTEGER AUTO_INCREMENT PRIMARY KEY,
    run_id VARCHAR(36) REFERENCES tb_runs(id) ON DELETE CASCADE NOT NULL,
    
    flag_type VARCHAR(32) NOT NULL,     -- MANUAL_REVIEW, CRITICAL_ERROR, PERFORMANCE, ANOMALY
    severity VARCHAR(16) NOT NULL,
    reason VARCHAR(256) NOT NULL,

    -- Object references
    object_type VARCHAR(8) NOT NULL,
    object_id BIGINT NOT NULL,
    object_guid VARCHAR(64) NOT NULL,

    object_fab VARCHAR(32),
    object_model_no INTEGER,
    object_data_code INTEGER,
    object_e2e_group_no INTEGER,
    object_markers VARCHAR(128),
    object_utility_no INTEGER,
    object_item_no INTEGER,
    object_type_no INTEGER,

    object_material_no INTEGER,
    object_flow VARCHAR(8),
    object_is_loopback BIT(1) NOT NULL,
    object_cost DOUBLE,
    
    -- Flag details
    path_context TEXT,                  -- JSON object with path context
    flag_data TEXT,                     -- JSON object with additional flag data
    
    -- Flag lifecycle
    status VARCHAR(20) DEFAULT 'OPEN', -- OPEN, ACKNOWLEDGED, RESOLVED, DISMISSED
    assigned_to VARCHAR(64),
    resolved_at TIMESTAMP,
    resolution_notes TEXT,
    
    created_at TIMESTAMP DEFAULT now() NOT NULL,
    notes VARCHAR(512)
);
    
CREATE INDEX idx_review_flags_run ON tb_review_flags (run_id),
CREATE INDEX idx_review_flags_flag_type ON tb_review_flags (flag_type),
CREATE INDEX idx_review_flags_severity ON tb_review_flags (severity)

-- 13. Run Summaries: Enhanced aggregated metrics
CREATE TABLE tb_run_summaries (
    run_id VARCHAR(36) PRIMARY KEY,
    
    -- Basic metrics
    total_attempts INTEGER NOT NULL,
    total_paths_found INTEGER NOT NULL,
    unique_paths INTEGER NOT NULL,
    
    -- Approach-specific metrics
    total_scenario_tests INTEGER NOT NULL DEFAULT 0,
    scenario_success_rate NUMERIC(5,2),
    
    -- Quality metrics
    total_errors INTEGER NOT NULL,
    total_reviews INTEGER NOT NULL,
    critical_errors INTEGER NOT NULL DEFAULT 0,
    
    -- Coverage metrics (for RANDOM approach)
    target_coverage FLOAT,
    achieved_coverage FLOAT,
    coverage_efficiency FLOAT,         -- achieved/target ratio
    
    -- Performance metrics
    total_nodes INTEGER NOT NULL,
    total_links INTEGER NOT NULL,
    
    avg_path_nodes NUMERIC(10,2),
    avg_path_links NUMERIC(10,2),
    avg_path_length NUMERIC(15,3),
    
    -- Success metrics
    success_rate NUMERIC(5,2),
    completion_status VARCHAR(20),      -- COMPLETED, PARTIAL, FAILED
    
    -- Timing
    execution_time_seconds NUMERIC(10,2),
    started_at TIMESTAMP NOT NULL,
    ended_at TIMESTAMP,
    
    summarized_at TIMESTAMP DEFAULT now() NOT NULL,
    
    FOREIGN KEY (run_id) REFERENCES tb_runs(id) ON DELETE CASCADE
);


-- Indexes for performance
CREATE INDEX idx_runs_approach_status ON tb_runs(approach, status);
CREATE INDEX idx_runs_fab_date ON tb_runs(fab, date);
CREATE INDEX idx_runs_scenario ON tb_runs(scenario_code, scenario_type);
```
