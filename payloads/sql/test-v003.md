```sql

-- Simplified Schema: No phase mapping table needed

-- Toolsets: Simple with unique code, phase stored as A/B/C/D
CREATE TABLE tb_toolsets (
    code VARCHAR(64) PRIMARY KEY,       -- Unique toolset code
    fab VARCHAR(10) NOT NULL,
    phase VARCHAR(8) NOT NULL,          -- Store as A, B, C, D (system nomenclature)
    name VARCHAR(128),                  -- Optional name
    description VARCHAR(512),           -- Optional description
    
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    INDEX idx_toolsets_fab (fab),
    INDEX idx_toolsets_phase (phase),
    INDEX idx_toolsets_fab_phase (fab, phase)
);

-- Equipment: Simple FK to toolset code
CREATE TABLE tb_equipments (
    id INTEGER AUTO_INCREMENT PRIMARY KEY,
    toolset VARCHAR(64) NOT NULL,  -- Simple FK to toolset code
    name VARCHAR(128) NOT NULL,
    guid VARCHAR(64) NOT NULL,
    node_id INTEGER NOT NULL,           -- Virtual equipment node
    kind VARCHAR(32),                   -- PRODUCTION, PROCESSING, SUPPLY, etc.
    
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (toolset) REFERENCES tb_toolsets(code),
    INDEX idx_equipments_toolset (toolset),
    UNIQUE KEY uk_equipments_guid (guid)
);

-- Equipment PoCs: Same as before
CREATE TABLE tb_equipment_pocs (
    id INTEGER AUTO_INCREMENT PRIMARY KEY,
    equipment_id INTEGER NOT NULL,
    code VARCHAR(8) NOT NULL,           -- POC01, POC02, IN01, OUT01
    node_id INTEGER NOT NULL,           -- Actual network node ID
    
    utility VARCHAR(32),        -- N2, CDA, PW, etc. - NULL if unused
    flow VARCHAR(8),            -- IN, OUT - NULL if unused
    is_used BOOLEAN DEFAULT FALSE,
    
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (equipment_id) REFERENCES tb_equipment(id) ON DELETE CASCADE,
    INDEX idx_pocs_equipment (equipment_id),
    INDEX idx_pocs_node (node_id),
    UNIQUE KEY uk_equipment_poc_code (equipment_id, code),
    UNIQUE KEY uk_poc_node (node_id)
);

-- Sample toolset data with A/B/C/D phases
INSERT INTO tb_toolsets (code, fab, phase, name, description) VALUES
('TS001', 'M16', 'A', '', ''),
('TS002', 'M16', 'B', '', ''),
('TS003', 'M15', 'A', 'Fab M15 Line 1', 'M15 production toolset'),
('TS004', 'M16', 'C', 'Processing Line 1', 'Post-processing toolset'),
('TS005', 'M16', 'D', 'Final Assembly', 'Final assembly toolset');

-- Much simpler queries:

-- Query 1: Get toolset by code
-- SELECT * FROM tb_toolsets WHERE code = 'TS001';

-- Query 2: Get all toolsets for a fab
-- SELECT * FROM tb_toolsets WHERE fab = 'M16';

-- Query 3: Get all toolsets for phase A (PHASE1)
-- SELECT * FROM tb_toolsets WHERE phase = 'A';

-- Query 4: Get equipment for a toolset
-- SELECT e.* FROM tb_equipment e WHERE e.toolset_code = 'TS001';

-- Query 5: Get toolsets by fab and phase
-- SELECT * FROM tb_toolsets WHERE fab = 'M16' AND phase = 'B';

-- Query 6: Join equipment with toolset info
-- SELECT e.*, t.fab, t.phase, t.name as toolset_name 
-- FROM tb_equipment e 
-- JOIN tb_toolsets t ON e.toolset_code = t.code;

-- No phase mapping table needed!
-- Phase conversion handled in Python enum:
-- Phase.PHASE1.value → 'A'
-- Phase.normalize('PHASE1') → Phase.PHASE1 (value='A')
-- Phase.normalize('1') → Phase.PHASE1 (value='A')
-- Phase.normalize('A') → Phase.PHASE1 (value='A')

```sql
-- Updated Database Schema for Path Analysis CLI v2.0
-- Supports Building enum, enhanced scenario handling, and improved run tracking

-- 1. Runs: CLI execution metadata and coverage summary
CREATE TABLE tb_runs (
    id VARCHAR(36) PRIMARY KEY,
    date DATE NOT NULL,
    approach VARCHAR(20) NOT NULL,      -- RANDOM, SCENARIO
    method VARCHAR(20) NOT NULL,        -- SIMPLE, STRATIFIED, PREDEFINED, SYNTHETIC, FILE
    coverage_target FLOAT NOT NULL,     -- Only relevant for RANDOM approach

    total_coverage FLOAT NOT NULL,
    total_nodes INTEGER NOT NULL,
    total_links INTEGER NOT NULL,

    fab VARCHAR(64),                    -- Building identifier (M15, M15X, M16) - NULL for SCENARIO
    toolset VARCHAR(128),               -- Toolset identifier - NULL for SCENARIO
    phase VARCHAR (8)            -- Phase identifier - NULL for SCENARIO
    
    -- Scenario-specific fields
    scenario_code VARCHAR(128),         -- Scenario code (PREXXXXXXX, SYNXXXXXXX) - NULL for RANDOM
    scenario_file VARCHAR(512),         -- Scenario file path - NULL for RANDOM
    scenario_type VARCHAR(20),          -- PREDEFINED, SYNTHETIC (auto-detected from code) - NULL for RANDOM

    tag VARCHAR(256) NOT NULL,          -- Auto-generated tag
    status VARCHAR(20) NOT NULL,        -- RUNNING, DONE, FAILED

    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    ended_at TIMESTAMP,
    
    -- Execution mode tracking
    execution_mode VARCHAR(20) DEFAULT 'DEFAULT',  -- DEFAULT, INTERACTIVE, UNATTENDED
    
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);



-- 3. Scenarios: Predefined and synthetic scenarios
CREATE TABLE tb_scenarios (
    id INTEGER AUTO_INCREMENT PRIMARY KEY,
    code VARCHAR(128) UNIQUE NOT NULL,  -- PREXXXXXXX, SYNXXXXXXX, FILXXXXXXX
    name VARCHAR(128) NOT NULL,
    description VARCHAR(512),
    
    scenario_type VARCHAR(20) NOT NULL, -- PREDEFINED, SYNTHETIC, FILE
    file_path VARCHAR(512),             -- Optional file path for file-based scenarios
    
    -- Scenario metadata
    expected_coverage FLOAT,            -- Expected coverage for this scenario
    expected_nodes INTEGER,             -- Expected number of nodes
    expected_links INTEGER,             -- Expected number of links
    expected_paths INTEGER,             -- Expected number of paths
    
    -- Validation settings
    expected_valid BOOLEAN DEFAULT TRUE, -- Should this scenario pass validation
    expected_criticality VARCHAR(32),   -- Expected criticality level
    
    -- Ownership and lifecycle
    created_by VARCHAR(64),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    is_active BOOLEAN DEFAULT TRUE
);



-- 6. Path Definitions: Enhanced with scenario support
CREATE TABLE tb_path_definitions (
    id INTEGER AUTO_INCREMENT PRIMARY KEY,
    path_hash VARCHAR(128) UNIQUE NOT NULL,

    -- Path classification
    source_type VARCHAR(20) NOT NULL,   -- RANDOM, SCENARIO
    building_code VARCHAR(10),          -- NULL for scenarios
    category VARCHAR(64) NOT NULL,
    scope VARCHAR(32) NOT NULL,         -- CONNECTIVITY, FLOW, MATERIAL
    
    -- Path metrics
    node_count INTEGER NOT NULL,
    link_count INTEGER NOT NULL,
    total_length_mm NUMERIC(15,3) NOT NULL,
    coverage FLOAT NOT NULL,

    -- Path data
    utilities TEXT NOT NULL,            -- JSON array of utility codes
    path_context TEXT NOT NULL,         -- JSON object with nodes/links sequence
    
    -- Scenario-specific data
    scenario_id INTEGER,                -- NULL for random paths
    scenario_context TEXT,              -- JSON object for scenario-specific data
    
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (building_code) REFERENCES tb_buildings(code),
    FOREIGN KEY (scenario_id) REFERENCES tb_scenarios(id),
    INDEX idx_path_source (source_type, building_code),
    INDEX idx_path_scenario (scenario_id)
);

-- 7. Attempt Paths: Random sampling attempts
CREATE TABLE tb_attempt_paths (
    id INTEGER AUTO_INCREMENT PRIMARY KEY,
    run_id VARCHAR(36) NOT NULL,
    path_definition_id INTEGER NOT NULL,

    start_node_id BIGINT NOT NULL,
    end_node_id BIGINT NOT NULL,

    building_code VARCHAR(10),          -- NULL for scenario-based paths
    category VARCHAR(64),
    utility VARCHAR(128),
    toolset VARCHAR(128),               -- NULL for scenario-based paths

    picked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    notes VARCHAR(512),
    
    FOREIGN KEY (run_id) REFERENCES tb_runs(id) ON DELETE CASCADE,
    FOREIGN KEY (path_definition_id) REFERENCES tb_path_definitions(id) ON DELETE CASCADE,
    FOREIGN KEY (building_code) REFERENCES tb_buildings(code),
    INDEX idx_attempt_run (run_id),
    INDEX idx_attempt_building (building_code)
);

-- 8. Scenario Executions: Scenario test executions
CREATE TABLE tb_scenario_executions (
    id INTEGER AUTO_INCREMENT PRIMARY KEY,
    run_id VARCHAR(36) NOT NULL,
    scenario_id INTEGER NOT NULL,
    path_definition_id INTEGER,         -- NULL if scenario failed to generate path

    execution_status VARCHAR(20) NOT NULL, -- SUCCESS, FAILED, ERROR
    execution_time_ms INTEGER,
    
    -- Results
    actual_nodes INTEGER,
    actual_links INTEGER,
    actual_coverage FLOAT,
    
    -- Validation results
    validation_passed BOOLEAN,
    validation_errors TEXT,             -- JSON array of validation errors
    
    executed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    notes TEXT,
    
    FOREIGN KEY (run_id) REFERENCES tb_runs(id) ON DELETE CASCADE,
    FOREIGN KEY (scenario_id) REFERENCES tb_scenarios(id),
    FOREIGN KEY (path_definition_id) REFERENCES tb_path_definitions(id),
    INDEX idx_scenario_exec_run (run_id),
    INDEX idx_scenario_exec_scenario (scenario_id)
);

-- 9. Path Tags: Enhanced with source tracking
CREATE TABLE tb_path_tags (
    id INTEGER AUTO_INCREMENT PRIMARY KEY,
    path_definition_id INTEGER NOT NULL,
    run_id VARCHAR(36),
    path_hash VARCHAR(128),

    tag_type VARCHAR(16) NOT NULL,      -- QA, RISK, INS, CRIT, UTY, CAT, DAT, FAB, SCENARIO
    tag_code VARCHAR(64) NOT NULL,
    tag_value VARCHAR(256),
    
    -- Tag metadata
    source VARCHAR(20),                 -- SYSTEM, USER, VALIDATION
    confidence FLOAT DEFAULT 1.0,      -- Confidence score for auto-generated tags
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(64),
    notes VARCHAR(512),
    
    FOREIGN KEY (path_definition_id) REFERENCES tb_path_definitions(id) ON DELETE CASCADE,
    FOREIGN KEY (run_id) REFERENCES tb_runs(id),
    INDEX idx_path_tags_definition (path_definition_id),
    INDEX idx_path_tags_type (tag_type, tag_code)
);

-- 10. Validation Tests: Enhanced validation framework
CREATE TABLE tb_validation_tests (
    id INTEGER AUTO_INCREMENT PRIMARY KEY,
    code VARCHAR(32) UNIQUE NOT NULL,
    name VARCHAR(128) NOT NULL,
    description VARCHAR(512),

    scope VARCHAR(32) NOT NULL,         -- FLOW, CONNECTIVITY, MATERIAL, QA, SCENARIO
    severity VARCHAR(16) NOT NULL,      -- LOW, MEDIUM, HIGH, CRITICAL
    test_type VARCHAR(32),              -- STRUCTURAL, LOGICAL, PERFORMANCE, COMPLIANCE
    
    -- Applicability
    applies_to_random BOOLEAN DEFAULT TRUE,
    applies_to_scenario BOOLEAN DEFAULT TRUE,
    building_specific BOOLEAN DEFAULT FALSE,
    
    -- Test configuration
    test_config TEXT,                   -- JSON configuration for the test
    
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

-- 11. Validation Errors: Enhanced error tracking
CREATE TABLE tb_validation_errors (
    id INTEGER AUTO_INCREMENT PRIMARY KEY,
    run_id VARCHAR(36) NOT NULL,
    path_definition_id INTEGER,
    validation_test_id INTEGER,

    severity VARCHAR(16) NOT NULL,
    error_scope VARCHAR(64) NOT NULL,
    error_type VARCHAR(64) NOT NULL,
    object_type VARCHAR(16) NOT NULL,   -- NODE, LINK, PATH, SCENARIO

    -- Object references
    node_id BIGINT,
    link_id BIGINT,
    scenario_id INTEGER,

    -- Context information
    building_code VARCHAR(10),
    category VARCHAR(64),
    utility VARCHAR(128),
    material VARCHAR(64),
    flow VARCHAR(32),
    item_name VARCHAR(128),

    -- Error details
    error_message TEXT,
    error_data TEXT,                    -- JSON object with additional error data
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    notes VARCHAR(512),
    
    FOREIGN KEY (run_id) REFERENCES tb_runs(id) ON DELETE CASCADE,
    FOREIGN KEY (path_definition_id) REFERENCES tb_path_definitions(id) ON DELETE CASCADE,
    FOREIGN KEY (validation_test_id) REFERENCES tb_validation_tests(id),
    FOREIGN KEY (building_code) REFERENCES tb_buildings(code),
    FOREIGN KEY (scenario_id) REFERENCES tb_scenarios(id),
    INDEX idx_validation_errors_run (run_id),
    INDEX idx_validation_errors_severity (severity),
    INDEX idx_validation_errors_type (error_type)
);

-- 12. Review Flags: Enhanced flagging system
CREATE TABLE tb_review_flags (
    id INTEGER AUTO_INCREMENT PRIMARY KEY,
    run_id VARCHAR(36) NOT NULL,
    
    flag_type VARCHAR(32) NOT NULL,     -- MANUAL_REVIEW, CRITICAL_ERROR, PERFORMANCE, ANOMALY
    severity VARCHAR(16) NOT NULL,
    reason VARCHAR(256) NOT NULL,
    object_type VARCHAR(16) NOT NULL,

    -- Object references
    start_node_id BIGINT,
    end_node_id BIGINT,
    link_id BIGINT,
    path_definition_id INTEGER,
    scenario_id INTEGER,

    -- Context
    building_code VARCHAR(10),
    utility VARCHAR(128),
    material VARCHAR(64),
    flow VARCHAR(32),
    
    -- Flag details
    path_context TEXT,                  -- JSON object with path context
    flag_data TEXT,                     -- JSON object with additional flag data
    
    -- Flag lifecycle
    status VARCHAR(20) DEFAULT 'OPEN', -- OPEN, ACKNOWLEDGED, RESOLVED, DISMISSED
    assigned_to VARCHAR(64),
    resolved_at TIMESTAMP,
    resolution_notes TEXT,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    notes VARCHAR(512),
    
    FOREIGN KEY (run_id) REFERENCES tb_runs(id) ON DELETE CASCADE,
    FOREIGN KEY (path_definition_id) REFERENCES tb_path_definitions(id),
    FOREIGN KEY (building_code) REFERENCES tb_buildings(code),
    FOREIGN KEY (scenario_id) REFERENCES tb_scenarios(id),
    INDEX idx_review_flags_run (run_id),
    INDEX idx_review_flags_status (status),
    INDEX idx_review_flags_severity (severity)
);

-- 13. Run Summaries: Enhanced aggregated metrics
CREATE TABLE tb_run_summaries (
    run_id VARCHAR(36) PRIMARY KEY,
    
    -- Basic metrics
    total_attempts INTEGER NOT NULL,
    total_paths_found INTEGER NOT NULL,
    unique_paths INTEGER NOT NULL,
    
    -- Approach-specific metrics
    total_scenario_tests INTEGER NOT NULL DEFAULT 0,
    scenario_success_rate FLOAT,
    
    -- Quality metrics
    total_errors INTEGER NOT NULL,
    total_review_flags INTEGER NOT NULL,
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
    summarized_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (run_id) REFERENCES tb_runs(id) ON DELETE CASCADE
);


-- Indexes for performance
CREATE INDEX idx_runs_approach_status ON tb_runs(approach, status);
CREATE INDEX idx_runs_building_date ON tb_runs(fab, date);
CREATE INDEX idx_runs_scenario ON tb_runs(scenario_code, scenario_type);
CREATE INDEX idx_runs_execution_mode ON tb_runs(execution_mode, verbose_mode);
```
