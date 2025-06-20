this is a test:

```sql
-- Updated Database Schema for Path Analysis CLI v2.0
-- Supports Building enum, enhanced scenario handling, and improved run tracking

-- 1. Runs: CLI execution metadata and coverage summary
CREATE TABLE tb_runs (
    id VARCHAR(36) PRIMARY KEY,
    date DATE NOT NULL,
    approach VARCHAR(20) NOT NULL,      -- RANDOM, SCENARIO
    method VARCHAR(20) NOT NULL,        -- SIMPLE, STRATIFIED, PREDEFINED, SYNTHETIC
    coverage_target FLOAT NOT NULL,     -- Only relevant for RANDOM approach

    total_coverage FLOAT NOT NULL,
    total_nodes INTEGER NOT NULL,
    total_links INTEGER NOT NULL,

    fab VARCHAR(64),                    -- Building identifier (M15, M15X, M16) - NULL for SCENARIO
    toolset VARCHAR(128),               -- Toolset identifier - NULL for SCENARIO
    
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
    verbose_mode BOOLEAN DEFAULT FALSE,
    
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

-- 2. Buildings: Valid building/fab identifiers
CREATE TABLE tb_buildings (
    code VARCHAR(10) PRIMARY KEY,       -- M15, M15X, M16
    name VARCHAR(64) NOT NULL,          -- Full building name
    description VARCHAR(256),
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert default buildings
INSERT INTO tb_buildings (code, name, description) VALUES
('M15', 'Building M15', 'Manufacturing Building M15'),
('M15X', 'Building M15X', 'Manufacturing Building M15X (Extended)'),
('M16', 'Building M16', 'Manufacturing Building M16');

-- 3. Scenarios: Predefined and synthetic scenarios
CREATE TABLE tb_scenarios (
    id INTEGER AUTO_INCREMENT PRIMARY KEY,
    code VARCHAR(128) UNIQUE NOT NULL,  -- PREXXXXXXX, SYNXXXXXXX
    name VARCHAR(128) NOT NULL,
    description VARCHAR(512),
    
    scenario_type VARCHAR(20) NOT NULL, -- PREDEFINED, SYNTHETIC
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

-- 4. Toolsets: Enhanced with building relationships
CREATE TABLE tb_toolsets (
    id INTEGER AUTO_INCREMENT PRIMARY KEY,
    toolset_id VARCHAR(128) NOT NULL,
    name VARCHAR(128) NOT NULL,
    building_code VARCHAR(10) NOT NULL, -- References tb_buildings.code
    category VARCHAR(64) NOT NULL,
    utility_codes TEXT,                 -- JSON array of utility codes
    
    -- Toolset metadata
    description VARCHAR(512),
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (building_code) REFERENCES tb_buildings(code),
    UNIQUE KEY uk_toolset_building (toolset_id, building_code)
);

-- 5. Equipment: Enhanced equipment definitions
CREATE TABLE tb_equipment (
    id INTEGER AUTO_INCREMENT PRIMARY KEY,
    equipment_id VARCHAR(128) NOT NULL,
    name VARCHAR(128) NOT NULL,
    toolset_id VARCHAR(128) NOT NULL,
    building_code VARCHAR(10) NOT NULL,
    
    category VARCHAR(64) NOT NULL,
    utility_codes TEXT,                 -- JSON array of utility codes
    poc_node_ids TEXT,                  -- JSON array of point of contact node IDs
    
    -- Equipment metadata
    description VARCHAR(512),
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (building_code) REFERENCES tb_buildings(code),
    INDEX idx_equipment_toolset (toolset_id, building_code),
    INDEX idx_equipment_building (building_code)
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

-- 14. System Configuration: Application settings
CREATE TABLE tb_system_config (
    id INTEGER AUTO_INCREMENT PRIMARY KEY,
    config_key VARCHAR(128) UNIQUE NOT NULL,
    config_value TEXT,
    config_type VARCHAR(32) NOT NULL,  -- STRING, INTEGER, FLOAT, BOOLEAN, JSON
    description VARCHAR(512),
    
    -- Configuration metadata
    category VARCHAR(64),               -- DATABASE, EXECUTION, VALIDATION, UI
    is_user_configurable BOOLEAN DEFAULT TRUE,
    requires_restart BOOLEAN DEFAULT FALSE,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

-- Insert default system configurations
INSERT INTO tb_system_config (config_key, config_value, config_type, description, category) VALUES
('default.coverage.target', '0.2', 'FLOAT', 'Default coverage target for RANDOM approach', 'EXECUTION'),
('default.building', 'M16', 'STRING', 'Default building when none specified', 'EXECUTION'),
('max.attempts.toolset', '5', 'INTEGER', 'Maximum attempts per toolset for bias mitigation', 'EXECUTION'),
('max.attempts.equipment', '3', 'INTEGER', 'Maximum attempts per equipment for bias mitigation', 'EXECUTION'),
('validation.enabled', 'true', 'BOOLEAN', 'Enable path validation', 'VALIDATION'),
('scenario.auto_detect', 'true', 'BOOLEAN', 'Auto-detect scenario type from code', 'EXECUTION');

-- Indexes for performance
CREATE INDEX idx_runs_approach_status ON tb_runs(approach, status);
CREATE INDEX idx_runs_building_date ON tb_runs(fab, date);
CREATE INDEX idx_runs_scenario ON tb_runs(scenario_code, scenario_type);
CREATE INDEX idx_runs_execution_mode ON tb_runs(execution_mode, verbose_mode);

-- Views for common queries
CREATE VIEW v_run_overview AS
SELECT 
    r.id,
    r.approach,
    r.method,
    r.fab as building_code,
    r.scenario_code,
    r.tag,
    r.status,
    r.execution_mode,
    r.started_at,
    r.ended_at,
    TIMESTAMPDIFF(SECOND, r.started_at, COALESCE(r.ended_at, NOW())) as duration_seconds,
    rs.total_attempts,
    rs.total_paths_found,
    rs.success_rate,
    rs.achieved_coverage
FROM tb_runs r
LEFT JOIN tb_run_summaries rs ON r.id = rs.run_id
ORDER BY r.started_at DESC;

CREATE VIEW v_scenario_performance AS
SELECT 
    s.code,
    s.name,
    s.scenario_type,
    COUNT(se.id) as total_executions,
    AVG(CASE WHEN se.execution_status = 'SUCCESS' THEN 1 ELSE 0 END) as success_rate,
    AVG(se.execution_time_ms) as avg_execution_time_ms,
    AVG(se.actual_coverage) as avg_coverage
FROM tb_scenarios s
LEFT JOIN tb_scenario_executions se ON s.id = se.scenario_id
WHERE s.is_active = TRUE
GROUP BY s.id, s.code, s.name, s.scenario_type;
```
