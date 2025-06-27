```sql
-- Equipment Connections Table
-- Stores which equipment connects to which equipment through which POCs
-- Based on spatial downstream analysis from each POC

CREATE TABLE tb_equipment_connections (
    id INTEGER AUTO_INCREMENT PRIMARY KEY,
    
    -- Source equipment and POC (where connection starts)
    source_equipment_id INTEGER NOT NULL,
    source_poc_id INTEGER NOT NULL,
    source_poc_code VARCHAR(8) NOT NULL,
    
    -- Target equipment and POC (where connection ends)
    target_equipment_id INTEGER NOT NULL,
    target_poc_id INTEGER NOT NULL,
    target_poc_code VARCHAR(8) NOT NULL,
    
    -- Connection metadata
    path_id INTEGER NOT NULL,           -- Reference to spatial path from nw_downstream
    link_count INTEGER NOT NULL,        -- Number of links in the spatial path
    utility VARCHAR(128),               -- Utility type (N2, CDA, PW, etc.) - NULL if mixed/unknown
    flow_direction VARCHAR(8),          -- Flow direction (IN, OUT) - NULL if unknown
    
    -- Path analysis
    has_intermediate_equipment BIT(1) NOT NULL DEFAULT 0,  -- TRUE if path passes through other equipment
    intermediate_count INTEGER DEFAULT 0,                  -- Number of intermediate equipment in path
    
    -- Audit fields
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Foreign key constraints
    CONSTRAINT fk_connections_source_equipment 
        FOREIGN KEY (source_equipment_id) REFERENCES tb_equipments(id) ON DELETE CASCADE,
    CONSTRAINT fk_connections_source_poc 
        FOREIGN KEY (source_poc_id) REFERENCES tb_equipment_pocs(id) ON DELETE CASCADE,
    CONSTRAINT fk_connections_target_equipment 
        FOREIGN KEY (target_equipment_id) REFERENCES tb_equipments(id) ON DELETE CASCADE,
    CONSTRAINT fk_connections_target_poc 
        FOREIGN KEY (target_poc_id) REFERENCES tb_equipment_pocs(id) ON DELETE CASCADE,
    
    -- Unique constraint to prevent duplicate connections
    CONSTRAINT uk_equipment_connections 
        UNIQUE (source_equipment_id, source_poc_id, target_equipment_id, target_poc_id)
);

-- Indexes for performance
CREATE INDEX idx_connections_source_eq ON tb_equipment_connections (source_equipment_id);
CREATE INDEX idx_connections_target_eq ON tb_equipment_connections (target_equipment_id);
CREATE INDEX idx_connections_source_poc ON tb_equipment_connections (source_poc_id);
CREATE INDEX idx_connections_target_poc ON tb_equipment_connections (target_poc_id);
CREATE INDEX idx_connections_utility ON tb_equipment_connections (utility);
CREATE INDEX idx_connections_flow ON tb_equipment_connections (flow_direction);
CREATE INDEX idx_connections_path ON tb_equipment_connections (path_id);
CREATE INDEX idx_connections_intermediate ON tb_equipment_connections (has_intermediate_equipment);

-- Composite indexes for common queries
CREATE INDEX idx_connections_source_composite ON tb_equipment_connections (source_equipment_id, utility, flow_direction);
CREATE INDEX idx_connections_target_composite ON tb_equipment_connections (target_equipment_id, utility, flow_direction);

-- Comments for documentation
ALTER TABLE tb_equipment_connections COMMENT = 'Equipment connectivity matrix based on spatial downstream analysis';
ALTER TABLE tb_equipment_connections MODIFY COLUMN source_equipment_id INTEGER NOT NULL COMMENT 'Source equipment ID (where connection originates)';
ALTER TABLE tb_equipment_connections MODIFY COLUMN source_poc_id INTEGER NOT NULL COMMENT 'Source POC ID (specific POC where connection starts)';
ALTER TABLE tb_equipment_connections MODIFY COLUMN source_poc_code VARCHAR(8) NOT NULL COMMENT 'Source POC code (P1, P2, IN01, OUT01, etc.)';
ALTER TABLE tb_equipment_connections MODIFY COLUMN target_equipment_id INTEGER NOT NULL COMMENT 'Target equipment ID (where connection terminates)';
ALTER TABLE tb_equipment_connections MODIFY COLUMN target_poc_id INTEGER NOT NULL COMMENT 'Target POC ID (specific POC where connection ends)';
ALTER TABLE tb_equipment_connections MODIFY COLUMN target_poc_code VARCHAR(8) NOT NULL COMMENT 'Target POC code (P1, P2, IN01, OUT01, etc.)';
ALTER TABLE tb_equipment_connections MODIFY COLUMN path_id INTEGER NOT NULL COMMENT 'Spatial path ID from nw_downstream analysis';
ALTER TABLE tb_equipment_connections MODIFY COLUMN link_count INTEGER NOT NULL COMMENT 'Number of spatial links in the connection path';
ALTER TABLE tb_equipment_connections MODIFY COLUMN utility VARCHAR(128) COMMENT 'Utility type if uniform across path (N2, CDA, PW, etc.)';
ALTER TABLE tb_equipment_connections MODIFY COLUMN flow_direction VARCHAR(8) COMMENT 'Flow direction from source POC (IN, OUT)';
ALTER TABLE tb_equipment_connections MODIFY COLUMN has_intermediate_equipment BIT(1) NOT NULL DEFAULT 0 COMMENT 'TRUE if other equipment exist between source and target';
ALTER TABLE tb_equipment_connections MODIFY COLUMN intermediate_count INTEGER DEFAULT 0 COMMENT 'Count of intermediate equipment in the path';
```

new

```sql
-- Equipment Connections Table
-- Stores which equipment connects to which equipment through which POCs
-- Based on spatial path analysis (nw_shortest_path or nw_downstream)

CREATE TABLE tb_equipment_connections (
    id INTEGER AUTO_INCREMENT PRIMARY KEY,

    -- Equipment endpoints
    from_equipment_id INTEGER NOT NULL REFERENCES tb_equipments(id) ON DELETE CASCADE,
    to_equipment_id   INTEGER NOT NULL REFERENCES tb_equipments(id) ON DELETE CASCADE,

    -- POC endpoints  
    from_poc_id INTEGER NOT NULL REFERENCES tb_equipment_pocs(id) ON DELETE CASCADE,
    to_poc_id   INTEGER NOT NULL REFERENCES tb_equipment_pocs(id) ON DELETE CASCADE,

    -- Spatial path reference
    path_id INTEGER NOT NULL,  -- ID returned by nw_shortest_path or nw_downstream
    is_valid BIT(1) NOT NULL DEFAULT 1,  -- Mark whether the path is usable or blocked

    -- Path metadata (optional, can be populated from spatial analysis)
    path_length_mm INTEGER,     -- Physical length from path metadata
    link_count INTEGER,         -- Number of links in the path
    node_count INTEGER,         -- Number of nodes in the path

    -- Connection classification
    connection_type VARCHAR(16), -- STRAIGHT, BRANCHED, LOOPBACK, INTERMEDIATE, etc.
    
    -- Audit
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Constraints
    UNIQUE KEY uk_equipment_poc_connection (from_equipment_id, from_poc_id, to_equipment_id, to_poc_id),
    
    -- Prevent self-connections at equipment level (optional business rule)
    CONSTRAINT chk_no_self_connection CHECK (from_equipment_id != to_equipment_id)
);

-- Performance indexes
CREATE INDEX idx_eq_conn_from_to ON tb_equipment_connections (from_equipment_id, to_equipment_id);
CREATE INDEX idx_eq_conn_from_eq ON tb_equipment_connections (from_equipment_id);
CREATE INDEX idx_eq_conn_to_eq ON tb_equipment_connections (to_equipment_id);
CREATE INDEX idx_eq_conn_path_id ON tb_equipment_connections (path_id);
CREATE INDEX idx_eq_conn_valid ON tb_equipment_connections (is_valid);
CREATE INDEX idx_eq_conn_type ON tb_equipment_connections (connection_type);

-- Comments for documentation
ALTER TABLE tb_equipment_connections COMMENT = 'Equipment connectivity matrix based on spatial path analysis';
ALTER TABLE tb_equipment_connections MODIFY COLUMN from_equipment_id INTEGER NOT NULL COMMENT 'Source equipment in the connection';
ALTER TABLE tb_equipment_connections MODIFY COLUMN to_equipment_id INTEGER NOT NULL COMMENT 'Target equipment in the connection';
ALTER TABLE tb_equipment_connections MODIFY COLUMN from_poc_id INTEGER NOT NULL COMMENT 'Source POC where connection originates';
ALTER TABLE tb_equipment_connections MODIFY COLUMN to_poc_id INTEGER NOT NULL COMMENT 'Target POC where connection terminates';
ALTER TABLE tb_equipment_connections MODIFY COLUMN path_id INTEGER NOT NULL COMMENT 'Spatial path ID from network analysis';
ALTER TABLE tb_equipment_connections MODIFY COLUMN is_valid BIT(1) NOT NULL DEFAULT 1 COMMENT 'Whether the connection path is currently valid/usable';
ALTER TABLE tb_equipment_connections MODIFY COLUMN path_length_mm INTEGER COMMENT 'Physical path length in millimeters';
ALTER TABLE tb_equipment_connections MODIFY COLUMN link_count INTEGER COMMENT 'Number of spatial links in the path';
ALTER TABLE tb_equipment_connections MODIFY COLUMN node_count INTEGER COMMENT 'Number of spatial nodes in the path';
ALTER TABLE tb_equipment_connections MODIFY COLUMN connection_type VARCHAR(16) COMMENT 'Connection classification (STRAIGHT, BRANCHED, LOOPBACK, etc.)';
```

first
```sql
CREATE TABLE tb_equipment_connections (
    id INTEGER AUTO_INCREMENT PRIMARY KEY,

    from_equipment_id INTEGER NOT NULL REFERENCES tb_equipments(id) ON DELETE CASCADE,
    to_equipment_id   INTEGER NOT NULL REFERENCES tb_equipments(id) ON DELETE CASCADE,

    from_poc_id INTEGER NOT NULL REFERENCES tb_equipment_pocs(id) ON DELETE CASCADE,
    to_poc_id   INTEGER NOT NULL REFERENCES tb_equipment_pocs(id) ON DELETE CASCADE,

    path_id INTEGER NOT NULL,  -- ID returned by nw_shortest_path
    is_valid BIT(1) NOT NULL DEFAULT 1,  -- Mark whether the path is usable or blocked

    path_length_mm INTEGER,     -- Optional: from metadata of path
    link_count INTEGER,         -- Optional: number of links in the path
    node_count INTEGER,         -- Optional: number of nodes

    connection_type VARCHAR(16), -- Optional: STRAIGHT, BRANCHED, LOOPBACK, etc.
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_eq_conn_from_to ON tb_equipment_connections (from_equipment_id, to_equipment_id);
CREATE INDEX idx_eq_conn_path_id ON tb_equipment_connections (path_id);
```
