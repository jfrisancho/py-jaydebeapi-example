```sql
-- OPTION 1: Composite Primary Key (Best for your case)
-- ✅ RECOMMENDED APPROACH
CREATE TABLE tb_toolsets (
    code VARCHAR(64) NOT NULL,
    fab VARCHAR(10) NOT NULL,
    phase VARCHAR(8) NOT NULL,
    
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    PRIMARY KEY (fab, code, phase),  -- Composite PK ensures uniqueness
    INDEX idx_toolsets_code (code),  -- For queries by code across fabs/phases
    INDEX idx_toolsets_fab (fab)     -- For queries by fab
);

-- OPTION 2: Auto-increment ID + Unique Constraint
-- ✅ ALSO GOOD (More traditional approach)
CREATE TABLE tb_toolsets (
    id INTEGER AUTO_INCREMENT PRIMARY KEY,  -- Surrogate key
    code VARCHAR(64) NOT NULL,
    fab VARCHAR(10) NOT NULL,
    phase VARCHAR(8) NOT NULL,
    
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE KEY uk_fab_code_phase (fab, code, phase),  -- Business uniqueness
    INDEX idx_toolsets_code (code),
    INDEX idx_toolsets_fab (fab)
);

-- OPTION 3: Natural Compound Key (What you had before)
-- ❌ NOT RECOMMENDED for this case
CREATE TABLE tb_toolsets (
    code VARCHAR(64) PRIMARY KEY,  -- This doesn't work because code isn't unique!
    fab VARCHAR(10) NOT NULL,
    phase VARCHAR(8) NOT NULL,
    -- This will cause primary key violations
);

-- COMPARISON TABLE
/*
+------------------+----------------+------------------+------------------+
| Approach         | Pros           | Cons             | Best For         |
+------------------+----------------+------------------+------------------+
| Composite PK     | - Natural      | - Complex joins  | - Small tables   |
| (fab,code,phase) | - No extra ID  | - Longer foreign | - Natural keys   |
|                  | - Self-docu.   |   keys           | - Your use case  |
+------------------+----------------+------------------+------------------+
| Auto-increment   | - Simple joins | - Extra column   | - Large tables   |
| ID + Unique      | - Standard     | - Less natural   | - Many FKs       |
|                  | - Tool support | - Index overhead | - General use    |
+------------------+----------------+------------------+------------------+
*/

-- FOR YOUR SPECIFIC CASE, I RECOMMEND OPTION 1 (Composite PK)
-- Because:
-- 1. toolsets table will be relatively small
-- 2. (fab, code, phase) naturally defines uniqueness
-- 3. Your queries often filter by fab and/or code
-- 4. No need for extra surrogate key

-- Updated equipment table with composite FK
CREATE TABLE tb_equipment (
    id INTEGER AUTO_INCREMENT PRIMARY KEY,
    toolset_code VARCHAR(64) NOT NULL,
    fab VARCHAR(10) NOT NULL,
    phase VARCHAR(8) NOT NULL,
    name VARCHAR(128) NOT NULL,
    guid VARCHAR(64) NOT NULL,
    node_id INTEGER NOT NULL,
    kind VARCHAR(32),
    
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Foreign key references composite primary key
    FOREIGN KEY (fab, toolset_code, phase) 
        REFERENCES tb_toolsets(fab, code, phase),
    
    INDEX idx_equipment_toolset (fab, toolset_code, phase),
    INDEX idx_equipment_fab (fab),
    UNIQUE KEY uk_equipment_guid (guid)
);

-- Sample data showing how composite PK works
INSERT INTO tb_toolsets (fab, code, phase) VALUES
('M16', 'TOOLSET_001', 'PHASE1'),  -- Valid
('M16', 'TOOLSET_001', 'PHASE2'),  -- Valid - same code, different phase
('M15', 'TOOLSET_001', 'PHASE1'),  -- Valid - same code, different fab
('M16', 'TOOLSET_002', 'PHASE1');  -- Valid - different code

-- This would fail (duplicate composite key):
-- INSERT INTO tb_toolsets (fab, code, phase) VALUES ('M16', 'TOOLSET_001', 'PHASE1');

-- Queries with composite PK
-- Query 1: Get specific toolset
SELECT * FROM tb_toolsets 
WHERE fab = 'M16' AND code = 'TOOLSET_001' AND phase = 'PHASE1';

-- Query 2: Get all phases for a toolset code in a fab
SELECT * FROM tb_toolsets 
WHERE fab = 'M16' AND code = 'TOOLSET_001';

-- Query 3: Get all toolsets for a fab
SELECT * FROM tb_toolsets 
WHERE fab = 'M16';

-- Query 4: Get all instances of a toolset code across fabs/phases
SELECT * FROM tb_toolsets 
WHERE code = 'TOOLSET_001';

-- Equipment queries with composite FK
-- Query 5: Get equipment for specific toolset
SELECT e.* FROM tb_equipment e
JOIN tb_toolsets t ON (e.fab = t.fab AND e.toolset_code = t.code AND e.phase = t.phase)
WHERE t.fab = 'M16' AND t.code = 'TOOLSET_001' AND t.phase = 'PHASE1';

-- Alternative approach if you prefer auto-increment ID
-- (This is also perfectly valid)
CREATE TABLE tb_toolsets_alternative (
    id INTEGER AUTO_INCREMENT PRIMARY KEY,
    code VARCHAR(64) NOT NULL,
    fab VARCHAR(10) NOT NULL,
    phase VARCHAR(8) NOT NULL,
    
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE KEY uk_fab_code_phase (fab, code, phase),
    INDEX idx_toolsets_code (code),
    INDEX idx_toolsets_fab (fab)
);

CREATE TABLE tb_equipment_alternative (
    id INTEGER AUTO_INCREMENT PRIMARY KEY,
    toolset_id INTEGER NOT NULL,  -- Simple integer FK
    name VARCHAR(128) NOT NULL,
    guid VARCHAR(64) NOT NULL,
    node_id INTEGER NOT NULL,
    kind VARCHAR(32),
    
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (toolset_id) REFERENCES tb_toolsets_alternative(id),
    INDEX idx_equipment_toolset (toolset_id),
    UNIQUE KEY uk_equipment_guid (guid)
);

-- With auto-increment approach, queries become simpler:
-- Get equipment for toolset
SELECT e.* FROM tb_equipment_alternative e
JOIN tb_toolsets_alternative t ON e.toolset_id = t.id
WHERE t.fab = 'M16' AND t.code = 'TOOLSET_001' AND t.phase = 'PHASE1';

-- DECISION MATRIX for your project:
/*
+------------------+------------------+------------------+
| Factor           | Composite PK     | Auto-increment   |
+------------------+------------------+------------------+
| Naturalness      | ✅ Very natural | ⚠️ Less natural  |
| Query simplicity | ⚠️ More complex  | ✅ Simpler       |
| Storage          | ✅ Less space    | ⚠️ More space    |
| Foreign keys     | ⚠️ Complex       | ✅ Simple        |
| Your use case    | ✅ Perfect fit   | ✅ Also good     |
+------------------+------------------+------------------+
*/

-- MY RECOMMENDATION FOR YOUR PROJECT:
-- Use COMPOSITE PRIMARY KEY (fab, code, phase) because:
-- 1. It perfectly matches your business logic
-- 2. Your toolsets table will be small
-- 3. Most queries will filter by fab anyway
-- 4. No unnecessary surrogate key
-- 5. Self-documenting structure

-- Final recommended schema:
CREATE TABLE tb_toolsets (
    code VARCHAR(64) NOT NULL,
    fab VARCHAR(10) NOT NULL,
    phase VARCHAR(8) NOT NULL,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (fab, code, phase)
);

CREATE TABLE tb_equipment (
    id INTEGER AUTO_INCREMENT PRIMARY KEY,
    toolset_code VARCHAR(64) NOT NULL,
    fab VARCHAR(10) NOT NULL,
    phase VARCHAR(8) NOT NULL,
    name VARCHAR(128) NOT NULL,
    guid VARCHAR(64) NOT NULL,
    node_id INTEGER NOT NULL,
    kind VARCHAR(32),
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (fab, toolset_code, phase) REFERENCES tb_toolsets(fab, code, phase),
    UNIQUE KEY uk_equipment_guid (guid)
);
```
