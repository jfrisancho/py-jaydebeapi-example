```sql
-- Table to store batch processing errors
CREATE TABLE tb_batch_errors (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_type VARCHAR(50) NOT NULL,           -- e.g., 'toolsets', 'equipments', 'equipment_pocs'
    batch_run_id VARCHAR(100),                 -- Optional: unique identifier for each batch run
    error_type VARCHAR(50) NOT NULL,           -- e.g., 'validation', 'insertion', 'deletion', 'fetch'
    error_code VARCHAR(20),                    -- Optional: specific error code
    error_message TEXT NOT NULL,               -- The actual error message
    error_details TEXT,                        -- Additional error context/stack trace
    record_identifier VARCHAR(255),            -- Identifier of the problematic record (e.g., code, id)
    record_data TEXT,                          -- JSON or string representation of the problematic record
    severity VARCHAR(20) DEFAULT 'ERROR',      -- 'ERROR', 'WARNING', 'CRITICAL'
    resolved BOOLEAN DEFAULT FALSE,            -- Whether the error has been resolved
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for better query performance
CREATE INDEX idx_batch_errors_type ON tb_batch_errors(batch_type);
CREATE INDEX idx_batch_errors_run_id ON tb_batch_errors(batch_run_id);
CREATE INDEX idx_batch_errors_severity ON tb_batch_errors(severity);
CREATE INDEX idx_batch_errors_resolved ON tb_batch_errors(resolved);
CREATE INDEX idx_batch_errors_created_at ON tb_batch_errors(created_at);
```
