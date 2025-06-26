```sql
CREATE TABLE tb_equipment_connections (
    id INTEGER AUTO_INCREMENT PRIMARY KEY,

    from_equipment_id INTEGER NOT NULL REFERENCES tb_equipments(id) ON DELETE CASCADE,
    to_equipment_id   INTEGER NOT NULL REFERENCES tb_equipments(id) ON DELETE CASCADE,

    from_poc_id INTEGER NOT NULL REFERENCES tb_equipment_pocs(id) ON DELETE CASCADE,
    to_poc_id   INTEGER NOT NULL REFERENCES tb_equipment_pocs(id) ON DELETE CASCADE,

    path_id INTEGER NOT NULL,  -- ID returned by nw_shortest_path
    is_valid BIT(1) NOT NULL,  -- Mark whether the path is usable or blocked

    path_length_mm INTEGER,     -- Optional: from metadata of path
    link_count INTEGER,         -- Optional: number of links in the path
    node_count INTEGER,         -- Optional: number of nodes

    connection_type VARCHAR(16), -- Optional: STRAIGHT, BRANCHED, LOOPBACK, etc.
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_eq_conn_from_to ON tb_equipment_connections (from_equipment_id, to_equipment_id);
CREATE INDEX idx_eq_conn_path_id ON tb_equipment_connections (path_id);
```

```sql
SELECT 
    ec.id,
    fe.name AS from_equipment,
    te.name AS to_equipment,
    fp.reference AS from_poc,
    tp.reference AS to_poc,
    ec.path_id,
    ec.path_length_mm
FROM tb_equipment_connections ec
JOIN tb_equipments fe ON ec.from_equipment_id = fe.id
JOIN tb_equipments te ON ec.to_equipment_id = te.id
JOIN tb_equipment_pocs fp ON ec.from_poc_id = fp.id
JOIN tb_equipment_pocs tp ON ec.to_poc_id = tp.id
ORDER BY fe.id, te.id;
```