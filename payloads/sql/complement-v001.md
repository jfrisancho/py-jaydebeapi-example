```python
# In your random service
def _select_toolset(self, fab: str, toolset_code: str = "") -> Optional[Dict]:
    """Select toolset using composite key."""
    if toolset_code and toolset_code != "ALL":
        sql = """
        SELECT code, fab, phase FROM tb_toolsets 
        WHERE fab = ? AND code = ? AND is_active = TRUE 
        ORDER BY RAND() LIMIT 1
        """
        params = [fab, toolset_code]
    else:
        sql = """
        SELECT code, fab, phase FROM tb_toolsets 
        WHERE fab = ? AND is_active = TRUE 
        ORDER BY RAND() LIMIT 1
        """
        params = [fab]
    
    # Query returns the composite key components
    result = self.db.query(sql, params)
    if result:
        return {'code': result[0][0], 'fab': result[0][1], 'phase': result[0][2]}
    return None

# Equipment FK references all three components
def _select_equipment_pair(self, toolset: Dict) -> Optional[Tuple[Dict, Dict]]:
    sql = """
    SELECT id, name, guid, node_id, kind FROM tb_equipment
    WHERE fab = ? AND toolset_code = ? AND phase = ? AND is_active = TRUE
    ORDER BY RAND()
    """
    results = self.db.query(sql, [toolset['fab'], toolset['code'], toolset['phase']])
    # ... rest of the method
```
