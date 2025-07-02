def _load_toolsets(self) -> List[Toolset]:
    """Load available toolsets from nw_nodes."""
    sql = """
    SELECT DISTINCT toolset_code, COUNT(*) as equipment_count 
    FROM nw_nodes 
    WHERE category = 'EQUIPMENT' AND toolset_code IS NOT NULL
    GROUP BY toolset_code
    ORDER BY toolset_code
    """
    # Process results...

def _load_equipment_for_toolset(self, toolset_code: str) -> List[Equipment]:
    """Load equipment from nw_nodes for a specific toolset."""
    sql = """
    SELECT id, name, toolset_code, attributes
    FROM nw_nodes 
    WHERE category = 'EQUIPMENT' AND toolset_code = ?
    ORDER BY name
    """

def fetch_available_fabs(db: Database) -> List[str]:
    """Get available fabs from nw_nodes or tb_runs."""
    try:
        # First try to get from existing runs
        sql = "SELECT DISTINCT fab FROM tb_runs WHERE fab IS NOT NULL ORDER BY fab"
        results = db.query(sql)
        if results:
            return [row[0] for row in results]
        
        # Fallback to network data if available
        sql = "SELECT DISTINCT building_code FROM nw_nodes WHERE building_code IS NOT NULL"
        results = db.query(sql)
        return [row[0] for row in results] if results else ["M16", "M15", "M14"]
    except Exception:
        return ["M16", "M15", "M14"]

def fetch_available_toolsets(db: Database, fab: str) -> List[str]:
    """Get available toolsets from nw_nodes."""
    try:
        sql = """
        SELECT DISTINCT toolset_code 
        FROM nw_nodes 
        WHERE category = 'EQUIPMENT' 
        AND toolset_code IS NOT NULL
        ORDER BY toolset_code
        """
        results = db.query(sql)
        toolsets = [row[0] for row in results] if results else []
        
        # Add standard options
        options = ["ALL", ""]
        options.extend(toolsets)
        return options
    except Exception:
        return ["ALL", "", "TOOLSET_001", "TOOLSET_002"]