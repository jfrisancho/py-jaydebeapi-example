def fetch_available_fabs(db: Database) -> List[str]:
    """Get available fabs from database."""
    try:
        # First try to get from existing runs
        sql = "SELECT DISTINCT fab FROM tb_runs WHERE fab IS NOT NULL ORDER BY fab"
        results = db.query(sql)
        fabs = [row[0] for row in results] if results else []
        
        # If no runs exist, try to get from network nodes (if building_code exists)
        if not fabs:
            try:
                sql = "SELECT DISTINCT building_code FROM nw_nodes WHERE building_code IS NOT NULL ORDER BY building_code"
                results = db.query(sql)
                fabs = [row[0] for row in results] if results else []
            except Exception:
                pass  # Fall back to defaults
        
        # Add some common default fabs if none found
        if not fabs:
            fabs = ["M16", "M15", "M15X"]
        
        return fabs
    except Exception as e:
        return ["M16", "M15", "M15X"]  # Default options


def fetch_available_scenarios(db: Database) -> List[str]:
    """Get available scenarios from database."""
    try:
        # Use the scenarios table we defined in schema
        sql = "SELECT DISTINCT code FROM tb_scenarios WHERE is_active = TRUE ORDER BY code"
        results = db.query(sql)
        scenarios = [row[0] for row in results] if results else []
        
        # Add some default options if none found
        if not scenarios:
            scenarios = ["PRE001", "PRE002", "SYN001", "SYN002"]
        
        return scenarios
    except Exception as e:
        return ["PRE001", "PRE002", "SYN001", "SYN002"]


def fetch_available_toolsets(db: Database, fab: str) -> List[str]:
    """Get available toolsets for a specific fab using nw_nodes."""
    try:
        # Query nw_nodes for equipment with toolset_code
        sql = """
        SELECT DISTINCT toolset_code 
        FROM nw_nodes 
        WHERE category = 'EQUIPMENT' 
        AND toolset_code IS NOT NULL
        ORDER BY toolset_code
        """
        results = db.query(sql)
        toolsets = [row[0] for row in results] if results else []
        
        # Add "ALL" option and empty option
        options = ["ALL", ""]
        if toolsets:
            options.extend(toolsets)
        else:
            # Default toolsets if none found
            options.extend(["TOOLSET_001", "TOOLSET_002", "TOOLSET_003"])
        
        return options
    except Exception as e:
        return ["ALL", "", "TOOLSET_001", "TOOLSET_002", "TOOLSET_003"]


def fetch_equipment_for_toolset(db: Database, toolset_code: str) -> List[dict]:
    """Get equipment from nw_nodes for a specific toolset."""
    try:
        sql = """
        SELECT id, name, toolset_code, attributes
        FROM nw_nodes 
        WHERE category = 'EQUIPMENT' 
        AND toolset_code = ?
        ORDER BY name
        """
        results = db.query(sql, [toolset_code])
        
        equipment = []
        for row in results:
            equipment.append({
                'id': row[0],
                'name': row[1],
                'toolset_code': row[2],
                'attributes': row[3]  # This might contain JSON with additional info
            })
        
        return equipment
    except Exception as e:
        return []


def fetch_nodes_for_equipment(db: Database, equipment_id: int) -> List[int]:
    """Get point of contact nodes for specific equipment."""
    try:
        # This assumes the equipment node itself can be a point of contact
        # Or there might be related nodes - adjust based on your data model
        sql = """
        SELECT id 
        FROM nw_nodes 
        WHERE id = ? 
        OR parent_equipment_id = ?
        """
        results = db.query(sql, [equipment_id, equipment_id])
        return [row[0] for row in results] if results else [equipment_id]
    except Exception as e:
        return [equipment_id] if equipment_id else []