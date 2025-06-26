def extract_poc_code(eq_poc_no: str) -> Optional[str]:
    """
    Extract POC code from eq_poc_no field based on client notation.
    
    Rules:
    1. If eq_poc_no is null/empty -> return None (code will be null)
    2. If eq_poc_no has whitespaces or starts with "PoC " -> report as invalid format
    3. If eq_poc_no is single element -> return that element as code
    4. If eq_poc_no is comma-separated list -> return first element as code
    
    Examples:
    - None/empty -> None
    - "X1" -> "X1"
    - "X1,X2,X3" -> "X1" (branches case)
    - "Y1,Y1(1),Y1(2)" -> "Y1" (different sizes case)
    - "PoC 123" -> None (invalid format, will be reported)
    - " X1 " -> None (invalid format due to whitespaces)
    
    Args:
        eq_poc_no: Raw equipment POC number from client
    
    Returns:
        Extracted POC code or None if invalid/null
    """
    # If eq_poc_no is null or empty, return None
    if not eq_poc_no:
        return None
    
    # Convert to string and check for invalid formatting
    eq_poc_str = str(eq_poc_no)
    
    # Check for whitespaces (invalid format)
    if ' ' in eq_poc_str:
        print(f"⚠ Invalid POC format (contains whitespaces): '{eq_poc_str}'")
        return None
    
    # Check if starts with "PoC " pattern (case insensitive)
    if eq_poc_str.lower().startswith('poc '):
        print(f"⚠ Invalid POC format (starts with 'PoC '): '{eq_poc_str}'")
        return None
    
    # Handle comma-separated list - take first element
    if ',' in eq_poc_str:
        first_element = eq_poc_str.split(',')[0].strip()
        
        # Validate first element doesn't have whitespaces after splitting
        if ' ' in first_element:
            print(f"⚠ Invalid POC format (first element contains whitespaces): '{eq_poc_str}'")
            return None
        
        return first_element if first_element else None
    
    # Single element case
    return eq_poc_str


def determine_flow_direction(eq_poc_no: str, utility: str, nwo_type: str) -> Optional[str]:
    """
    Determine flow direction (IN/OUT) based on available data.
    
    Args:
        eq_poc_no: Raw equipment POC number
        utility: Utility type
        nwo_type: Network object type
    
    Returns:
        Flow direction ('IN', 'OUT') or None
    """
    if not eq_poc_no and not nwo_type:
        return None
    
    # Check nwo_type first
    if nwo_type:
        nwo_upper = nwo_type.upper()
        if 'IN' in nwo_upper:
            return 'IN'
        elif 'OUT' in nwo_upper:
            return 'OUT'
    
    # Check eq_poc_no
    if eq_poc_no:
        poc_upper = str(eq_poc_no).upper()
        if poc_upper.startswith('IN'):
            return 'IN'
        elif poc_upper.startswith('OUT'):
            return 'OUT'
    
    return None


def transform_poc_data(raw_data: List[Tuple], equipment_mapping: Dict[str, int]) -> List[Tuple]:
    """
    Transform raw POC data into format for tb_equipment_pocs.
    
    Args:
        raw_data: Raw data from source query
        equipment_mapping: Mapping of equipment GUID to equipment ID
    
    Returns:
        List of tuples: (equipment_id, node_id, code, eq_poc_no, is_used, utility, flow)
    """
    transformed_data = []
    missing_equipment = set()
    invalid_poc_formats = []
    
    for row in raw_data:
        node_id, equipment_guid, toolset, eq_poc_no, utility, nwo_type, category, connection_status = row
        
        # Look up equipment ID
        equipment_id = equipment_mapping.get(equipment_guid)
        
        if not equipment_id:
            missing_equipment.add(equipment_guid)
            continue
        
        # Extract POC code using the corrected logic
        code = extract_poc_code(eq_poc_no)
        
        # Determine if used
        is_used = 1 if connection_status == 'USED' else 0
        
        # Determine flow direction
        flow = determine_flow_direction(eq_poc_no, utility, nwo_type)
        
        # Clean utility - only set if POC is used
        cleaned_utility = utility.strip() if (utility and is_used) else None
        
        # Store original eq_poc_no for reference
        original_eq_poc_no = eq_poc_no
        
        transformed_data.append((
            equipment_id, node_id, code, original_eq_poc_no, is_used, cleaned_utility, flow
        ))
    
    if missing_equipment:
        print(f"⚠ Warning: {len(missing_equipment)} POCs reference missing equipment:")
        for guid in sorted(list(missing_equipment)[:10]):  # Show first 10
            print(f"  - {guid}")
        if len(missing_equipment) > 10:
            print(f"  - ... and {len(missing_equipment) - 10} more")
        print("  These POCs will be skipped. Ensure equipments are loaded first.")
    
    return transformed_data
