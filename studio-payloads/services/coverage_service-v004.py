"""
Service for tracking coverage using bitsets for nodes and links.
Updated for simplified schema with new building/fab terminology.
"""

from typing import Set, List
from models import CoverageStats, PathDefinition


class CoverageService:
    """Service for tracking path coverage using bitsets."""
    
    def __init__(self, db):
        self.db = db
        # Bitsets for tracking covered nodes and links
        self._covered_nodes: Set[int] = set()
        self._covered_links: Set[int] = set()
        self._total_nodes = 0
        self._total_links = 0
    
    def initialize_coverage(self, building_code: str) -> CoverageStats:
        """Initialize coverage tracking for a specific building/fab."""
        self._covered_nodes.clear()
        self._covered_links.clear()
        
        # Get total node and link counts for the building
        # building_code here corresponds to 'fab' in tb_runs
        self._total_nodes, self._total_links = self._get_building_totals(building_code)
        
        return CoverageStats(
            nodes_covered=0,
            links_covered=0,
            total_nodes=self._total_nodes,
            total_links=self._total_links,
            coverage_percentage=0.0
        )
    
    def update_coverage(self, path_definition: PathDefinition, 
                       current_stats: CoverageStats) -> CoverageStats: # current_stats is not used
        """Update coverage with a new path and return updated statistics."""
        
        # Extract nodes and links from path context
        path_nodes = path_definition.path_context.get('nodes', [])
        path_links = path_definition.path_context.get('links', [])
        
        # Track new nodes and links
        # new_nodes_count = 0 # Not used
        # new_links_count = 0 # Not used
        
        for node_id in path_nodes:
            if node_id not in self._covered_nodes:
                self._covered_nodes.add(node_id)
                # new_nodes_count += 1
        
        for link_id in path_links:
            if link_id not in self._covered_links:
                self._covered_links.add(link_id)
                # new_links_count += 1
        
        # Calculate updated statistics
        total_covered = len(self._covered_nodes) + len(self._covered_links)
        total_possible = self._total_nodes + self._total_links
        
        # Ensure total_possible is not zero to prevent DivisionByZeroError
        # This check also correctly handles the case where a building might have no nodes/links.
        coverage_percentage = (total_covered / total_possible) if total_possible > 0 else 0.0
        
        return CoverageStats(
            nodes_covered=len(self._covered_nodes),
            links_covered=len(self._covered_links),
            total_nodes=self._total_nodes,
            total_links=self._total_links,
            coverage_percentage=coverage_percentage
        )
    
    def get_uncovered_areas(self, building_code: str) -> dict:
        """Get information about uncovered nodes and links."""
        
        # Get all node and link IDs for the building
        all_nodes = self._get_all_nodes(building_code)
        all_links = self._get_all_links(building_code)
        
        uncovered_nodes = [node_id for node_id in all_nodes if node_id not in self._covered_nodes]
        uncovered_links = [link_id for link_id in all_links if link_id not in self._covered_links]
        
        return {
            'uncovered_nodes': uncovered_nodes,
            'uncovered_links': uncovered_links,
            'uncovered_node_count': len(uncovered_nodes),
            'uncovered_link_count': len(uncovered_links)
        }
    
    def calculate_path_coverage_contribution(self, path_definition: PathDefinition) -> float:
        """Calculate how much coverage a path would add."""
        path_nodes = path_definition.path_context.get('nodes', [])
        path_links = path_definition.path_context.get('links', [])
        
        new_coverage_items = 0
        
        # Count new nodes
        for node_id in path_nodes:
            if node_id not in self._covered_nodes:
                new_coverage_items += 1
        
        # Count new links
        for link_id in path_links:
            if link_id not in self._covered_links:
                new_coverage_items += 1
        
        total_possible = self._total_nodes + self._total_links
        return new_coverage_items / total_possible if total_possible > 0 else 0.0
    
    def get_coverage_by_category(self, building_code: str) -> dict:
        """Get coverage statistics broken down by category."""
        # This query assumes nw_nodes and nw_links have a 'category' and 'building_code' field.
        # And a 'categories' table exists.
        sql = """
        SELECT 
            c.category,
            COUNT(DISTINCT n.id) as total_nodes,
            COUNT(DISTINCT l.id) as total_links
        FROM categories c
        LEFT JOIN nw_nodes n ON c.category = n.category AND n.building_code = ?
        LEFT JOIN nw_links l ON c.category = l.category AND l.building_code = ?
        GROUP BY c.category
        """
        
        try:
            # building_code here corresponds to 'fab'
            results = self.db.query(sql, [building_code, building_code])
            category_stats = {}
            
            for row in results:
                category = row[0]
                total_nodes_in_cat = row[1] or 0 # Ensure 0 if NULL
                total_links_in_cat = row[2] or 0 # Ensure 0 if NULL
                
                # Count covered items in this category
                covered_nodes_in_category = self._count_covered_nodes_in_category(category, building_code)
                covered_links_in_category = self._count_covered_links_in_category(category, building_code)
                
                total_category_items = total_nodes_in_cat + total_links_in_cat
                covered_category_items = covered_nodes_in_category + covered_links_in_category
                
                category_coverage_percentage = (covered_category_items / total_category_items 
                                   if total_category_items > 0 else 0.0)
                
                category_stats[category] = {
                    'total_nodes': total_nodes_in_cat,
                    'total_links': total_links_in_cat,
                    'covered_nodes': covered_nodes_in_category,
                    'covered_links': covered_links_in_category,
                    'coverage_percentage': category_coverage_percentage
                }
            
            return category_stats
            
        except Exception as e:
            # Consider more specific logging or error handling
            print(f"Error getting coverage by category for {building_code}: {e}")
            return {} # Return empty dict on error
    
    def get_coverage_gaps(self, building_code: str, min_gap_size: int = 5) -> List[dict]:
        """Identify significant coverage gaps (areas with many uncovered consecutive items)."""
        
        # This is a simplified implementation based on consecutive node IDs.
        # In practice, you'd analyze the topology to find actual gaps.
        
        uncovered_info = self.get_uncovered_areas(building_code)
        gaps = []
        
        # Group uncovered nodes by proximity (simplified approach - assumes sorted IDs)
        uncovered_nodes = sorted(list(set(uncovered_info['uncovered_nodes']))) # Ensure sorted unique nodes
        
        if not uncovered_nodes: # No uncovered nodes, so no gaps
            return gaps

        if len(uncovered_nodes) >= min_gap_size:
            consecutive_groups: List[List[int]] = []
            current_group: List[int] = [uncovered_nodes[0]]
            
            for i in range(1, len(uncovered_nodes)):
                # Check for consecutive IDs. This is a very simple definition of a "gap".
                if uncovered_nodes[i] == uncovered_nodes[i-1] + 1:
                    current_group.append(uncovered_nodes[i])
                else:
                    if len(current_group) >= min_gap_size:
                        consecutive_groups.append(list(current_group)) # Store a copy
                    current_group = [uncovered_nodes[i]] # Start new group
            
            # Add the last group if it's large enough
            if len(current_group) >= min_gap_size:
                consecutive_groups.append(list(current_group))
            
            # Convert groups to gap information
            for group in consecutive_groups:
                if group: # Ensure group is not empty
                    gaps.append({
                        'gap_type': 'consecutive_nodes',
                        'start_node': group[0],
                        'end_node': group[-1],
                        'size': len(group),
                        'building_code': building_code # Corresponds to fab
                    })
        
        return gaps
    
    # Helper methods for database queries (assuming nw_nodes and nw_links tables)
    
    def _get_building_totals(self, building_code: str) -> tuple:
        """Get total node and link counts for a building (fab)."""
        if not building_code or building_code == "SCENARIO":
            # For scenarios or empty building codes, totals are not applicable from nw_ tables
            return 0, 0
        
        try:
            # Get node count
            node_sql = "SELECT COUNT(*) FROM nw_nodes WHERE building_code = ?"
            node_result = self.db.query(node_sql, [building_code])
            node_count = node_result[0][0] if node_result and node_result[0] else 0
            
            # Get link count
            link_sql = "SELECT COUNT(*) FROM nw_links WHERE building_code = ?"
            link_result = self.db.query(link_sql, [building_code])
            link_count = link_result[0][0] if link_result and link_result[0] else 0
            
            return node_count, link_count
            
        except Exception as e:
            print(f"Error getting building totals for {building_code}: {e}")
            return 0, 0
    
    def _get_all_nodes(self, building_code: str) -> List[int]:
        """Get all node IDs for a building (fab)."""
        if not building_code or building_code == "SCENARIO":
            return []
        
        try:
            sql = "SELECT id FROM nw_nodes WHERE building_code = ?"
            results = self.db.query(sql, [building_code])
            return [row[0] for row in results] if results else []
        except Exception as e:
            print(f"Error getting all nodes for building {building_code}: {e}")
            return []
    
    def _get_all_links(self, building_code: str) -> List[int]:
        """Get all link IDs for a building (fab)."""
        if not building_code or building_code == "SCENARIO":
            return []
        
        try:
            sql = "SELECT id FROM nw_links WHERE building_code = ?"
            results = self.db.query(sql, [building_code])
            return [row[0] for row in results] if results else []
        except Exception as e:
            print(f"Error getting all links for building {building_code}: {e}")
            return []
    
    def _count_covered_nodes_in_category(self, category: str, building_code: str) -> int:
        """Count covered nodes in a specific category for a building (fab)."""
        if not self._covered_nodes:
            return 0
        
        try:
            # Get nodes in this category that are also covered
            # Using list() to ensure the set is converted for parameter binding
            node_ids_list = list(self._covered_nodes)
            if not node_ids_list: # If the list is empty, no IN clause needed.
                return 0

            placeholders = ','.join(['?'] * len(node_ids_list))
            sql = f"""
            SELECT COUNT(*) FROM nw_nodes 
            WHERE building_code = ? AND category = ? AND id IN ({placeholders})
            """
            params = [building_code, category] + node_ids_list
            
            result = self.db.query(sql, params)
            return result[0][0] if result and result[0] else 0
            
        except Exception as e:
            print(f"Error counting covered nodes in category '{category}' for building {building_code}: {e}")
            return 0
    
    def _count_covered_links_in_category(self, category: str, building_code: str) -> int:
        """Count covered links in a specific category for a building (fab)."""
        if not self._covered_links:
            return 0
        
        try:
            # Get links in this category that are also covered
            link_ids_list = list(self._covered_links)
            if not link_ids_list:
                return 0
                
            placeholders = ','.join(['?'] * len(link_ids_list))
            sql = f"""
            SELECT COUNT(*) FROM nw_links 
            WHERE building_code = ? AND category = ? AND id IN ({placeholders})
            """
            params = [building_code, category] + link_ids_list
            
            result = self.db.query(sql, params)
            return result[0][0] if result and result[0] else 0
            
        except Exception as e:
            print(f"Error counting covered links in category '{category}' for building {building_code}: {e}")
            return 0