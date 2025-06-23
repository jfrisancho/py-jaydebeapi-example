"""
Service for tracking coverage using bitsets for nodes and links.
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
        """Initialize coverage tracking for a specific building."""
        self._covered_nodes.clear()
        self._covered_links.clear()
        
        # Get total node and link counts for the building
        self._total_nodes, self._total_links = self._get_building_totals(building_code)
        
        return CoverageStats(
            nodes_covered=0,
            links_covered=0,
            total_nodes=self._total_nodes,
            total_links=self._total_links,
            coverage_percentage=0.0
        )
    
    def update_coverage(self, path_definition: PathDefinition, 
                       current_stats: CoverageStats) -> CoverageStats:
        """Update coverage with a new path and return updated statistics."""
        
        # Extract nodes and links from path context
        path_nodes = path_definition.path_context.get('nodes', [])
        path_links = path_definition.path_context.get('links', [])
        
        # Track new nodes and links
        new_nodes = 0
        new_links = 0
        
        for node_id in path_nodes:
            if node_id not in self._covered_nodes:
                self._covered_nodes.add(node_id)
                new_nodes += 1
        
        for link_id in path_links:
            if link_id not in self._covered_links:
                self._covered_links.add(link_id)
                new_links += 1
        
        # Calculate updated statistics
        total_covered = len(self._covered_nodes) + len(self._covered_links)
        total_possible = self._total_nodes + self._total_links
        coverage_percentage = total_covered / total_possible if total_possible > 0 else 0.0
        
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
        
        new_coverage = 0
        
        # Count new nodes
        for node_id in path_nodes:
            if node_id not in self._covered_nodes:
                new_coverage += 1
        
        # Count new links
        for link_id in path_links:
            if link_id not in self._covered_links:
                new_coverage += 1
        
        total_possible = self._total_nodes + self._total_links
        return new_coverage / total_possible if total_possible > 0 else 0.0
    
    def get_coverage_by_category(self, building_code: str) -> dict:
        """Get coverage statistics broken down by category."""
        sql = """
        SELECT 
            category,
            COUNT(DISTINCT n.id) as total_nodes,
            COUNT(DISTINCT l.id) as total_links
        FROM categories c
        LEFT JOIN nw_nodes n ON c.category = n.category AND n.building_code = ?
        LEFT JOIN nw_links l ON c.category = l.category AND l.building_code = ?
        GROUP BY category
        """
        
        try:
            results = self.db.query(sql, [building_code, building_code])
            category_stats = {}
            
            for row in results:
                category = row[0]
                total_nodes = row[1] or 0
                total_links = row[2] or 0
                
                # Count covered items in this category
                covered_nodes_in_category = self._count_covered_nodes_in_category(category, building_code)
                covered_links_in_category = self._count_covered_links_in_category(category, building_code)
                
                total_category_items = total_nodes + total_links
                covered_category_items = covered_nodes_in_category + covered_links_in_category
                
                category_coverage = (covered_category_items / total_category_items 
                                   if total_category_items > 0 else 0.0)
                
                category_stats[category] = {
                    'total_nodes': total_nodes,
                    'total_links': total_links,
                    'covered_nodes': covered_nodes_in_category,
                    'covered_links': covered_links_in_category,
                    'coverage_percentage': category_coverage
                }
            
            return category_stats
            
        except Exception as e:
            print(f"Error getting coverage by category: {e}")
            return {}
    
    def get_coverage_gaps(self, building_code: str, min_gap_size: int = 5) -> List[dict]:
        """Identify significant coverage gaps (areas with many uncovered consecutive items)."""
        
        # This is a simplified implementation
        # In practice, you'd analyze the topology to find actual gaps
        
        uncovered_info = self.get_uncovered_areas(building_code)
        gaps = []
        
        # Group uncovered nodes by proximity (simplified -
