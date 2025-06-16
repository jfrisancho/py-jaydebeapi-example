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
    
    def initialize_coverage(self, fab: str) -> CoverageStats:
        """Initialize coverage tracking for a specific fab."""
        self._covered_nodes.clear()
        self._covered_links.clear()
        
        # Get total node and link counts for the fab
        self._total_nodes, self._total_links = self._get_fab_totals(fab)
        
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
    
    def get_uncovered_areas(self, fab: str) -> dict:
        """Get information about uncovered nodes and links."""
        
        # Get all node and link IDs for the fab
        all_nodes = self._get_all_nodes(fab)
        all_links = self._get_all_links(fab)
        
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
    
    def get_coverage_by_category(self, fab: str) -> dict:
        """Get coverage statistics broken down by category."""
        sql = """
        SELECT 
            category,
            COUNT(DISTINCT n.id) as total_nodes,
            COUNT(DISTINCT l.id) as total_links
        FROM categories c
        LEFT JOIN nodes n ON c.category = n.category AND n.fab = ?
        LEFT JOIN links l ON c.category = l.category AND l.fab = ?
        GROUP BY category
        """
        
        try:
            results = self.db.query(sql, [fab, fab])
            category_stats = {}
            
            for row in results:
                category = row[0]
                total_nodes = row[1] or 0
                total_links = row[2] or 0
                
                # Count covered items in this category
                covered_nodes_in_category = self._count_covered_nodes_in_category(category, fab)
                covered_links_in_category = self._count_covered_links_in_category(category, fab)
                
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
    
    def get_coverage_gaps(self, fab: str, min_gap_size: int = 5) -> List[dict]:
        """Identify significant coverage gaps (areas with many uncovered consecutive items)."""
        
        # This is a simplified implementation
        # In practice, you'd analyze the topology to find actual gaps
        
        uncovered_info = self.get_uncovered_areas(fab)
        gaps = []
        
        # Group uncovered nodes by proximity (simplified - assumes sequential IDs)
        uncovered_nodes = sorted(uncovered_info['uncovered_nodes'])
        current_gap = []
        
        for i, node_id in enumerate(uncovered_nodes):
            if not current_gap:
                current_gap = [node_id]
            elif node_id - uncovered_nodes[i-1] <= 2:  # Adjacent or close nodes
                current_gap.append(node_id)
            else:
                # Gap ended
                if len(current_gap) >= min_gap_size:
                    gaps.append({
                        'type': 'nodes',
                        'start_id': current_gap[0],
                        'end_id': current_gap[-1],
                        'size': len(current_gap),
                        'ids': current_gap
                    })
                current_gap = [node_id]
        
        # Handle last gap
        if len(current_gap) >= min_gap_size:
            gaps.append({
                'type': 'nodes',
                'start_id': current_gap[0],
                'end_id': current_gap[-1],
                'size': len(current_gap),
                'ids': current_gap
            })
        
        return gaps
    
    def _get_fab_totals(self, fab: str) -> tuple:
        """Get total node and link counts for a fab."""
        node_sql = "SELECT COUNT(*) FROM nodes WHERE fab = ?"
        link_sql = "SELECT COUNT(*) FROM links WHERE fab = ?"
        
        try:
            node_result = self.db.query(node_sql, [fab])
            link_result = self.db.query(link_sql, [fab])
            
            total_nodes = node_result[0][0] if node_result else 0
            total_links = link_result[0][0] if link_result else 0
            
            return total_nodes, total_links
            
        except Exception as e:
            print(f"Error getting fab totals: {e}")
            return 0, 0
    
    def _get_all_nodes(self, fab: str) -> List[int]:
        """Get all node IDs for a fab."""
        sql = "SELECT id FROM nodes WHERE fab = ? ORDER BY id"
        
        try:
            results = self.db.query(sql, [fab])
            return [row[0] for row in results]
        except Exception as e:
            print(f"Error getting all nodes: {e}")
            return []
    
    def _get_all_links(self, fab: str) -> List[int]:
        """Get all link IDs for a fab."""
        sql = "SELECT id FROM links WHERE fab = ? ORDER BY id"
        
        try:
            results = self.db.query(sql, [fab])
            return [row[0] for row in results]
        except Exception as e:
            print(f"Error getting all links: {e}")
            return []
    
    def _count_covered_nodes_in_category(self, category: str, fab: str) -> int:
        """Count covered nodes in a specific category."""
        sql = "SELECT id FROM nodes WHERE category = ? AND fab = ?"
        
        try:
            results = self.db.query(sql, [category, fab])
            category_node_ids = [row[0] for row in results]
            
            covered_count = sum(1 for node_id in category_node_ids 
                              if node_id in self._covered_nodes)
            return covered_count
            
        except Exception as e:
            print(f"Error counting covered nodes in category {category}: {e}")
            return 0
    
    def _count_covered_links_in_category(self, category: str, fab: str) -> int:
        """Count covered links in a specific category."""
        sql = "SELECT id FROM links WHERE category = ? AND fab = ?"
        
        try:
            results = self.db.query(sql, [category, fab])
            category_link_ids = [row[0] for row in results]
            
            covered_count = sum(1 for link_id in category_link_ids 
                              if link_id in self._covered_links)
            return covered_count
            
        except Exception as e:
            print(f"Error counting covered links in category {category}: {e}")
            return 0
    
    def reset_coverage(self):
        """Reset coverage tracking."""
        self._covered_nodes.clear()
        self._covered_links.clear()