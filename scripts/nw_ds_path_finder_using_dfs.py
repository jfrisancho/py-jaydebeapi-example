#!/usr/bin/env python3

"""
Network Downstream Path Finder using DFS
- Enumerates ALL downstream paths from a start node, stopping at:
  * a leaf (no further valid outgoing links),
  * or the first occurrence of a node whose data_code matches any in the provided data_codes list.

Filtering for utility_no, toolset_id, eq_poc_no applies to outgoing neighbor nodes, not path endings.
"""

from typing import List, Dict, Tuple, Set, Optional
from collections import defaultdict
from dataclasses import dataclass

@dataclass
class NetworkNode:
    node_id: int
    data_code: int
    utility_no: int
    toolset_id: int
    eq_poc_no: str
    net_obj_type: int

@dataclass
class NetworkLink:
    link_id: int
    start_node_id: int
    end_node_id: int
    is_bidirected: bool
    cost: float
    net_obj_type: int

@dataclass
class PathResult:
    start_node_id: int
    end_node_id: int
    total_cost: float
    links: List[Tuple[int, int, int, int, float, bool]]  # (seq, link_id, start_node, end_node, cost, reverse)

class NetworkPathFinderDFS:
    def __init__(self, db):
        self.db = db
        self.nodes: Dict[int, NetworkNode] = {}
        self.links: Dict[int, NetworkLink] = {}
        self.graph = defaultdict(list)  # node_id -> list of (neighbor_id, link_id, cost, reverse)
        self.loaded = False

    def load_network_data(self, utility_no=0, toolset_id=0, eq_poc_no=''):
        # LOAD ALL nodes
        sql = "SELECT node_id, data_code, utility_no, toolset_id, COALESCE(eq_poc_no,''), net_obj_type FROM nw_nodes"
        for row in self.db.query(sql):
            node_id, data_code, u, t, poc, net_type = row
            self.nodes[node_id] = NetworkNode(node_id, data_code, u, t, poc, net_type)
        if not self.nodes:
            raise Exception('No nodes loaded')
        # LOAD ALL links that are between loaded nodes
        node_ids = set(self.nodes.keys())
        sql = "SELECT id, start_node_id, end_node_id, is_bidirected, cost, net_obj_type FROM nw_links"
        for row in self.db.query(sql):
            lid, start, end, is_bidirected, cost, net_type = row
            if start in node_ids and end in node_ids:
                link = NetworkLink(lid, start, end, bool(is_bidirected), float(cost), net_type)
                self.links[lid] = link
                self.graph[start].append((end, lid, link.cost, False))
                if link.is_bidirected:
                    self.graph[end].append((start, lid, link.cost, True))
        self.loaded = True

    def _node_passes_filters(self, node: NetworkNode, utility_no: int, toolset_id: int, eq_poc_no: str) -> bool:
        if utility_no and node.utility_no != utility_no:
            return False
        if toolset_id and node.toolset_id != toolset_id:
            return False
        if eq_poc_no and (not node.eq_poc_no or eq_poc_no not in node.eq_poc_no):
            return False
        return True

    def enumerate_downstream_paths(
        self,
        start_node_id: int,
        ignore_node_id: int = 0,
        utility_no: int = 0,
        toolset_id: int = 0,
        eq_poc_no: str = '',
        data_codes: str = ''
    ) -> List[PathResult]:
        if not self.loaded:
            raise RuntimeError("Call load_network_data() first")
        if start_node_id not in self.nodes:
            raise ValueError("start_node_id not in network")
        # Set of data_codes (as int) that signal "target" (end) nodes for early stop
        target_codes = {int(x) for x in data_codes.split(',') if x.strip().isdigit()} if data_codes and data_codes != '0' else set()
        all_paths: List[PathResult] = []

        def dfs(curr_node_id: int, path_links: List, path_nodes: List[int], total_cost: float, visited: Set[int]):
            node = self.nodes[curr_node_id]
            # Rule: Ignore
            if curr_node_id == ignore_node_id or curr_node_id in visited:
                return
            # End if matches data_codes (and is not the starting node itself)
            if curr_node_id != start_node_id and target_codes and node.data_code in target_codes:
                all_paths.append(
                    PathResult(
                        start_node_id=path_nodes[0],
                        end_node_id=curr_node_id,
                        total_cost=total_cost,
                        links=list(path_links)
                    )
                )
                return
            # Find neighbors that pass filters
            next_neighbors = []
            for nbr, link_id, cost, reverse in self.graph.get(curr_node_id, []):
                if nbr == ignore_node_id or nbr in visited:
                    continue
                nbr_node = self.nodes[nbr]
                if self._node_passes_filters(nbr_node, utility_no, toolset_id, eq_poc_no):
                    next_neighbors.append((nbr, link_id, cost, reverse))
            if not next_neighbors:
                # Dead end: only accept as a leaf if NOT already a data_code "end"
                if not target_codes or node.data_code not in target_codes or curr_node_id == start_node_id:
                    all_paths.append(
                        PathResult(
                            start_node_id=path_nodes[0],
                            end_node_id=curr_node_id,
                            total_cost=total_cost,
                            links=list(path_links)
                        )
                    )
                return
            # Continue DFS
            visited.add(curr_node_id)
            for nbr, link_id, cost, reverse in next_neighbors:
                path_links.append((
                    len(path_links) + 1, link_id,
                    curr_node_id if not reverse else nbr,
                    nbr if not reverse else curr_node_id,
                    cost, reverse
                ))
                dfs(nbr, path_links, path_nodes + [nbr], total_cost + cost, visited)
                path_links.pop()
            visited.remove(curr_node_id)

        dfs(start_node_id, [], [start_node_id], 0.0, set())
        return all_paths

# Example for DB interface (dummy):
class DummyDB:
    def query(self, sql, params=None):
        # Should be implemented to return actual results as per the schema.
        raise NotImplementedError()

# Example usage:
if __name__ == '__main__':
    db = DummyDB()  # Replace with your actual DB class
    finder = NetworkPathFinderDFS(db)
    finder.load_network_data(
        utility_no=0, toolset_id=0, eq_poc_no=''
    )
    paths = finder.enumerate_downstream_paths(
        start_node_id=12345,
        ignore_node_id=12340,
        utility_no=0,
        toolset_id=0,
        eq_poc_no='',
        data_codes='15000,107'
    )
    print(f"Found {len(paths)} paths.")
    # Save paths to DB, or use as needed
