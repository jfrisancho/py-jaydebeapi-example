#!/usr/bin/env python3

"""
Network Path Finder — DFS All Downstream Paths

Finds *all* downstream paths from a start node to every possible leaf/target node,
applying filtering (utility, toolset, eq_poc_no, data_code). For each path,
saves to nw_paths/nw_path_links.

This replaces the buggy ST_NW_DOWNSTREAM stored procedure with a full path-coverage tool.
"""

from typing import Dict, List, Set, Tuple
from collections import defaultdict
from dataclasses import dataclass

@dataclass
class NetworkNode:
    node_id: int
    data_code: int = 0
    utility_no: int = 0
    toolset_id: int = 0
    eq_poc_no: str = ''
    net_obj_type: int = 0

@dataclass
class NetworkLink:
    link_id: int
    guid: str
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
    """
    DFS-based downstream path enumerator for network graphs.
    """
    def __init__(self, db):
        self.db = db
        self.nodes: Dict[int, NetworkNode] = {}
        self.links: Dict[int, NetworkLink] = {}
        self.graph = defaultdict(list)  # node_id -> list of (neighbor_id, link_id, cost, reverse)
        self.loaded = False

    def load_network_data(self, utility_no=0, toolset_id=0, eq_poc_no='', data_codes=''):
        # --- Same as before; loads self.nodes, self.links, self.graph ---
        # ... (For brevity, use the implementation from your previous message.)
        # (Keep as in previous version: query nodes/links, populate graph.)
        pass  # Replace with your actual implementation

    def _is_node_allowed(self, node: NetworkNode, utility_no, toolset_id, eq_poc_no):
        if utility_no and node.utility_no != utility_no:
            return False
        if toolset_id and node.toolset_id != toolset_id:
            return False
        if eq_poc_no and (not node.eq_poc_no or eq_poc_no not in node.eq_poc_no):
            return False
        return True

    def _is_target(self, node: NetworkNode, target_codes: Set[int]):
        return node.data_code in target_codes if target_codes else False

    def enumerate_paths(self, start_node_id, ignore_node_id=0, utility_no=0,
                       toolset_id=0, eq_poc_no='', data_codes='') -> List[PathResult]:
        if not self.loaded:
            raise RuntimeError('Network data not loaded.')
        if start_node_id not in self.nodes:
            raise ValueError(f'Start node {start_node_id} not loaded.')
        # Prepare target data_code set (if any)
        target_codes = {int(x) for x in data_codes.split(',') if x.strip().isdigit()} if data_codes and data_codes != '0' else set()
        # Prepare all paths
        all_paths: List[PathResult] = []
        path_seq = 1

        def dfs(node_id, path_nodes, path_links, total_cost, visited: Set[int]):
            node = self.nodes[node_id]
            if node_id == ignore_node_id or node_id in visited:
                return
            # Leaf: either matches target data_code (if any), or is a dead-end (no outgoing)
            is_leaf = False
            if target_codes and self._is_target(node, target_codes) and len(path_nodes) > 0:
                # Found target
                all_paths.append(PathResult(
                    start_node_id=path_nodes[0],
                    end_node_id=node_id,
                    total_cost=total_cost,
                    links=list(path_links)
                ))
                return
            # If no outgoing links (besides ignored), it's a leaf
            neighbors = [(nbr, link_id, cost, rev) for nbr, link_id, cost, rev in self.graph.get(node_id, [])
                         if nbr != ignore_node_id]
            if not neighbors:
                is_leaf = True
            if is_leaf and (not target_codes):
                all_paths.append(PathResult(
                    start_node_id=path_nodes[0],
                    end_node_id=node_id,
                    total_cost=total_cost,
                    links=list(path_links)
                ))
                return
            for nbr, link_id, cost, reverse in neighbors:
                if nbr in visited:
                    continue  # Prevent cycles
                nbr_node = self.nodes[nbr]
                # Apply filters
                if not self._is_node_allowed(nbr_node, utility_no, toolset_id, eq_poc_no):
                    continue
                path_links.append((
                    len(path_links)+1, link_id,
                    node_id if not reverse else nbr,
                    nbr if not reverse else node_id,
                    cost, reverse
                ))
                visited.add(node_id)
                dfs(nbr, path_nodes + [nbr], path_links, total_cost + cost, visited)
                visited.remove(node_id)
                path_links.pop()

        # Start DFS
        dfs(start_node_id, [start_node_id], [], 0.0, set())
        return all_paths

    def save_paths_to_db(self, paths: List[PathResult], algorithm='dfs-downstream'):
        # ... (Same as before: save nw_paths and nw_path_links, see earlier skeleton)
        pass

def find_network_downstream(db, start_node_id: int, ignore_node_id: int = 0,
                          utility_no: int = 0, toolset_id: int = 0,
                          eq_poc_no: str = '', data_codes: str = '') -> List[int]:
    pathfinder = NetworkPathFinderDFS(db)
    pathfinder.load_network_data(utility_no, toolset_id, eq_poc_no, data_codes)
    paths = pathfinder.enumerate_paths(
        start_node_id, ignore_node_id, utility_no, toolset_id, eq_poc_no, data_codes
    )
    return pathfinder.save_paths_to_db(paths, algorithm='dfs-downstream')

# --- Example usage (see previous script) ---
