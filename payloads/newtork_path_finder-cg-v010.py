from typing import List, Dict, Set, Tuple
from collections import defaultdict, deque
from dataclasses import dataclass
import heapq

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

class NetworkPathFinder:
    def __init__(self, db):
        self.db = db
        self.nodes: Dict[int, NetworkNode] = {}
        self.links: Dict[int, NetworkLink] = {}
        self.graph = defaultdict(list)  # node_id -> list of (neighbor_id, link_id, cost, reverse)
        self.loaded = False

    def load_network_data(self, utility_no=0, toolset_id=0, eq_poc_no=''):
        where = []
        params = []
        if utility_no:
            where.append("utility_no = ?")
            params.append(utility_no)
        if toolset_id:
            where.append("toolset_id = ?")
            params.append(toolset_id)
        if eq_poc_no:
            where.append("eq_poc_no LIKE ?")
            params.append(f"%{eq_poc_no}%")
        sql = (
            "SELECT node_id, data_code, utility_no, toolset_id, COALESCE(eq_poc_no,''), net_obj_type FROM nw_nodes"
        )
        if where:
            sql += " WHERE " + " AND ".join(where)
        for row in self.db.query(sql, params):
            node_id, data_code, u, t, poc, net_type = row
            self.nodes[node_id] = NetworkNode(node_id, data_code, u, t, poc, net_type)
        if not self.nodes:
            raise Exception('No nodes loaded')
        node_ids = set(self.nodes.keys())
        idph = ','.join(['?'] * len(node_ids))
        sql = (
            f"SELECT id, start_node_id, end_node_id, is_bidirected, cost, net_obj_type "
            f"FROM nw_links WHERE start_node_id IN ({idph}) AND end_node_id IN ({idph})"
        )
        ids_params = list(node_ids) + list(node_ids)
        for row in self.db.query(sql, ids_params):
            lid, start, end, is_bidirected, cost, net_type = row
            link = NetworkLink(lid, start, end, bool(is_bidirected), float(cost), net_type)
            self.links[lid] = link
            self.graph[start].append((end, lid, link.cost, False))
            if link.is_bidirected:
                self.graph[end].append((start, lid, link.cost, True))
        self.loaded = True

    def _parse_target_codes(self, data_codes: str) -> Set[int]:
        return {int(x) for x in data_codes.split(',') if x.strip().isdigit()} if data_codes and data_codes != '0' else set()

    def _node_passes_filters(self, node: NetworkNode, utility_no: int, toolset_id: int, eq_poc_no: str) -> bool:
        if utility_no and node.utility_no != utility_no:
            return False
        if toolset_id and node.toolset_id != toolset_id:
            return False
        if eq_poc_no and (not node.eq_poc_no or eq_poc_no not in node.eq_poc_no):
            return False
        return True

    def _find_next_neighbors(self, curr_node_id: int, visited: Set[int], utility_no: int, toolset_id: int, eq_poc_no: str, ignore_node_id: int) -> List[Tuple[int,int,float,bool]]:
        neighbors = []
        for nbr, link_id, cost, reverse in self.graph.get(curr_node_id, []):
            if nbr == ignore_node_id or nbr in visited:
                continue
            nbr_node = self.nodes[nbr]
            if self._node_passes_filters(nbr_node, utility_no, toolset_id, eq_poc_no):
                neighbors.append((nbr, link_id, cost, reverse))
        return neighbors

    def find_downstream_paths_dfs(
        self,
        start_node_id: int,
        ignore_node_id: int = 0,
        utility_no: int = 0,
        toolset_id: int = 0,
        eq_poc_no: str = '',
        data_codes: str = ''
    ) -> List[PathResult]:
        target_codes = self._parse_target_codes(data_codes)
        all_paths: List[PathResult] = []
        def dfs(curr_node_id: int, path_links: List, path_nodes: List[int], total_cost: float, visited: Set[int]):
            node = self.nodes[curr_node_id]
            if curr_node_id == ignore_node_id or curr_node_id in visited:
                return
            if curr_node_id != start_node_id and target_codes and node.data_code in target_codes:
                all_paths.append(PathResult(
                    start_node_id=path_nodes[0],
                    end_node_id=curr_node_id,
                    total_cost=total_cost,
                    links=list(path_links)
                ))
                return
            next_neighbors = self._find_next_neighbors(curr_node_id, visited, utility_no, toolset_id, eq_poc_no, ignore_node_id)
            if not self.graph.get(curr_node_id):
                all_paths.append(PathResult(
                    start_node_id=path_nodes[0],
                    end_node_id=curr_node_id,
                    total_cost=total_cost,
                    links=list(path_links)
                ))
                return
            if self.graph.get(curr_node_id) and not next_neighbors:
                all_paths.append(PathResult(
                    start_node_id=path_nodes[0],
                    end_node_id=curr_node_id,
                    total_cost=total_cost,
                    links=list(path_links)
                ))
                return
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

    def find_downstream_paths_bfs(
        self,
        start_node_id: int,
        ignore_node_id: int = 0,
        utility_no: int = 0,
        toolset_id: int = 0,
        eq_poc_no: str = '',
        data_codes: str = ''
    ) -> List[PathResult]:
        target_codes = self._parse_target_codes(data_codes)
        all_paths: List[PathResult] = []
        visited_paths = set()
        queue = deque()
        queue.append((start_node_id, [], [start_node_id], 0.0))
        while queue:
            curr_node_id, path_links, path_nodes, total_cost = queue.popleft()
            node = self.nodes[curr_node_id]
            if curr_node_id == ignore_node_id:
                continue
            if curr_node_id != start_node_id and target_codes and node.data_code in target_codes:
                key = (curr_node_id, tuple(path_nodes))
                if key not in visited_paths:
                    all_paths.append(PathResult(
                        start_node_id=path_nodes[0],
                        end_node_id=curr_node_id,
                        total_cost=total_cost,
                        links=list(path_links)
                    ))
                    visited_paths.add(key)
                continue
            next_neighbors = self._find_next_neighbors(curr_node_id, set(path_nodes), utility_no, toolset_id, eq_poc_no, ignore_node_id)
            if not self.graph.get(curr_node_id):
                key = (curr_node_id, tuple(path_nodes))
                if key not in visited_paths:
                    all_paths.append(PathResult(
                        start_node_id=path_nodes[0],
                        end_node_id=curr_node_id,
                        total_cost=total_cost,
                        links=list(path_links)
                    ))
                    visited_paths.add(key)
                continue
            if self.graph.get(curr_node_id) and not next_neighbors:
                key = (curr_node_id, tuple(path_nodes))
                if key not in visited_paths:
                    all_paths.append(PathResult(
                        start_node_id=path_nodes[0],
                        end_node_id=curr_node_id,
                        total_cost=total_cost,
                        links=list(path_links)
                    ))
                    visited_paths.add(key)
                continue
            for nbr, link_id, cost, reverse in next_neighbors:
                if nbr in path_nodes:
                    continue
                new_links = path_links + [(
                    len(path_links) + 1, link_id,
                    curr_node_id if not reverse else nbr,
                    nbr if not reverse else curr_node_id,
                    cost, reverse
                )]
                queue.append((nbr, new_links, path_nodes + [nbr], total_cost + cost))
        return all_paths

    def find_downstream_paths_dijkstra(
        self,
        start_node_id: int,
        ignore_node_id: int = 0,
        utility_no: int = 0,
        toolset_id: int = 0,
        eq_poc_no: str = '',
        data_codes: str = ''
    ) -> List[PathResult]:
        target_codes = self._parse_target_codes(data_codes)
        all_paths: List[PathResult] = []
        heap = [(0.0, start_node_id, [], [start_node_id])]
        best_costs = {start_node_id: 0.0}
        ends_seen = set()
        while heap:
            cost, curr_node_id, path_links, path_nodes = heapq.heappop(heap)
            if curr_node_id == ignore_node_id:
                continue
            node = self.nodes[curr_node_id]
            if curr_node_id != start_node_id and target_codes and node.data_code in target_codes:
                if curr_node_id not in ends_seen:
                    all_paths.append(PathResult(
                        start_node_id=path_nodes[0],
                        end_node_id=curr_node_id,
                        total_cost=cost,
                        links=list(path_links)
                    ))
                    ends_seen.add(curr_node_id)
                continue
            next_neighbors = self._find_next_neighbors(curr_node_id, set(path_nodes), utility_no, toolset_id, eq_poc_no, ignore_node_id)
            if not self.graph.get(curr_node_id):
                if curr_node_id not in ends_seen:
                    all_paths.append(PathResult(
                        start_node_id=path_nodes[0],
                        end_node_id=curr_node_id,
                        total_cost=cost,
                        links=list(path_links)
                    ))
                    ends_seen.add(curr_node_id)
                continue
            if self.graph.get(curr_node_id) and not next_neighbors:
                if curr_node_id not in ends_seen:
                    all_paths.append(PathResult(
                        start_node_id=path_nodes[0],
                        end_node_id=curr_node_id,
                        total_cost=cost,
                        links=list(path_links)
                    ))
                    ends_seen.add(curr_node_id)
                continue
            for nbr, link_id, ncost, reverse in next_neighbors:
                if nbr in path_nodes:
                    continue
                new_cost = cost + ncost
                if nbr not in best_costs or new_cost < best_costs[nbr]:
                    best_costs[nbr] = new_cost
                    heapq.heappush(heap, (
                        new_cost,
                        nbr,
                        path_links + [(len(path_links) + 1, link_id,
                                       curr_node_id if not reverse else nbr,
                                       nbr if not reverse else curr_node_id,
                                       ncost, reverse)],
                        path_nodes + [nbr]
                    ))
        return all_paths

def find_downstream_paths(
    db,
    algorithm: str = "dfs",
    start_node_id: int = None,
    ignore_node_id: int = 0,
    utility_no: int = 0,
    toolset_id: int = 0,
    eq_poc_no: str = '',
    data_codes: str = ''
) -> List[PathResult]:
    """
    Entry point to find downstream paths using a selected algorithm (dfs, bfs, dijkstra).
    Calls load_network_data and the corresponding finder method.
    """
    finder = NetworkPathFinder(db)
    finder.load_network_data(utility_no, toolset_id, eq_poc_no)
    if algorithm == "dfs":
        return finder.find_downstream_paths_dfs(
            start_node_id, ignore_node_id, utility_no, toolset_id, eq_poc_no, data_codes)
    elif algorithm == "bfs":
        return finder.find_downstream_paths_bfs(
            start_node_id, ignore_node_id, utility_no, toolset_id, eq_poc_no, data_codes)
    elif algorithm == "dijkstra":
        return finder.find_downstream_paths_dijkstra(
            start_node_id, ignore_node_id, utility_no, toolset_id, eq_poc_no, data_codes)
    else:
        raise ValueError(f"Unknown algorithm: {algorithm}")

# Example usage and testing
if __name__ == '__main__':
    from db import Database
    db = Database()
    try:
        # Example: Find downstream from equipment POC node 12345
        # Ignore the equipment logical node 12340
        # Filter for equipment connections (data_code 15000)
        paths = find_downstream_paths(
            db,
            algorithm="dfs",
            start_node_id=12345,
            ignore_node_id=12340,
            utility_no=0,
            toolset_id=0,
            eq_poc_no='',
            data_codes='15000'
        )
        print(f'Created {len(paths)} paths: {[p.end_node_id for p in paths]}')
    finally:
        db.close()
