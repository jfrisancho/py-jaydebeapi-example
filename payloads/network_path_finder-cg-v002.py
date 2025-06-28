#!/usr/bin/env python3

"""
Network Path Finder — Downstream Path Enumeration & Storage

Finds all downstream paths from a starting node to leaf or 'target' nodes, 
with filtering (utility, toolset, eq_poc_no, data_code targets).
This replaces the buggy ST_NW_DOWNSTREAM stored procedure.

Main output: inserts path/step data into nw_paths and nw_path_links.
"""

import heapq
from typing import Dict, List, Tuple, Set, Optional
from collections import defaultdict
from dataclasses import dataclass

@dataclass
class NetworkNode:
    node_id: int
    data_code: int = 0
    utility_no: int = 0
    toolset_id: int = 0
    eq_poc_no: str = ''
    net_obj_type: int = 0  # 1=logical, 2=poc, 3=virtual

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
    path_id: int
    start_node_id: int
    end_node_id: int
    total_cost: float
    links: List[Tuple[int, int, int, int, float, bool]]  # (seq, link_id, start_node, end_node, cost, reverse)

class NetworkPathFinder:
    """
    Finds all downstream paths using Dijkstra and node/link filtering.
    """
    def __init__(self, db):
        self.db = db
        self.graph = defaultdict(list)   # node_id -> List[(neighbor_id, link_id, cost, reverse)]
        self.nodes: Dict[int, NetworkNode] = {}
        self.links: Dict[int, NetworkLink] = {}
        self.loaded = False

    def load_network_data(self, utility_no=0, toolset_id=0, eq_poc_no='', data_codes=''):
        # Build filter SQL
        node_conditions = ['1=1']
        params = []
        if utility_no > 0:
            node_conditions.append('n.utility_no = ?')
            params.append(utility_no)
        if toolset_id > 0:
            node_conditions.append('n.toolset_id = ?')
            params.append(toolset_id)
        if eq_poc_no.strip():
            node_conditions.append('n.eq_poc_no LIKE ?')
            params.append(f'%{eq_poc_no.strip()}%')
        if data_codes.strip() and data_codes != '0':
            codes = [code.strip() for code in data_codes.split(',') if code.strip()]
            placeholders = ','.join(['?'] * len(codes))
            node_conditions.append(f'n.data_code IN ({placeholders})')
            params.extend(codes)
        node_filter = ' AND '.join(node_conditions)

        # Load nodes
        nodes_query = f'''
            SELECT n.node_id, n.data_code, n.utility_no, n.toolset_id, 
                   COALESCE(n.eq_poc_no, ''), n.net_obj_type
            FROM nw_nodes n
            WHERE {node_filter}
        '''
        for row in self.db.query(nodes_query, params):
            node_id, data_code, utility_no, toolset_id, eq_poc_no, net_obj_type = row
            self.nodes[node_id] = NetworkNode(
                node_id, data_code, utility_no, toolset_id, eq_poc_no, net_obj_type
            )
        if not self.nodes:
            print('No nodes found for given filters.')
            return

        node_ids = list(self.nodes.keys())
        idph = ','.join(['?'] * len(node_ids))
        links_query = f'''
            SELECT l.id, l.guid, l.start_node_id, l.end_node_id, 
                   l.is_bidirected, l.cost, l.net_obj_type
            FROM nw_links l
            WHERE l.start_node_id IN ({idph})
              AND l.end_node_id IN ({idph})
        '''
        link_rows = self.db.query(links_query, node_ids + node_ids)
        for row in link_rows:
            link_id, guid, start, end, is_bidirected, cost, net_obj_type = row
            link = NetworkLink(link_id, guid, start, end, bool(is_bidirected), float(cost), net_obj_type)
            self.links[link_id] = link
            self.graph[start].append((end, link_id, link.cost, False))
            if link.is_bidirected:
                self.graph[end].append((start, link_id, link.cost, True))
        self.loaded = True

    def find_target_nodes(self, ignore_node_id=0, data_codes='') -> Set[int]:
        # Target by data_code or else by being a leaf node
        if not self.loaded:
            raise RuntimeError('Network data not loaded.')
        target_nodes = set()
        if data_codes.strip() and data_codes != '0':
            codes = {int(code) for code in data_codes.split(',') if code.strip().isdigit()}
            for nid, node in self.nodes.items():
                if nid != ignore_node_id and node.data_code in codes:
                    target_nodes.add(nid)
        else:
            for nid in self.nodes:
                if nid == ignore_node_id:
                    continue
                # If no outgoing connections (not counting ignored node), it's a leaf
                has_outgoing = any(nbr != ignore_node_id for nbr, *_ in self.graph.get(nid, []))
                if not has_outgoing:
                    target_nodes.add(nid)
        return target_nodes

    def find_downstream_paths(self, start_node_id, ignore_node_id=0, data_codes='') -> List[PathResult]:
        if not self.loaded:
            raise RuntimeError('Network data not loaded.')
        if start_node_id not in self.nodes:
            raise ValueError('Start node not found.')
        targets = self.find_target_nodes(ignore_node_id, data_codes)
        if not targets:
            return []
        # Dijkstra for shortest path to each target, but can return all downstream (will not duplicate for same end)
        distances = {start_node_id: 0.0}
        previous = {}
        visited = set()
        heap = [(0.0, start_node_id)]
        while heap:
            cost, nid = heapq.heappop(heap)
            if nid in visited:
                continue
            visited.add(nid)
            if nid == ignore_node_id:
                continue
            for nbr, link_id, link_cost, reverse in self.graph.get(nid, []):
                if nbr == ignore_node_id or nbr in visited:
                    continue
                new_cost = cost + link_cost
                if nbr not in distances or new_cost < distances[nbr]:
                    distances[nbr] = new_cost
                    previous[nbr] = (nid, link_id, reverse)
                    heapq.heappush(heap, (new_cost, nbr))
        # Backtrack for each target
        paths = []
        path_id = 1
        for target in targets:
            if target not in distances:
                continue
            links = []
            current = target
            seq = 0
            while current in previous:
                prev, link_id, reverse = previous[current]
                link = self.links[link_id]
                links.append((seq+1, link_id, prev if not reverse else current,
                              current if not reverse else prev, link.cost, reverse))
                current = prev
                seq += 1
            links.reverse()
            if links:
                paths.append(PathResult(
                    path_id, start_node_id, target, distances[target], links
                ))
                path_id += 1
        return paths

    def save_paths_to_db(self, paths: List[PathResult], algorithm='downstream'):
        if not paths:
            return []
        created_ids = []
        for path in paths:
            sql = 'INSERT INTO nw_paths (algorithm, start_node_id, end_node_id, cost) VALUES (?, ?, ?, ?)'
            self.db.update(sql, [algorithm, path.start_node_id, path.end_node_id, path.total_cost])
            result = self.db.query('SELECT LAST_INSERT_ID()')
            if not result or not result[0][0]:
                continue
            db_path_id = result[0][0]
            created_ids.append(db_path_id)
            for seq, link_id, start_id, end_id, cost, reverse in path.links:
                start_node = self.nodes.get(start_id)
                end_node = self.nodes.get(end_id)
                link_sql = '''
                INSERT INTO nw_path_links (
                    path_id, seq, link_id, length, start_node_id, start_node_data_code, start_node_utility_no,
                    end_node_id, end_node_data_code, end_node_utility_no, reverse, group_no, sub_group_no, end_node_flag
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                '''
                self.db.update(link_sql, [
                    db_path_id, seq, link_id, cost,
                    start_id, getattr(start_node, 'data_code', 0), getattr(start_node, 'utility_no', 0),
                    end_id, getattr(end_node, 'data_code', 0), getattr(end_node, 'utility_no', 0),
                    1 if reverse else 0, 1, 1, 'E' if end_id == path.end_node_id else 'I'
                ])
        return created_ids

def find_network_downstream(db, start_node_id: int, ignore_node_id: int = 0,
                          utility_no: int = 0, toolset_id: int = 0,
                          eq_poc_no: str = '', data_codes: str = '') -> List[int]:
    finder = NetworkPathFinder(db)
    finder.load_network_data(utility_no, toolset_id, eq_poc_no, data_codes)
    paths = finder.find_downstream_paths(start_node_id, ignore_node_id, data_codes)
    return finder.save_paths_to_db(paths, algorithm='downstream')

# Example usage (pseudo-code for db connection)
if __name__ == '__main__':
    from db import Database  # implement this or adapt to your ORM
    db = Database()
    path_ids = find_network_downstream(
        db,
        start_node_id=12345,
        ignore_node_id=12340,
        utility_no=0,
        toolset_id=0,
        eq_poc_no='',
        data_codes='15000'
    )
    print('Created paths:', path_ids)
    db.close()
