from collections import defaultdict, deque
from typing import List, Dict, Any, Optional, Set, Tuple

class DownstreamFinder:
    def __init__(
        self,
        nodes: Dict[int, Dict[str, Any]],
        links: List[Dict[str, Any]]
    ):
        self.nodes = nodes  # {node_id: {...}}
        self.links = links
        # Build adjacency list: node_id -> List of (link, to_node_id)
        self.adj = defaultdict(list)
        for link in links:
            s, e = link['start_node_id'], link['end_node_id']
            self.adj[s].append((link, e))
            if link['is_bidirected']:
                self.adj[e].append((link, s))

    def find_paths(
        self,
        start_node_id: int,
        ignore_node_id: Optional[int] = None,
        utility_no: int = 0,
        toolset_id: int = 0,
        eq_poc_no: str = '',
        data_codes: str = ''
    ) -> List[List[Dict[str, Any]]]:
        data_code_targets = {int(dc.strip()) for dc in data_codes.split(',') if dc.strip() and dc != '0'}
        all_paths = []
        stack = [([start_node_id], [], 0.0)]  # (node_seq, link_seq, total_cost)

        while stack:
            node_seq, link_seq, total_cost = stack.pop()
            curr_node = node_seq[-1]
            node = self.nodes[curr_node]
            
            # Stopping conditions (target, leaf)
            is_leaf = True
            if data_code_targets and node['data_code'] in data_code_targets and len(node_seq) > 1:
                # This is a target node, path ends here (not start node itself)
                all_paths.append((node_seq[:], link_seq[:], total_cost))
                continue

            # Explore neighbors
            for link, nbr in self.adj[curr_node]:
                if nbr == ignore_node_id:
                    continue
                # Node filter
                nbr_node = self.nodes[nbr]
                if utility_no and nbr_node['utility_no'] != utility_no:
                    continue
                if toolset_id and nbr_node['toolset_id'] != toolset_id:
                    continue
                if eq_poc_no and (not nbr_node['eq_poc_no'] or eq_poc_no not in nbr_node['eq_poc_no']):
                    continue
                if nbr in node_seq:
                    continue  # avoid cycles

                is_leaf = False
                stack.append((
                    node_seq + [nbr],
                    link_seq + [link],
                    total_cost + link['cost']
                ))

            # If leaf, add path
            if is_leaf:
                all_paths.append((node_seq[:], link_seq[:], total_cost))

        return all_paths

    def path_to_nw_pathlinks(
        self, node_seq: List[int], link_seq: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Convert a path to list of path_link records, including node_flags."""
        path_links = []
        n = len(node_seq)
        for i, (start, end) in enumerate(zip(node_seq[:-1], node_seq[1:])):
            link = link_seq[i]
            # node_flag logic
            if i == 0:
                node_flag = 'S'  # Start
            elif i == n - 2:
                node_flag = 'E'  # End/target
            else:
                node_flag = 'I'
            path_links.append({
                'seq': i,
                'link_id': link['id'],
                'length': link['cost'],
                'start_node_id': start,
                'start_node_data_code': self.nodes[start]['data_code'],
                'start_node_utility_no': self.nodes[start]['utility_no'],
                'end_node_id': end,
                'end_node_data_code': self.nodes[end]['data_code'],
                'end_node_utility_no': self.nodes[end]['utility_no'],
                'reverse': 0,  # If traversed backwards, set 1
                'node_flag': node_flag
                # add group_no/sub_group_no if needed
            })
        return path_links

# --- Example Usage ---

# 1. Load nodes/links from DB into Python dicts/lists
# nodes = {row['id']: row for row in db_nodes}
# links = [row for row in db_links]

# 2. Create finder:
# finder = DownstreamFinder(nodes, links)

# 3. Find all downstream paths:
# paths = finder.find_paths(
#     start_node_id=42, utility_no=5, toolset_id=0, eq_poc_no='B1', data_codes='15000,107'
# )

# 4. For each path:
# for node_seq, link_seq, total_cost in paths:
#     path_links = finder.path_to_nw_pathlinks(node_seq, link_seq)
#     # Insert into nw_paths, nw_path_links
