"""
Path storage and retrieval management for network connectivity analysis.
"""
import logging
import json
import hashlib
from typing import Optional, Dict, Any, List, Tuple, Set
from datetime import datetime
import sqlite3


class PathManager:
    """Manages path finding, storage, and retrieval operations."""
    
    def __init__(self, db_connection: sqlite3.Connection):
        self.db = db_connection
        self.logger = logging.getLogger(__name__)
        self._path_cache = {}  # Simple in-memory cache for recently found paths
        
    def find_path(self, start_node_id: int, end_node_id: int, 
                  max_depth: int = 50) -> Optional[Dict[str, Any]]:
        """
        Find path between two nodes using database-based graph traversal.
        
        Returns:
            Dict containing path information or None if no path found
        """
        
        # Check cache first
        cache_key = f"{start_node_id}-{end_node_id}"
        if cache_key in self._path_cache:
            return self._path_cache[cache_key]
        
        try:
            # Use recursive CTE for path finding
            path_query = """
            WITH RECURSIVE path_finder(
                current_node, 
                target_node, 
                path_nodes, 
                path_links, 
                depth,
                visited_nodes
            ) AS (
                -- Base case: starting node
                SELECT 
                    ?, -- start_node_id
                    ?, -- end_node_id
                    json_array(?), -- path starts with start_node
                    json_array(), -- empty links initially
                    0,
                    ?  -- visited nodes tracker
                
                UNION ALL
                
                -- Recursive case: find connections
                SELECT 
                    conn.to_poc_id,
                    pf.target_node,
                    json_insert(pf.path_nodes, '$[#]', conn.to_poc_id),
                    json_insert(pf.path_links, '$[#]', conn.id),
                    pf.depth + 1,
                    pf.visited_nodes || ',' || conn.to_poc_id
                FROM path_finder pf
                JOIN tb_equipment_poc_connections conn ON pf.current_node = conn.from_poc_id
                WHERE pf.current_node != pf.target_node
                  AND pf.depth < ?
                  AND conn.is_valid = 1
                  AND instr(pf.visited_nodes, ',' || conn.to_poc_id || ',') = 0
            )
            SELECT 
                path_nodes,
                path_links,
                depth
            FROM path_finder 
            WHERE current_node = target_node
            ORDER BY depth ASC
            LIMIT 1
            """
            
            # Execute path finding query
            cursor = self.db.execute(path_query, [
                start_node_id, 
                end_node_id, 
                start_node_id,
                f",{start_node_id},",  # visited nodes format
                max_depth
            ])
            
            result = cursor.fetchone()
            
            if not result:
                return None
            
            # Parse the result
            nodes_json = result[0]
            links_json = result[1]
            depth = result[2]
            
            path_nodes = json.loads(nodes_json) if nodes_json else []
            path_links = json.loads(links_json) if links_json else []
            
            # Get additional path information
            path_info = self._enrich_path_data(path_nodes, path_links)
            
            path_result = {
                'nodes': path_nodes,
                'links': path_links,
                'depth': depth,
                'node_count': len(path_nodes),
                'link_count': len(path_links),
                **path_info
            }
            
            # Cache the result
            self._path_cache[cache_key] = path_result
            
            return path_result
            
        except Exception as e:
            self.logger.error(f"Error finding path from {start_node_id} to {end_node_id}: {e}")
            return None
    
    def _enrich_path_data(self, path_nodes: List[int], path_links: List[int]) -> Dict[str, Any]:
        """Enrich path data with additional information."""
        
        if not path_nodes:
            return {
                'total_length_mm': result[9],
            'coverage': result[10],
            'path_context': path_context,
            'data_codes': data_codes,
            'utilities': utilities,
            'references': references,
            'created_at': result[15]
        }
    
    def get_paths_by_run(self, run_id: str) -> List[Dict[str, Any]]:
        """Get all paths for a specific run."""
        
        query = """
        SELECT DISTINCT pd.id
        FROM tb_path_definitions pd
        JOIN tb_attempt_paths ap ON pd.id = ap.path_definition_id
        WHERE ap.run_id = ?
        ORDER BY pd.created_at
        """
        
        cursor = self.db.execute(query, [run_id])
        path_ids = [row[0] for row in cursor.fetchall()]
        
        return [self.get_path_by_id(pid) for pid in path_ids if self.get_path_by_id(pid)]
    
    def get_similar_paths(self, path_nodes: List[int], similarity_threshold: float = 0.7) -> List[Dict[str, Any]]:
        """Find similar paths based on node overlap."""
        
        if not path_nodes:
            return []
        
        # Get all paths and calculate similarity
        query = """
        SELECT id, path_context, node_count
        FROM tb_path_definitions
        WHERE node_count > 0
        """
        
        cursor = self.db.execute(query)
        all_paths = cursor.fetchall()
        
        similar_paths = []
        path_nodes_set = set(path_nodes)
        
        for path_row in all_paths:
            path_id, path_context_json, node_count = path_row
            
            if not path_context_json:
                continue
            
            try:
                path_context = json.loads(path_context_json)
                other_nodes = set(path_context.get('nodes', []))
                
                if not other_nodes:
                    continue
                
                # Calculate Jaccard similarity
                intersection = len(path_nodes_set & other_nodes)
                union = len(path_nodes_set | other_nodes)
                
                if union > 0:
                    similarity = intersection / union
                    
                    if similarity >= similarity_threshold:
                        path_detail = self.get_path_by_id(path_id)
                        if path_detail:
                            path_detail['similarity'] = similarity
                            similar_paths.append(path_detail)
            
            except json.JSONDecodeError:
                continue
        
        # Sort by similarity (descending)
        similar_paths.sort(key=lambda x: x['similarity'], reverse=True)
        
        return similar_paths
    
    def get_path_statistics(self, filters: Dict[str, Any] = None) -> Dict[str, Any]:
        """Get comprehensive path statistics."""
        
        base_query = """
        SELECT 
            COUNT(*) as total_paths,
            AVG(node_count) as avg_nodes,
            AVG(link_count) as avg_links,
            AVG(total_length_mm) as avg_length,
            MIN(node_count) as min_nodes,
            MAX(node_count) as max_nodes,
            MIN(link_count) as min_links,
            MAX(link_count) as max_links,
            COUNT(DISTINCT target_fab) as unique_fabs,
            COUNT(DISTINCT target_model_no) as unique_models,
            COUNT(DISTINCT target_phase_no) as unique_phases
        FROM tb_path_definitions
        WHERE 1=1
        """
        
        params = []
        
        if filters:
            if filters.get('source_type'):
                base_query += " AND source_type = ?"
                params.append(filters['source_type'])
            
            if filters.get('target_fab'):
                base_query += " AND target_fab = ?"
                params.append(filters['target_fab'])
            
            if filters.get('target_model_no'):
                base_query += " AND target_model_no = ?"
                params.append(filters['target_model_no'])
            
            if filters.get('target_phase_no'):
                base_query += " AND target_phase_no = ?"
                params.append(filters['target_phase_no'])
        
        cursor = self.db.execute(base_query, params)
        stats = cursor.fetchone()
        
        # Get source type distribution
        source_query = """
        SELECT source_type, COUNT(*) as count
        FROM tb_path_definitions
        WHERE 1=1
        """
        
        if filters:
            if filters.get('target_fab'):
                source_query += " AND target_fab = ?"
        
        source_query += " GROUP BY source_type"
        
        cursor = self.db.execute(source_query, params[:1] if filters and filters.get('target_fab') else [])
        source_distribution = {row[0]: row[1] for row in cursor.fetchall()}
        
        # Get complexity distribution
        complexity_query = """
        SELECT 
            CASE 
                WHEN node_count <= 5 THEN 'SIMPLE'
                WHEN node_count <= 15 THEN 'MEDIUM'
                ELSE 'COMPLEX'
            END as complexity,
            COUNT(*) as count
        FROM tb_path_definitions
        WHERE 1=1
        """
        
        if filters and filters.get('target_fab'):
            complexity_query += " AND target_fab = ?"
        
        complexity_query += " GROUP BY complexity"
        
        cursor = self.db.execute(complexity_query, params[:1] if filters and filters.get('target_fab') else [])
        complexity_distribution = {row[0]: row[1] for row in cursor.fetchall()}
        
        return {
            'total_paths': stats[0] or 0,
            'avg_nodes': float(stats[1]) if stats[1] else 0.0,
            'avg_links': float(stats[2]) if stats[2] else 0.0,
            'avg_length_mm': float(stats[3]) if stats[3] else 0.0,
            'min_nodes': stats[4] or 0,
            'max_nodes': stats[5] or 0,
            'min_links': stats[6] or 0,
            'max_links': stats[7] or 0,
            'unique_fabs': stats[8] or 0,
            'unique_models': stats[9] or 0,
            'unique_phases': stats[10] or 0,
            'source_distribution': source_distribution,
            'complexity_distribution': complexity_distribution
        }
    
    def find_paths_by_criteria(self, criteria: Dict[str, Any], 
                              limit: int = 100) -> List[Dict[str, Any]]:
        """Find paths matching specific criteria."""
        
        query = """
        SELECT id FROM tb_path_definitions
        WHERE 1=1
        """
        
        params = []
        
        # Add filters
        if criteria.get('min_nodes'):
            query += " AND node_count >= ?"
            params.append(criteria['min_nodes'])
        
        if criteria.get('max_nodes'):
            query += " AND node_count <= ?"
            params.append(criteria['max_nodes'])
        
        if criteria.get('min_links'):
            query += " AND link_count >= ?"
            params.append(criteria['min_links'])
        
        if criteria.get('max_links'):
            query += " AND link_count <= ?"
            params.append(criteria['max_links'])
        
        if criteria.get('source_type'):
            query += " AND source_type = ?"
            params.append(criteria['source_type'])
        
        if criteria.get('target_fab'):
            query += " AND target_fab = ?"
            params.append(criteria['target_fab'])
        
        if criteria.get('utilities'):
            # Check if path contains any of the specified utilities
            utility_conditions = []
            for utility in criteria['utilities']:
                utility_conditions.append("utilities_scope LIKE ?")
                params.append(f'%"{utility}"%')
            
            if utility_conditions:
                query += " AND (" + " OR ".join(utility_conditions) + ")"
        
        if criteria.get('data_codes'):
            # Check if path contains any of the specified data codes
            code_conditions = []
            for code in criteria['data_codes']:
                code_conditions.append("data_codes_scope LIKE ?")
                params.append(f'%"{code}"%')
            
            if code_conditions:
                query += " AND (" + " OR ".join(code_conditions) + ")"
        
        # Add ordering and limit
        query += " ORDER BY created_at DESC LIMIT ?"
        params.append(limit)
        
        cursor = self.db.execute(query, params)
        path_ids = [row[0] for row in cursor.fetchall()]
        
        return [self.get_path_by_id(pid) for pid in path_ids if self.get_path_by_id(pid)]
    
    def delete_path(self, path_def_id: int) -> bool:
        """Delete a path definition and related records."""
        
        try:
            # Delete related records first (foreign key constraints)
            self.db.execute("DELETE FROM tb_path_tags WHERE path_definition_id = ?", [path_def_id])
            self.db.execute("DELETE FROM tb_attempt_paths WHERE path_definition_id = ?", [path_def_id])
            self.db.execute("DELETE FROM tb_validation_errors WHERE path_definition_id = ?", [path_def_id])
            
            # Delete the path definition
            cursor = self.db.execute("DELETE FROM tb_path_definitions WHERE id = ?", [path_def_id])
            
            self.db.commit()
            
            return cursor.rowcount > 0
            
        except Exception as e:
            self.logger.error(f"Error deleting path {path_def_id}: {e}")
            self.db.rollback()
            return False
    
    def update_path_coverage(self, path_def_id: int, coverage: float) -> bool:
        """Update path coverage value."""
        
        try:
            cursor = self.db.execute(
                "UPDATE tb_path_definitions SET coverage = ? WHERE id = ?",
                [coverage, path_def_id]
            )
            self.db.commit()
            return cursor.rowcount > 0
            
        except Exception as e:
            self.logger.error(f"Error updating coverage for path {path_def_id}: {e}")
            return False
    
    def get_path_tags(self, path_def_id: int) -> List[Dict[str, Any]]:
        """Get all tags for a specific path."""
        
        query = """
        SELECT tag_type, tag_code, tag, source, confidence, created_at, notes
        FROM tb_path_tags
        WHERE path_definition_id = ?
        ORDER BY created_at
        """
        
        cursor = self.db.execute(query, [path_def_id])
        tags = []
        
        for row in cursor.fetchall():
            tags.append({
                'tag_type': row[0],
                'tag_code': row[1],
                'tag': row[2],
                'source': row[3],
                'confidence': row[4],
                'created_at': row[5],
                'notes': row[6]
            })
        
        return tags
    
    def add_custom_tag(self, run_id: str, path_def_id: int, tag_type: str,
                      tag_code: str, tag: str, notes: str = None) -> bool:
        """Add a custom tag to a path."""
        
        try:
            self._create_path_tag(
                run_id, path_def_id, tag_type, tag_code, tag,
                source="USER", confidence=1.0
            )
            
            if notes:
                # Update the tag with notes
                self.db.execute(
                    "UPDATE tb_path_tags SET notes = ? WHERE run_id = ? AND path_definition_id = ? AND tag_code = ?",
                    [notes, run_id, path_def_id, tag_code]
                )
                self.db.commit()
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error adding custom tag: {e}")
            return False
    
    def clear_cache(self) -> None:
        """Clear the path cache."""
        self._path_cache.clear()
        self.logger.info("Path cache cleared")
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        return {
            'cache_size': len(self._path_cache),
            'cached_paths': list(self._path_cache.keys())
        }
    
    def export_paths(self, run_id: Optional[str] = None, 
                    format: str = 'json') -> str:
        """Export paths to various formats."""
        
        if run_id:
            paths = self.get_paths_by_run(run_id)
        else:
            # Get all paths (limit for safety)
            paths = self.find_paths_by_criteria({}, limit=1000)
        
        if format.lower() == 'json':
            return json.dumps(paths, indent=2, default=str)
        else:
            raise ValueError(f"Unsupported export format: {format}")
    
    def validate_path_integrity(self, path_def_id: int) -> Dict[str, Any]:
        """Validate path data integrity."""
        
        path = self.get_path_by_id(path_def_id)
        if not path:
            return {'valid': False, 'errors': ['Path not found']}
        
        errors = []
        warnings = []
        
        # Check if nodes and links are consistent
        path_context = path.get('path_context', {})
        nodes = path_context.get('nodes', [])
        links = path_context.get('links', [])
        
        # Validate node count
        if len(nodes) != path['node_count']:
            errors.append(f"Node count mismatch: expected {path['node_count']}, found {len(nodes)}")
        
        # Validate link count
        if len(links) != path['link_count']:
            errors.append(f"Link count mismatch: expected {path['link_count']}, found {len(links)}")
        
        # Check if all nodes exist in database
        if nodes:
            node_placeholders = ','.join(['?'] * len(nodes))
            check_query = f"""
            SELECT COUNT(*) FROM tb_equipment_pocs 
            WHERE node_id IN ({node_placeholders})
            """
            cursor = self.db.execute(check_query, nodes)
            existing_nodes = cursor.fetchone()[0]
            
            if existing_nodes != len(nodes):
                warnings.append(f"Some nodes may no longer exist in database: {existing_nodes}/{len(nodes)} found")
        
        # Check if all links exist in database
        if links:
            link_placeholders = ','.join(['?'] * len(links))
            check_query = f"""
            SELECT COUNT(*) FROM tb_equipment_poc_connections 
            WHERE id IN ({link_placeholders})
            """
            cursor = self.db.execute(check_query, links)
            existing_links = cursor.fetchone()[0]
            
            if existing_links != len(links):
                warnings.append(f"Some links may no longer exist in database: {existing_links}/{len(links)} found")
        
        return {
            'valid': len(errors) == 0,
            'errors': errors,
            'warnings': warnings,
            'path_id': path_def_id
        }': 0.0,
                'data_codes': [],
                'utilities': [],
                'references': [],
                'equipment_ids': []
            }
        
        # Get node details
        node_placeholders = ','.join(['?'] * len(path_nodes))
        node_query = f"""
        SELECT 
            ep.node_id,
            ep.equipment_id,
            ep.utility_no,
            ep.reference,
            ep.markers,
            e.data_code,
            e.guid as equipment_guid
        FROM tb_equipment_pocs ep
        JOIN tb_equipments e ON ep.equipment_id = e.id
        WHERE ep.node_id IN ({node_placeholders})
        """
        
        cursor = self.db.execute(node_query, path_nodes)
        node_details = {row[0]: row[1:] for row in cursor.fetchall()}
        
        # Get link details for length calculation
        if path_links:
            link_placeholders = ','.join(['?'] * len(path_links))
            link_query = f"""
            SELECT id, connection_type
            FROM tb_equipment_poc_connections
            WHERE id IN ({link_placeholders})
            """
            cursor = self.db.execute(link_query, path_links)
            link_details = cursor.fetchall()
        else:
            link_details = []
        
        # Extract information
        data_codes = set()
        utilities = set()
        references = set()
        equipment_ids = set()
        
        for node_id in path_nodes:
            if node_id in node_details:
                detail = node_details[node_id]
                equipment_ids.add(detail[0])  # equipment_id
                if detail[1]:  # utility_no
                    utilities.add(detail[1])
                if detail[2]:  # reference
                    references.add(detail[2])
                if detail[4]:  # data_code
                    data_codes.add(detail[4])
        
        # Calculate approximate length (simplified calculation)
        total_length = len(path_links) * 1000.0  # Assume 1m per link as approximation
        
        return {
            'total_length_mm': total_length,
            'data_codes': list(data_codes),
            'utilities': list(utilities),
            'references': list(references),
            'equipment_ids': list(equipment_ids)
        }
    
    def store_path(self, run_id: str, path_data: Dict[str, Any], 
                   source_type: str = "RANDOM", **metadata) -> int:
        """Store path definition in database."""
        
        # Generate path hash for uniqueness
        path_hash = self._generate_path_hash(path_data['nodes'], path_data['links'])
        
        # Check if path already exists
        existing_query = "SELECT id FROM tb_path_definitions WHERE path_hash = ?"
        cursor = self.db.execute(existing_query, [path_hash])
        existing = cursor.fetchone()
        
        if existing:
            return existing[0]
        
        # Prepare path context
        path_context = {
            'nodes': path_data['nodes'],
            'links': path_data['links'],
            'node_count': path_data['node_count'],
            'link_count': path_data['link_count'],
            'depth': path_data.get('depth', 0)
        }
        
        # Insert new path definition
        insert_sql = """
        INSERT INTO tb_path_definitions (
            path_hash, source_type, scope, target_fab, target_model_no, 
            target_phase_no, target_toolset_no, target_data_codes, 
            target_utilities, target_references, node_count, link_count, 
            total_length_mm, coverage, data_codes_scope, utilities_scope, 
            references_scope, path_context
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        # Prepare parameters
        data_codes_json = json.dumps(path_data.get('data_codes', []))
        utilities_json = json.dumps(path_data.get('utilities', []))
        references_json = json.dumps(path_data.get('references', []))
        path_context_json = json.dumps(path_context)
        
        # Calculate coverage (placeholder - will be updated by coverage manager)
        coverage = self._calculate_path_coverage(path_data, metadata)
        
        params = [
            path_hash,
            source_type,
            metadata.get('scope', 'CONNECTIVITY'),
            metadata.get('target_fab'),
            metadata.get('target_model_no'),
            metadata.get('target_phase_no'),
            metadata.get('target_toolset_no'),
            ','.join(map(str, path_data.get('data_codes', []))),
            ','.join(map(str, path_data.get('utilities', []))),
            ','.join(path_data.get('references', [])),
            path_data['node_count'],
            path_data['link_count'],
            path_data.get('total_length_mm', 0.0),
            coverage,
            data_codes_json,
            utilities_json,
            references_json,
            path_context_json
        ]
        
        cursor = self.db.execute(insert_sql, params)
        self.db.commit()
        
        path_def_id = cursor.lastrowid
        
        # Auto-generate tags for the path
        self._generate_automatic_tags(run_id, path_def_id, path_data, metadata)
        
        return path_def_id
    
    def _generate_path_hash(self, nodes: List[int], links: List[int]) -> str:
        """Generate unique hash for path."""
        
        # Create consistent representation
        path_repr = f"nodes:{','.join(map(str, sorted(nodes)))};links:{','.join(map(str, sorted(links)))}"
        
        # Generate SHA-256 hash
        return hashlib.sha256(path_repr.encode('utf-8')).hexdigest()
    
    def _calculate_path_coverage(self, path_data: Dict[str, Any], metadata: Dict[str, Any]) -> float:
        """Calculate initial coverage estimate for path."""
        
        # Simple coverage calculation based on unique nodes/links
        # This will be refined by the coverage manager
        
        node_count = path_data.get('node_count', 0)
        link_count = path_data.get('link_count', 0)
        
        # Base coverage on path size (will be normalized by coverage manager)
        return float(node_count + link_count)
    
    def _generate_automatic_tags(self, run_id: str, path_def_id: int, 
                               path_data: Dict[str, Any], metadata: Dict[str, Any]) -> None:
        """Generate automatic tags for the path."""
        
        tags_to_create = []
        
        # Fabric tag
        if metadata.get('target_fab'):
            tags_to_create.append({
                'tag_type': 'FAB',
                'tag_code': metadata['target_fab'],
                'tag': f"FAB_{metadata['target_fab']}"
            })
        
        # Model tag
        if metadata.get('target_model_no'):
            model_name = 'BIM' if metadata['target_model_no'] == 1 else '5D'
            tags_to_create.append({
                'tag_type': 'DAT',
                'tag_code': str(metadata['target_model_no']),
                'tag': f"MODEL_{model_name}"
            })
        
        # Phase tag
        if metadata.get('target_phase_no'):
            tags_to_create.append({
                'tag_type': 'DAT',
                'tag_code': str(metadata['target_phase_no']),
                'tag': f"PHASE_{metadata['target_phase_no']}"
            })
        
        # Utility tags
        for utility in path_data.get('utilities', []):
            tags_to_create.append({
                'tag_type': 'UTY',
                'tag_code': str(utility),
                'tag': f"UTILITY_{utility}"
            })
        
        # Data code tags
        for data_code in path_data.get('data_codes', []):
            tags_to_create.append({
                'tag_type': 'DAT',
                'tag_code': str(data_code),
                'tag': f"DATACODE_{data_code}"
            })
        
        # Path complexity tags
        node_count = path_data.get('node_count', 0)
        if node_count <= 5:
            complexity = 'SIMPLE'
        elif node_count <= 15:
            complexity = 'MEDIUM'
        else:
            complexity = 'COMPLEX'
        
        tags_to_create.append({
            'tag_type': 'QA',
            'tag_code': complexity,
            'tag': f"COMPLEXITY_{complexity}"
        })
        
        # Insert tags
        for tag_info in tags_to_create:
            self._create_path_tag(run_id, path_def_id, **tag_info)
    
    def _create_path_tag(self, run_id: str, path_def_id: int, tag_type: str,
                        tag_code: str, tag: str, source: str = "SYSTEM",
                        confidence: float = 1.0) -> None:
        """Create a path tag."""
        
        insert_sql = """
        INSERT INTO tb_path_tags (
            run_id, path_definition_id, tag_type, tag_code, tag, 
            source, confidence
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """
        
        self.db.execute(insert_sql, [
            run_id, path_def_id, tag_type, tag_code, tag, source, confidence
        ])
        self.db.commit()
    
    def get_path_by_id(self, path_def_id: int) -> Optional[Dict[str, Any]]:
        """Retrieve path definition by ID."""
        
        query = """
        SELECT 
            path_hash, source_type, scope, target_fab, target_model_no,
            target_phase_no, target_toolset_no, node_count, link_count,
            total_length_mm, coverage, path_context, data_codes_scope,
            utilities_scope, references_scope, created_at
        FROM tb_path_definitions
        WHERE id = ?
        """
        
        cursor = self.db.execute(query, [path_def_id])
        result = cursor.fetchone()
        
        if not result:
            return None
        
        # Parse JSON fields
        path_context = json.loads(result[11]) if result[11] else {}
        data_codes = json.loads(result[12]) if result[12] else []
        utilities = json.loads(result[13]) if result[13] else []
        references = json.loads(result[14]) if result[14] else []
        
        return {
            'id': path_def_id,
            'path_hash': result[0],
            'source_type': result[1],
            'scope': result[2],
            'target_fab': result[3],
            'target_model_no': result[4],
            'target_phase_no': result[5],
            'target_toolset_no': result[6],
            'node_count': result[7],
            'link_count': result[8],
            'total_length_mm