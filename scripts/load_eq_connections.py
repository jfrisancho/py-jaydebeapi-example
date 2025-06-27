#!/usr/bin/env python3
"""
Script to load equipment-to-equipment connectivity into tb_equipment_connections,
using nw_downstream from either each PoC or the logical node per equipment.

Usage:
    python load_equipment_connections.py --mode poc     # Downstream from every PoC (default)
    python load_equipment_connections.py --mode logical # Downstream from logical node per equipment
    python load_equipment_connections.py -y             # Unattended mode
"""

import sys
import os
import argparse
from typing import List, Tuple, Dict, Optional

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db import Database

def create_parser():
    parser = argparse.ArgumentParser(
        description="Load equipment connections using nw_downstream",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--mode", choices=["poc", "logical"], default="poc",
        help="Downstream mode: 'poc' (each PoC) or 'logical' (logical node per equipment)"
    )
    parser.add_argument(
        '-y', '--yes',
        action='store_true',
        help='Auto-confirm without prompting'
    )
    return parser

def fetch_logical_nodes(db: Database) -> Dict[int, Dict]:
    """
    Returns: equipment_id -> {node_id, toolset, data_code, guid}
    """
    sql = """SELECT id, node_id, toolset, data_code, guid
             FROM tb_equipments
             WHERE is_active = 1"""
    rows = db.query(sql)
    return {row[0]: {'node_id': row[1], 'toolset': row[2], 'data_code': row[3], 'guid': row[4]} for row in rows}

def fetch_pocs(db: Database) -> List[Tuple]:
    """
    Returns: (poc_id, equipment_id, node_id, utility, eq_poc_no)
    """
    sql = """SELECT id, equipment_id, node_id, utility, eq_poc_no
             FROM tb_equipment_pocs
             WHERE is_used = 1"""
    return db.query(sql)

def clear_existing_connections(db: Database) -> int:
    sql = "DELETE FROM tb_equipment_connections"
    return db.update(sql)

def call_downstream(
    db: Database,
    node_id: int,
    ignore_node_id: int = 0,
    utility: Optional[int] = 0,
    toolset_id: Optional[int] = 0,
    eq_poc_no: Optional[str] = '',
    data_codes: Optional[str] = '15000'
) -> Optional[int]:
    sql = "SELECT nw_downstream(?, ?, ?, ?, ?, ?)"
    params = [
        node_id,
        ignore_node_id,
        utility if utility else 0,
        toolset_id if toolset_id else 0,
        eq_poc_no or '',
        data_codes or '15000'
    ]
    try:
        result = db.query(sql, params)
        return result[0][0] if result and result[0][0] > 0 else None
    except Exception as e:
        print(f"  ⚠ Error in nw_downstream({params}): {e}")
        return None

def insert_connection(
    db: Database,
    from_eq: int,
    to_eq: int,
    from_poc: Optional[int],
    to_poc: Optional[int],
    path_id: int
) -> bool:
    sql = """
        INSERT INTO tb_equipment_connections (
            from_equipment_id, to_equipment_id,
            from_poc_id, to_poc_id, path_id
        ) VALUES (?, ?, ?, ?, ?)
    """
    try:
        return db.update(sql, [from_eq, to_eq, from_poc, to_poc, path_id]) > 0
    except Exception as e:
        print(f"✗ Failed to insert connection {from_eq} → {to_eq} (path {path_id}): {e}")
        return False

def process_poc_mode(db: Database):
    """
    For each PoC, find downstream path and store connections (precise per-PoC resolution)
    """
    pocs = fetch_pocs(db)
    eq_map = fetch_logical_nodes(db)
    inserted = 0
    seen_paths = set()

    print(f"✓ Processing {len(pocs)} used PoCs (mode: PoC)")

    for idx, (poc_id, equipment_id, node_id, utility, eq_poc_no) in enumerate(pocs, 1):
        meta = eq_map.get(equipment_id)
        if not meta:
            print(f"  ⚠ Equipment {equipment_id} not found for PoC {poc_id}")
            continue

        # Call downstream from this PoC
        path_id = call_downstream(
            db,
            node_id=node_id,
            ignore_node_id=0,
            utility=utility or 0,
            toolset_id=meta['toolset'],
            eq_poc_no=eq_poc_no or '',
            data_codes='15000'
        )

        if not path_id or (poc_id, path_id) in seen_paths:
            continue

        # Find the end node (target PoC/equipment) for this path
        # Optionally you can query nw_path_links and resolve to to_poc_id/to_eq_id if you wish
        # For simplicity, we'll just store from/to as self for now unless more info is needed

        # NOTE: Add logic to resolve to_poc_id, to_equipment_id from path_id if desired

        success = insert_connection(db, equipment_id, None, poc_id, None, path_id)
        if success:
            inserted += 1
            seen_paths.add((poc_id, path_id))
            print(f"✓ [{inserted}] Equipment {equipment_id} PoC {poc_id} via path {path_id}")

        if idx % 50 == 0:
            print(f"  ...processed {idx} PoCs")

    return inserted

def process_logical_mode(db: Database):
    """
    For each equipment, call downstream from the logical node, ignoring itself.
    Less precise but much faster for bulk connectivity.
    """
    eq_map = fetch_logical_nodes(db)
    inserted = 0
    seen_paths = set()

    print(f"✓ Processing {len(eq_map)} equipment logical nodes (mode: logical)")

    for idx, (eq_id, meta) in enumerate(eq_map.items(), 1):
        path_id = call_downstream(
            db,
            node_id=meta['node_id'],
            ignore_node_id=meta['node_id'],  # Prevent revisiting self
            utility=0,
            toolset_id=meta['toolset'],
            eq_poc_no='',
            data_codes='15000'
        )

        if not path_id or (eq_id, path_id) in seen_paths:
            continue

        success = insert_connection(db, eq_id, None, None, None, path_id)
        if success:
            inserted += 1
            seen_paths.add((eq_id, path_id))
            print(f"✓ [{inserted}] Equipment {eq_id} via path {path_id}")

        if idx % 20 == 0:
            print(f"  ...processed {idx} logical nodes")
    return inserted

def verify_loaded_connections(db: Database):
    print("\nVerifying loaded equipment connections...")
    count_sql = """
        SELECT 
            COUNT(*) AS total_connections,
            COUNT(DISTINCT from_equipment_id) AS unique_sources,
            COUNT(DISTINCT to_equipment_id) AS unique_targets,
            COUNT(DISTINCT path_id) AS unique_paths
        FROM tb_equipment_connections
    """
    result = db.query(count_sql)
    if result:
        total, sources, targets, paths = result[0]
        print(f"  ✓ Total connections: {total}")
        print(f"  ✓ Unique source equipments: {sources}")
        print(f"  ✓ Unique target equipments: {targets}")
        print(f"  ✓ Unique paths: {paths}")

    duplicates_sql = """
        SELECT from_equipment_id, from_poc_id, COUNT(*) 
        FROM tb_equipment_connections
        GROUP BY from_equipment_id, from_poc_id
        HAVING COUNT(*) > 1
    """
    duplicates = db.query(duplicates_sql)
    if duplicates:
        print(f"  ✗ Found {len(duplicates)} duplicate connection entries")
        for from_eq, from_poc, count in duplicates[:5]:
            print(f"    - from_equipment {from_eq} from_poc {from_poc}: {count} times")
    else:
        print("  ✓ No duplicate equipment/PoC connection pairs found")

def main():
    parser = create_parser()
    args = parser.parse_args()

    print("Starting equipment connections loader...")

    db = None
    try:
        db = Database()
        print("✓ Connected to database")

        if not args.yes:
            response = input("\nProceed with loading? (y/N): ").strip().lower()
            if response not in ['y', 'yes']:
                print("Loading cancelled by user.")
                return
        else:
            print("✓ Auto-confirmed due to -y flag...")

        print("\nClearing existing connections...")
        deleted = clear_existing_connections(db)
        print(f"✓ Removed {deleted} existing records\n")

        print(f"→ Processing equipment connections (mode: {args.mode})...")
        if args.mode == "poc":
            inserted = process_poc_mode(db)
        else:
            inserted = process_logical_mode(db)
        print(f"\n✓ Finished. Total connections inserted: {inserted}\n")

        verify_loaded_connections(db)

    except Exception as e:
        print(f"✗ Fatal error: {e}")
        sys.exit(1)
    finally:
        if db:
            print("\nClosing database connection...")
            db.close()
            print("✓ Database connection closed")

    print("\nEquipment connections loading process completed successfully!")

if __name__ == "__main__":
    main()
