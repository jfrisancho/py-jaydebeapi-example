#!/usr/bin/env python3
"""
Script to populate equipment-to-equipment connectivity in tb_equipment_connections
using nw_shortest_path(from_id, to_id).

Usage:
    python load_equipment_connections.py
    python load_equipment_connections.py -y
"""

import sys
import os
import argparse
from typing import List, Tuple, Dict, Set

# Add parent directory to path to import db module
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db import Database

def create_parser():
    parser = argparse.ArgumentParser(
        description="Load equipment connections via spatial path finder",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python load_equipment_connections.py       # With confirmation
  python load_equipment_connections.py -y    # Auto-confirm
        """
    )
    parser.add_argument('-y', '--yes', action='store_true', help='Auto-confirm insertion')
    return parser

def fetch_equipment_pocs(db: Database) -> List[Tuple[int, int, int, str]]:
    sql = """
        SELECT 
            ep.equipment_id, ep.id as poc_id, ep.node_id, e.guid
        FROM tb_equipment_pocs ep
        JOIN tb_equipments e ON ep.equipment_id = e.id
        WHERE ep.is_used = 1
    """
    return db.query(sql)

def clear_existing_connections(db: Database) -> int:
    sql = "DELETE FROM tb_equipment_connections"
    return db.update(sql)

def insert_connection(db: Database, from_eq: int, to_eq: int, from_poc: int, to_poc: int, path_id: int) -> bool:
    sql = """
        INSERT INTO tb_equipment_connections (
            from_equipment_id, to_equipment_id,
            from_poc_id, to_poc_id, path_id
        ) VALUES (?, ?, ?, ?, ?)
    """
    try:
        return db.update(sql, [from_eq, to_eq, from_poc, to_poc, path_id]) > 0
    except Exception as e:
        print(f"✗ Failed to insert path {path_id} ({from_eq} -> {to_eq}): {e}")
        return False

def load_connections(db: Database, pocs: List[Tuple[int, int, int, str]]) -> int:
    inserted = 0
    seen_paths: Set[Tuple[int, int]] = set()
    total = len(pocs)
    for i, (from_eq, from_poc, from_node, _) in enumerate(pocs):
        for j, (to_eq, to_poc, to_node, _) in enumerate(pocs):
            if from_eq == to_eq:
                continue

            key = (from_poc, to_poc)
            if key in seen_paths:
                continue

            try:
                result = db.query("SELECT nw_shortest_path(?, ?)", [from_node, to_node])
                if not result or result[0][0] <= 0:
                    continue
                path_id = result[0][0]
                if insert_connection(db, from_eq, to_eq, from_poc, to_poc, path_id):
                    inserted += 1
                    seen_paths.add(key)
                    print(f"✓ [{inserted}] {from_eq} → {to_eq} via path {path_id}")
            except Exception as e:
                print(f"⚠ Error on {from_poc} -> {to_poc}: {e}")
                continue

        print(f"→ Processed {i+1}/{total} source PoCs")
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

    try:
        result = db.query(count_sql)
        if result:
            total, sources, targets, paths = result[0]
            print(f"  ✓ Total connections: {total}")
            print(f"  ✓ Unique source equipments: {sources}")
            print(f"  ✓ Unique target equipments: {targets}")
            print(f"  ✓ Unique paths: {paths}")

        duplicates_sql = """
            SELECT from_poc_id, to_poc_id, COUNT(*) 
            FROM tb_equipment_connections
            GROUP BY from_poc_id, to_poc_id
            HAVING COUNT(*) > 1
        """
        duplicates = db.query(duplicates_sql)
        if duplicates:
            print(f"  ✗ Found {len(duplicates)} duplicate connection entries")
            for from_poc, to_poc, count in duplicates[:5]:
                print(f"    - from_poc {from_poc} → to_poc {to_poc}: {count} times")
        else:
            print("  ✓ No duplicate PoC-to-PoC connections")

    except Exception as e:
        print(f"✗ Error verifying loaded connections: {e}")


def main():
    parser = create_parser()
    args = parser.parse_args()

    print("Starting equipment connections loader...")
    db = None
    try:
        db = Database()
        print("✓ Connected to database")

        print("→ Fetching equipment PoCs...")
        pocs = fetch_equipment_pocs(db)
        print(f"✓ Retrieved {len(pocs)} used PoCs")

        if not pocs:
            print("No valid PoCs to process. Exiting.")
            return

        if not args.yes:
            confirm = input("Proceed with loading? (y/N): ").strip().lower()
            if confirm not in ['y', 'yes']:
                print("✗ Aborted by user.")
                return
        else:
            print("✓ Auto-confirmed.")

        print("→ Clearing existing connections...")
        deleted = clear_existing_connections(db)
        print(f"✓ Removed {deleted} existing records")

        print("→ Generating new connections...")
        count = load_connections(db, pocs)

        print(f"\n✓ Finished. Total connections inserted: {count}")
        verify_loaded_connections(db)

    except Exception as e:
        print(f"✗ Fatal error: {e}")
        sys.exit(1)
    finally:
        if db:
            db.close()
            print("✓ Closed database connection")

if __name__ == "__main__":
    main()
