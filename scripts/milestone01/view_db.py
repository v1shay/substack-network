#!/usr/bin/env python3
"""
Print cartographer DB tables in a readable table form.
Default: cartographer.db in current working directory (same as the crawler).
Usage: python view_db.py [path/to/cartographer.db]
       python view_db.py --counts  # table sizes only
"""

import argparse
import os
import sqlite3
import sys
from pathlib import Path

def view_table(cursor, table: str, limit: int = 50):
    cursor.execute(f"SELECT * FROM {table} LIMIT {limit}")
    rows = cursor.fetchall()
    if not rows:
        print(f"  (empty)\n")
        return
    colnames = [d[0] for d in cursor.description]
    colwidths = [max(len(str(colnames[i])), *(len(str(r[i])[:40]) for r in rows)) for i in range(len(colnames))]
    colwidths = [min(w + 1, 42) for w in colwidths]
    fmt = "  ".join(f"{{:<{w}}}" for w in colwidths)
    print(fmt.format(*colnames))
    print("-" * (sum(colwidths) + 2 * (len(colwidths) - 1)))
    for row in rows:
        print(fmt.format(*[str(v)[:40] if v is not None else "" for v in row]))
    if len(rows) == limit:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        total = cursor.fetchone()[0]
        print(f"  ... ({total} rows total, showing {limit})")
    print()

def main():
    # Default: CARTOGRAPHER_ROOT/cartographer.db or cwd/cartographer.db
    root = Path(os.environ.get("CARTOGRAPHER_ROOT", Path.cwd())).resolve()
    db_path = root / "cartographer.db"
    parser = argparse.ArgumentParser(description="View cartographer DB tables.")
    parser.add_argument("db_path", nargs="?", default=None, help="Path to cartographer.db")
    parser.add_argument("--counts", "-c", action="store_true", help="Print only table row counts")
    args = parser.parse_args()
    if args.db_path:
        db_path = Path(args.db_path).resolve()
    if not db_path.exists():
        print(f"DB not found: {db_path}")
        sys.exit(1)
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    tables = [r[0] for r in cur.fetchall()]
    print(f"Database: {db_path}\n")
    if args.counts:
        for table in tables:
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            n = cur.fetchone()[0]
            print(f"  {table}: {n} rows")
        if "publications" in tables:
            cur.execute("SELECT COUNT(*) FROM publications")
            n_pub = cur.fetchone()[0]
            print(f"\nPublications: {n_pub}")
        conn.close()
        return
    conn.row_factory = sqlite3.Row
    for table in tables:
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        n = cur.fetchone()[0]
        print(f"=== {table} ({n} rows) ===")
        view_table(cur, table)
    # Show publications count at bottom (so it's visible without scrolling up)
    if "publications" in tables:
        cur.execute("SELECT COUNT(*) FROM publications")
        n_pub = cur.fetchone()[0]
        print(f"Publications: {n_pub}")
    conn.close()

if __name__ == "__main__":
    main()
