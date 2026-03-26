#!/usr/bin/env python3
"""
Print a short summary of cartographer.db: table row counts only (no row data).
Default: cartographer.db in current working directory.
Usage: python summarize_db.py [path/to/cartographer.db]
"""

import argparse
import os
import sqlite3
import sys
from pathlib import Path


def main():
    root = Path(os.environ.get("CARTOGRAPHER_ROOT", Path.cwd())).resolve()
    db_path = root / "cartographer.db"
    parser = argparse.ArgumentParser(description="Summarize cartographer DB (row counts, no data).")
    parser.add_argument("db_path", nargs="?", default=None, help="Path to cartographer.db")
    args = parser.parse_args()
    if args.db_path:
        db_path = Path(args.db_path).resolve()
    if not db_path.exists():
        print(f"DB not found: {db_path}", file=sys.stderr)
        sys.exit(1)

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    tables = [r[0] for r in cur.fetchall()]

    print(f"Database: {db_path}\n")
    for table in tables:
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        n = cur.fetchone()[0]
        print(f"  {table}: {n} rows")
    if "queue" in tables:
        cur.execute("SELECT status, COUNT(*) FROM queue GROUP BY status ORDER BY status")
        for (status, count) in cur.fetchall():
            print(f"    → {status}: {count}")
    if "publications" in tables:
        cur.execute("SELECT COUNT(*) FROM publications")
        n_pub = cur.fetchone()[0]
        print(f"\nPublications: {n_pub}")
    conn.close()


if __name__ == "__main__":
    main()
