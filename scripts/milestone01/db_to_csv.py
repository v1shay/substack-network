#!/usr/bin/env python3
"""
Export cartographer DB tables to CSV files.
Default: reads cartographer.db from cwd, writes to data/ (one CSV per table).
Usage: python db_to_csv.py [path/to/cartographer.db]
       python db_to_csv.py -o other_dir
"""

import argparse
import csv
import os
import sqlite3
import sys
from pathlib import Path


def main():
    root = Path(os.environ.get("CARTOGRAPHER_ROOT", Path.cwd())).resolve()
    db_path = root / "cartographer.db"
    out_dir = root / "data"
    parser = argparse.ArgumentParser(description="Export cartographer DB to CSV (one file per table).")
    parser.add_argument("db_path", nargs="?", default=None, help="Path to cartographer.db")
    parser.add_argument("-o", "--output", type=str, default=None, help="Output directory for CSV files (default: data/)")
    args = parser.parse_args()
    if args.db_path:
        db_path = Path(args.db_path).resolve()
    if args.output:
        out_dir = Path(args.output).resolve()
    if not db_path.exists():
        print(f"DB not found: {db_path}", file=sys.stderr)
        sys.exit(1)
    out_dir.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    tables = [r[0] for r in cur.fetchall()]

    for table in tables:
        cur.execute(f"SELECT * FROM {table}")
        rows = cur.fetchall()
        colnames = [d[0] for d in cur.description]
        csv_path = out_dir / f"{table}.csv"
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(colnames)
            for row in rows:
                writer.writerow([str(v) if v is not None else "" for v in row])
        print(f"  {csv_path} ({len(rows)} rows)")

    conn.close()
    print(f"Done. Wrote {len(tables)} CSV files to {out_dir}")


if __name__ == "__main__":
    main()
