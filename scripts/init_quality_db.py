from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser(description="Initialize SQLite quality database schema.")
    parser.add_argument(
        "--db-path",
        type=Path,
        default=Path("quality_checks.db"),
        help="SQLite database path",
    )
    parser.add_argument(
        "--schema-file",
        type=Path,
        default=Path("sql/quality_schema.sql"),
        help="Path to SQL schema file",
    )
    args = parser.parse_args()

    if not args.schema_file.exists():
        raise FileNotFoundError(f"Schema file not found: {args.schema_file}")

    sql = args.schema_file.read_text(encoding="utf-8")
    conn = sqlite3.connect(args.db_path)
    try:
        conn.executescript(sql)
        conn.commit()
    finally:
        conn.close()

    print(f"Initialized DB: {args.db_path}")


if __name__ == "__main__":
    main()
