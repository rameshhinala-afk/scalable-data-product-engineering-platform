"""Warehouse layer: SQLite-backed, versioned data products.

Each publish creates an immutable table `<name>_v<N>` and repoints the view
`<name>` at the newest version. Metadata lives in the `data_products` table.
"""
import json
import re
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterator, List, Sequence, Tuple

_IDENT = re.compile(r"^[a-z][a-z0-9_]{0,40}$")

_META_DDL = """
CREATE TABLE IF NOT EXISTS data_products (
    name         TEXT NOT NULL,
    version      INTEGER NOT NULL,
    table_name   TEXT NOT NULL,
    created_at   TEXT NOT NULL,
    row_count    INTEGER NOT NULL,
    owner        TEXT,
    description  TEXT,
    schema_json  TEXT NOT NULL,
    quality_json TEXT NOT NULL,
    lineage_json TEXT NOT NULL,
    PRIMARY KEY (name, version)
)
"""


def check_ident(value: str) -> str:
    if not _IDENT.match(value):
        raise ValueError(f"Invalid identifier: {value!r}")
    return value


class Warehouse:
    def __init__(self, path: Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.connect() as conn:
            conn.execute(_META_DDL)

    @contextmanager
    def connect(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        try:
            with conn:  # commit / rollback as one transaction
                yield conn
        finally:
            conn.close()

    def publish(self, name: str, columns: Sequence[Tuple[str, str]],
                rows: Sequence[tuple], *, owner: str, description: str,
                quality: List[Dict], lineage: Dict) -> int:
        """Atomically publish a new version. Returns the version number."""
        check_ident(name)
        for col, _ in columns:
            check_ident(col)
        with self.connect() as conn:
            current = conn.execute(
                "SELECT COALESCE(MAX(version), 0) FROM data_products WHERE name = ?",
                (name,)).fetchone()[0]
            version = current + 1
            table = f"{name}_v{version}"
            coldef = ", ".join(f"{c} {t}" for c, t in columns)
            conn.execute(f"CREATE TABLE {table} ({coldef})")
            marks = ", ".join("?" for _ in columns)
            conn.executemany(f"INSERT INTO {table} VALUES ({marks})", rows)
            conn.execute(f"DROP VIEW IF EXISTS {name}")
            conn.execute(f"CREATE VIEW {name} AS SELECT * FROM {table}")
            conn.execute(
                "INSERT INTO data_products VALUES (?,?,?,?,?,?,?,?,?,?)",
                (name, version, table,
                 datetime.now(timezone.utc).isoformat(timespec="seconds"),
                 len(rows), owner, description,
                 json.dumps([{"name": c, "type": t} for c, t in columns]),
                 json.dumps(quality), json.dumps(lineage)))
        return version
