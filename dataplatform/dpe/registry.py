"""Data product definitions + read-side catalog.

PRODUCT_SPECS is the contract for each data product: schema, owner, key and
quality rules. The Catalog is what consumers (API, CLI) use to discover and
read published products.
"""
import json
from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence, Tuple

from .storage import Warehouse, check_ident


@dataclass(frozen=True)
class ProductSpec:
    name: str
    description: str
    owner: str
    columns: Tuple[Tuple[str, str], ...]
    key_index: int
    non_negative_indexes: Tuple[int, ...]


PRODUCT_SPECS: Dict[str, ProductSpec] = {
    "daily_sales": ProductSpec(
        name="daily_sales",
        description="Revenue, orders and units per day (completed + shipped orders).",
        owner="sales-analytics",
        columns=(("order_date", "TEXT"), ("orders", "INTEGER"),
                 ("units", "INTEGER"), ("revenue", "REAL")),
        key_index=0,
        non_negative_indexes=(1, 2, 3),
    ),
    "customer_summary": ProductSpec(
        name="customer_summary",
        description="Lifetime orders, revenue and value segment per customer.",
        owner="customer-insights",
        columns=(("customer_id", "TEXT"), ("orders", "INTEGER"),
                 ("revenue", "REAL"), ("last_order_date", "TEXT"),
                 ("segment", "TEXT")),
        key_index=0,
        non_negative_indexes=(1, 2),
    ),
}


class Catalog:
    def __init__(self, warehouse: Warehouse):
        self.wh = warehouse

    def list_products(self) -> List[Dict]:
        with self.wh.connect() as conn:
            rows = conn.execute("""
                SELECT p.* FROM data_products p
                JOIN (SELECT name, MAX(version) v FROM data_products GROUP BY name) m
                  ON p.name = m.name AND p.version = m.v
                ORDER BY p.name""").fetchall()
        return [self._summary(r) for r in rows]

    def get_product(self, name: str, version: Optional[int] = None) -> Optional[Dict]:
        row = self._row(name, version)
        if row is None:
            return None
        out = self._summary(row)
        out["schema"] = json.loads(row["schema_json"])
        out["quality"] = json.loads(row["quality_json"])
        out["lineage"] = json.loads(row["lineage_json"])
        with self.wh.connect() as conn:
            out["versions"] = [r[0] for r in conn.execute(
                "SELECT version FROM data_products WHERE name = ? ORDER BY version",
                (name,))]
        return out

    def read_data(self, name: str, limit: int = 100, offset: int = 0,
                  version: Optional[int] = None) -> Optional[Sequence[Dict]]:
        row = self._row(name, version)
        if row is None:
            return None
        table = row["table_name"]
        check_ident(table)
        with self.wh.connect() as conn:
            cur = conn.execute(f"SELECT * FROM {table} LIMIT ? OFFSET ?",
                               (limit, offset))
            return [dict(r) for r in cur.fetchall()]

    def _row(self, name: str, version: Optional[int]):
        with self.wh.connect() as conn:
            if version is None:
                return conn.execute(
                    "SELECT * FROM data_products WHERE name = ? "
                    "ORDER BY version DESC LIMIT 1", (name,)).fetchone()
            return conn.execute(
                "SELECT * FROM data_products WHERE name = ? AND version = ?",
                (name, version)).fetchone()

    @staticmethod
    def _summary(r) -> Dict:
        return {"name": r["name"], "version": r["version"],
                "description": r["description"], "owner": r["owner"],
                "row_count": r["row_count"], "created_at": r["created_at"]}
