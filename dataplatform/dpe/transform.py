"""Transformation layer (map-reduce style).

map:    process_partition()  - validate + pre-aggregate ONE partition (parallel)
reduce: merge_partials()     - combine partial aggregates into data products
"""
from collections import Counter
from datetime import date
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from .ingest import read_partition

REVENUE_STATUSES = {"completed", "shipped"}
VALID_STATUSES = REVENUE_STATUSES | {"cancelled", "returned"}


def validate_row(row: Dict[str, str]) -> Tuple[Optional[Dict], Optional[str]]:
    """Return (clean_row, None) or (None, rejection_reason)."""
    try:
        qty = int(row["quantity"])
        price = float(row["unit_price"])
    except (KeyError, TypeError, ValueError):
        return None, "bad_number"
    if not (row.get("customer_id") or "").strip():
        return None, "missing_customer"
    if qty <= 0:
        return None, "non_positive_quantity"
    if price <= 0:
        return None, "non_positive_price"
    try:
        order_date = date.fromisoformat(row["order_date"]).isoformat()
    except (KeyError, TypeError, ValueError):
        return None, "bad_date"
    status = (row.get("status") or "").strip().lower()
    if status not in VALID_STATUSES:
        return None, "bad_status"
    return {
        "order_id": row["order_id"],
        "customer_id": row["customer_id"].strip(),
        "quantity": qty,
        "unit_price": price,
        "order_date": order_date,
        "status": status,
    }, None


def process_partition(path: str) -> Dict:
    """MAP step. Top-level function + plain return types so it is picklable."""
    daily: Dict[str, List[float]] = {}      # date -> [orders, units, revenue]
    customers: Dict[str, List] = {}         # cid  -> [orders, revenue, last_date]
    rejected: Counter = Counter()
    total = 0

    for raw in read_partition(Path(path)):
        total += 1
        row, reason = validate_row(raw)
        if row is None:
            rejected[reason] += 1
            continue
        if row["status"] not in REVENUE_STATUSES:
            continue
        revenue = row["quantity"] * row["unit_price"]

        d = daily.setdefault(row["order_date"], [0, 0, 0.0])
        d[0] += 1
        d[1] += row["quantity"]
        d[2] += revenue

        c = customers.setdefault(row["customer_id"], [0, 0.0, ""])
        c[0] += 1
        c[1] += revenue
        c[2] = max(c[2], row["order_date"])

    return {"source": Path(path).name, "total": total,
            "rejected": dict(rejected), "daily": daily, "customers": customers}


def _segment(revenue: float) -> str:
    if revenue >= 5000:
        return "gold"
    if revenue >= 2000:
        return "silver"
    return "bronze"


def merge_partials(partials: List[Dict]) -> Dict:
    """REDUCE step: merge partition aggregates into final data products."""
    daily: Dict[str, List[float]] = {}
    customers: Dict[str, List] = {}
    rejected: Counter = Counter()
    total = 0

    for part in partials:
        total += part["total"]
        rejected.update(part["rejected"])
        for key, (orders, units, rev) in part["daily"].items():
            d = daily.setdefault(key, [0, 0, 0.0])
            d[0] += orders
            d[1] += units
            d[2] += rev
        for key, (orders, rev, last) in part["customers"].items():
            c = customers.setdefault(key, [0, 0.0, ""])
            c[0] += orders
            c[1] += rev
            c[2] = max(c[2], last)

    daily_rows = [(k, v[0], v[1], round(v[2], 2)) for k, v in sorted(daily.items())]
    customer_rows = [(k, v[0], round(v[1], 2), v[2], _segment(v[1]))
                     for k, v in sorted(customers.items())]
    return {
        "daily_sales": daily_rows,
        "customer_summary": customer_rows,
        "stats": {"total_rows": total,
                  "rejected_rows": sum(rejected.values()),
                  "rejection_reasons": dict(rejected),
                  "partitions": [p["source"] for p in partials]},
    }
