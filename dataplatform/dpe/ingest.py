"""Ingestion layer: sample data generation and streaming CSV reads.

Raw data is stored as independent CSV partitions. Each partition can be
processed in isolation, which is what makes the pipeline horizontally scalable.
"""
import csv
import random
from datetime import date, timedelta
from pathlib import Path
from typing import Dict, Iterator, List

COLUMNS = ["order_id", "customer_id", "product", "quantity",
           "unit_price", "order_date", "status"]

PRODUCTS = {
    "keyboard": 49.99, "mouse": 24.50, "monitor": 189.00,
    "laptop": 899.00, "headset": 79.90, "webcam": 59.00,
}
STATUSES = ["completed"] * 7 + ["shipped"] * 2 + ["cancelled", "returned"]


def generate_sample_data(raw_dir: Path, partitions: int = 4,
                         rows_per_partition: int = 5000,
                         bad_row_rate: float = 0.01, seed: int = 42) -> List[Path]:
    """Write synthetic order partitions (with a small share of dirty rows)."""
    raw_dir = Path(raw_dir)
    raw_dir.mkdir(parents=True, exist_ok=True)
    rng = random.Random(seed)
    start = date(2026, 1, 1)
    names = list(PRODUCTS)
    paths = []

    for p in range(partitions):
        path = raw_dir / f"orders_part_{p:03d}.csv"
        with path.open("w", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=COLUMNS)
            writer.writeheader()
            for n in range(rows_per_partition):
                product = rng.choice(names)
                row = {
                    "order_id": f"O{p:03d}-{n:06d}",
                    "customer_id": f"C{rng.randint(1, 500):04d}",
                    "product": product,
                    "quantity": rng.randint(1, 5),
                    "unit_price": PRODUCTS[product],
                    "order_date": (start + timedelta(days=rng.randint(0, 89))).isoformat(),
                    "status": rng.choice(STATUSES),
                }
                if rng.random() < bad_row_rate:
                    _corrupt(row, rng)
                writer.writerow(row)
        paths.append(path)
    return paths


def _corrupt(row: Dict, rng: random.Random) -> None:
    defect = rng.choice(["neg_qty", "no_customer", "bad_date", "bad_price"])
    if defect == "neg_qty":
        row["quantity"] = -1
    elif defect == "no_customer":
        row["customer_id"] = ""
    elif defect == "bad_date":
        row["order_date"] = "not-a-date"
    else:
        row["unit_price"] = "abc"


def list_partitions(raw_dir: Path) -> List[Path]:
    return sorted(Path(raw_dir).glob("orders_part_*.csv"))


def read_partition(path: Path) -> Iterator[Dict[str, str]]:
    """Stream rows one at a time so memory stays flat regardless of file size."""
    with Path(path).open(newline="") as fh:
        yield from csv.DictReader(fh)
