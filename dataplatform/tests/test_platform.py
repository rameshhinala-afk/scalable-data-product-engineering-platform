import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dpe.ingest import generate_sample_data
from dpe.pipeline import QualityGateError, run_pipeline
from dpe.registry import Catalog
from dpe.storage import Warehouse, check_ident
from dpe.transform import validate_row


class ValidateRowTests(unittest.TestCase):
    good = {"order_id": "O1", "customer_id": "C1", "quantity": "2",
            "unit_price": "10.0", "order_date": "2026-01-05", "status": "completed"}

    def test_valid(self):
        row, reason = validate_row(self.good)
        self.assertIsNone(reason)
        self.assertEqual(row["quantity"], 2)

    def test_rejections(self):
        cases = {"quantity": ("-1", "non_positive_quantity"),
                 "customer_id": ("", "missing_customer"),
                 "order_date": ("nope", "bad_date"),
                 "unit_price": ("abc", "bad_number"),
                 "status": ("weird", "bad_status")}
        for field, (value, expected) in cases.items():
            bad = dict(self.good, **{field: value})
            self.assertEqual(validate_row(bad)[1], expected, field)


class PipelineTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.raw = Path(self.tmp.name) / "raw"
        self.db = Path(self.tmp.name) / "wh.db"

    def tearDown(self):
        self.tmp.cleanup()

    def test_end_to_end_and_versioning(self):
        generate_sample_data(self.raw, partitions=3, rows_per_partition=500)
        first = run_pipeline(self.raw, self.db, workers=2)
        self.assertEqual(first["versions"], {"daily_sales": 1, "customer_summary": 1})
        self.assertGreater(first["rejected_rows"], 0)

        second = run_pipeline(self.raw, self.db, workers=1)
        self.assertEqual(second["versions"]["daily_sales"], 2)

        catalog = Catalog(Warehouse(self.db))
        names = [p["name"] for p in catalog.list_products()]
        self.assertEqual(names, ["customer_summary", "daily_sales"])
        self.assertEqual(catalog.get_product("daily_sales")["versions"], [1, 2])
        self.assertEqual(len(catalog.read_data("daily_sales", limit=5)), 5)
        self.assertIsNone(catalog.get_product("missing"))

    def test_parallel_matches_serial(self):
        generate_sample_data(self.raw, partitions=3, rows_per_partition=300)
        run_pipeline(self.raw, self.db, workers=1)
        run_pipeline(self.raw, self.db, workers=3)
        catalog = Catalog(Warehouse(self.db))
        v1 = catalog.read_data("daily_sales", 1000, version=1)
        v2 = catalog.read_data("daily_sales", 1000, version=2)
        self.assertEqual(v1, v2)

    def test_quality_gate_blocks_publish(self):
        generate_sample_data(self.raw, partitions=2, rows_per_partition=300,
                             bad_row_rate=0.5)
        with self.assertRaises(QualityGateError):
            run_pipeline(self.raw, self.db, workers=1)
        self.assertEqual(Catalog(Warehouse(self.db)).list_products(), [])

    def test_identifier_validation(self):
        with self.assertRaises(ValueError):
            check_ident("x; DROP TABLE data_products")


if __name__ == "__main__":
    unittest.main()
