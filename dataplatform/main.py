"""CLI entry point.

    python main.py generate [--partitions 4 --rows 5000]
    python main.py run [--workers 4]
    python main.py list
    python main.py serve [--port 8000]
"""
import argparse
import json
import logging

from dpe import config
from dpe.api import serve
from dpe.ingest import generate_sample_data
from dpe.pipeline import QualityGateError, run_pipeline
from dpe.registry import Catalog
from dpe.storage import Warehouse


def main() -> int:
    parser = argparse.ArgumentParser(description="Data Product Engineering Platform")
    sub = parser.add_subparsers(dest="cmd", required=True)

    g = sub.add_parser("generate", help="create sample raw data partitions")
    g.add_argument("--partitions", type=int, default=4)
    g.add_argument("--rows", type=int, default=5000, help="rows per partition")

    r = sub.add_parser("run", help="run the pipeline and publish data products")
    r.add_argument("--workers", type=int, default=config.WORKERS)

    sub.add_parser("list", help="list published data products")

    s = sub.add_parser("serve", help="start the REST API")
    s.add_argument("--host", default=config.API_HOST)
    s.add_argument("--port", type=int, default=config.API_PORT)

    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    if args.cmd == "generate":
        paths = generate_sample_data(config.RAW_DIR, args.partitions, args.rows)
        print(f"Wrote {len(paths)} partitions to {config.RAW_DIR}")
    elif args.cmd == "run":
        try:
            print(json.dumps(run_pipeline(workers=args.workers), indent=2))
        except QualityGateError as exc:
            print(f"FAILED: {exc}")
            return 1
    elif args.cmd == "list":
        print(json.dumps(Catalog(Warehouse(config.WAREHOUSE_PATH)).list_products(), indent=2))
    elif args.cmd == "serve":
        serve(Catalog(Warehouse(config.WAREHOUSE_PATH)), args.host, args.port)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
