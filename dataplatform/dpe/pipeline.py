"""Orchestration: ingest -> transform (parallel) -> quality gate -> publish."""
import logging
import time
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from typing import Dict, Optional

from . import config
from .ingest import list_partitions
from .quality import all_passed, run_checks
from .registry import PRODUCT_SPECS
from .storage import Warehouse
from .transform import merge_partials, process_partition

log = logging.getLogger("dpe.pipeline")


class QualityGateError(RuntimeError):
    """Raised when a data product fails its quality checks (nothing is published)."""


def run_pipeline(raw_dir: Optional[Path] = None, warehouse_path: Optional[Path] = None,
                 workers: Optional[int] = None,
                 max_reject_rate: Optional[float] = None) -> Dict:
    raw_dir = Path(raw_dir or config.RAW_DIR)
    workers = workers or config.WORKERS
    max_reject_rate = config.MAX_REJECT_RATE if max_reject_rate is None else max_reject_rate

    started = time.perf_counter()
    partitions = list_partitions(raw_dir)
    if not partitions:
        raise FileNotFoundError(f"No partitions found in {raw_dir}. Run 'generate' first.")
    log.info("Processing %d partitions with %d workers", len(partitions), workers)

    # MAP: one task per partition, run in parallel processes
    paths = [str(p) for p in partitions]
    if workers == 1 or len(paths) == 1:
        partials = [process_partition(p) for p in paths]
    else:
        with ProcessPoolExecutor(max_workers=min(workers, len(paths))) as pool:
            partials = list(pool.map(process_partition, paths))

    # REDUCE
    merged = merge_partials(partials)
    stats = merged["stats"]
    reject_rate = stats["rejected_rows"] / stats["total_rows"] if stats["total_rows"] else 1.0

    # QUALITY GATE (all products are checked before any is published)
    checks = {}
    for name, spec in PRODUCT_SPECS.items():
        checks[name] = run_checks(merged[name], spec.key_index,
                                  spec.non_negative_indexes, reject_rate, max_reject_rate)
        for c in checks[name]:
            log.info("[%s] %-26s %s  %s", name, c.name, "PASS" if c.passed else "FAIL", c.detail)
    failed = {n: [c.name for c in cs if not c.passed]
              for n, cs in checks.items() if not all_passed(cs)}
    if failed:
        raise QualityGateError(f"Quality checks failed, nothing published: {failed}")

    # PUBLISH
    wh = Warehouse(warehouse_path or config.WAREHOUSE_PATH)
    lineage = {"sources": stats["partitions"], "source_rows": stats["total_rows"],
               "rejected_rows": stats["rejected_rows"],
               "rejection_reasons": stats["rejection_reasons"]}
    versions = {}
    for name, spec in PRODUCT_SPECS.items():
        versions[name] = wh.publish(
            name, spec.columns, merged[name], owner=spec.owner,
            description=spec.description,
            quality=[c.to_dict() for c in checks[name]], lineage=lineage)

    summary = {"versions": versions, "rows": {n: len(merged[n]) for n in PRODUCT_SPECS},
               "source_rows": stats["total_rows"], "rejected_rows": stats["rejected_rows"],
               "rejection_reasons": stats["rejection_reasons"],
               "seconds": round(time.perf_counter() - started, 3)}
    log.info("Pipeline finished: %s", summary)
    return summary
