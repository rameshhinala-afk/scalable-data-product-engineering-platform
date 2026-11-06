"""Data quality gate. A product is only published if every check passes."""
from dataclasses import dataclass, asdict
from typing import Dict, List, Sequence


@dataclass
class CheckResult:
    name: str
    passed: bool
    detail: str = ""

    def to_dict(self) -> Dict:
        return asdict(self)


def run_checks(rows: Sequence[tuple], key_index: int, non_negative_indexes: Sequence[int],
               reject_rate: float, max_reject_rate: float) -> List[CheckResult]:
    results = [
        CheckResult("not_empty", len(rows) > 0, f"{len(rows)} rows"),
    ]

    keys = [r[key_index] for r in rows]
    nulls = sum(1 for k in keys if k in (None, ""))
    results.append(CheckResult("key_not_null", nulls == 0, f"{nulls} null keys"))

    dupes = len(keys) - len(set(keys))
    results.append(CheckResult("key_unique", dupes == 0, f"{dupes} duplicate keys"))

    negatives = sum(1 for r in rows for i in non_negative_indexes if r[i] < 0)
    results.append(CheckResult("non_negative_metrics", negatives == 0,
                               f"{negatives} negative values"))

    results.append(CheckResult(
        "reject_rate_within_limit", reject_rate <= max_reject_rate,
        f"{reject_rate:.2%} rejected (limit {max_reject_rate:.2%})"))
    return results


def all_passed(results: Sequence[CheckResult]) -> bool:
    return all(r.passed for r in results)
