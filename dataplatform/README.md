# Scalable Data Product Engineering Platform

A small, dependency-free (Python 3.9+ standard library only) base project that shows the
core ideas of a data product platform:

```
raw CSV partitions ──► parallel map (validate + pre-aggregate per partition)
                              │
                              ▼
                       reduce / merge ──► quality gate ──► versioned data products (SQLite)
                                                                   │
                                                          catalog + REST API
```

| Concern            | Where                | Idea                                                              |
|--------------------|----------------------|-------------------------------------------------------------------|
| Ingestion          | `dpe/ingest.py`      | Partitioned CSV, streamed row by row (flat memory)                |
| Scalable transform | `dpe/transform.py`   | Map-reduce; each partition is independent                         |
| Parallelism        | `dpe/pipeline.py`    | `ProcessPoolExecutor`, one task per partition                     |
| Quality gate       | `dpe/quality.py`     | Nothing is published unless all checks pass                       |
| Versioned storage  | `dpe/storage.py`     | Immutable `<name>_vN` tables + a view pointing at the latest      |
| Product contracts  | `dpe/registry.py`    | Schema, owner, description, lineage and quality stored per version|
| Serving            | `dpe/api.py`         | Read-only JSON API                                                |

## Quick start

```bash
python main.py generate --partitions 8 --rows 20000   # create sample raw data
python main.py run --workers 4                        # build + publish data products
python main.py list                                   # see what is published
python main.py serve --port 8000                      # start the API
python -m unittest discover -s tests -v               # run the tests
```

## API

```
GET /health
GET /products
GET /products/daily_sales
GET /products/daily_sales/data?limit=10&offset=0
GET /products/daily_sales/data?version=1
```

## Data products

- `daily_sales(order_date, orders, units, revenue)`
- `customer_summary(customer_id, orders, revenue, last_order_date, segment)`

Only `completed` and `shipped` orders count toward revenue. Invalid rows are rejected and
counted in the lineage metadata; if more than 5% are rejected (`DPE_MAX_REJECT_RATE`) the
quality gate fails and nothing is published.

## Where to scale next

- Swap SQLite for Postgres/BigQuery/Snowflake and CSV for Parquet on object storage.
- Replace `ProcessPoolExecutor` with Spark, Dask, or a task queue (Celery/Airflow) running
  one task per partition; the map/reduce functions carry over unchanged.
- Add incremental processing (only new partitions) and cross-partition de-duplication.
- Put the API behind FastAPI + auth, and add schema-evolution rules to the registry.
