# PLN Exchange Rate Analysis

This document explains how I approached the assignment: the decisions I made, why I made them, and what I'd do differently with more time. The technical implementation details (setup, running instructions, code walkthrough) are added separately below this section.

## The task

Analyze PLN exchange rates against EUR, USD, GBP, and JPY over the last 5 years, using the NBP API, and answer four questions about extreme values (highest depreciation, highest appreciation, biggest single-day swing in each direction). Deliverable should look like a production data engineering pipeline, not just a notebook that produces the right numbers.

## First step: API testing

Before writing any pipeline code, I hit the NBP API directly in Postman to see the actual response shape and limits, rather than building around what I assumed the API would do. I initially assumed a 93-day limit per request, as stated in the documentation. Testing showed the multi-currency table endpoint (`/rates/A/{start}/{end}`, which returns all published currencies for a date range in one call) actually caps at 93 days. The single-currency endpoint (`/rates/A/{code}/{start}/{end}`) does allow 367 days, but only for one currency per call.

The trade-off:
- Single-currency endpoint: fewer days per request, but the request count scales with the number of currencies I'm tracking.
- Multi-currency endpoint: more requests for the same date range, but the count stays flat no matter how many currencies I add later. But extending the date range increases the amount of requests.

At exactly 4 currencies over 5 years, both approaches land around 20 requests, roughly a coincidence at this specific currency count. I went with the multi-currency endpoint anyway, since I thought it to be more of a data engineering task to filter the dataframe, rather than filtering in the API requests. It also means my raw layer captures every currency NBP publishes, not just the four I'm analyzing right now, so extending the analysis later doesn't require re-extracting anything.

I also noticed that weekends are not included, so I cam across the issue that if a date range is entirely in the weekend I will get a 404 Not Found error. I added logic into the extract, since it is possible (although changes are low) that my get_date_windows function will give back an date range that falls entirely on a weekend.

## Design decisions on the data layers and pipeline structure

I thought about this in terms of the medallion pattern I use at work (raw (bronze), curated (silver), aggregated (gold)), but made a deliberate call not to build a full three-table version of it here. 

Instead I created three classes for extract, transform and load. The classes get called in a single notebook which orchestrates the entire ingestion from API to delta table. Besides the small dataset, I had two other reasons to combine the full ETL process in a single databricks job task:

1. Notebook tasks in a Databricks Job don't share an in-memory Spark session, so splitting transform and load into separate tasks would have meant introducing a hand-off between them just to preserve the three-task shape, with no real benefit.

2. Given the time constraint, a single well-structured, well-tested pipeline script demonstrates the same engineering judgment without extra orchestration overhead that wasn't earning its keep here.

Extract, transform, and load are still built as separate, independently testable classes (`Extract`, `Transform`, `Load`), so the separation of concerns is there in the code even though it isn't expressed as separate job tasks.

## What I'd do with more time

- Implement medallion architecture, so we can see the full audit trail, lineage and contribute to a larger data model.
- Deploy via Databricks Asset Bundles instead of creating the job manually through the UI, so the job definition is version-controlled and reviewable, and deployable consistently across DTAP.
- Add incremental loading instead of re-pulling the full 5-year range on every run.
- Expand test coverage, particularly around the transform step's validation logic.
- Generally try to normalize/parameterize/standardize the code. For example, if multiple different source types need to be ingested I would create an ABC class and within specific classes for each file type. Then create a factory component which selects the right reader. Also, I would create table_config.yaml which stores the tables columns, data types, comments, tags based on the requirements. Then I would also standardize components like delta table writer, which would be used by Bronze, Silver and Gold. And probably many more things have crossed my mind. 

---

## Technical Implementation

### Setup

```bash
python -m venv env && source env/bin/activate
pip install -r requirements.txt
pytest tests/
```

The pipeline itself only runs on Databricks: `Load` writes to a three-level Unity Catalog namespace (`catalog.schema.table`), which a local Spark session can't resolve.

### Repository structure

```
src/
  utils.py      load_config() - reads config.yaml
  extract.py    Extract - pulls raw NBP tables into a Spark DataFrame
  transform.py  Transform - flattens/filters/renames into the final shape
  load.py       Load - writes to a Delta table
notebooks/
  01_pipeline.py    orchestrates Extract -> Transform -> Load
  02_visualize.py   reads the Delta table, plots per-currency charts
  03_analyze.py     SQL answering the four business questions
config.yaml     api settings, tracked currencies, date range
tests/          pytest suite, one file per src module
```

### `src/extract.py` - `Extract`

- `ingest()` resolves the date range from config (`years_back`, or explicit `start_date`/
  `end_date`), splits it into windows no larger than `api.max_days_per_request`
  (`get_date_windows`), and calls the NBP multi-currency table endpoint once per window
  (`fetch_window`).
- Each window's raw JSON is parsed straight into a Spark DataFrame against a fixed
  `TABLE_SCHEMA`, then unioned together.
- A 404 (only weekend in that window) is treated as "no data," not an error. Any other failed request (connection error, timeout, or a non-2xx status) is retried up to `api.max_retries` times, sleeping `api.retry_backoff_seconds` between attempts, before giving up and failing the whole `ingest()` call.

### `src/transform.py` - `Transform`

- `transform(raw_df)` explodes the nested `rates` array into one row per currency per day,
  casts `effectiveDate` to a real `DateType`, filters down to the currencies listed in
  `config.yaml`'s `currencies` (`filter_currencies`), and renames columns to `effective_date` /
  `currency_name` / `currency_code` / `exchange_rate`.

### `src/load.py` - `Load`

- Takes `catalog_name`, `schema_name`, `table_name` as constructor arguments rather than
  reading them from `config.yaml` - they come from the Databricks Job's notebook parameters
  instead (see `01_pipeline.py`'s widgets).
- `write(df)` creates an empty Delta table matching `df`'s schema if the target table doesn't
  exist yet (`ensure_table_exists`), then writes with `overwriteSchema=true`.

### Notebooks

All three are Databricks notebooks and declare `catalog_name`/`schema_name`/`table_name` as `dbutils.widgets`, so a Databricks Job can pass them in as parameters.

- **`01_pipeline.py`**: `Extract` -> `Transform` -> `Load`, end to end.
- **`02_visualize.py`**: reads the Delta table and plots one line-chart subplot per currency
  (No shared axis, since JPY at ~0.035 PLN and EUR/GBP at ~4-5 PLN aren't visually comparable on the same scale).
- **`03_analyze.py`**: registers the Delta table as a temp view and answers the four business
  questions via `spark.sql(...)` calls (plain Python strings rather than `%sql` magic cells, so
  the SQL itself carries no per-line comment noise). Day-over-day swings (Q3/Q4) are ranked by
  percentage change rather than absolute change, for the same cross-currency-scale reason.

### Tests

- `test_utils.py`, `test_extract.py`, and `test_transform.py` use a local `SparkSession`
  fixture; `test_load.py` mocks `spark`/`df` entirely, since a real `catalog.schema.table`
  namespace needs Unity Catalog, which only exists on Databricks.
- `tests/test_config.yaml` is a fixture file mirroring the real `config.yaml`'s shape, used by
  `test_utils.py` so the success-path test reflects realistic content instead of a throwaway dict.

