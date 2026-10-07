# E.ON Weather Intelligence Pipeline — Codex Implementation Plan

## Project context and global rules

You are working on an existing Python Data Engineering application called `eon-weather-pipeline`.

The current implementation uses:
- `main.py` — executes the complete pipeline.
- `src/extract.py` — retrieves Open-Meteo API data and saves Bronze JSON.
- `src/transform.py` — transforms Bronze data into Silver CSV and Gold daily aggregations.
- `src/deliver.py` — produces a Markdown report.
- `src/constants.py` — defines local storage paths.
- `requirements.txt` — Python dependencies.
- `data/bronze`, `data/silver`, `data/gold` — existing Medallion storage folders.

The objective is to evolve this into a professional, production-style Data Engineering portfolio project relevant to an energy company.

**Technology constraints:**
- Python 3.12+.
- Pandas for local data transformations.
- PostgreSQL for relational storage and SQL analytics.
- SQLAlchemy 2.x and Alembic.
- Pydantic Settings for configuration.
- PyArrow for Parquet.
- pytest for testing.
- `uv` for dependency management.
- Docker Compose for local PostgreSQL.

**Do not integrate Databricks, PySpark, Azure, or Airflow yet.** Design for their eventual integration, but do not add unused dependencies or placeholder implementations.

**Rules for every phase:**
1. Inspect the current repository before modifying it.
2. Preserve existing functionality unless the requested phase intentionally changes it.
3. Implement only the current phase. Do not implement later phases prematurely.
4. Keep responsibilities separated and avoid unnecessary abstraction.
5. Use type hints, meaningful errors, structured logging, and documented public interfaces.
6. Add relevant automated tests alongside each implementation.
7. Never commit API secrets, passwords, local database contents, or generated datasets.
8. Use environment variables for configuration and commit a safe `.env.example`.
9. Do not silently ignore failures or generate misleading success reports.
10. Update the README and documentation relevant to the phase.
11. At completion, provide changed files, implementation details, executed tests and results, remaining issues, and suggested commit message.
12. Stop after completing the current phase for review.

---

# PHASE 1 — Project Refactoring and Foundations

## Objective

Refactor the existing application into a well-structured, maintainable Python package while preserving the current end-to-end weather pipeline.

Do not introduce PostgreSQL persistence, Parquet processing, additional API endpoints, or scheduling in this phase.

## Tasks

### 1.1 — Analyze existing implementation
- Inspect `main.py`, `src/extract.py`, `src/transform.py`, `src/deliver.py`, `src/constants.py`, `requirements.txt`, and `README.md`.
- Document the current sequence of operations and output files.
- Identify hardcoded configuration, tightly coupled functions, and missing error handling.
- Preserve the existing calculation behavior for this phase, except for changes needed to avoid crashes.

### 1.2 — Introduce a package structure

Refactor into:

```text
src/weather_pipeline/
    __init__.py
    cli.py
    config/
    ingestion/
    transformations/
    storage/
    reporting/
    orchestration/
    models/
```

Move existing implementations gradually without adding empty modules unnecessarily.

### 1.3 — Introduce environment configuration
- Use `pydantic-settings`.
- Provide `.env.example`.
- Configure API base URL, latitude, longitude, timezone, output directories, and logging level.
- Support different local environments without editing Python source.
- Validate configuration and report actionable errors.
- Resolve project paths independently of the current working directory.

### 1.4 — Dependency management
- Add `pyproject.toml`.
- Configure `uv`.
- Generate and commit `uv.lock`.
- Add linting and formatting with Ruff.
- Avoid unnecessary dependencies.

### 1.5 — Logging and error handling
- Replace `print()` statements with structured, contextual logging.
- Support DEBUG, INFO, WARNING, and ERROR levels.
- Add an execution identifier to logs.
- Ensure exceptions are not silently swallowed.
- Provide useful process exit codes.

### 1.6 — Command-line interface
Implement:

```bash
uv run weather-pipeline run
uv run weather-pipeline --help
```

The `run` command must execute the same end-to-end pipeline as the original `main.py`.

Keep `main.py` as a compatibility entry point if practical.

### 1.7 — Basic testing
- Add pytest configuration.
- Test configuration loading and validation.
- Test CLI argument handling.
- Test a complete local pipeline using mocked API responses.
- Ensure tests never require a real internet connection.

## Acceptance criteria
- Existing end-to-end functionality is preserved.
- CLI execution works from the project directory.
- Configuration is externalized.
- Logging is consistent.
- All phase tests pass.
- The README explains setup and execution.

**Stop after Phase 1.**

---

# PHASE 2 — Production-Style API Ingestion

## Objective

Build a reliable weather ingestion engine supporting multiple locations, forecasts, and historical weather data while preserving immutable Bronze records.

## Tasks

### 2.1 — Build a reusable Open-Meteo client
- Use `requests.Session` or an equivalent synchronous HTTP client.
- Configure connection/read timeouts.
- Implement retries with exponential backoff for transient failures.
- Respect HTTP 429 and applicable Retry-After headers.
- Handle network exceptions, malformed JSON, unexpected schemas, and HTTP errors.
- Do not retry permanent client errors indiscriminately.
- Keep the client independently testable.

### 2.2 — Support multiple locations
Create `config/locations.yaml` with initial examples:
- Iași
- Bucharest
- Cluj-Napoca
- Timișoara
- Constanța

Each location must have a stable identifier, name, latitude, longitude, and timezone.

Validate unique IDs and coordinate ranges.

### 2.3 — Forecast and historical endpoints
- Support configurable Open-Meteo forecast and historical/archive data ingestion.
- Use the appropriate API host and parameters for each dataset.
- Clearly distinguish forecasts from historical modeled/reanalysis data.
- Validate supported date ranges and API variables.
- Explicitly request consistent temperature, wind-speed, and radiation units.
- Record forecast issue/retrieval time separately from weather valid time.
- Do not label historical model data as measured station observations.

### 2.4 — Bronze storage
Store original successful API response payloads without changing their weather values.

Use a deterministic directory layout such as:

```text
data/bronze/
  source=open_meteo/
    dataset=forecast/
      location=iasi/
        ingestion_date=2026-10-04/
          <run-id>.json
```

Do not overwrite previous successful extractions.

### 2.5 — Extraction metadata
Record:
- Run ID
- Extraction ID
- API provider
- Endpoint
- Location
- Dataset type
- Request parameters
- Retrieval timestamp in UTC
- Request duration
- HTTP status
- Response checksum
- Bronze file path
- Extraction status

Initially store metadata in a machine-readable local manifest. Database persistence will be added in Phase 4.

### 2.6 — Ingestion failure isolation
- Process locations independently.
- Define behavior for partial failures.
- Continue with unaffected locations where appropriate.
- Report failed extractions explicitly.
- Do not treat a partially successful run as fully successful.

## Acceptance criteria
- Multiple locations can be extracted.
- Forecast and historical datasets are supported.
- Request failures are handled reliably.
- Original Bronze payloads are preserved.
- Every successful extraction has metadata.
- Retries, timeouts, HTTP errors, and partial failures are tested.

**Stop after Phase 2.**

---

# PHASE 3 — Medallion Data Processing

## Objective

Implement a well-defined Bronze-to-Silver-to-Gold transformation pipeline using Pandas, explicit schemas, reliable data-quality checks, and Parquet files.

## Tasks

### 3.1 — Define schemas
Create typed models for:
- Weather extraction metadata
- Silver weather records
- Forecast weather records
- Historical weather records
- Gold daily weather summaries
- Data-quality failures

Use stable column names, explicit data types, and documented measurement units.

### 3.2 — Bronze to Silver
- Load raw Bronze JSON.
- Validate required metadata and weather arrays.
- Verify consistent array lengths.
- Preserve location and dataset provenance.
- Normalize timestamps to UTC.
- Retain location timezone information for local-day aggregation.
- Normalize weather units.
- Use nulls for valid but missing optional measurements.
- Reject or quarantine structurally invalid records.
- Record validation failures and counts.
- Deduplicate using a documented natural key appropriate to each dataset.

Forecast identity must include sufficient information to retain different forecast vintages; do not overwrite all forecasts for the same valid hour.

### 3.3 — Data-quality rules
Implement tests for:
- Missing timestamps
- Invalid numeric values
- Missing mandatory identifiers
- Invalid or inconsistent measurement units
- Duplicate business keys
- Impossible physical values where scientifically justified
- Unexpected time intervals
- Missing or incomplete time series

Distinguish warnings from errors and hard failures.

Avoid arbitrary thresholds that reject legitimate extreme weather.

### 3.4 — Silver storage
Write analytical Silver datasets in Parquet.

Use consistent naming and partitioning. Partition only on useful, low-cardinality fields; avoid excessive tiny files.

Support repeatable processing without uncontrolled duplicates.

### 3.5 — Silver to Gold
Calculate useful daily metrics:
- Average temperature in °C
- Minimum temperature
- Maximum temperature
- Maximum wind speed in m/s
- Average wind speed
- Daily solar irradiation in kWh/m², when supported by the hourly source data
- Number of valid observations
- Coverage percentage
- Dataset type
- Location and local calendar date

Document exactly how solar irradiation is calculated from radiation values and interval duration.

Never sum instantaneous W/m² measurements and present the raw sum as energy.

### 3.6 — Business indicators
Add carefully documented descriptive indicators such as:
- Heating degree days
- Cooling degree days
- Windy-hour counts
- Temperature anomalies only when a valid reference baseline exists

Make assumptions configurable.

Do not present proxy weather indicators as measured energy production.

### 3.7 — File reliability
- Use temporary files followed by atomic replacement where appropriate.
- Prevent partially written Parquet files from being considered valid.
- Preserve source lineage from Gold back to Silver and Bronze.

## Acceptance criteria
- Silver and Gold have explicit schemas.
- Weather units are correct and documented.
- Forecasts and historical datasets remain distinguishable.
- Data-quality checks produce useful results.
- Parquet output is readable with Pandas/PyArrow.
- Transformation reruns produce deterministic results for the same input.
- Unit tests cover malformed data, duplicates, timezones, and aggregations.

**Stop after Phase 3.**

---

# PHASE 4 — PostgreSQL Integration

## Objective

Introduce PostgreSQL as the local relational and analytical database, with migrations, reliable persistence, run history, and SQL analytics.

## Tasks

### 4.1 — Database infrastructure
- Add PostgreSQL to `docker-compose.yml`.
- Provide database environment variables.
- Add connection health checks.
- Use SQLAlchemy 2.x.
- Use connection pooling with sensible settings.
- Never hardcode credentials.
- Do not expose PostgreSQL publicly by default.

### 4.2 — Database migrations
Use Alembic to create:

```text
metadata
bronze
silver
gold
```

Implement versioned migrations rather than creating tables dynamically at application startup.

### 4.3 — Metadata tables
Create tables for:
- Locations
- Pipeline runs
- Pipeline steps
- API extractions
- Bronze file manifests
- Data-quality results

Include appropriate run status, timestamps, error details, row counts, and relationships.

### 4.4 — Analytical tables
Create:
- `silver.weather_observations`
- `silver.weather_forecasts`
- `gold.daily_weather_summary`
- Additional Gold tables only when justified by actual analytical requirements.

Design appropriate constraints, indexes, and uniqueness rules.

Ensure forecast records can preserve multiple forecast issue times.

### 4.5 — Idempotent persistence
- Use PostgreSQL `INSERT ... ON CONFLICT`.
- Prevent duplicate records during retries and backfills.
- Define stable natural keys.
- Use transactional writes.
- Support efficient bulk loading.
- Define explicit synchronization responsibilities between Parquet and PostgreSQL.
- Avoid situations where a failed database transaction leaves a run marked successful.

### 4.6 — PostgreSQL integration in the pipeline
- Load validated data into SQL tables.
- Track database writes and row counts.
- Preserve lineage to Bronze.
- Implement correct transaction rollback and failure reporting.
- Avoid row-by-row database insertion for large datasets.

### 4.7 — Analytical SQL examples
Add queries for:
- Average monthly temperature per city
- Maximum daily wind speed
- Solar irradiation comparisons
- Daily weather trends
- Ingestion freshness
- Missing or incomplete data
- Pipeline execution history

Store examples in `sql/analytics/`.

### 4.8 — Integration tests
Use an isolated PostgreSQL test database.

Test:
- Migrations
- Constraints
- Initial loading
- Repeated loading
- UPSERT behavior
- Transaction rollback
- Schema compatibility
- Query correctness

## Acceptance criteria
- Fresh PostgreSQL setup succeeds through migrations.
- Validated records are stored in SQL.
- Repeated runs do not create unintended duplicates.
- Forecast history is preserved correctly.
- Database failures are properly reported.
- Analytical queries return correct results.
- Integration tests pass.

**Stop after Phase 4.**

---

# PHASE 5 — Analyst-Ready Data Delivery

## Objective

Create professional analytical deliverables that another team member can understand and consume without accessing the implementation code.

## Tasks

### 5.1 — Reporting service
Refactor the existing Markdown reporting function into reusable reporting components.

Report generation must be independent of API ingestion.

### 5.2 — Supported output formats
Implement:
- CSV
- Excel (.xlsx)
- Markdown
- Parquet reuse from Gold

Use consistent data from validated Gold outputs.

### 5.3 — Daily analytical report
Include:
- Report creation time in UTC
- Data coverage period
- Locations
- Data sources
- Forecast versus historical classification
- Temperature summary
- Wind summary
- Solar irradiation summary
- Data-quality and completeness indicators
- Significant conditions backed by actual metrics

Clearly distinguish analysis from predictions or unsupported conclusions.

### 5.4 — Excel workbook
Create sheets such as:
- Executive Summary
- Daily Weather
- Location Comparison
- Data Quality
- Data Dictionary
- Pipeline Metadata

Use professional, readable formatting.

### 5.5 — Data dictionary
Document every exported field:
- Name
- Description
- Data type
- Measurement unit
- Nullable status
- Calculation method where applicable

### 5.6 — Delivery tracking
- Assign unique delivery IDs.
- Link deliveries to pipeline runs.
- Record format, file path, creation time, status, checksum, and row count.
- Prevent empty or invalid datasets from generating misleading successful reports.
- Use safe file naming and avoid unintended overwrite.

### 5.7 — CLI
Support:

```bash
uv run weather-pipeline report --date 2026-10-03
```

The report command should use already persisted analytical data without re-extracting the API.

## Acceptance criteria
- Reports can be regenerated independently.
- CSV, Excel, and Markdown outputs are available.
- Units and provenance are documented.
- All reported values originate from validated datasets.
- Delivery metadata is recorded.
- Generated outputs are tested for correctness.

**Stop after Phase 5.**

---

# PHASE 6 — Automation and Reliability

## Objective

Make the pipeline suitable for unattended local execution, manual reprocessing, and recovery after failures.

Do not integrate Airflow yet.

## Tasks

### 6.1 — Independent pipeline steps
Expose commands for:

```bash
weather-pipeline ingest
weather-pipeline transform
weather-pipeline load
weather-pipeline report
weather-pipeline run
```

Each command should have documented inputs and outputs.

### 6.2 — Scheduling
Provide an example local cron or systemd timer configuration.

Keep scheduling external to the business logic.

Do not implement a competing custom scheduling framework.

### 6.3 — Historical backfills
Implement bounded, configurable historical data backfills.

Support:
- Start and end dates
- Selected locations
- Dataset type
- Clear execution summaries

Honor API limitations and prevent unbounded requests.

### 6.4 — Rerun behavior
- Reuse valid Bronze data when appropriate.
- Allow selected processing stages to be rerun.
- Detect existing data.
- Avoid duplicate records.
- Explicitly document when a fresh API extraction is requested.
- Preserve forecast versions.

### 6.5 — Execution state
Track:
- PENDING
- RUNNING
- COMPLETED
- PARTIAL
- FAILED
- CANCELLED, if cancellation is supported

Track each pipeline stage individually.

### 6.6 — Failure recovery
- Handle interrupted processing.
- Ensure failed stages are visible.
- Prevent partially written results from being marked complete.
- Define transaction and file-write boundaries.
- Support safe restart after failure.

### 6.7 — Concurrency
Prevent two executions from accidentally processing the same scope simultaneously.

Use a documented locking or coordination strategy appropriate for PostgreSQL and local file storage.

### 6.8 — Monitoring
Add CLI commands such as:

```bash
weather-pipeline status
weather-pipeline runs
```

Display:
- Latest run
- Current status
- Duration
- Processed rows
- Rejected rows
- Failed locations
- Last successful execution

## Acceptance criteria
- Scheduled execution can run unattended.
- Backfills support historical periods.
- Interrupted runs can be safely retried.
- Reprocessing is idempotent.
- Execution metadata is queryable.
- Failures and partial results are distinguishable.
- Concurrency behavior is tested.

**Stop after Phase 6.**

---

# PHASE 7 — Testing and Quality Assurance

## Objective

Strengthen automated testing and establish continuous integration.

Tests must be introduced during earlier phases. This phase expands and audits their completeness.

## Tasks

### 7.1 — Unit tests
Review coverage for:
- Configuration
- API parameter construction
- Data validation
- Unit conversions
- Timezone handling
- Deduplication
- Daily aggregation
- Reporting

### 7.2 — API simulation
Use realistic fixtures and mocked HTTP responses.

Test:
- 200 success
- 429 rate limiting
- 500/503 server errors
- Connection timeouts
- Malformed JSON
- Missing data
- Partial location failures

### 7.3 — Integration tests
Use a disposable PostgreSQL database and temporary filesystem.

Cover:
- Migrations
- Complete ingestion workflow
- Bronze manifests
- Silver persistence
- Gold aggregation
- SQL loading
- Reporting
- Execution tracking

### 7.4 — End-to-end test
Provide a reproducible end-to-end test that:
1. Starts from known API fixtures.
2. Writes Bronze.
3. Produces Silver.
4. Produces Gold.
5. Persists the expected PostgreSQL records.
6. Generates analytical output.
7. Verifies expected values.
8. Repeats processing and verifies idempotency.

### 7.5 — Static quality checks
Configure:
- Ruff linting
- Ruff formatting
- Type checking using mypy or Pyright
- pytest
- Coverage reporting

Set practical initial coverage expectations and focus on critical business logic rather than chasing a percentage alone.

### 7.6 — GitHub Actions
Add CI that:
- Installs Python and uv
- Uses locked dependencies
- Runs lint and formatting checks
- Runs type checks
- Starts PostgreSQL for integration tests
- Runs the test suite
- Reports failures

Use safe test-only credentials.

### 7.7 — Dependency and repository hygiene
- Review pinned dependency versions.
- Ensure secrets are excluded.
- Ensure generated data is ignored appropriately.
- Keep sample fixtures small and safe to publish.
- Check that a clean checkout works.

## Acceptance criteria
- Tests execute without depending on live weather APIs.
- PostgreSQL integration tests are reproducible.
- Critical workflows are covered.
- CI runs automatically on pull requests and pushes.
- The full pipeline is tested end-to-end.
- Documentation explains local test execution.

**Stop after Phase 7.**

---

# PHASE 8 — GitHub Portfolio Readiness

## Objective

Prepare the project for review by an experienced Data Engineer or hiring team at an energy company.

The repository should demonstrate technical competence, data modeling, reliability, documentation, and sound engineering tradeoffs.

## Tasks

### 8.1 — Professional README
Include:
- Project overview
- Business problem
- Architecture summary
- Tech stack
- Medallion explanation
- Quick start
- Database setup
- Pipeline execution
- Testing
- Example outputs
- Troubleshooting
- Current limitations
- Future improvements

Avoid claiming production deployment or enterprise integrations that do not exist.

### 8.2 — Architecture diagram
Create a Mermaid diagram showing:

```text
Open-Meteo APIs
    ↓
Python ingestion
    ↓
Bronze raw JSON
    ↓
Silver validated Parquet
    ↓
Gold analytical datasets
    ↓
PostgreSQL / SQL
    ↓
Analyst reports
```

Also illustrate metadata, logging, quality controls, and execution tracking.

### 8.3 — Document engineering decisions
Create architecture decision records covering:
- Why Medallion architecture
- Why PostgreSQL
- Why Parquet
- Why Pandas instead of Spark initially
- Why external scheduling
- Forecast versus historical data modeling
- Idempotency strategy
- Storage consistency and lineage

### 8.4 — Sample data and reports
Provide small, publishable sample fixtures and generated examples.

Do not commit large production datasets.

Ensure examples accurately reflect actual application behavior.

### 8.5 — Reproducible demonstration
Create documented steps for a reviewer to:
1. Clone the repository.
2. Configure the environment.
3. Start PostgreSQL.
4. Apply migrations.
5. Run the pipeline.
6. Inspect Bronze/Silver/Gold.
7. Execute analytical SQL queries.
8. Generate a report.
9. Run automated tests.

Keep the demonstration practical and concise.

### 8.6 — Future technology roadmap
Create `docs/future-integrations.md`.

Explain how the existing architecture could evolve toward:

**Databricks**
- Move analytical transformations into Databricks jobs.
- Evaluate Delta Lake for managed analytical tables.

**PySpark**
- Replace selected Pandas processing with Spark DataFrames.
- Adapt transformation implementations while preserving business rules.

**Microsoft Azure**
- Move object storage to Azure Blob Storage or ADLS Gen2.
- Implement managed identities and cloud secret management.
- Define managed deployment and monitoring options.

**Apache Airflow**
- Replace local scheduling with DAG orchestration.
- Invoke independently executable pipeline stages.
- Use task retries, dependencies, and execution monitoring.

Clearly mark these as future plans, not implemented features.

### 8.7 — Repository cleanup
- Remove unused code and temporary artifacts.
- Verify `.gitignore`.
- Ensure no credentials are included.
- Check filenames and directory structure.
- Verify CI and documented commands.
- Recommend a suitable open-source license for owner approval.
- Check that every documentation command works.

### 8.8 — Final engineering audit
Review the complete project for:
- Correctness
- Maintainability
- Data quality
- Security
- Performance
- Reliability
- Reproducibility
- Documentation completeness
- Future migration readiness

Report any remaining shortcomings honestly, with severity and recommended fixes.

## Acceptance criteria
- A new reviewer can run the project from a fresh checkout.
- The README matches actual functionality.
- Architecture and data flow are understandable.
- Example reports and SQL queries demonstrate real analytical value.
- Tests and CI pass.
- Future integration opportunities are documented without being misrepresented as completed.

**Stop after Phase 8 and produce a final project-readiness report.**