# Manga Library Tracker

A full-stack web application that aggregates manga availability across two Florida county library systems — **Leon County Public Library (LCPL)** and **Broward County Library (BCL)** — and cross-references with MyAnimeList rankings, scores, and cover art.

---

## What It Does

Most library catalogs are clunky, slow, and designed for finding a single book title. This app solves a specific problem: *"Which volumes of this manga series are available at my library right now, and at which branch?"*

It answers that across 500+ manga titles simultaneously, with real-time per-branch status.

---

## Features

**Public-facing**
- Instant title search with typeahead autocomplete
- Filter by manga type (Manga, Manhwa, Light Novel, etc.), branch, library system, and availability status
- Cover art sourced from MyAnimeList
- Per-volume, per-branch availability grid for LCPL (7 branches)
- Title-level availability summary for Broward County Library
- MAL score, volume count, and author displayed per title

**Admin dashboard** *(password-protected)*
- Three-step data pipeline:
  1. Fetch manga rankings from the **MyAnimeList API** (batches of 500, with OAuth2 token refresh)
  2. Scrape **LCPL** availability via the SirsiDynix ILSWS REST API
  3. Scrape **Broward County** availability via catalog HTML + AJAX parsing
- Live job progress tracking with stop controls — jobs run on a Redis-backed
  queue (RQ), so progress is visible no matter which Gunicorn worker handles
  the polling request
- Range and "new only" targeting for incremental scrapes
- Manual overrides: update volume counts, delete entries, clear stale data
- Collection stats broken down by branch and library
- Job history log
- Full database reset with CSV re-seed

---

## Tech Stack

| Layer | Technology |
|---|---|
| Backend | Python · Flask |
| Database | MySQL |
| Job Queue | RQ + Redis (background scrape/MAL-fetch jobs, status shared across every Gunicorn worker) |
| Scraping | Requests · BeautifulSoup |
| External APIs | MyAnimeList API v2 (OAuth2) · SirsiDynix ILSWS |
| Frontend | Vanilla HTML/CSS/JS (no framework) |
| Fonts | Space Mono · Syne · Noto Sans JP |
| Testing | pytest · mocked MySQL (no live DB required) · a real local Redis for job-queue tests |
| CI/CD | GitHub Actions — lint (ruff), type-check (mypy), test matrix across Python 3.11–3.13, with a Redis service container for the job-queue tests |
| Containerization | Docker · docker-compose (Flask + MySQL + Redis + nginx) |

---

## Getting Started

### With Docker (recommended)

```bash
cp .env.example .env       # fill in DB password, admin password, etc.
docker compose up --build
```

This starts five containers — `nginx`, `app` (gunicorn), `worker` (the RQ
worker that actually runs scrapes), `db` (MySQL), and `redis` — see
`docker-compose.yml` for service details. Open `http://localhost`. On first
run, log into `/admin` and click **Reset Database** to create the schema
and seed library/branch data from `data/*.csv`.

**After changing code:** rebuild the app *and* worker containers —
`docker compose up -d --build app worker` (both run the same image; the
worker needs rebuilding too or it'll keep executing stale scraper code).
Static assets in `web/static/` *are* bind-mounted and update live with no
rebuild.

### Without Docker

```bash
pip install -e .
# point a .env at a local MySQL instance: DB_HOST / DB_USER / DB_PASSWORD / DB_NAME
redis-server &                                       # job queue backing store
python web/backend.py                                # dev server on :5000
rq worker manga-jobs --url redis://localhost:6379/0  # in a second terminal —
                                                       # this is what actually
                                                       # runs scrape/MAL jobs
```

For production without Docker, `manga.service` and `manga-worker.service`
(systemd units) and `manga.nginx` (reverse proxy config) in the repo root
show the original gunicorn + RQ worker + nginx deployment this app was
built around. Both services must be installed and running — `manga.service`
only *enqueues* jobs; `manga-worker.service` is what dequeues and runs them.

---

## Testing

```bash
pip install -e ".[dev]"
redis-server &                                             # required for test_job_runner.py
pytest                                                      # run everything
pytest tests/test_search_utils.py                           # one module
pytest -k normalize_status                                  # by name
pytest --cov=utils --cov=web --cov-report=term-missing      # with coverage
```

Unit (pure-function) and integration (Flask route) tests, run in CI on
every push across Python 3.11–3.13. The DB layer is mocked in
`tests/conftest.py` — no live MySQL or network access required for the
rest of the suite. The one exception is `tests/test_job_runner.py`, which
exercises the real Redis/RQ-backed job queue against an actual local
Redis instance (no fake/mock Redis) — the CI workflow spins one up
automatically as a service container; for local runs, just `redis-server &`
first.

`.github/workflows/ci.yml` runs ruff, mypy, and the full test suite with coverage on every push, across Python 3.11–3.13.

---

## Architecture

```
┌───────────────────────────────────────────────────────────┐
│              Flask App (N Gunicorn workers)                │
│  /search  /admin  /api/stats  /api/job/*  /api/suggestions │
└──────────────┬───────────────────────┬─────────────────────┘
               │                       │ enqueue job / poll status
    ┌──────────▼────────┐    ┌─────────▼─────────────────┐
    │   MySQL Database  │    │  Redis                    │
    │  manga            │    │  job:state:{name}  (HASH)  │
    │  library          │    │  job:history       (LIST)  │
    │  branch           │    │  manga-jobs        (queue) │
    │  availability     │    └─────────┬─────────────────┘
    │  branch_avail...  │              │ dequeue
    └─────────▲─────────┘    ┌─────────▼─────────────────┐
              │              │  rq worker process(es)     │
              └──────────────┤  get_manga.py              │
                              │  leon_scraper.py           │
                              │  broward_scraper.py        │
                              └────────────────────────────┘
```

---

## Data Model

```
manga ─── availability ─── branch_availability_status ─── branch ─── library
```

Each `availability` row represents one volume of one title. Each `branch_availability_status` row is that volume's status at a specific branch (Available / Checked Out / On Hold / In Transit).

---

## Scraping Strategy

**LCPL** uses the SirsiDynix ILSWS REST endpoint to get structured JSON per catalog key, including per-branch item-level status. A multi-page search handles series with many volumes.

**Broward** reverse-engineers a two-step AJAX flow used by the SirsiDynix Enterprise catalog UI — an init POST to prime the session, then an availability POST that returns a JSON payload with available/total/hold counts.

Both scrapers pull their title/author list from the database, support range and index targeting for incremental runs, and write results back to MySQL with safe delete-then-insert logic scoped to the correct library. They run inside an `rq worker` process (not inside a web request), dispatched by `utils/job_runner.py`.

---

## Scale

- **7 LCPL branches** with per-volume, per-branch status
- **37 BCL branches** with per-volume, per-branch status
- **500-title batches** from MAL with automatic OAuth2 token refresh
- Incremental scraping — "new only" mode skips already-scraped titles
- Job state lives in Redis, not process memory — Gunicorn can run multiple
  workers (or multiple container replicas) without job status going stale
