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
- Live job progress tracking with stop controls
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
| Scheduling | APScheduler (SQLAlchemy job store) |
| Scraping | Requests · BeautifulSoup |
| External APIs | MyAnimeList API v2 (OAuth2) · SirsiDynix ILSWS |
| Frontend | Vanilla HTML/CSS/JS (no framework) |
| Fonts | Space Mono · Syne · Noto Sans JP |
| Testing | pytest · 217 tests · mocked MySQL, no live DB required |
| CI/CD | GitHub Actions — lint (ruff), type-check (mypy), test matrix across Python 3.11–3.13 |
| Containerization | Docker · docker-compose (Flask + MySQL + nginx) |

---

## Getting Started

### With Docker (recommended)

```bash
cp .env.example .env       # fill in DB password, admin password, etc.
docker compose up --build
```

Open `http://localhost`. On first run, log into `/admin` and click **Reset Database** to create the schema and seed library/branch data from `data/*.csv`.

The stack is three containers — `nginx` in front, `app` (gunicorn), and `db` (MySQL) — mirroring the bare-metal layout below. See `docker-compose.yml` for service details.

**After changing code:** rebuild just the app container — `docker compose up -d --build app`. Code is copied into the image at build time, not bind-mounted, so `restart` alone won't pick up changes. Static assets in `web/static/` *are* bind-mounted and update live with no rebuild.

### Without Docker

```bash
pip install -e .
# point a .env at a local MySQL instance: DB_HOST / DB_USER / DB_PASSWORD / DB_NAME
python web/backend.py        # dev server on :5000
```

For production without Docker, `manga.service` (systemd unit) and `manga.nginx` (reverse proxy config) in the repo root show the original gunicorn + nginx deployment this app was built around.

---

## Testing

```bash
pip install -e ".[dev]"
pytest                                                    # run everything
pytest tests/test_search_utils.py                         # one module
pytest -k normalize_status                                 # by name
pytest --cov=utils --cov=web --cov-report=term-missing    # with coverage
```

217 tests across unit (pure-function) and integration (Flask route) layers. No live MySQL or network access required — the DB layer is mocked in `tests/conftest.py`, so the suite runs in under 2 seconds and is safe to run in CI.

`.github/workflows/ci.yml` runs ruff, mypy, and the full test suite with coverage on every push, across Python 3.11–3.13.

---

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                        Flask App                        │
│  /search  /admin  /api/stats  /api/suggestions  …       │
└──────────────┬──────────────────────┬───────────────────┘
               │                      │
    ┌──────────▼───────┐     ┌─────────▼────────────┐
    │   MySQL Database │     │  APScheduler Jobs    │
    │  manga           │     │  get_manga.py        │
    │  library         │     │  scrapper.py         │
    │  branch          │     │  broward_scrapper.py │
    │  availability    │     └──────────────────────┘
    │  branch_avail... │
    └──────────────────┘
           ▲
    ┌──────┴──────────────────────────┐
    │         Data Sources            │
    │  MyAnimeList API  ─ rankings,   │
    │                     scores,     │
    │                     cover art   │
    │  LCPL ILSWS REST  ─ per-branch  │
    │  Broward Catalog  ─ title-level │
    └──────────────────────────────────┘
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

Both scrapers pull their title/author list from the database, support range and index targeting for incremental runs, and write results back to MySQL with safe delete-then-insert logic scoped to the correct library.

---

## Scale

- **7 LCPL branches** with per-volume, per-branch status
- **37 BCL branches** with per-volume, per-branch status
- **500-title batches** from MAL with automatic OAuth2 token refresh
- Incremental scraping — "new only" mode skips already-scraped titles
