# Package setup

## Install (once, in your virtualenv)

```bash
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -e ".[dev]"
```

`pip install -e .` reads `pyproject.toml`, discovers all packages under `.`,
and adds the repo root to `sys.path` via an editable `.pth` file — so every
module (`config`, `utils`, `services`, `scripts`) is importable without any
`sys.path.append` hacks.

## Run

```bash
flask --app backend run --debug          # dev
gunicorn -w 1 -b 127.0.0.1:5000 backend:app   # prod (1 worker — in-process job state)
```

## Gunicorn worker note

The job runner keeps state in process memory (`_jobs` dict).  One worker is
therefore required until the job system is migrated to Redis + Celery/RQ.
This is a known, documented trade-off, not a bug.

## Future: multi-worker with Redis/RQ

```bash
pip install rq redis
# Replace start_job / stop_job / get_job in job_runner.py with RQ equivalents.
# Run: rq worker manga-jobs
# gunicorn -w 4 ...   # now safe to scale
```

## __init__.py files required

Each sub-package needs an empty `__init__.py` for setuptools to discover it:

```
config/__init__.py
utils/__init__.py
services/__init__.py
scripts/__init__.py
```
