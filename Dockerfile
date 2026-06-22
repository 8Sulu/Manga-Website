# Dockerfile
#
# WHY EDITABLE INSTALL (`pip install -e .`) INSTEAD OF A NORMAL INSTALL:
#   config/settings.py, services/api_service.py, and utils/job_runner.py all
#   resolve paths via `Path(__file__).parent.parent` (DATA_DIR, ENV_FILE,
#   the job_history.json location, etc.). A normal `pip install .` copies
#   the package into site-packages, which would silently break every one
#   of those relative-path lookups — they'd resolve inside site-packages
#   instead of /app. Editable install keeps __file__ pointing at the real
#   source tree, so the existing path-resolution logic (and the .env /
#   data/ layout) keeps working unmodified, same as your dev venv.
#
# TWO-STAGE COPY (stub packages, then real source):
#   Lets Docker cache the `pip install -e .` layer (which resolves and
#   downloads every third-party dependency) independently of application
#   source changes. Editing a .py file won't bust the dependency-install
#   cache and trigger a full reinstall on every build.

FROM python:3.13-slim AS base
# ↑ bump to python:3.14-slim if you want to match your dev machine exactly —
#   pyproject.toml only requires >=3.11, so either works.

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Dedicated low-privilege user — mirrors the `manga` system user used by
# manga.service for the bare-metal/systemd deployment.
RUN groupadd -r manga && useradd -r -g manga -d /app -s /sbin/nologin manga

WORKDIR /app

# ── Dependency layer (cached unless pyproject.toml changes) ──────────────
COPY pyproject.toml ./
RUN mkdir -p web config utils services scripts \
    && touch web/__init__.py config/__init__.py utils/__init__.py \
             services/__init__.py scripts/__init__.py \
    && pip install -e . gunicorn

# ── Application source ────────────────────────────────────────────────────
COPY . .
COPY docker/entrypoint.sh /entrypoint.sh

RUN chmod +x /entrypoint.sh \
    && mkdir -p /app/data/flask_sessions \
    && chown -R manga:manga /app

USER manga
EXPOSE 8000

ENTRYPOINT ["/entrypoint.sh"]
CMD ["gunicorn", "--config", "web/gunicorn.docker.conf.py"]
