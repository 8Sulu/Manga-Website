"""
utils/template_filters.py

Custom Jinja2 filters for the Flask app.
Call register_filters(app) once after the Flask app is created.

Exports:
    register_filters(app)     — attach all filters to a Flask app instance
    cover_gradient_filter(id) — inline style string for manga card backgrounds
"""

from __future__ import annotations

from urllib.parse import quote_plus as _quote_plus

# Eight distinct dark palettes; index is manga_id % 8 so colours are stable
# across page loads and never rely on insertion order.
_COVER_PALETTES = [
    ("hsl(260,40%,14%)", "hsl(260,60%,55%)"),
    ("hsl(340,40%,13%)", "hsl(340,60%,55%)"),
    ("hsl(200,45%,12%)", "hsl(200,65%,50%)"),
    ("hsl(30, 50%,13%)", "hsl(30, 70%,55%)"),
    ("hsl(150,40%,12%)", "hsl(150,55%,46%)"),
    ("hsl(290,35%,14%)", "hsl(290,55%,58%)"),
    ("hsl(10, 45%,13%)", "hsl(10, 65%,55%)"),
    ("hsl(220,42%,14%)", "hsl(220,62%,58%)"),
]


def cover_gradient_filter(manga_id) -> str:
    """Return an inline CSS background for a manga card placeholder."""
    idx = int(manga_id or 0) % len(_COVER_PALETTES)
    base, accent = _COVER_PALETTES[idx]
    return (
        f"background: linear-gradient(160deg, {base} 0%, "
        f"color-mix(in srgb, {accent} 18%, {base}) 100%);"
    )


def register_filters(app) -> None:
    """Attach all custom Jinja2 filters to *app*."""
    app.template_filter("cover_gradient")(cover_gradient_filter)
    app.jinja_env.filters["urlencode"] = lambda s: _quote_plus(str(s or ""))
