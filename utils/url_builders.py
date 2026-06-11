"""
utils/url_builders.py

Catalog search URL builders for Leon County (LCPL) and Broward County
Library (BCL).  Both functions produce deep-link search URLs that open
the catalog pre-filtered to the correct title/author combination.

Exports:
    lcpl_search_url(title, author, manga_type, volume)
    broward_search_url(title, author, manga_type, volume)
"""
from __future__ import annotations

from urllib.parse import quote_plus as _q

from utils.scraper_utils import is_novel


def lcpl_search_url(
    title: str,
    author: str,
    manga_type: str = '',
    volume: int | None = None,
) -> str:
    """
    Return a SirsiDynix Enterprise search URL for the LCPL catalog.

    Light novels get an extra -SUBJECT=Comic exclusion filter so graphic
    novel editions don't appear in results.
    """
    et = _q(title)
    ea = _q(author or '')

    if is_novel(manga_type):
        url = (
            'https://lcpl.ent.sirsi.net/client/en_US/lcpl/search/results'
            f'?qu=&qu=TITLE%3D{et}&qu=AUTHOR%3D{ea}'
            '&qu=-SUBJECT%3DComic&te=ILS&lm=BOOKS'
        )
    else:
        url = (
            'https://lcpl.ent.sirsi.net/client/en_US/lcpl/search/results'
            f'?qu=&qu=TITLE%3D{et}&qu=AUTHOR%3D{ea}&te=ILS&lm=BOOKS'
        )

    if volume is not None and volume > 0:
        url += f'&qu={volume}'
    return url


def broward_search_url(
    title: str,
    author: str,
    manga_type: str = '',
    volume: int | None = None,
) -> str:
    """
    Return a SirsiDynix Enterprise search URL for the Broward catalog.

    Light novels get an extra subject exclusion filter.  The &st=PA suffix
    keeps the results sorted by publication date (most-recent first), which
    surfaces the latest volumes more reliably than relevance ranking.
    """
    et   = _q(title)
    ea   = _q(author or '')
    base = (
        'https://broward.ent.sirsi.net/client/en_US/default/search/results'
        f'?qu=TITLE%3D{et}&qu=AUTHOR%3D{ea}'
        '&qf=FORMAT%09Special+Format%09BOOK%09Books'
    )

    if is_novel(manga_type):
        base += '&qu=-SUBJECT%3DComic'

    if volume is not None and volume > 0:
        base += f'&qu={volume}'

    base += '&st=PA'
    return base
