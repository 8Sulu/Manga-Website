"""
utils/fulltext.py

Helpers for MySQL FULLTEXT BOOLEAN MODE search, used by both the
/api/suggestions typeahead and /search's title filter — replaces the old
`Title LIKE '%q%'` scan, which can't use any index and forces MySQL to
examine every row in `manga` on every keystroke.

WHY BOOLEAN MODE WITH PREFIX TRUNCATION (`word*`), NOT NATURAL LANGUAGE MODE:
NATURAL LANGUAGE MODE only matches whole indexed words (after MySQL's
built-in stopword filtering) — it has no concept of "starts with". That's
wrong for a typeahead: a user typing "ber" expects "Berserk" to show up
before they've finished typing the word. BOOLEAN MODE's `*` truncation
operator gives prefix matching, and the `+` operator makes each word
required (an AND across words) so "one piece" doesn't match every title
containing just "one" OR just "piece".

KNOWN LIMITATION — innodb_ft_min_token_size:
MySQL's default InnoDB FULLTEXT config never indexes words shorter than
3 characters. A query for "a" or "of" will never FULLTEXT-match anything,
regardless of truncation. build_boolean_query drops any word under
MIN_INDEXED_WORD_LEN from the boolean expression entirely; callers should
fall back to a LIKE query when this function returns an empty string (the
caller's query was nothing BUT short words / FULLTEXT operator
characters) so search degrades gracefully instead of silently returning
nothing.
"""

from __future__ import annotations

import re

# Characters with special meaning inside MySQL BOOLEAN MODE search strings.
# Stripped from user input so a typed "+", "-", "*", etc. can't change
# query semantics (e.g. force-exclude a word, group terms, etc.) or break
# the query outright.
_FT_OPERATOR_RE = re.compile(r'[+\-><()~*"@]')

# Matches InnoDB's default innodb_ft_min_token_size — words shorter than
# this are never written to the FULLTEXT index, so searching for them
# (even as a prefix) can never match anything.
MIN_INDEXED_WORD_LEN = 3


def build_boolean_query(raw: str) -> str:
    """
    Convert free-text user input into a MySQL BOOLEAN MODE search string
    that requires every sufficiently-long word to match as a prefix.

        "berserk"     -> "+berserk*"
        "one piece"   -> "+one* +piece*"
        "a piece"     -> "+piece*"        ('a' is below MIN_INDEXED_WORD_LEN)
        "+++"         -> ""               (nothing left after stripping)

    Returns "" when no usable word remains, signalling the caller should
    fall back to a LIKE query instead.
    """
    cleaned = _FT_OPERATOR_RE.sub(" ", raw)
    words = [w for w in cleaned.split() if len(w) >= MIN_INDEXED_WORD_LEN]
    return " ".join(f"+{w}*" for w in words)
