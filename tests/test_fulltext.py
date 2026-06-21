"""
tests/test_fulltext.py

Unit tests for utils/fulltext.py (build_boolean_query).

Pure string transformation — no DB, no Flask, no MySQL needed to test the
logic. Whether MySQL actually returns the rows you'd expect for a given
boolean-mode string is exercised manually against a real DB, not here.
"""

from utils.fulltext import MIN_INDEXED_WORD_LEN, build_boolean_query


class TestBuildBooleanQuery:
    def test_single_word(self):
        assert build_boolean_query("berserk") == "+berserk*"

    def test_multiple_words_all_required(self):
        assert build_boolean_query("one piece") == "+one* +piece*"

    def test_case_preserved(self):
        # MySQL FULLTEXT is case-insensitive by default collation; no need
        # to lowercase here, just confirm we don't mangle casing ourselves.
        assert build_boolean_query("Berserk") == "+Berserk*"

    def test_short_word_dropped(self):
        # 'a' is below MIN_INDEXED_WORD_LEN (3) — innodb_ft_min_token_size
        # means it was never written to the index, so searching for it
        # (even as a prefix) can never match. Dropped, not encoded.
        result = build_boolean_query("a piece")
        assert result == "+piece*"
        assert "a*" not in result

    def test_all_words_too_short_returns_empty(self):
        assert build_boolean_query("a of") == ""

    def test_operator_characters_stripped(self):
        # Characters with special meaning in BOOLEAN MODE are stripped so
        # user input can't change query semantics (force-exclude a word,
        # group terms, etc.) or break the query outright.
        assert build_boolean_query("one+piece") == "+one* +piece*"
        assert build_boolean_query('"quoted"') == "+quoted*"
        assert build_boolean_query("-excluded") == "+excluded*"

    def test_only_operator_characters_returns_empty(self):
        assert build_boolean_query("+++") == ""
        assert build_boolean_query("***") == ""

    def test_empty_string_returns_empty(self):
        assert build_boolean_query("") == ""

    def test_whitespace_only_returns_empty(self):
        assert build_boolean_query("   ") == ""

    def test_min_indexed_word_len_is_three(self):
        # Documents the InnoDB default this module is built around — if
        # MySQL's innodb_ft_min_token_size config ever changes, this
        # constant (and migrations/versions/0002) need to move together.
        assert MIN_INDEXED_WORD_LEN == 3
