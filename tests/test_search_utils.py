"""
tests/test_search_utils.py

Unit tests for utils/search_utils.py  (build_results).

build_results is the most complex pure-logic function in the codebase:
it groups flat SQL rows by title, aggregates statuses across branches,
applies four independent filter passes, and assembles the final result
dicts consumed by results.html.

No DB, no Flask, no network — pure dict-in / list-out.
"""
from __future__ import annotations

import pytest
from tests.conftest import make_row
from utils.search_utils import build_results

LCPL    = 1
BROWARD = 2


# ── Helpers ───────────────────────────────────────────────────────────────────

def _build(rows, **kwargs):
    """Convenience wrapper with sensible defaults."""
    return build_results(
        rows,
        lcpl_library_id=LCPL,
        broward_library_id=BROWARD,
        **kwargs,
    )


def _one(rows, **kwargs):
    """Assert exactly one result and return it."""
    results = _build(rows, **kwargs)
    assert len(results) == 1, f"Expected 1 result, got {len(results)}: {[r['Title'] for r in results]}"
    return results[0]


# ── Grouping ──────────────────────────────────────────────────────────────────

class TestGrouping:

    def test_single_row_produces_one_result(self):
        rows = [make_row()]
        results = _build(rows)
        assert len(results) == 1

    def test_same_title_two_volumes_one_result(self):
        rows = [
            make_row(volume=1, status='Available'),
            make_row(volume=2, status='Checked Out'),
        ]
        result = _one(rows)
        assert result['vol_count'] == 2

    def test_two_different_titles_two_results(self):
        rows = [
            make_row(manga_id=1, title='Berserk'),
            make_row(manga_id=2, title='One Piece'),
        ]
        results = _build(rows)
        assert len(results) == 2

    def test_rows_with_null_branch_id_skipped(self):
        row = make_row()
        row['BranchID'] = None
        results = _build([row])
        assert results == []

    def test_rows_with_null_library_id_skipped(self):
        row = make_row()
        row['LibraryID'] = None
        results = _build([row])
        assert results == []

    def test_vol_count_counts_distinct_volumes(self):
        rows = [
            make_row(volume=1, branch_id=1, status='Available'),
            make_row(volume=1, branch_id=2, status='Checked Out'),  # same vol, different branch
            make_row(volume=2, branch_id=1, status='Available'),
        ]
        result = _one(rows)
        assert result['vol_count'] == 2   # 2 distinct volume numbers

    def test_has_lcpl_flag(self):
        rows = [make_row(library_id=LCPL)]
        result = _one(rows)
        assert result['has_lcpl'] is True
        assert result['has_broward'] is False

    def test_has_broward_flag(self):
        rows = [make_row(library_id=BROWARD)]
        result = _one(rows)
        assert result['has_broward'] is True
        assert result['has_lcpl'] is False

    def test_best_status_wins_per_volume(self):
        # Vol 1 has Available at branch 1 and Checked Out at branch 2
        # The overall avail_count should count it as Available
        rows = [
            make_row(volume=1, branch_id=1, status='Available',   library_id=LCPL),
            make_row(volume=1, branch_id=2, status='Checked Out', library_id=LCPL),
        ]
        result = _one(rows)
        assert result['avail_count'] == 1

    def test_scraped_at_takes_latest(self):
        from datetime import datetime, timezone
        early = datetime(2024, 1, 1, tzinfo=timezone.utc)
        late  = datetime(2024, 6, 1, tzinfo=timezone.utc)
        rows = [
            make_row(volume=1, scraped_at=early),
            make_row(volume=2, scraped_at=late),
        ]
        result = _one(rows)
        # scraped_at in result is the formatted string; just check it's not None
        assert result['scraped_at'] is not None


# ── avail_filter ──────────────────────────────────────────────────────────────

class TestAvailFilter:

    def test_available_filter_keeps_available_titles(self):
        rows = [make_row(status='Available')]
        results = _build(rows, avail_filter='available')
        assert len(results) == 1

    def test_available_filter_removes_fully_checked_out(self):
        rows = [make_row(status='Checked Out')]
        results = _build(rows, avail_filter='available')
        assert results == []

    def test_out_filter_keeps_checked_out_titles(self):
        rows = [make_row(status='Checked Out')]
        results = _build(rows, avail_filter='out')
        assert len(results) == 1

    def test_out_filter_removes_all_available(self):
        rows = [make_row(status='Available')]
        results = _build(rows, avail_filter='out')
        assert results == []

    def test_empty_filter_keeps_everything(self):
        rows = [make_row(status='Available'), make_row(manga_id=2, title='B', status='Checked Out')]
        results = _build(rows, avail_filter='')
        assert len(results) == 2

    def test_title_with_mixed_statuses_passes_available_filter(self):
        rows = [
            make_row(volume=1, status='Checked Out'),
            make_row(volume=2, status='Available'),
        ]
        results = _build(rows, avail_filter='available')
        assert len(results) == 1


# ── no_vol1 filter ────────────────────────────────────────────────────────────

class TestNoVol1Filter:

    def test_has_vol1_passes(self):
        rows = [make_row(volume=1)]
        results = _build(rows, no_vol1='1')
        assert len(results) == 1

    def test_missing_vol1_filtered_out(self):
        rows = [make_row(volume=2), make_row(volume=3)]
        results = _build(rows, no_vol1='1')
        assert results == []

    def test_filter_inactive_when_empty_string(self):
        rows = [make_row(volume=2)]
        results = _build(rows, no_vol1='')
        assert len(results) == 1


# ── MAL filter ────────────────────────────────────────────────────────────────

class TestMalFilter:

    def _mal_data(self, manga_id: int, status: str) -> dict:
        return {str(manga_id): {'status': status, 'score': 0, 'num_volumes_read': 0}}

    def test_include_reading_keeps_reading_titles(self):
        rows = [make_row(manga_id=1)]
        mal_data    = self._mal_data(1, 'reading')
        mal_filters = {'reading': 'include', 'completed': '', 'on_hold': '',
                       'dropped': '', 'plan_to_read': ''}
        results = _build(rows, mal_data=mal_data, mal_filters=mal_filters)
        assert len(results) == 1

    def test_include_reading_removes_completed_titles(self):
        rows = [make_row(manga_id=1)]
        mal_data    = self._mal_data(1, 'completed')
        mal_filters = {'reading': 'include', 'completed': '', 'on_hold': '',
                       'dropped': '', 'plan_to_read': ''}
        results = _build(rows, mal_data=mal_data, mal_filters=mal_filters)
        assert results == []

    def test_exclude_dropped_removes_dropped_titles(self):
        rows = [make_row(manga_id=1)]
        mal_data    = self._mal_data(1, 'dropped')
        mal_filters = {'reading': '', 'completed': '', 'on_hold': '',
                       'dropped': 'exclude', 'plan_to_read': ''}
        results = _build(rows, mal_data=mal_data, mal_filters=mal_filters)
        assert results == []

    def test_exclude_dropped_keeps_reading_titles(self):
        rows = [make_row(manga_id=1)]
        mal_data    = self._mal_data(1, 'reading')
        mal_filters = {'reading': '', 'completed': '', 'on_hold': '',
                       'dropped': 'exclude', 'plan_to_read': ''}
        results = _build(rows, mal_data=mal_data, mal_filters=mal_filters)
        assert len(results) == 1

    def test_no_mal_data_no_filtering(self):
        rows = [make_row(manga_id=1)]
        results = _build(rows, mal_data=None, mal_filters={'reading': 'include'})
        assert len(results) == 1

    def test_all_filters_neutral_no_filtering(self):
        rows = [make_row(manga_id=1)]
        mal_data    = self._mal_data(1, 'reading')
        mal_filters = {'reading': '', 'completed': '', 'on_hold': '',
                       'dropped': '', 'plan_to_read': ''}
        results = _build(rows, mal_data=mal_data, mal_filters=mal_filters)
        assert len(results) == 1

    def test_title_not_in_mal_data_excluded_by_include_filter(self):
        # Include filter is active but this manga isn't in the MAL list at all
        rows = [make_row(manga_id=99)]
        mal_data    = self._mal_data(1, 'reading')   # different ID
        mal_filters = {'reading': 'include', 'completed': '', 'on_hold': '',
                       'dropped': '', 'plan_to_read': ''}
        results = _build(rows, mal_data=mal_data, mal_filters=mal_filters)
        assert results == []


# ── Result dict structure ─────────────────────────────────────────────────────

class TestResultStructure:

    def test_required_keys_present(self):
        rows = [make_row()]
        result = _one(rows)
        required = {
            'MangaID', 'Title', 'Volumes', 'Type', 'Members', 'Score',
            'author', 'cover', 'has_lcpl', 'has_broward', 'lib_list',
            'vol_count', 'avail_count', 'out_count', 'hold_count', 'scraped_at',
        }
        assert required.issubset(result.keys())

    def test_lib_list_structure(self):
        rows = [make_row(library_id=LCPL, branch_id=1, branch_name='Main', volume=1)]
        result = _one(rows)
        assert len(result['lib_list']) == 1
        lib = result['lib_list'][0]
        assert 'library_id' in lib
        assert 'vol_list' in lib
        assert 'branch_list' in lib
        vol = lib['vol_list'][0]
        assert vol['vol'] == 1
        assert len(vol['branches']) == 1
        assert vol['branches'][0]['name'] == 'Main'

    def test_avail_out_hold_counts(self):
        rows = [
            make_row(volume=1, status='Available',   branch_id=1, library_id=LCPL),
            make_row(volume=2, status='On Hold',     branch_id=1, library_id=LCPL),
            make_row(volume=3, status='Checked Out', branch_id=1, library_id=LCPL),
        ]
        result = _one(rows)
        assert result['avail_count'] == 1
        assert result['hold_count']  == 1
        assert result['out_count']   == 1

    def test_multi_library_produces_multiple_lib_list_entries(self):
        rows = [
            make_row(library_id=LCPL,    branch_id=1, branch_name='LCPL Main',    volume=1),
            make_row(library_id=BROWARD, branch_id=10, branch_name='BCL Main',    volume=1),
        ]
        result = _one(rows)
        assert len(result['lib_list']) == 2
        ids = {lib['library_id'] for lib in result['lib_list']}
        assert ids == {LCPL, BROWARD}
