"""
utils/search_utils.py

Assembles raw DB rows from the /search query into the structured list of
result dicts consumed by results.html.

Exports:
    build_results(rows, lcpl_library_id, broward_library_id,
                  avail_filter, no_vol1, mal_data, mal_filters)
    → list[dict]
"""
from __future__ import annotations

from utils.format_utils import fmt_scraped_at
from utils.scraper_utils import STATUS_PRIORITY, branch_short, normalize_status


def build_results(
    rows: list[dict],
    lcpl_library_id: int,
    broward_library_id: int,
    avail_filter: str = '',
    no_vol1: str = '',
    mal_data: dict | None = None,
    mal_filters: dict | None = None,
) -> list[dict]:
    """
    Convert a flat list of SQL rows (one per volume × branch × library) into
    the grouped, filtered result list consumed by results.html.

    Filters applied (in order):
        1. avail_filter  — 'available' | 'out' | '' (no filter)
        2. no_vol1       — '1' means only show titles that have vol 1 present
        3. MAL filter    — include/exclude by the user's MAL reading status

    Each result dict contains all data the template needs to render a manga
    card in both compact and expanded states.
    """
    # ── Pass 1: group rows by title ───────────────────────────────────────────
    titles_map: dict[str, dict] = {}

    for r in rows:
        t   = r['Title']
        lid = r['LibraryID']
        bid = r['BranchID']
        if lid is None or bid is None:
            continue

        if t not in titles_map:
            titles_map[t] = {
                'MangaID':    r['MangaID'],
                'Title':      t,
                'Volumes':    r['Volumes'],
                'Type':       r['Type'],
                'Members':    r['Members'],
                'Score':      r['Score'],
                'author':     r.get('Author') or '',
                'cover':      r.get('CoverMedium') or '',
                'lib_data':   {},
                'has_lcpl':   False,
                'has_broward': False,
                'scraped_at': None,
            }

        td = titles_map[t]

        row_scraped = r.get('ScrapedAt')
        if row_scraped is not None:
            if td['scraped_at'] is None or row_scraped > td['scraped_at']:
                td['scraped_at'] = row_scraped

        td['lib_data'].setdefault(lid, {
            'library_id':   lid,
            'library_name': (
                'Broward County Library'
                if lid == broward_library_id
                else 'Leon County Public Library'
            ),
            'volumes':      {},
            'branch_names': {},
        })
        ld = td['lib_data'][lid]

        vol         = r['Volume'] if r['Volume'] is not None else 0
        norm_status = normalize_status(r.get('Status') or '')

        ld['volumes'].setdefault(vol, {})
        current = ld['volumes'][vol].get(bid)
        if current is None or STATUS_PRIORITY[norm_status] > STATUS_PRIORITY.get(current, -1):
            ld['volumes'][vol][bid] = norm_status

        ld['branch_names'][bid] = r['BranchName']

        if lid == broward_library_id:
            td['has_broward'] = True
        elif lid == lcpl_library_id:
            td['has_lcpl'] = True

    # ── Pass 2: apply filters and assemble final result dicts ─────────────────
    grouped: list[dict] = []

    for td in titles_map.values():
        volume_best: dict[int, str] = {}
        has_vol1 = False

        for linfo in td['lib_data'].values():
            for vol_num, b_dict in linfo['volumes'].items():
                if vol_num == 1:
                    has_vol1 = True
                for bid, status in b_dict.items():
                    cur = volume_best.get(vol_num)
                    if cur is None or STATUS_PRIORITY[status] > STATUS_PRIORITY.get(cur, -1):
                        volume_best[vol_num] = status

        avail_count = sum(1 for s in volume_best.values() if s == 'Available')
        hold_count  = sum(1 for s in volume_best.values() if s == 'On Hold')
        out_count   = sum(1 for s in volume_best.values()
                          if s not in ('Available', 'On Hold'))

        if avail_filter == 'available' and avail_count == 0:
            continue
        if avail_filter == 'out' and out_count == 0:
            continue

        if no_vol1 == '1' and not has_vol1:
            continue

        if mal_data and mal_filters:
            include_statuses = [k for k, v in mal_filters.items() if v == 'include']
            exclude_statuses = [k for k, v in mal_filters.items() if v == 'exclude']
            if include_statuses or exclude_statuses:
                mid_str     = str(td['MangaID'])
                entry       = mal_data.get(mid_str) or mal_data.get(td['MangaID'])
                user_status = entry.get('status', '') if entry else ''
                if include_statuses and user_status not in include_statuses:
                    continue
                if user_status in exclude_statuses:
                    continue

        lib_list: list[dict] = []

        for linfo in sorted(td['lib_data'].values(), key=lambda x: x['library_id']):
            branch_best: dict[int, str] = {}
            vol_list: list[dict] = []

            for vol_num in sorted(linfo['volumes'].keys()):
                branch_statuses = linfo['volumes'][vol_num]
                vol_branches: list[dict] = []

                for bid in sorted(branch_statuses.keys(),
                                   key=lambda b: linfo['branch_names'].get(b, '')):
                    status = branch_statuses[bid]
                    bname  = linfo['branch_names'].get(bid, '')
                    vol_branches.append({
                        'name':   bname,
                        'short':  branch_short(bname, linfo['library_id'], broward_library_id),
                        'status': status,
                    })
                    cur_best = branch_best.get(bid)
                    if cur_best is None or STATUS_PRIORITY[status] > STATUS_PRIORITY.get(cur_best, -1):
                        branch_best[bid] = status

                vol_list.append({'vol': vol_num, 'branches': vol_branches})

            branch_list = [
                {
                    'name':   linfo['branch_names'].get(bid, ''),
                    'short':  branch_short(
                        linfo['branch_names'].get(bid, ''),
                        linfo['library_id'],
                        broward_library_id,
                    ),
                    'status': branch_best[bid],
                }
                for bid in sorted(branch_best.keys(),
                                   key=lambda b: linfo['branch_names'].get(b, ''))
            ]

            lib_list.append({
                'library_id':   linfo['library_id'],
                'library_name': linfo['library_name'],
                'branch_list':  branch_list,
                'vol_list':     vol_list,
            })

        grouped.append({
            'MangaID':     td['MangaID'],
            'Title':       td['Title'],
            'Volumes':     td['Volumes'],
            'Type':        td.get('Type', ''),
            'Members':     td['Members'],
            'Score':       td['Score'],
            'author':      td.get('author', ''),
            'cover':       td['cover'],
            'has_lcpl':    td['has_lcpl'],
            'has_broward': td['has_broward'],
            'lib_list':    lib_list,
            'vol_count':   len({v for li in td['lib_data'].values() for v in li['volumes']}),
            'avail_count': avail_count,
            'out_count':   out_count,
            'hold_count':  hold_count,
            'scraped_at':  fmt_scraped_at(td.get('scraped_at')),
        })

    return grouped
