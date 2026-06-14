/**
 * static/catalog_url.js
 *
 * Builds SirsiDynix catalog search URLs on the client side, on card expand.
 * This replaces the server-side _lcpl_search_url / _broward_search_url
 * construction that previously ran for every row regardless of whether the
 * user ever opened the card (#4).
 *
 * Data attributes required on .manga-card:
 *   data-title   — manga title string
 *   data-author  — author string
 *   data-type    — manga type ("Manga", "Light Novel", etc.)
 *
 * Exported (module-level) for use in results.js:
 *   buildLcplUrl(title, author, type)    → URL string
 *   buildBrowardUrl(title, author, type) → URL string
 *   buildVolUrl(baseUrl, volNum)         → URL string  (append volume filter)
 */

const LCPL_BASE    = 'https://lcpl.ent.sirsi.net/client/en_US/lcpl/search/results';
const BROWARD_BASE = 'https://broward.ent.sirsi.net/client/en_US/default/search/results';

/** SirsiDynix novel subject-exclusion qf params, applied when type is a novel. */
const NOVEL_EXCLUSIONS = [
    ['qf', '-SUBJECT\tSubject\tGraphic novels.\tGraphic novels.'],
    ['qf', '-SUBJECT\tSubject\tComic books, strips, etc.\tComic books, strips, etc.'],
];

function _isNovel(type_) {
    const t = (type_ || '').trim().toLowerCase();
    return t === 'light novel' || t === 'novel' || t === 'ln';
}

function _buildParams(title, author, isNovel) {
    const p = new URLSearchParams();
    p.append('qu', `TITLE="${title}"`);
    p.append('qu', `AUTHOR=${author}`);
    p.append('te', 'ILS');
    if (isNovel) {
        NOVEL_EXCLUSIONS.forEach(([k, v]) => p.append(k, v));
    }
    return p;
}

export function buildLcplUrl(title, author, type_) {
    const p = _buildParams(title, author, _isNovel(type_));
    p.append('lm', 'BOOKS');
    return `${LCPL_BASE}?${p}`;
}

export function buildBrowardUrl(title, author, type_) {
    const p = _buildParams(title, author, _isNovel(type_));
    p.append('qf', 'FORMAT\tSpecial Format\tBOOK\tBooks');
    return `${BROWARD_BASE}?${p}`;
}

/** Append a volume number as an extra TITLE= qualifier. */
export function buildVolUrl(baseUrl, volNum) {
    if (!volNum || volNum === 0) return baseUrl;
    return `${baseUrl}&qu=TITLE=${encodeURIComponent(String(volNum))}`;
}
