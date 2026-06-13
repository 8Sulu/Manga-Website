/**
 * static/results.js
 *
 * Search results page JS.  Catalog URLs are built client-side on card expand
 * rather than server-side for every row (#4).
 */
import { buildLcplUrl, buildBrowardUrl, buildVolUrl } from './catalog_url.js';

// ── MAL panel toggle ───────────────────────────────────────────────────────────

function toggleMalPanel() {
    document.getElementById('mal-panel').classList.toggle('open');
}

// ── Library filter pills ───────────────────────────────────────────────────────

document.getElementById('lib-filter-row').addEventListener('click', e => {
    const pill = e.target.closest('.lib-filter-pill');
    if (!pill) return;
    e.preventDefault();
    const url = new URL(window.location.href);
    if (pill.dataset.val) url.searchParams.set('library', pill.dataset.val);
    else                  url.searchParams.delete('library');
    url.searchParams.delete('page');
    window.location.href = url.toString();
});

// ── Tab switching ──────────────────────────────────────────────────────────────

function switchLibTab(btn, panelId) {
    const card = btn.closest('.manga-card');
    card.querySelectorAll('.lib-tab').forEach(t => t.className = t.className.replace(/\s*active-\S+/g, ''));
    card.querySelectorAll('.lib-panel').forEach(p => p.classList.remove('visible'));
    btn.classList.add(btn.textContent.includes('Broward') ? 'active-broward' : 'active-lcpl');
    document.getElementById(panelId)?.classList.add('visible');
}

// ── Card expand / collapse ─────────────────────────────────────────────────────

function _injectCatalogLinks(card) {
    /** Build catalog URLs lazily on first expand — not computed server-side. */
    if (card.dataset.urlsInjected) return;
    card.dataset.urlsInjected = '1';

    const title  = card.dataset.title  || '';
    const author = card.dataset.author || '';
    const type_  = card.dataset.type   || '';

    const lcplBase    = buildLcplUrl(title, author, type_);
    const browardBase = buildBrowardUrl(title, author, type_);

    // Wire library-level "open catalog" buttons
    card.querySelectorAll('[data-catalog-link="lcpl"]').forEach(a => {
        a.href = lcplBase;
    });
    card.querySelectorAll('[data-catalog-link="broward"]').forEach(a => {
        a.href = browardBase;
    });

    // Wire volume chips  — data-vol attribute carries the volume number
    card.querySelectorAll('.vol-chip[data-vol][data-lib]').forEach(a => {
        const vol    = parseInt(a.dataset.vol, 10);
        const lib    = a.dataset.lib;          // "lcpl" | "broward"
        const base   = lib === 'broward' ? browardBase : lcplBase;
        a.href = buildVolUrl(base, vol);
    });
}

function toggleCard(card) {
    if (card.classList.contains('open')) { closeCard(card); return; }
    document.querySelectorAll('.manga-card.open').forEach(closeCard);
    card.classList.add('open');
    const [compact, expanded] = card.querySelectorAll('.manga-body');
    compact.style.display        = 'none';
    expanded.style.display       = 'flex';
    expanded.style.flexDirection = 'column';
    _injectCatalogLinks(card);
    card.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
}

function closeCard(card) {
    card.classList.remove('open');
    const [compact, expanded] = card.querySelectorAll('.manga-body');
    compact.style.display  = '';
    expanded.style.display = 'none';
}

// ── Server-side sort (persisted in URL, works across all pages) ────────────────

function sortCards(key) {
    const url    = new URL(window.location.href);
    const curKey = url.searchParams.get('sort')     || 'score';
    const curDir = url.searchParams.get('sort_dir') || 'desc';
    const newDir = (key === curKey && curDir === 'desc') ? 'asc' : 'desc';
    url.searchParams.set('sort',     key);
    url.searchParams.set('sort_dir', newDir);
    url.searchParams.delete('page');
    window.location.href = url.toString();
}

(function highlightActiveSort() {
    const key = new URL(window.location.href).searchParams.get('sort') || 'score';
    document.querySelectorAll('.sort-btn').forEach(b => b.classList.remove('active'));
    document.getElementById('sort-' + key)?.classList.add('active');
})();

// ── Filter chip removal ────────────────────────────────────────────────────────

function removeFilter(key) {
    const url = new URL(window.location.href);
    url.searchParams.delete(key);
    url.searchParams.delete('page');
    window.location.href = url.toString();
}

// ── MAL list fetch — async background job (#15) ────────────────────────────────

async function loadMalList() {
    const loadBtn = document.getElementById('mal-load-btn');
    const loading = document.getElementById('mal-loading');
    const errorEl = document.getElementById('mal-error');

    loadBtn.style.display = 'none';
    loading.style.display = 'inline';
    errorEl.style.display = 'none';

    try {
        const startResp = await fetch('/api/mal/mangalist');
        const startJson = await startResp.json();
        if (!startJson.ok) throw new Error(startJson.message || 'Failed to start MAL fetch');

        const data = await _pollMalJob(startJson.job_id);

        const setResp = await fetch('/api/mal/set_filter', {
            method:  'POST',
            headers: { 'Content-Type': 'application/json' },
            body:    JSON.stringify({
                data:    data,
                filters: { reading: '', completed: '', on_hold: '', dropped: '', plan_to_read: '' },
            }),
        });
        if (!setResp.ok) throw new Error('Failed to save MAL session');

        const url = new URL(window.location.href);
        url.searchParams.delete('page');
        window.location.href = url.toString();
    } catch (e) {
        loading.style.display = 'none';
        errorEl.textContent   = '⚠ ' + e.message;
        errorEl.style.display = 'inline';
        loadBtn.style.display = 'inline';
    }
}

async function _pollMalJob(jobId) {
    const MAX_POLLS    = 60;
    const POLL_INTERVAL = 2000;
    for (let i = 0; i < MAX_POLLS; i++) {
        await new Promise(r => setTimeout(r, POLL_INTERVAL));
        const resp = await fetch(`/api/mal/mangalist/status/${jobId}`);
        const json = await resp.json();
        if (!json.ok && json.status !== 'running') throw new Error(json.message || 'MAL fetch failed');
        if (json.status === 'done') return json.data;
        const loading = document.getElementById('mal-loading');
        if (loading) loading.textContent = `Loading MAL list… (${(i + 1) * 2}s)`;
    }
    throw new Error('MAL list fetch timed out');
}

// ── MAL filter pill toggle ─────────────────────────────────────────────────────

function toggleMalStatus(status, el) {
    const states = ['', 'include', 'exclude'];
    const icons  = { '': '◦', include: '✓', exclude: '✕' };
    const next   = states[(states.indexOf(el.dataset.state || '') + 1) % 3];
    el.dataset.state = next;
    el.querySelector('.mal-status-icon').textContent = icons[next];
    el.classList.toggle('pending', next !== (el.dataset.applied || ''));
    const anyPending = [...document.querySelectorAll('.mal-status-item')]
        .some(p => (p.dataset.state || '') !== (p.dataset.applied || ''));
    document.getElementById('mal-apply-btn')?.classList.toggle('visible', anyPending);
}

// ── MAL apply / clear ──────────────────────────────────────────────────────────

async function applyMalFilters() {
    const applyBtn = document.getElementById('mal-apply-btn');
    applyBtn?.classList.add('busy');
    const filters = {};
    document.querySelectorAll('.mal-status-item[data-status]').forEach(p => {
        filters[p.dataset.status] = p.dataset.state || '';
    });
    try {
        const resp = await fetch('/api/mal/set_filter', {
            method:  'POST',
            headers: { 'Content-Type': 'application/json' },
            body:    JSON.stringify({ filters }),
        });
        if (!resp.ok) throw new Error('Failed to save filter');
        const url = new URL(window.location.href);
        url.searchParams.delete('page');
        window.location.href = url.toString();
    } catch (e) {
        applyBtn?.classList.remove('busy');
        console.error('MAL apply failed:', e);
    }
}

async function clearMalFilter() {
    try {
        await fetch('/api/mal/clear_filter', { method: 'POST' });
        const url = new URL(window.location.href);
        url.searchParams.delete('page');
        window.location.href = url.toString();
    } catch (e) {
        console.error('MAL clear failed:', e);
    }
}

// Snapshot applied state on load
document.querySelectorAll('.mal-status-item[data-status]').forEach(p => {
    p.dataset.applied = p.dataset.state || '';
});

// Expose functions needed by inline onclick handlers in results.html
Object.assign(window, {
    toggleMalPanel, switchLibTab, toggleCard, closeCard,
    sortCards, removeFilter, loadMalList, toggleMalStatus,
    applyMalFilters, clearMalFilter,
});
