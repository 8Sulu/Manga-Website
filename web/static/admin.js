/**
 * static/admin.js
 *
 * Admin dashboard JS — extracted from inline <script> in admin.html (#6).
 * Handles job polling, stats, branch chart, and all admin actions.
 */

// ── Config ─────────────────────────────────────────────────────────────────────

const POLL_INTERVAL   = 1500;     // ms between job status polls
const STATS_INTERVAL  = 30_000;   // ms between background stats refreshes
const MISSING_INTERVAL = 10_000;  // ms between missing-titles polls

// Read CSRF token injected into a <meta> tag by admin.html
function csrfToken() {
    return document.querySelector('meta[name="csrf-token"]')?.content || '';
}

// ── Utilities ──────────────────────────────────────────────────────────────────

function $(id) { return document.getElementById(id); }

function setHtml(id, html) {
    const el = $(id);
    if (el) el.innerHTML = html;
}

function setText(id, text) {
    const el = $(id);
    if (el) el.textContent = text;
}

function showEl(id)  { const el = $(id); if (el) el.style.display = ''; }
function hideEl(id)  { const el = $(id); if (el) el.style.display = 'none'; }

async function postJson(url, body) {
    return fetch(url, {
        method:  'POST',
        headers: { 'Content-Type': 'application/json', 'X-CSRF-Token': csrfToken() },
        body:    JSON.stringify(body),
    });
}

// ── Job polling ────────────────────────────────────────────────────────────────

const _pollers = {};   // jobName → interval id

function startPolling(jobName) {
    if (_pollers[jobName]) return;
    _setJobUi(jobName, true, 0, 'starting…');
    _pollers[jobName] = setInterval(() => _pollOnce(jobName), POLL_INTERVAL);
}

async function _pollOnce(jobName) {
    try {
        const resp = await fetch(`/api/job/${jobName}`);
        if (!resp.ok) return;
        const { running, progress, message } = await resp.json();
        _setJobUi(jobName, running, progress, message);
        if (!running) {
            clearInterval(_pollers[jobName]);
            delete _pollers[jobName];
            loadStats();
            loadMissingCount();
            loadHistory();
        }
    } catch { /* network blip — keep polling */ }
}

function _setJobUi(jobName, running, progress, message) {
    // Progress bar
    const bar = $(`progress-${jobName}`);
    if (bar) {
        bar.style.width   = `${progress}%`;
        bar.style.display = running ? '' : 'none';
    }
    // Status text
    const st = $(`status-${jobName}`);
    if (st) {
        st.textContent = message || (running ? 'running…' : '');
        st.className   = 'job-status-text ' + (running ? 'running' : (
            message?.includes('error') ? 'error' : 'done'
        ));
    }
    // Start / stop buttons
    const startBtn = $(`start-btn-${jobName}`);
    const stopBtn  = $(`stop-btn-${jobName}`);
    if (startBtn) startBtn.disabled = running;
    if (stopBtn)  stopBtn.style.display = running ? 'inline-flex' : 'none';
}

// ── Stop job ───────────────────────────────────────────────────────────────────

async function stopJob(jobName) {
    const resp = await postJson(`/api/job/stop/${jobName}`, {});
    if (resp.ok) setText(`status-${jobName}`, 'stop signal sent…');
}

// ── Stats ──────────────────────────────────────────────────────────────────────

async function loadStats() {
    try {
        const resp = await fetch('/api/stats');
        if (!resp.ok) return;
        const { volumes, titles, last_scraped } = await resp.json();
        setText('stat-volumes',      volumes?.toLocaleString() ?? '—');
        setText('stat-titles',       titles?.toLocaleString()  ?? '—');
        setText('stat-last-scraped', last_scraped ?? '—');
    } catch { /* ignore */ }
}

// ── Missing titles count ───────────────────────────────────────────────────────

async function loadMissingCount() {
    try {
        const resp = await fetch('/api/missing_titles');
        if (!resp.ok) return;
        const { count, broward_count, total_titles } = await resp.json();
        setText('missing-lcpl',    `${count} / ${total_titles} titles not yet scraped (LCPL)`);
        setText('missing-broward', `${broward_count} / ${total_titles} titles not yet scraped (Broward)`);
    } catch { /* ignore */ }
}

// ── Job history ────────────────────────────────────────────────────────────────

async function loadHistory() {
    try {
        const resp = await fetch('/api/job_history');
        if (!resp.ok) return;
        const entries = await resp.json();
        const rows = entries.slice(0, 30).map(e => {
            const at  = e.at ? new Date(e.at).toLocaleString() : '—';
            const cls = e.status === 'error' ? 'hist-error' : (e.status === 'done' ? 'hist-done' : '');
            return `<tr class="${cls}">
                      <td class="hist-job">${e.job}</td>
                      <td class="hist-status">${e.status}</td>
                      <td class="hist-msg">${e.message || ''}</td>
                      <td class="hist-at">${at}</td>
                    </tr>`;
        }).join('');
        setHtml('history-tbody', rows || '<tr><td colspan="4">No history yet</td></tr>');
    } catch { /* ignore */ }
}

// ── Branch stats chart ─────────────────────────────────────────────────────────
// Rendered server-side as data attributes; JS draws the bars.

function renderBranchChart() {
    const container = $('branch-chart');
    if (!container) return;

    // Group bars by library (each .chart-group has data-max and .bar-row children)
    container.querySelectorAll('.chart-group').forEach(group => {
        const groupMax = parseInt(group.dataset.max, 10) || 1;
        group.querySelectorAll('.bar-row').forEach(row => {
            const count = parseInt(row.dataset.count, 10) || 0;
            const pct   = Math.round((count / groupMax) * 100);
            const bar   = row.querySelector('.bar-fill');
            if (bar) bar.style.width = `${pct}%`;
            const label = row.querySelector('.bar-pct');
            if (label) label.textContent = `${pct}%`;
        });
    });
}

// ── Scrape form handling ───────────────────────────────────────────────────────

async function submitScrapeForm(jobName) {
    const form   = $(`form-${jobName}`);
    const data   = new FormData(form);
    const body   = Object.fromEntries(data.entries());
    body.action  = jobName;

    const resp = await postJson('/admin', body);
    const json = await resp.json();

    if (json.ok) {
        startPolling(jobName);
    } else {
        setText(`status-${jobName}`, `⚠ ${json.message}`);
    }
}

// ── DB reset ───────────────────────────────────────────────────────────────────

async function resetDatabase() {
    if (!confirm('This will drop and recreate all tables. Are you sure?')) return;
    const btn = $('reset-btn');
    if (btn) { btn.disabled = true; btn.textContent = 'Resetting…'; }

    try {
        const resp = await postJson('/admin/reset', {});
        const json = await resp.json();
        const msgs = (json.messages || [json.message || 'Unknown error']).join('\n');
        alert(json.ok ? `✓ Done:\n${msgs}` : `✗ Error:\n${msgs}`);
        if (json.ok) { loadStats(); loadMissingCount(); }
    } catch (e) {
        alert(`Network error: ${e.message}`);
    } finally {
        if (btn) { btn.disabled = false; btn.textContent = 'Reset Database'; }
    }
}

// ── Delete title availability ──────────────────────────────────────────────────

async function deleteTitleResults(mangaId, libraryId) {
    if (!confirm(`Clear availability for ID ${mangaId}?`)) return;
    const resp = await postJson('/api/delete_title_results', {
        manga_id: mangaId,
        library:  libraryId || null,
    });
    const json = await resp.json();
    if (json.ok) {
        const row = document.querySelector(`[data-manga-id="${mangaId}"]`);
        row?.remove();
        loadMissingCount();
    } else {
        alert(`Error: ${json.message}`);
    }
}

// ── Find & Re-scrape Title ─────────────────────────────────────────────────────

let _rescrapeSelected = null;
let _rescrapeTimer    = null;

// Create dropdown once at body level — fully outside all stacking contexts
const _dd = document.createElement('div');
_dd.id = 'rescrape-dropdown';
_dd.className = 'rescrape-dropdown';
_dd.style.display = 'none';
document.body.appendChild(_dd);

function _positionDropdown() {
    const input = $('rescrape-search');
    if (!input) return;
    const r = input.getBoundingClientRect();
    _dd.style.top   = `${r.bottom + window.scrollY + 3}px`;
    _dd.style.left  = `${r.left   + window.scrollX}px`;
    _dd.style.width = `${r.width}px`;
}

async function rescrapeSearch(q) {
    clearTimeout(_rescrapeTimer);
    if (q.length < 2) { _dd.style.display = 'none'; return; }

    _rescrapeTimer = setTimeout(async () => {
        try {
            const resp = await fetch(`/api/suggestions?q=${encodeURIComponent(q)}`);
            if (!resp.ok) return;
            const results = await resp.json();
            if (!results.length) { _dd.style.display = 'none'; return; }

            _dd.innerHTML = results.map(r =>
                `<div class="rescrape-option"
                      onclick="rescrapeSelect(${r.manga_id}, ${JSON.stringify(r.title)})">
                   <span class="rescrape-opt-title">${r.title}</span>
                   <span class="rescrape-opt-meta">${r.type} · ID ${r.manga_id}${r.score ? ' · ★' + r.score : ''}</span>
                 </div>`
            ).join('');

            _positionDropdown();
            _dd.style.display = 'block';
        } catch { /* ignore */ }
    }, 250);
}

function rescrapeSelect(mangaId, title) {
    _rescrapeSelected = { manga_id: mangaId, title };
    _dd.style.display = 'none';
    $('rescrape-search').value            = '';
    $('rescrape-title-label').textContent = title;
    $('rescrape-id-badge').textContent    = `ID ${mangaId}`;
    $('rescrape-selected').style.display  = '';
    $('rescrape-btns').style.display      = '';
}

function rescrapeClear() {
    _rescrapeSelected = null;
    $('rescrape-search').value            = '';
    $('rescrape-selected').style.display  = 'none';
    $('rescrape-btns').style.display      = 'none';
    _dd.style.display                     = 'none';
}

async function rescrapeSelected(jobName) {
    if (!_rescrapeSelected) return;
    const { manga_id, title } = _rescrapeSelected;

    const body = new FormData();
    body.append('action',     jobName);
    body.append('manga_id',   String(manga_id));
    body.append('csrf_token', csrfToken());

    const resp = await fetch('/admin', { method: 'POST', body });
    const json = await resp.json();
    if (json.ok) {
        startPolling(jobName);
        setText(`status-${jobName}`, `Re-scraping "${title}" (ID ${manga_id})…`);
    } else {
        alert(`⚠ ${json.message}`);
    }
}

// Reposition on scroll/resize so fixed coords stay accurate
window.addEventListener('scroll', () => { if (_dd.style.display !== 'none') _positionDropdown(); }, { passive: true });
window.addEventListener('resize', () => { if (_dd.style.display !== 'none') _positionDropdown(); }, { passive: true });

// Close when clicking outside
document.addEventListener('click', e => {
    if (!e.target.closest('#rescrape-card') && !e.target.closest('#rescrape-dropdown')) {
        _dd.style.display = 'none';
    }
});

// ── Init ───────────────────────────────────────────────────────────────────────

document.addEventListener('DOMContentLoaded', () => {
    loadStats();
    loadMissingCount();
    loadHistory();
    renderBranchChart();

    // Resume polling for any jobs that were already running before page load
    JOB_NAMES.forEach(name => {
        fetch(`/api/job/${name}`)
            .then(r => r.ok ? r.json() : null)
            .then(data => { if (data?.running) startPolling(name); })
            .catch(() => {});
    });

    setInterval(loadStats,        STATS_INTERVAL);
    setInterval(loadMissingCount, MISSING_INTERVAL);
    setInterval(loadHistory,      STATS_INTERVAL);
});

// JOB_NAMES is injected into the page by admin.html as a small inline JSON blob
// so this module doesn't need to hardcode them:
//   <script>const JOB_NAMES = {{ job_names | tojson }};</script>
// That tiny inline script is acceptable — it's data, not logic.
