/* results.js — Manga Tracker search results JS */

// ── MAL panel toggle ───────────────────────────────────────────
function toggleMalPanel() {
  document.getElementById('mal-panel').classList.toggle('open');
}

// ── Library filter pills ───────────────────────────────────────
document.getElementById('lib-filter-row').addEventListener('click', e => {
  const pill = e.target.closest('.lib-filter-pill');
  if (!pill) return;
  e.preventDefault();
  const url = new URL(window.location.href);
  const val = pill.dataset.val;
  if (val) url.searchParams.set('library', val);
  else     url.searchParams.delete('library');
  url.searchParams.delete('page');
  window.location.href = url.toString();
});

// ── Tab switching ──────────────────────────────────────────────
function switchLibTab(btn, panelId) {
  const card = btn.closest('.manga-card');
  card.querySelectorAll('.lib-tab').forEach(t => t.className = t.className.replace(/\s*active-\S+/g, ''));
  card.querySelectorAll('.lib-panel').forEach(p => p.classList.remove('visible'));
  btn.classList.add(btn.textContent.includes('Broward') ? 'active-broward' : 'active-lcpl');
  document.getElementById(panelId)?.classList.add('visible');
}

// ── Card expand/collapse ───────────────────────────────────────
function toggleCard(card) {
  if (card.classList.contains('open')) { closeCard(card); return; }
  document.querySelectorAll('.manga-card.open').forEach(closeCard);
  card.classList.add('open');
  const [compact, expanded] = card.querySelectorAll('.manga-body');
  compact.style.display        = 'none';
  expanded.style.display       = 'flex';
  expanded.style.flexDirection = 'column';
  card.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
}

function closeCard(card) {
  card.classList.remove('open');
  const [compact, expanded] = card.querySelectorAll('.manga-body');
  compact.style.display  = '';
  expanded.style.display = 'none';
}

// ── Sort — server-side, persisted in URL (works across pages) ─────────────
// Sort state is carried in the URL as ?sort=<key>&sort_dir=<asc|desc>
// so every page navigation preserves the chosen sort order.

function sortCards(key) {
  const url    = new URL(window.location.href);
  const curKey = url.searchParams.get('sort')     || 'score';
  const curDir = url.searchParams.get('sort_dir') || 'desc';

  // Toggle direction when clicking the active sort key, else default descending
  let newDir = 'desc';
  if (key === curKey) {
    newDir = curDir === 'desc' ? 'asc' : 'desc';
  }

  url.searchParams.set('sort',     key);
  url.searchParams.set('sort_dir', newDir);
  url.searchParams.delete('page');   // reset to page 1 on sort change
  window.location.href = url.toString();
}

// Highlight the active sort button on load
(function highlightActiveSort() {
  const url    = new URL(window.location.href);
  const curKey = url.searchParams.get('sort') || 'score';
  document.querySelectorAll('.sort-btn').forEach(b => b.classList.remove('active'));
  const btn = document.getElementById('sort-' + curKey);
  if (btn) btn.classList.add('active');
})();

// ── Filter chip removal ────────────────────────────────────────
function removeFilter(key) {
  const url = new URL(window.location.href);
  url.searchParams.delete(key);
  url.searchParams.delete('page');
  window.location.href = url.toString();
}

// ── MAL — load (async poll, #15) ──────────────────────────────────────────
// POST to /api/mal/mangalist starts a background job; poll status until done.

async function loadMalList() {
  const loadBtn = document.getElementById('mal-load-btn');
  const loading = document.getElementById('mal-loading');
  const errorEl = document.getElementById('mal-error');

  loadBtn.style.display = 'none';
  loading.style.display = 'inline';
  errorEl.style.display = 'none';

  try {
    // Start background job
    const startResp = await fetch('/api/mal/mangalist');
    const startJson = await startResp.json();
    if (!startJson.ok) throw new Error(startJson.message || 'Failed to start MAL fetch');

    const jobId = startJson.job_id;

    // Poll until done or error
    const data = await _pollMalJob(jobId);

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
  } catch(e) {
    loading.style.display = 'none';
    errorEl.textContent   = '⚠ ' + e.message;
    errorEl.style.display = 'inline';
    loadBtn.style.display = 'inline';
  }
}

async function _pollMalJob(jobId) {
  const MAX_ATTEMPTS = 60;   // 60 × 2s = 2 min max
  const POLL_INTERVAL = 2000;

  for (let i = 0; i < MAX_ATTEMPTS; i++) {
    await new Promise(r => setTimeout(r, POLL_INTERVAL));
    const resp = await fetch(`/api/mal/mangalist/status/${jobId}`);
    const json = await resp.json();

    if (!json.ok && json.status !== 'running') {
      throw new Error(json.message || 'MAL fetch failed');
    }
    if (json.status === 'done') {
      return json.data;
    }
    // still running — update loading text
    const loading = document.getElementById('mal-loading');
    if (loading) loading.textContent = `Loading MAL list… (${(i + 1) * 2}s)`;
  }
  throw new Error('MAL list fetch timed out');
}

// ── MAL — toggle pill ──────────────────────────────────────────
function toggleMalStatus(status, el) {
  const states = ['', 'include', 'exclude'];
  const icons  = { '': '◦', 'include': '✓', 'exclude': '✕' };
  const next   = states[(states.indexOf(el.dataset.state || '') + 1) % 3];

  el.dataset.state = next;
  el.querySelector('.mal-status-icon').textContent = icons[next];
  el.classList.toggle('pending', next !== (el.dataset.applied || ''));

  const anyPending = [...document.querySelectorAll('.mal-status-item')]
    .some(p => (p.dataset.state || '') !== (p.dataset.applied || ''));
  document.getElementById('mal-apply-btn')?.classList.toggle('visible', anyPending);
}

// ── MAL — apply ────────────────────────────────────────────────
async function applyMalFilters() {
  const applyBtn = document.getElementById('mal-apply-btn');
  if (applyBtn) applyBtn.classList.add('busy');

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
  } catch(e) {
    if (applyBtn) applyBtn.classList.remove('busy');
    console.error('MAL apply failed:', e);
  }
}

// ── MAL — clear ────────────────────────────────────────────────
async function clearMalFilter() {
  try {
    await fetch('/api/mal/clear_filter', { method: 'POST' });
    const url = new URL(window.location.href);
    url.searchParams.delete('page');
    window.location.href = url.toString();
  } catch(e) {
    console.error('MAL clear failed:', e);
  }
}

// Snapshot applied state on load
document.querySelectorAll('.mal-status-item[data-status]').forEach(p => {
  p.dataset.applied = p.dataset.state || '';
});
