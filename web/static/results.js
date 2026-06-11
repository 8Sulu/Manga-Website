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

// ── Sort ───────────────────────────────────────────────────────
// State: { key, dir } — dir true = ascending
const SORT_STORAGE_KEY = 'manga_sort';

let sortState = (() => {
  try {
    const saved = sessionStorage.getItem(SORT_STORAGE_KEY);
    return saved ? JSON.parse(saved) : { key: 'score', dir: false };
  } catch { return { key: 'score', dir: false }; }
})();

function _applySort(key, dir) {
  const grid  = document.getElementById('results-grid');
  if (!grid) return;
  const cards = [...grid.querySelectorAll('.manga-card')];
  cards.sort((a, b) => {
    const va = a.dataset[key], vb = b.dataset[key];
    const na = parseFloat(va),  nb = parseFloat(vb);
    const d  = dir ? 1 : -1;
    if (!isNaN(na) && !isNaN(nb)) return (na - nb) * d;
    return (va < vb ? -1 : va > vb ? 1 : 0) * d;
  });
  cards.forEach(c => grid.appendChild(c));
}

function _highlightSortBtn(key) {
  document.querySelectorAll('.sort-btn').forEach(b => b.classList.remove('active'));
  const btn = document.getElementById('sort-' + key);
  if (btn) btn.classList.add('active');
}

function sortCards(key, btn) {
  // Toggle direction if same key, else default to descending (false)
  if (sortState.key === key) {
    sortState.dir = !sortState.dir;
  } else {
    sortState = { key, dir: false };
  }
  try { sessionStorage.setItem(SORT_STORAGE_KEY, JSON.stringify(sortState)); } catch {}
  _highlightSortBtn(key);
  _applySort(key, sortState.dir);
}

// Restore sort on load
_highlightSortBtn(sortState.key);
_applySort(sortState.key, sortState.dir);

// ── Filter chip removal ────────────────────────────────────────
function removeFilter(key) {
  const url = new URL(window.location.href);
  url.searchParams.delete(key);
  url.searchParams.delete('page');
  window.location.href = url.toString();
}

// ── MAL — load ─────────────────────────────────────────────────
async function loadMalList() {
  const loadBtn = document.getElementById('mal-load-btn');
  const loading = document.getElementById('mal-loading');
  const errorEl = document.getElementById('mal-error');

  loadBtn.style.display = 'none';
  loading.style.display = 'inline';
  errorEl.style.display = 'none';

  try {
    const resp = await fetch('/api/mal/mangalist');
    const json = await resp.json();
    if (!json.ok) throw new Error(json.message || 'Failed to load MAL list');

    const setResp = await fetch('/api/mal/set_filter', {
      method:  'POST',
      headers: { 'Content-Type': 'application/json' },
      body:    JSON.stringify({
        data:    json.data,
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
