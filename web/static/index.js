/**
 * static/index.js — Home page JS
 * Stats display, typeahead search, and filter pill toggles.
 */

// ── Stats ──────────────────────────────────────────────────────────────────────

async function loadStats() {
  try {
    const d = await fetch('/api/stats').then(r => r.json());
    const fmt = n => (n != null ? Number(n).toLocaleString() : '—');
    document.getElementById('stat-volumes').textContent = fmt(d.volumes);
    document.getElementById('stat-titles').textContent  = fmt(d.titles);
  } catch {}
}

loadStats();

// ── Typeahead ──────────────────────────────────────────────────────────────────

const input    = document.getElementById('search-input');
const dropdown = document.getElementById('typeahead');
let debounce, activeIdx = -1, lastQuery = '';

function highlight(text, query) {
  if (!query) return text;
  const escaped = query.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  return text.replace(new RegExp(`(${escaped})`, 'gi'), '<em>$1</em>');
}

async function fetchSuggestions(q) {
  if (q.length < 2) { closeDropdown(); return; }
  if (q === lastQuery) return;
  lastQuery = q;
  try {
    const items = await fetch(`/api/suggestions?q=${encodeURIComponent(q)}`).then(r => r.json());
    renderDropdown(items, q);
  } catch {}
}

function renderDropdown(items, q) {
  activeIdx = -1;
  if (!items.length) {
    dropdown.innerHTML = `<div class="ta-empty">No results for "${q}"</div>`;
    dropdown.classList.add('open');
    return;
  }
  dropdown.innerHTML = items.map((item, i) => `
    <div class="ta-item" data-title="${item.title.replace(/"/g, '&quot;')}" data-idx="${i}">
      <span class="ta-title">${highlight(item.title, q)}</span>
      <span class="ta-type">${item.type || ''}</span>
      ${item.score ? `<span class="ta-score">★ ${parseFloat(item.score).toFixed(2)}</span>` : ''}
    </div>`).join('');
  dropdown.classList.add('open');
  dropdown.querySelectorAll('.ta-item').forEach(el =>
    el.addEventListener('mousedown', e => { e.preventDefault(); navigateTo(el.dataset.title); }));
}

function closeDropdown() { dropdown.classList.remove('open'); activeIdx = -1; }

function navigateTo(title) {
  const p = new URLSearchParams({ title });
  const hidden = { type: 'f-type', branch: 'f-branch', avail: 'f-avail', library: 'f-library', no_vol1: 'f-no-vol1' };
  Object.entries(hidden).forEach(([k, id]) => {
    const v = document.getElementById(id)?.value;
    if (v) p.set(k, v);
  });
  window.location.href = '/search?' + p.toString();
}

input.addEventListener('input', () => {
  clearTimeout(debounce);
  debounce = setTimeout(() => fetchSuggestions(input.value.trim()), 220);
});

input.addEventListener('keydown', e => {
  const items = dropdown.querySelectorAll('.ta-item');
  if (!dropdown.classList.contains('open')) return;
  if (e.key === 'ArrowDown') {
    e.preventDefault();
    activeIdx = Math.min(activeIdx + 1, items.length - 1);
    items.forEach((el, i) => el.classList.toggle('active', i === activeIdx));
  } else if (e.key === 'ArrowUp') {
    e.preventDefault();
    activeIdx = Math.max(activeIdx - 1, -1);
    items.forEach((el, i) => el.classList.toggle('active', i === activeIdx));
  } else if (e.key === 'Enter' && activeIdx >= 0) {
    e.preventDefault();
    navigateTo(items[activeIdx].dataset.title);
  } else if (e.key === 'Escape') {
    closeDropdown();
  }
});

document.addEventListener('click', e => {
  if (!e.target.closest('.search-wrap')) closeDropdown();
});

// ── Filter pills ───────────────────────────────────────────────────────────────

document.getElementById('filter-pills').addEventListener('click', e => {
  const pill = e.target.closest('.pill-btn');
  if (!pill || !pill.dataset.group) return;
  e.preventDefault();

  const group = pill.dataset.group;
  const val   = pill.dataset.val;

  if (group === 'no_vol1') {
    const isActive = pill.classList.contains('active');
    pill.classList.toggle('active', !isActive);
    document.getElementById('f-no-vol1').value = isActive ? '' : '1';
    return;
  }

  document.querySelectorAll(`.pill-btn[data-group="${group}"]`).forEach(p => p.classList.remove('active'));
  pill.classList.add('active');
  document.getElementById(group === 'avail' ? 'f-avail' : 'f-library').value = val;
});
