/* admin.js — Manga Tracker admin panel JS */

// ── CSRF helper ────────────────────────────────────────────────
function getCsrf() {
  return document.querySelector('meta[name="csrf-token"]')?.content || '';
}

// ── Utility ────────────────────────────────────────────────────
function toast(msg, type = '') {
  const el = Object.assign(document.createElement('div'),
    { className: 'toast ' + type, textContent: msg });
  document.getElementById('toasts').appendChild(el);
  setTimeout(() => el.remove(), 5000);
}

function fmtDate(isoStr) {
  if (!isoStr) return null;
  const s = isoStr.trim();
  const d = new Date(s.includes('T') ? s : s.replace(' ', 'T'));
  if (isNaN(d)) return isoStr.slice(0, 16).replace('T', ' ');
  return d.toLocaleDateString('en-US', { month: 'short', day: 'numeric' })
       + ' at ' + d.toLocaleTimeString('en-US', { hour: 'numeric', minute: '2-digit' });
}

function daysSince(isoStr) {
  if (!isoStr) return Infinity;
  const d = new Date(isoStr.includes('T') ? isoStr : isoStr.replace(' ', 'T'));
  return (Date.now() - d) / (1000 * 60 * 60 * 24);
}

// ── Stats & status strip ───────────────────────────────────────
async function loadStats() {
  try {
    const d = await fetch('/api/stats').then(r => r.json());
    document.getElementById('stat-volumes').textContent = d.volumes ?? '—';
    document.getElementById('stat-titles').textContent  = d.titles  ?? '—';
    document.getElementById('admin-status').textContent =
      d.last_scraped ? '漫画追跡システム — ' + d.last_scraped : '漫画追跡システム — never scraped';
    if (d.titles) {
      document.getElementById('hint-db-titles').textContent = `${d.titles} titles currently in DB`;
      document.getElementById('ssc-titles').textContent     = d.titles;
      document.getElementById('ssc-titles-sub').textContent = `${d.volumes} volume entries`;
    }
  } catch {}
}

async function loadMissingCount() {
  try {
    const d = await fetch('/api/missing_titles').then(r => r.json());

    const lcplScraped    = (d.total_titles || 0) - (d.count || 0);
    const browardScraped = (d.total_titles || 0) - (d.broward_count || 0);

    document.getElementById('ssc-lcpl-volumes').textContent    = lcplScraped;
    document.getElementById('ssc-lcpl-missing').textContent    = d.count > 0 ? `${d.count} titles missing` : 'All titles scraped';
    document.getElementById('ssc-broward-volumes').textContent = browardScraped;
    document.getElementById('ssc-broward-missing').textContent = d.broward_count > 0 ? `${d.broward_count} titles missing` : 'All titles scraped';

    document.getElementById('lcpl-scrape-hint').innerHTML =
      d.count > 0 ? `<b>${d.count} titles</b> missing LCPL data` : 'All titles have LCPL data';
    document.getElementById('broward-scrape-hint').innerHTML =
      d.broward_count > 0 ? `<b>${d.broward_count} titles</b> missing Broward data` : 'All titles have Broward data';

    updateStaleIndicators();
  } catch {}
}

async function updateStaleIndicators() {
  try {
    const history = await fetch('/api/job_history').then(r => r.json());
    const lastScrape  = history.find(h => h.job === 'scrape'         && h.status === 'done');
    const lastBroward = history.find(h => h.job === 'scrape_broward' && h.status === 'done');

    function applyStale(job, staleElId, hintElId) {
      const staleEl = document.getElementById(staleElId);
      const hintEl  = document.getElementById(hintElId);
      if (hintEl) hintEl.querySelectorAll('.stale-appended').forEach(el => el.remove());

      if (!job) {
        staleEl.style.display = 'block';
        staleEl.textContent   = 'Never scraped';
        return;
      }
      const age   = daysSince(job.at);
      const label = 'Last scraped ' + fmtDate(job.at);
      if (hintEl) {
        const span = document.createElement('span');
        span.className   = 'stale-appended ' + (age > 3 ? 'stale' : '');
        span.textContent = ' · ' + label;
        hintEl.appendChild(span);
      }
      if (age > 7) {
        staleEl.style.display = 'block';
        staleEl.textContent   = `⚠ Scraped ${Math.floor(age)} days ago`;
      }
    }

    applyStale(lastScrape,  'ssc-lcpl-stale',    'lcpl-scrape-hint');
    applyStale(lastBroward, 'ssc-broward-stale', 'broward-scrape-hint');
  } catch {}
}

loadStats();
loadMissingCount();

// ── Reset ──────────────────────────────────────────────────────
function checkResetConfirm() {
  document.getElementById('btn-reset').disabled =
    document.getElementById('reset-confirm').value !== 'RESET';
}

async function doReset() {
  const btn      = document.getElementById('btn-reset');
  const resultEl = document.getElementById('reset-result');
  btn.disabled        = true;
  resultEl.textContent = 'Resetting…';
  resultEl.style.color = 'var(--text3)';
  try {
    const form = new FormData();
    form.append('csrf_token', getCsrf());
    const d = await fetch('/admin/reset', { method: 'POST', body: form }).then(r => r.json());
    resultEl.style.color = d.ok ? 'var(--green)' : 'var(--red)';
    resultEl.textContent = (d.messages || ['Reset failed']).join(' · ');
    if (d.ok) {
      document.getElementById('reset-confirm').value = '';
      loadStats();
      loadMissingCount();
      toast('Database reset complete', 'ok');
      refreshHistory();
    }
  } catch(e) {
    resultEl.style.color = 'var(--red)';
    resultEl.textContent = 'Error: ' + e.message;
  }
}

// ── Clear title availability ───────────────────────────────────
let _lastClearedTitle   = null;
let _lastClearedMangaID = null;

async function deleteTitleResultsConfirmed() {
  const title   = document.getElementById('del-results-title').value.trim();
  const library = document.getElementById('del-results-lib').value;
  if (!title) return;
  if (!confirm(`Clear all availability data for "${title}"?`)) return;

  const msgEl  = document.getElementById('del-results-msg');
  const undoEl = document.getElementById('del-results-undo');
  msgEl.style.color = 'var(--text3)';
  msgEl.textContent = 'Clearing…';
  undoEl.classList.remove('visible');

  try {
    const d = await fetch('/api/delete_title_results', {
      method:  'POST',
      headers: { 'Content-Type': 'application/json', 'X-CSRF-Token': getCsrf() },
      body:    JSON.stringify({ title, library: library || null }),
    }).then(r => r.json());

    msgEl.style.color = d.ok ? 'var(--green)' : 'var(--red)';
    msgEl.textContent = d.message;

    if (d.ok) {
      _lastClearedTitle   = title;
      _lastClearedMangaID = d.manga_id;
      document.getElementById('del-results-title-saved').textContent = title;
      undoEl.classList.add('visible');
      loadStats();
      loadMissingCount();
      setTimeout(() => undoEl.classList.remove('visible'), 15000);
    }
  } catch(e) {
    msgEl.style.color = 'var(--red)';
    msgEl.textContent = 'Error: ' + e.message;
  }
}

async function undoClear() {
  if (!_lastClearedTitle || !_lastClearedMangaID) return;
  const title   = _lastClearedTitle;
  const mangaID = _lastClearedMangaID;
  document.getElementById('del-results-undo').classList.remove('visible');
  await rescrapeById(mangaID, title, 'scrape');
  await rescrapeById(mangaID, title, 'scrape_broward');
  _lastClearedTitle   = null;
  _lastClearedMangaID = null;
}

// ── Batch search + re-scrape ───────────────────────────────────
let batchDebounce;
async function batchSearch(q) {
  clearTimeout(batchDebounce);
  batchDebounce = setTimeout(async () => {
    const el = document.getElementById('batch-results');
    if (q.length < 2) {
      el.innerHTML = '<div style="font-family:var(--mono);font-size:.7rem;color:var(--text3);padding:.6rem .75rem">Type to search</div>';
      return;
    }
    try {
      const items = await fetch(`/api/suggestions?q=${encodeURIComponent(q)}`).then(r => r.json());
      el.innerHTML = items.length
        ? items.map(item => {
            const safeTitle = item.title.replace(/'/g, "\\'").replace(/"/g, '&quot;');
            return `
            <div class="batch-title-row">
              <span class="batch-title-name">${item.title}</span>
              <span class="batch-title-meta">${item.type || ''} ${item.score ? '· ★ ' + parseFloat(item.score).toFixed(2) : ''}</span>
              <button onclick="clearBatchTitle(${item.manga_id}, '${safeTitle}')" class="danger" style="font-size:.6rem;padding:2px 7px;margin-left:6px">Clear</button>
              <button onclick="rescrapeById(${item.manga_id}, '${safeTitle}', 'scrape')" class="btn-rescrape">↺ LCPL</button>
              <button onclick="rescrapeById(${item.manga_id}, '${safeTitle}', 'scrape_broward')" class="btn-rescrape btn-rescrape-broward">↺ BCL</button>
            </div>`;
          }).join('')
        : '<div style="font-family:var(--mono);font-size:.7rem;color:var(--text3);padding:.6rem .75rem">No results</div>';
    } catch {}
  }, 250);
}

async function clearBatchTitle(mangaID, title) {
  if (!confirm(`Clear all availability data for "${title}"?`)) return;
  try {
    const d = await fetch('/api/delete_title_results', {
      method:  'POST',
      headers: { 'Content-Type': 'application/json', 'X-CSRF-Token': getCsrf() },
      body:    JSON.stringify({ manga_id: mangaID }),
    }).then(r => r.json());
    toast(d.message, d.ok ? 'ok' : 'err');
    if (d.ok) { loadStats(); loadMissingCount(); }
  } catch(e) { toast('Error: ' + e.message, 'err'); }
}

async function rescrapeById(mangaID, title, action = 'scrape') {
  try {
    if (pollers[action]) { toast(`A ${action.replace(/_/g, ' ')} is already running`, 'err'); return; }

    const form = new FormData();
    form.append('action',     action);
    form.append('manga_id',   mangaID);
    form.append('csrf_token', getCsrf());

    const btn     = document.getElementById('btn-' + action);
    const stopBtn = document.getElementById('stop-' + action);
    if (btn) btn.disabled = true;
    if (stopBtn) stopBtn.style.display = 'inline-flex';

    const resp = await fetch('/admin', { method: 'POST', body: form }).then(r => r.json());
    if (!resp.ok) {
      toast(resp.message, 'err');
      if (btn) btn.disabled = false;
      if (stopBtn) stopBtn.style.display = 'none';
      return;
    }
    toast(`Re-scraping "${title}" [${action === 'scrape_broward' ? 'Broward' : 'LCPL'}]…`, '');
    showJob(action, 0, 'starting…');
    updateStickyFooter(action, 0, 'starting…', true);
    pollers[action] = setInterval(() => pollJob(action, btn, stopBtn), POLL_MS);
  } catch(e) { toast('Re-scrape failed: ' + e.message, 'err'); }
}

// ── Job history ────────────────────────────────────────────────
async function refreshHistory() {
  try {
    const items = await fetch('/api/job_history').then(r => r.json());
    const list  = document.getElementById('history-list');
    list.innerHTML = items.length
      ? items.map(h => `
          <div class="history-entry ${h.status === 'error' ? 'history-err' : 'history-ok'}">
            <span class="history-icon">${h.status === 'error' ? '✗' : '✓'}</span>
            <span class="history-job">${h.job}</span>
            <span class="history-msg" title="${h.message}">${h.message}</span>
            <span class="history-time">${fmtDate(h.at) || h.at.slice(0,16).replace('T',' ')}</span>
          </div>`).join('')
      : '<div style="font-family:var(--mono);font-size:.72rem;color:var(--text3);padding:.5rem 0">No history yet.</div>';
    list.scrollTop = 0;
  } catch {}
}

// ── Sticky footer ──────────────────────────────────────────────
const stickyFooter = document.getElementById('sticky-footer');
let activeJobName  = null;

function updateStickyFooter(jobName, pct, msg, running) {
  if (!running && activeJobName === jobName) {
    stickyFooter.classList.remove('visible');
    activeJobName = null;
    return;
  }
  if (running) {
    activeJobName = jobName;
    stickyFooter.classList.add('visible');
    document.getElementById('sf-job-name').textContent = jobName.toUpperCase().replace(/_/g, ' ');
    document.getElementById('sf-bar').value  = pct;
    document.getElementById('sf-pct').textContent = pct + '%';
    document.getElementById('sf-msg').textContent  = msg;
    document.getElementById('sf-stop-btn').onclick = () => stopJob(jobName);
  }
}

// ── Job system ─────────────────────────────────────────────────
const POLL_MS  = 1000;
const pollers  = {};

function startJob(evt, jobName) {
  evt.preventDefault();
  const btn     = document.getElementById('btn-' + jobName);
  const stopBtn = document.getElementById('stop-' + jobName);
  if (pollers[jobName]) return false;
  btn.disabled = true;
  if (stopBtn) stopBtn.style.display = 'inline-flex';
  fetch('/admin', { method: 'POST', body: new FormData(evt.target) })
    .then(r => {
      if (r.status === 401 || r.status === 302) throw new Error('session_expired');
      return r.json();
    })
    .then(d => {
      if (!d.ok) {
        toast(d.message, 'err');
        btn.disabled = false;
        if (stopBtn) stopBtn.style.display = 'none';
        return;
      }
      showJob(jobName, 0, 'starting…');
      updateStickyFooter(jobName, 0, 'starting…', true);
      pollers[jobName] = setInterval(() => pollJob(jobName, btn, stopBtn), POLL_MS);
    })
    .catch(e => {
      if (e.message === 'session_expired') {
        document.getElementById('session-' + jobName)?.classList.add('visible');
      } else {
        toast('Failed: ' + e.message, 'err');
      }
      btn.disabled = false;
      if (stopBtn) stopBtn.style.display = 'none';
    });
  return false;
}

function stopJob(jobName) {
  fetch('/api/job/stop/' + jobName, { method: 'POST' })
    .then(r => r.json())
    .then(d => toast(d.message, d.ok ? '' : 'err'))
    .catch(() => {});
}

function pollJob(jobName, btn, stopBtn) {
  fetch('/api/job/' + jobName)
    .then(r => {
      if (r.status === 401) throw new Error('session_expired');
      return r.json();
    })
    .then(d => {
      if (!d || !Object.keys(d).length) return;
      const isErr = (d.message || '').startsWith('error');
      showJob(jobName, d.progress || 0, d.message || '', isErr);
      updateStickyFooter(jobName, d.progress || 0, d.message || '', d.running);
      if (!d.running) {
        clearInterval(pollers[jobName]);
        delete pollers[jobName];
        if (btn)     btn.disabled = false;
        if (stopBtn) stopBtn.style.display = 'none';
        if (!isErr) {
          toast(jobName.replace(/_/g,' ') + ' complete', 'ok');
          loadStats();
          loadMissingCount();
        } else {
          toast('Error in ' + jobName + ': ' + d.message, 'err');
        }
        setTimeout(() => { hideJob(jobName); refreshHistory(); }, 3000);
      }
    })
    .catch(e => {
      if (e.message === 'session_expired') {
        clearInterval(pollers[jobName]);
        delete pollers[jobName];
        document.getElementById('session-' + jobName)?.classList.add('visible');
        if (btn)     btn.disabled = false;
        if (stopBtn) stopBtn.style.display = 'none';
      }
    });
}

function showJob(jobName, pct, msg, isError = false) {
  const box = document.getElementById('job-' + jobName);
  if (!box) return;
  box.classList.add('visible');
  box.classList.toggle('error', isError);
  box.classList.toggle('done', !isError && pct >= 100);
  document.getElementById('job-' + jobName + '-bar').value = pct;
  document.getElementById('job-' + jobName + '-pct').textContent = pct + '%';
  const m = document.getElementById('job-' + jobName + '-msg');
  m.textContent = msg;
  m.title       = msg;
}

function hideJob(jobName) {
  document.getElementById('job-' + jobName)?.classList.remove('visible','done','error','busy');
}

// ── Restore running jobs on load ───────────────────────────────
['get_manga','scrape','scrape_broward'].forEach(j => {
  fetch('/api/job/' + j)
    .then(r => {
      if (r.status === 401) throw new Error('session_expired');
      return r.json();
    })
    .then(d => {
      if (d && d.running) {
        const btn     = document.getElementById('btn-' + j);
        const stopBtn = document.getElementById('stop-' + j);
        showJob(j, d.progress || 0, d.message || '');
        updateStickyFooter(j, d.progress || 0, d.message || '', true);
        if (btn)     btn.disabled = true;
        if (stopBtn) stopBtn.style.display = 'inline-flex';
        pollers[j] = setInterval(() => pollJob(j, btn, stopBtn), POLL_MS);
      }
    })
    .catch(e => {
      if (e.message === 'session_expired')
        document.getElementById('session-' + j)?.classList.add('visible');
    });
});
