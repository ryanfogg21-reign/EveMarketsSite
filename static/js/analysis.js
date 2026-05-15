'use strict';

let pollTimer = null;

// ── Formatting helpers ────────────────────────────────────────────────────────
function fmtISK(v) {
  if (!v && v !== 0) return '—';
  const a = Math.abs(v);
  const s = v < 0 ? '-' : '';
  if (a >= 1e9) return s + (a / 1e9).toFixed(2) + ' B';
  if (a >= 1e6) return s + (a / 1e6).toFixed(2) + ' M';
  if (a >= 1e3) return s + (a / 1e3).toFixed(1) + ' k';
  return s + a.toFixed(0);
}

function fmtDelta(v) {
  if (v === null || v === undefined) return { text: '—', cls: 'delta-neu' };
  const text = (v >= 0 ? '+' : '') + v.toFixed(1) + '%';
  const cls  = v > 1 ? 'delta-pos' : v < -1 ? 'delta-neg' : 'delta-neu';
  return { text, cls };
}

function fmtSat(v) {
  if (v === null || v === undefined) return { text: '—', cls: 'sat-none' };
  const text = v.toFixed(0) + '%';
  const cls  = v < 50 ? 'sat-low' : v < 150 ? 'sat-mid' : 'sat-high';
  return { text, cls };
}

function escHtml(str) {
  return String(str)
    .replace(/&/g, '&amp;').replace(/</g, '&lt;')
    .replace(/>/g, '&gt;').replace(/"/g, '&quot;');
}

// ── State management ──────────────────────────────────────────────────────────
function showState(name) {
  ['stateIdle', 'stateRunning', 'stateError', 'stateResults'].forEach(id => {
    const el = document.getElementById(id);
    if (el) el.classList.toggle('d-none', id !== name);
  });
}

// ── Tab switching ─────────────────────────────────────────────────────────────
function switchTab(btn) {
  document.querySelectorAll('.rtab').forEach(b => b.classList.remove('active'));
  btn.classList.add('active');
  const section = btn.dataset.section;
  ['strong_buy', 'good_buy', 'hold', 'avoid'].forEach(s => {
    const el = document.getElementById('section-' + s);
    if (el) el.classList.toggle('d-none', s !== section);
  });
}

// ── Card rendering ────────────────────────────────────────────────────────────
function renderItemCard(item, rating) {
  const tid      = item.type_id;
  const iconUrl  = `https://images.evetech.net/types/${tid}/icon`;
  const evRefUrl = `https://everef.net/type/${tid}`;
  const mktUrl   = `https://evemarketer.com/types/${tid}`;

  const delta  = fmtDelta(item.price_mom_pct);
  const sat    = fmtSat(item.saturation_pct);
  const dailyV = item.daily_isk_vol ? fmtISK(item.daily_isk_vol) + '/day' : '—';

  const isCompact  = (rating === 'hold' || rating === 'avoid');
  const ratingCls  = rating.replace('_', '-');

  // Header
  const header = `
    <div class="ic-header">
      <img class="ic-icon" src="${iconUrl}" alt="" onerror="this.style.visibility='hidden'" loading="lazy">
      <div class="ic-title">
        <a class="ic-name" href="${evRefUrl}" target="_blank" rel="noopener">${escHtml(item.name)}</a>
        <div class="ic-meta">${escHtml(item.category || '')}${item.group ? ' · ' + escHtml(item.group) : ''}</div>
      </div>
      <div class="ic-links">
        <a class="ic-link" href="${evRefUrl}" target="_blank" rel="noopener">EVR</a>
        <a class="ic-link" href="${mktUrl}"   target="_blank" rel="noopener">MKT</a>
      </div>
    </div>`;

  // Stats row (only for non-compact cards)
  let stats = '';
  if (!isCompact && item.sell_price !== undefined) {
    stats = `
      <div class="ic-stats">
        <div class="ic-stat">
          <div class="ic-stat-label">Sell</div>
          <div class="ic-stat-value">${fmtISK(item.sell_price)}</div>
        </div>
        <div class="ic-stat">
          <div class="ic-stat-label">Δ vs 7d avg</div>
          <div class="ic-stat-value ${delta.cls}">${delta.text}</div>
        </div>
        <div class="ic-stat">
          <div class="ic-stat-label">Daily Vol</div>
          <div class="ic-stat-value">${dailyV}</div>
        </div>
        <div class="ic-stat">
          <div class="ic-stat-label">Saturation</div>
          <div class="ic-stat-value ${sat.cls}">${sat.text}</div>
        </div>
        <div class="ic-stat">
          <div class="ic-stat-label">7d Avg</div>
          <div class="ic-stat-value">${fmtISK(item.avg_price)}</div>
        </div>
        <div class="ic-stat">
          <div class="ic-stat-label">Used in T2 BPs</div>
          <div class="ic-stat-value">${item.dep_count != null ? item.dep_count : '—'}</div>
        </div>
      </div>`;
  }

  // Parent component usage row (strong_buy / good_buy only)
  let parentRow = '';
  if (!isCompact && item.parents && item.parents.length > 0) {
    const chips = item.parents.map(p => `<span class="ic-parent-chip">${escHtml(p)}</span>`).join('');
    parentRow = `<div class="ic-parents"><span class="ic-parents-label">Used in:</span> ${chips}</div>`;
  }

  // Upside row (strong_buy / good_buy only)
  let upside = '';
  if ((rating === 'strong_buy' || rating === 'good_buy') && item.projected_upside_pct) {
    const confCls  = item.confidence === 'high' ? 'conf-high'
                   : item.confidence === 'medium' ? 'conf-medium' : 'conf-low';
    const confText = (item.confidence || 'medium').toUpperCase();
    upside = `
      <div class="ic-upside">
        <span class="upside-pct ${ratingCls}">↑ +${item.projected_upside_pct}% projected</span>
        <span class="confidence-badge ${confCls}">${confText}</span>
      </div>`;
  } else if (rating === 'avoid') {
    upside = `
      <div class="ic-upside">
        <span class="upside-pct avoid">↓ Overvalued / oversupplied</span>
      </div>`;
  }

  // Reasoning
  const reasoning = item.reasoning
    ? `<div class="ic-reasoning">${escHtml(item.reasoning)}</div>`
    : '';

  const card = document.createElement('div');
  card.className = `item-card ${ratingCls}${isCompact ? ' compact' : ''}`;
  card.innerHTML = header + stats + parentRow + upside + reasoning;
  return card;
}

// ── Result rendering ──────────────────────────────────────────────────────────
function renderResults(result) {
  // Summary
  document.getElementById('marketSummary').textContent = result.market_summary || '';

  // Insights
  const ul = document.getElementById('keyInsights');
  ul.innerHTML = '';
  (result.key_insights || []).forEach(insight => {
    const li = document.createElement('li');
    li.textContent = insight;
    ul.appendChild(li);
  });

  // Rating sections
  const sections = ['strong_buy', 'good_buy', 'hold', 'avoid'];
  sections.forEach(section => {
    const items = result[section] || [];
    document.getElementById('cnt-' + section).textContent = items.length;
    const container = document.getElementById('section-' + section);
    container.innerHTML = '';
    if (items.length === 0) {
      container.innerHTML = '<p class="text-muted py-3" style="font-size:0.8rem">No items in this category.</p>';
    } else {
      items.forEach(item => container.appendChild(renderItemCard(item, section)));
    }
  });

  // Footer
  document.getElementById('generatedAt').textContent =
    result.generated_at ? 'Generated ' + result.generated_at : '';

  // Cache age badge in nav
  if (result.generated_at) {
    document.getElementById('cacheAge').textContent = 'Last run: ' + result.generated_at;
  }

  showState('stateResults');
}

// ── API calls ─────────────────────────────────────────────────────────────────
function triggerAnalysis() {
  const btn = document.getElementById('btnRunAnalysis');
  if (btn) { btn.disabled = true; btn.textContent = '✦ Running…'; }

  showState('stateRunning');

  fetch('/api/analysis/run', { method: 'POST' })
    .then(r => r.json())
    .then(data => {
      if (data.status === 'ready' && data.result) {
        renderResults(data.result);
        if (btn) { btn.disabled = false; btn.textContent = '✦ Run AI Analysis'; }
        return;
      }
      // started or running — begin polling
      pollTimer = setInterval(pollResult, 2500);
    })
    .catch(err => {
      console.error('triggerAnalysis:', err);
      document.getElementById('errorMsg').textContent = err.message;
      showState('stateError');
      if (btn) { btn.disabled = false; btn.textContent = '✦ Run AI Analysis'; }
    });
}

function pollResult() {
  fetch('/api/analysis/result')
    .then(r => r.json())
    .then(data => {
      if (data.status === 'running') {
        const msg = document.getElementById('runningMsg');
        if (msg && data.message) msg.textContent = data.message;
        return;
      }
      clearInterval(pollTimer);
      pollTimer = null;
      const btn = document.getElementById('btnRunAnalysis');
      if (btn) { btn.disabled = false; btn.textContent = '✦ Run AI Analysis'; }

      if (data.status === 'ready' && data.result) {
        renderResults(data.result);
      } else if (data.status === 'error') {
        document.getElementById('errorMsg').textContent = data.message || 'Unknown error';
        showState('stateError');
      } else {
        showState('stateIdle');
      }
    })
    .catch(() => { /* ignore transient errors */ });
}

// ── Boot ──────────────────────────────────────────────────────────────────────
document.addEventListener('DOMContentLoaded', () => {
  // Check for a cached result on page load
  fetch('/api/analysis/result')
    .then(r => r.json())
    .then(data => {
      if (data.status === 'ready' && data.result) {
        renderResults(data.result);
      } else if (data.status === 'running') {
        showState('stateRunning');
        pollTimer = setInterval(pollResult, 2500);
      } else {
        showState('stateIdle');
      }
    })
    .catch(() => showState('stateIdle'));
});
