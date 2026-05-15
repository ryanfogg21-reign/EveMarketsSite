/* EveIndustry&Markets – frontend logic
 *
 * All profit calculations live here so that ME%, SCI%, broker fee, and sales
 * tax changes update the table instantly without a server round-trip.
 *
 * Profit formulas (mirrors what the server documents):
 *
 *  For each material:
 *    eff_qty       = max(1, ceil(base_qty × (1 - ME/100)))
 *    adjusted_total += mat.adjusted_price × eff_qty      ← for manufacturing tax
 *
 *  Manufacturing tax:
 *    mfg_tax = adjusted_total × (SCI / 100)
 *
 *  Material cost – Buy scenario  (buy materials from sell orders, no buyer fee):
 *    mat_cost_buy = Σ(mat.sell_pct × eff_qty) + mfg_tax
 *
 *  Material cost – Opt scenario  (post buy orders for materials, pay broker fee):
 *    mat_cost_opt = Σ(mat.buy_pct × eff_qty × (1 + bfee/100)) + mfg_tax
 *
 *  Revenue – Buy scenario  (fulfil someone's buy order, no broker fee, pay sales tax):
 *    rev_buy = product.buy_max × prod_qty × (1 - stax/100)
 *
 *  Revenue – Opt scenario  (post sell order, pay broker fee + sales tax):
 *    rev_opt = product.sell_pct × prod_qty × (1 - bfee/100 - stax/100)
 *
 *  Revenue – 7d avg Buy  (same structure as Buy but using avg_price):
 *    rev_avg_buy = product.avg_price × prod_qty × (1 - stax/100)
 *
 *  Revenue – 7d avg Opt:
 *    rev_avg_opt = product.avg_price × prod_qty × (1 - bfee/100 - stax/100)
 *
 *  For 7d avg material cost, use avg_price for all materials (no broker fee
 *  distinction since we only have one avg price per item):
 *    mat_cost_avg = Σ(mat.avg_price × eff_qty) + mfg_tax
 */

'use strict';

// ── Column indices (must match <th> order in index.html) ──────────────────────
const COL = {
  product:    0,
  group:      1,
  tech:       2,
  meta:       3,
  buyISK:     4,
  buyPct:     5,
  buyIpH:     6,
  optISK:     7,
  optPct:     8,
  optIpH:     9,
  avgBuyISK:  10,
  avgBuyPct:  11,
  avgBuyIpH:  12,
  avgOptISK:  13,
  avgOptPct:  14,
  avgOptIpH:  15,
  sat:        16,
  links:      17,
};

// Column groups for visibility toggling
const COL_GROUPS = {
  tech:   [COL.tech, COL.meta],
  buy:    [COL.buyISK,    COL.buyPct,    COL.buyIpH],
  opt:    [COL.optISK,    COL.optPct,    COL.optIpH],
  avgbuy: [COL.avgBuyISK, COL.avgBuyPct, COL.avgBuyIpH],
  avgopt: [COL.avgOptISK, COL.avgOptPct, COL.avgOptIpH],
  sat:    [COL.sat],
};

// CSS class applied per column group (left border + text colour)
const COL_CLASSES = {
  [COL.buyISK]:    'th-buy',  [COL.buyPct]:    'th-buy',  [COL.buyIpH]:    'th-buy',
  [COL.optISK]:    'th-opt',  [COL.optPct]:    'th-opt',  [COL.optIpH]:    'th-opt',
  [COL.avgBuyISK]: 'th-avgbuy', [COL.avgBuyPct]: 'th-avgbuy', [COL.avgBuyIpH]: 'th-avgbuy',
  [COL.avgOptISK]: 'th-avgopt', [COL.avgOptPct]: 'th-avgopt', [COL.avgOptIpH]: 'th-avgopt',
  [COL.sat]:       'th-sat',
  [COL.links]:     'th-links',
};

// ── Globals ───────────────────────────────────────────────────────────────────
let dataTable   = null;
let rawData     = [];     // raw items from API (pre-calculation)
let pollTimer   = null;

// ── Settings ──────────────────────────────────────────────────────────────────
function getSettings() {
  return {
    me:   parseFloat(document.getElementById('setME').value)   || 0,
    sci:  parseFloat(document.getElementById('setSCI').value)  || 0,
    stax: parseFloat(document.getElementById('setSTax').value) || 0,
    bfee: parseFloat(document.getElementById('setBFee').value) || 0,
  };
}

// ── Profit calculation (pure JS, runs on every settings change) ───────────────
function calcItemProfits(item, s) {
  const meFactor  = 1 - (s.me   / 100);
  const sciRate   = s.sci  / 100;
  const staxRate  = s.stax / 100;
  const bfeeRate  = s.bfee / 100;
  const pqty      = item.product_qty || 1;
  const durationH = (item.duration_sec || 0) / 3600;

  let matCostBuy  = 0;   // Σ sell_pct × eff_qty
  let matCostOpt  = 0;   // Σ buy_pct  × eff_qty × (1 + bfee)
  let matCostAvg  = 0;   // Σ avg_price × eff_qty
  let adjTotal    = 0;   // Σ adjusted_price × eff_qty  (for manufacturing tax)

  for (const mat of item.materials) {
    const effQty = Math.max(1, Math.ceil(mat.base_qty * meFactor));
    matCostBuy += (mat.sell_pct || 0) * effQty;
    matCostOpt += (mat.buy_pct  || 0) * effQty * (1 + bfeeRate);
    matCostAvg += (mat.avg_price || 0) * effQty;
    adjTotal   += (mat.adjusted_price || 0) * effQty;
  }

  const mfgTax = adjTotal * sciRate;

  const totalBuy = matCostBuy + mfgTax;
  const totalOpt = matCostOpt + mfgTax;
  const totalAvg = matCostAvg + mfgTax;

  // Revenue per manufacturing run (fees applied to product sale)
  const revBuy    = (item.product.buy_max  || 0) * pqty * (1 - staxRate);
  const revOpt    = (item.product.sell_pct || 0) * pqty * (1 - bfeeRate - staxRate);
  const revAvgBuy = (item.product.avg_price || 0) * pqty * (1 - staxRate);
  const revAvgOpt = (item.product.avg_price || 0) * pqty * (1 - bfeeRate - staxRate);

  function row(rev, cost) {
    const isk = rev - cost;
    const pct = cost > 0 ? (isk / cost) * 100 : 0;
    const iph = durationH > 0 ? isk / durationH : 0;
    return { isk, pct, iph };
  }

  return {
    buy:     row(revBuy,    totalBuy),
    opt:     row(revOpt,    totalOpt),
    avg_buy: row(revAvgBuy, totalAvg),
    avg_opt: row(revAvgOpt, totalAvg),
  };
}

// ── Formatting ────────────────────────────────────────────────────────────────
function fmtISK(v) {
  if (v === null || v === undefined) return '—';
  const a = Math.abs(v);
  const s = v < 0 ? '-' : '';
  if (a >= 1e9) return s + (a / 1e9).toFixed(2) + ' B';
  if (a >= 1e6) return s + (a / 1e6).toFixed(2) + ' M';
  if (a >= 1e3) return s + (a / 1e3).toFixed(1) + ' k';
  return s + a.toFixed(0);
}
function fmtPct(v) { return (v === null || v === undefined) ? '—' : v.toFixed(1) + '%'; }
function fmtIpH(v) { return (!v) ? '—' : fmtISK(v) + '/h'; }
function fmtSat(v) {
  if (v === null || v === undefined) return '—';
  return v.toFixed(0) + '%';
}

function profCls(v) {
  if (!v || v === 0) return 'profit-zero';
  return v > 0 ? 'profit-pos' : 'profit-neg';
}

function satCls(v) {
  if (v === null || v === undefined) return '';
  if (v <  50) return 'sat-low';
  if (v < 150) return 'sat-mid';
  return 'sat-high';
}

// ── Row building ──────────────────────────────────────────────────────────────
// Each cell is { display: html, sort: number } — DataTables uses render() to pick.
function makeNumCell(value, fmtFn, extraCls) {
  const cls  = [profCls(value), extraCls || ''].join(' ').trim();
  const html = `<span class="${cls}">${(fmtFn || fmtISK)(value)}</span>`;
  return { display: html, sort: (value === null || value === undefined) ? -Infinity : value };
}

function buildRows(items, settings) {
  return items.map(item => {
    const p = calcItemProfits(item, settings);

    const techLabel = item.tech === 2  ? 'Tech II'
                    : item.tech === 14 ? 'Tech III'
                    : item.tech === 1  ? 'Tech I'
                    : `Meta ${item.tech || 0}`;

    const evRefUrl = `https://everef.net/type/${item.type_id}`;
    const mktUrl   = `https://evemarketer.com/types/${item.type_id}`;
    const iconUrl  = `https://images.evetech.net/types/${item.type_id}/icon`;

    // Build material tooltip (shown on hover)
    const matTip = item.materials.map(m => {
      const me   = settings.me / 100;
      const eff  = Math.max(1, Math.ceil(m.base_qty * (1 - me)));
      return `${m.name} × ${eff.toLocaleString()}`;
    }).join('\n');

    const nameTd = `
      <a href="${evRefUrl}" target="_blank" rel="noopener" title="${escHtml(item.name)}\n\nMaterials:\n${escHtml(matTip)}">
        <img src="${iconUrl}" width="18" height="18"
             style="vertical-align:middle;margin-right:4px;border-radius:2px"
             onerror="this.style.display='none'" loading="lazy">
        ${escHtml(item.name)}
      </a>`;

    const links = [
      `<a href="${evRefUrl}" target="_blank" rel="noopener" title="EveRef">EVR</a>`,
      `<a href="${mktUrl}"   target="_blank" rel="noopener" title="EveMarketer">MKT</a>`,
    ].join('');

    const satHtml = item.saturation !== null && item.saturation !== undefined
      ? `<span class="sat-badge ${satCls(item.saturation)}">${fmtSat(item.saturation)}</span>`
      : '<span class="profit-zero">—</span>';

    return [
      { display: nameTd,                                              sort: item.name },
      { display: escHtml(item.group || '—'),                         sort: item.group || '' },
      { display: `<span class="badge-tech">${techLabel}</span>`,     sort: item.tech  || 0 },
      { display: String(item.meta || 0),                             sort: item.meta  || 0 },
      // Buy scenario
      makeNumCell(p.buy.isk),
      makeNumCell(p.buy.pct, fmtPct),
      makeNumCell(p.buy.iph, fmtIpH),
      // Opt scenario
      makeNumCell(p.opt.isk),
      makeNumCell(p.opt.pct, fmtPct),
      makeNumCell(p.opt.iph, fmtIpH),
      // 7d avg – Buy
      makeNumCell(p.avg_buy.isk),
      makeNumCell(p.avg_buy.pct, fmtPct),
      makeNumCell(p.avg_buy.iph, fmtIpH),
      // 7d avg – Opt
      makeNumCell(p.avg_opt.isk),
      makeNumCell(p.avg_opt.pct, fmtPct),
      makeNumCell(p.avg_opt.iph, fmtIpH),
      // Saturation
      { display: satHtml, sort: item.saturation !== null ? item.saturation : Infinity },
      // Links
      { display: `<span class="link-cell">${links}</span>`, sort: '' },
    ];
  });
}

// ── DataTable initialisation ──────────────────────────────────────────────────
function initTable(items) {
  const settings = getSettings();
  const rows = buildRows(items, settings);

  if (dataTable) {
    dataTable.clear();
    dataTable.rows.add(rows).draw();
    return;
  }

  dataTable = $('#manuTable').DataTable({
    data: rows,
    columns: Object.keys(COL).map(() => ({})),
    order: [[COL.optISK, 'desc']],
    pageLength: 50,
    lengthMenu: [25, 50, 100, 200],
    dom: "<'row mb-2'<'col-sm-6'l><'col-sm-6'f>>" +
         "<'row'<'col-12'tr>>" +
         "<'row mt-2'<'col-sm-5'i><'col-sm-7'p>>",
    language: { search: '', searchPlaceholder: 'Search items…' },
    columnDefs: [
      {
        targets: '_all',
        render(data, type) {
          if (type === 'display') return (data && data.display !== undefined) ? data.display : (data || '');
          if (type === 'sort' || type === 'type') return (data && data.sort !== undefined) ? data.sort : (data || '');
          return (data && data.display !== undefined) ? data.display : (data || '');
        },
      },
    ],
    createdRow(row) {
      const tds = row.querySelectorAll('td');
      Object.entries(COL_CLASSES).forEach(([idx, cls]) => {
        if (tds[idx]) tds[idx].classList.add(cls, 'num');
      });
      if (tds[COL.links]) tds[COL.links].classList.add('th-links');
    },
  });
}

// ── Column visibility ─────────────────────────────────────────────────────────
function toggleColumnGroup(group) {
  if (!dataTable) return;
  const cols = COL_GROUPS[group];
  if (!cols) return;
  // Derive the checkbox ID from group name
  const chkId = 'chkHide' + group.charAt(0).toUpperCase() + group.slice(1);
  const chk = document.getElementById(chkId);
  const hidden = chk ? chk.checked : false;
  cols.forEach(c => dataTable.column(c).visible(!hidden));
}

// ── Settings change ───────────────────────────────────────────────────────────
function onSettingChange() {
  rebuildTable();
}

function rebuildTable() {
  if (!dataTable || !rawData.length) return;
  const settings  = getSettings();
  const filtered  = getFilteredItems();
  const rows      = buildRows(filtered, settings);
  dataTable.clear();
  dataTable.rows.add(rows).draw();
}

// ── Filters ───────────────────────────────────────────────────────────────────
function getFilteredItems() {
  const minISK = parseFloat(document.getElementById('fltMinISK').value);
  const minPct = parseFloat(document.getElementById('fltMinPct').value);
  const minIpH = parseFloat(document.getElementById('fltMinIpH').value);
  const maxSat = parseFloat(document.getElementById('fltMaxSat').value);
  const mode   = document.getElementById('fltMode').value;   // buy | opt | avg_buy | avg_opt
  const s      = getSettings();

  return rawData.filter(item => {
    const p = calcItemProfits(item, s);
    const bucket = p[mode] || p.buy;
    if (!isNaN(minISK) && bucket.isk < minISK) return false;
    if (!isNaN(minPct) && bucket.pct < minPct) return false;
    if (!isNaN(minIpH) && bucket.iph < minIpH) return false;
    if (!isNaN(maxSat) && item.saturation !== null && item.saturation > maxSat) return false;
    return true;
  });
}

function applyFilters() { rebuildTable(); }

function clearFilters() {
  ['fltMinISK', 'fltMinPct', 'fltMinIpH', 'fltMaxSat'].forEach(id => {
    document.getElementById(id).value = '';
  });
  document.getElementById('fltMode').value = 'buy';
  rebuildTable();
}

// ── API ───────────────────────────────────────────────────────────────────────
function fetchComponents() {
  fetch('/api/components')
    .then(r => { if (!r.ok) throw new Error(`HTTP ${r.status}`); return r.json(); })
    .then(data => {
      rawData = data;
      document.getElementById('loadingState').classList.add('d-none');
      document.getElementById('tableSection').classList.remove('d-none');
      initTable(data);
      document.getElementById('lastUpdated').textContent =
        'Last updated: ' + new Date().toLocaleTimeString();
      setEsiStatus('ok', `ESI available · ${data.length} items`);
    })
    .catch(err => {
      console.error('fetchComponents:', err);
      setEsiStatus('err', 'Data error – see console');
    });
}

function pollStatus() {
  fetch('/api/status')
    .then(r => r.json())
    .then(s => {
      document.getElementById('loadingMsg').textContent = s.message || 'Loading…';
      if (s.status === 'ready') {
        clearInterval(pollTimer);
        pollTimer = null;
        fetchComponents();
      } else if (s.status === 'error') {
        clearInterval(pollTimer);
        pollTimer = null;
        document.getElementById('loadingMsg').textContent = '⚠ Error: ' + s.message;
        setEsiStatus('err', 'Init error');
      }
    })
    .catch(() => { /* server still starting */ });
}

function triggerRefresh() {
  const btn = document.getElementById('btnRefresh');
  btn.disabled = true;
  btn.textContent = '↻ Refreshing…';
  fetch('/api/refresh', { method: 'POST' })
    .then(r => r.json())
    .then(() => {
      // Give the background thread a moment, then re-fetch
      setTimeout(() => {
        fetchComponents();
        btn.disabled = false;
        btn.textContent = '↻ Refresh Prices';
      }, 5000);
    })
    .catch(() => {
      btn.disabled = false;
      btn.textContent = '↻ Refresh Prices';
    });
}

// ── Helpers ───────────────────────────────────────────────────────────────────
function setEsiStatus(state, msg) {
  const el = document.getElementById('esiStatus');
  el.textContent = msg;
  el.className = 'esi-badge ' + (state || '');
}

function escHtml(str) {
  return String(str)
    .replace(/&/g, '&amp;').replace(/</g, '&lt;')
    .replace(/>/g, '&gt;').replace(/"/g, '&quot;');
}

// ── Boot ──────────────────────────────────────────────────────────────────────
document.addEventListener('DOMContentLoaded', () => {
  pollTimer = setInterval(pollStatus, 2000);
  pollStatus();
});
