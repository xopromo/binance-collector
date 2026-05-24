/**
 * dashboard.js — Trading Strategy Dashboard
 */

import { renderPnlChart, STRATEGY_COLORS } from './charts.js';

const REFRESH_INTERVAL = 60;
const BASE_PATH = '';

let signalsData = null;
let tradesData  = null;
let logsData    = [];
let strategies  = {};
let activeTab   = 'all';
let secondsLeft = REFRESH_INTERVAL;

// Persisted user preferences (localStorage)
function getPref(key, def) {
  try { const v = localStorage.getItem('td_' + key); return v !== null ? JSON.parse(v) : def; }
  catch (_) { return def; }
}
function setPref(key, val) {
  try { localStorage.setItem('td_' + key, JSON.stringify(val)); } catch (_) {}
}

let selectedSymbol    = getPref('symbol', '');
let selectedTimeframe = getPref('tf', '1');

// ── Bootstrap ─────────────────────────────────────────────────────────────────
document.addEventListener('DOMContentLoaded', async () => {
  await loadData();
  renderAll();
  startRefreshLoop();
  setupControls();
  setupNewStrategyForm();
  initTradingViewChart();
});

// ── Data Loading ──────────────────────────────────────────────────────────────
async function loadData() {
  const [sigRes, trdRes, logRes] = await Promise.allSettled([
    fetch(`${BASE_PATH}signals/latest.json?v=${Date.now()}`),
    fetch(`${BASE_PATH}trades/log.json?v=${Date.now()}`),
    fetch(`${BASE_PATH}logs/analysis.log.json?v=${Date.now()}`),
  ]);

  if (sigRes.status === 'fulfilled' && sigRes.value.ok) signalsData = await sigRes.value.json();
  if (trdRes.status === 'fulfilled' && trdRes.value.ok) tradesData  = await trdRes.value.json();
  if (logRes.status === 'fulfilled' && logRes.value.ok) {
    const raw = await logRes.value.json();
    logsData = Array.isArray(raw) ? raw : [];
  }

  // Build merged strategies index
  strategies = {};
  if (tradesData?.strategies) {
    Object.entries(tradesData.strategies).forEach(([id, s]) => {
      strategies[id] = { id, ...s };
    });
  }
  if (signalsData?.strategies) {
    Object.entries(signalsData.strategies).forEach(([id, sig]) => {
      if (!strategies[id]) strategies[id] = { id };
      strategies[id].currentSignal = sig;
      // Copy symbol/timeframe from signal data if missing
      if (!strategies[id].symbol && sig.symbol) strategies[id].symbol = sig.symbol;
    });
  }

  updateLastUpdated(signalsData?.updated_at);
}

// ── Render All ────────────────────────────────────────────────────────────────
function renderAll() {
  renderTabs();
  renderView();
  renderLogs();
}

function renderView() {
  if (activeTab === 'all') renderAllView();
  else renderSingleView(activeTab);
  renderChart();
}

// ── Tabs ──────────────────────────────────────────────────────────────────────
function renderTabs() {
  const el = document.getElementById('strategyTabs');
  if (!el) return;
  const ids = Object.keys(strategies);
  let html = `<button class="tab all ${activeTab === 'all' ? 'active' : ''}" data-id="all">
    <span class="tab-dot" style="background:linear-gradient(135deg,#bc8cff,#58a6ff)"></span>
    Все стратегии
  </button>`;
  ids.forEach((id, i) => {
    const s = strategies[id];
    const color = STRATEGY_COLORS[i % STRATEGY_COLORS.length].line;
    const sig = s.currentSignal?.signal || 'NO_SIGNAL';
    const sc = sig === 'LONG' ? '#3fb950' : sig === 'SHORT' ? '#f85149' : '#484f58';
    html += `<button class="tab ${activeTab === id ? 'active' : ''}" data-id="${id}">
      <span class="tab-dot" style="background:${color}"></span>
      ${s.name || id}
      <span style="font-size:0.7rem;color:${sc};font-weight:700">${sig}</span>
    </button>`;
  });
  el.innerHTML = html;
  el.querySelectorAll('.tab').forEach(btn => {
    btn.addEventListener('click', () => { activeTab = btn.dataset.id; renderTabs(); renderView(); renderLogs(); });
  });
}

// ── All Strategies View ───────────────────────────────────────────────────────
function renderAllView() {
  document.getElementById('allSignalsSection').style.display = '';
  document.getElementById('signalSection').style.display  = 'none';
  document.getElementById('statsSection').style.display   = 'none';
  document.getElementById('historySection').style.display = 'none';
  document.getElementById('configSection').style.display  = 'none';
  renderMiniSignals();
}

function renderMiniSignals() {
  const container = document.getElementById('miniSignalsGrid');
  if (!container) return;

  let ids = Object.keys(strategies);

  // Filter by symbol — but only if the filter actually matches something
  if (selectedSymbol) {
    const filtered = ids.filter(id => strategies[id].symbol === selectedSymbol);
    if (filtered.length > 0) ids = filtered;
    // else silently show all (stale filter)
  }

  if (ids.length === 0) {
    container.innerHTML = `<div class="empty-state"><div class="empty-state-icon">📡</div>
      <div class="empty-state-text">Нет стратегий</div></div>`;
    return;
  }

  container.innerHTML = ids.map((id, i) => {
    const s = strategies[id];
    const sig = s.currentSignal || {};
    const signal = sig.signal || 'NO_SIGNAL';
    const stats = s.stats || {};
    const color = STRATEGY_COLORS[i % STRATEGY_COLORS.length].line;
    const ov = getOverrides(id);
    const desc = ov.description || s.description || '';
    const tp   = ov.tp_percent !== undefined ? ov.tp_percent : (s.tp_percent || '—');
    const sl   = ov.sl_percent !== undefined ? ov.sl_percent : (s.sl_percent || '—');
    const entry = ov.entry_conditions || s.entry_conditions || '';
    const exit_  = ov.exit_conditions  || s.exit_conditions  || '';
    const wr   = (stats.win_rate || 0).toFixed(1);
    const pnl  = (stats.total_pnl_percent || 0);

    return `<div class="mini-signal" data-id="${id}">
      <!-- VIEW MODE -->
      <div class="mini-signal-view">
        <div class="mini-signal-header">
          <div>
            <div class="mini-signal-name" style="color:${color}">${s.name || id}</div>
            <div class="mini-signal-symbol">${s.symbol || ''} · ${s.timeframe ? s.timeframe + 'm' : ''}</div>
          </div>
          <div class="mini-signal-direction ${signal}">${signal}</div>
        </div>
        <div style="display:grid;grid-template-columns:1fr 1fr;gap:8px;font-size:0.8rem;margin-bottom:8px">
          <div><div style="color:var(--text-muted);font-size:0.72rem">Win Rate</div>
            <div style="font-weight:700;color:${parseFloat(wr)>=50?'var(--accent-green)':'var(--accent-red)'}">${wr}%</div></div>
          <div><div style="color:var(--text-muted);font-size:0.72rem">PnL</div>
            <div style="font-weight:700;color:${pnl>=0?'var(--accent-green)':'var(--accent-red)'}">${pnl>=0?'+':''}${pnl.toFixed(2)}%</div></div>
          <div><div style="color:var(--text-muted);font-size:0.72rem">Сделок</div>
            <div style="font-weight:700">${stats.total_trades||0}</div></div>
          <div><div style="color:var(--text-muted);font-size:0.72rem">TP/SL</div>
            <div style="font-weight:700;font-size:0.75rem;color:var(--accent-blue)">${tp}% / ${sl}%</div></div>
        </div>
        ${desc ? `<div style="font-size:0.78rem;color:var(--text-muted);line-height:1.4;margin-bottom:8px">${desc}</div>` : ''}
        ${sig.reasoning ? `<div style="font-size:0.78rem;color:var(--text-muted);line-height:1.4;border-top:1px solid var(--border);padding-top:8px">${sig.reasoning}</div>` : ''}
        <div class="mini-signal-actions">
          <button class="mini-signal-edit-btn" data-id="${id}">✎ Редактировать</button>
        </div>
      </div>
      <!-- EDIT MODE -->
      <div class="inline-edit-fields" data-id="${id}">
        <div style="font-size:0.8rem;font-weight:600;color:var(--accent-blue);margin-bottom:4px">Редактирование: ${s.name||id}</div>
        <div>
          <div class="inline-edit-label">Описание</div>
          <textarea class="form-textarea ie-description" style="min-height:60px">${desc}</textarea>
        </div>
        <div>
          <div class="inline-edit-label">Условия входа (промпт для AI)</div>
          <textarea class="form-textarea ie-entry" style="min-height:80px">${entry}</textarea>
        </div>
        <div>
          <div class="inline-edit-label">Условия выхода</div>
          <textarea class="form-textarea ie-exit" style="min-height:60px">${exit_}</textarea>
        </div>
        <div class="inline-edit-row">
          <div>
            <div class="inline-edit-label">TP %</div>
            <input type="number" class="form-input ie-tp" value="${tp !== '—' ? tp : ''}" step="0.1">
          </div>
          <div>
            <div class="inline-edit-label">SL %</div>
            <input type="number" class="form-input ie-sl" value="${sl !== '—' ? sl : ''}" step="0.1">
          </div>
        </div>
        <div class="inline-edit-actions">
          <button class="btn btn-cancel ie-cancel-btn" data-id="${id}">Отмена</button>
          <button class="btn btn-dl ie-download-btn" data-id="${id}">📥 Скачать JSON</button>
          <button class="btn btn-save ie-save-btn" data-id="${id}">Сохранить</button>
        </div>
      </div>
    </div>`;
  }).join('');

  // Card click → switch tab
  container.querySelectorAll('.mini-signal').forEach(el => {
    el.addEventListener('click', (e) => {
      if (e.target.closest('.mini-signal-edit-btn, .ie-cancel-btn, .ie-save-btn, .ie-download-btn, .inline-edit-fields, button')) return;
      activeTab = el.dataset.id;
      renderTabs(); renderView();
    });
  });

  // Edit button → toggle edit mode
  container.querySelectorAll('.mini-signal-edit-btn').forEach(btn => {
    btn.addEventListener('click', e => {
      e.stopPropagation();
      const card = btn.closest('.mini-signal');
      card.classList.add('editing');
      card.querySelector('.inline-edit-fields').style.display = 'flex';
      card.querySelector('.mini-signal-view').style.display   = 'none';
    });
  });

  // Cancel
  container.querySelectorAll('.ie-cancel-btn').forEach(btn => {
    btn.addEventListener('click', e => {
      e.stopPropagation();
      const card = btn.closest('.mini-signal');
      card.classList.remove('editing');
      card.querySelector('.inline-edit-fields').style.display = 'none';
      card.querySelector('.mini-signal-view').style.display   = '';
    });
  });

  // Save (localStorage)
  container.querySelectorAll('.ie-save-btn').forEach(btn => {
    btn.addEventListener('click', e => {
      e.stopPropagation();
      const id   = btn.dataset.id;
      const card = btn.closest('.mini-signal');
      saveInlineEdit(id, card);
      card.classList.remove('editing');
      card.querySelector('.inline-edit-fields').style.display = 'none';
      card.querySelector('.mini-signal-view').style.display   = '';
      showToast('Сохранено в браузере', 'success');
      renderMiniSignals();
    });
  });

  // Download JSON
  container.querySelectorAll('.ie-download-btn').forEach(btn => {
    btn.addEventListener('click', e => {
      e.stopPropagation();
      const id   = btn.dataset.id;
      const card = btn.closest('.mini-signal');
      saveInlineEdit(id, card);
      downloadStrategyJson(id, card);
    });
  });
}

function saveInlineEdit(id, card) {
  const desc  = card.querySelector('.ie-description')?.value || '';
  const entry = card.querySelector('.ie-entry')?.value || '';
  const exit_ = card.querySelector('.ie-exit')?.value  || '';
  const tp    = card.querySelector('.ie-tp')?.value;
  const sl    = card.querySelector('.ie-sl')?.value;
  const ov = {};
  ov.description = desc;
  ov.entry_conditions = entry;
  ov.exit_conditions  = exit_;
  if (tp) ov.tp_percent = parseFloat(tp);
  if (sl) ov.sl_percent = parseFloat(sl);
  saveOverrides(id, ov);
}

function downloadStrategyJson(id, card) {
  const s  = strategies[id] || {};
  const ov = getOverrides(id);
  const desc  = card?.querySelector('.ie-description')?.value ?? (ov.description || s.description || '');
  const entry = card?.querySelector('.ie-entry')?.value ?? (ov.entry_conditions || s.entry_conditions || '');
  const exit_ = card?.querySelector('.ie-exit')?.value  ?? (ov.exit_conditions  || s.exit_conditions  || '');
  const tp    = parseFloat(card?.querySelector('.ie-tp')?.value  || ov.tp_percent || s.tp_percent || 2.0);
  const sl    = parseFloat(card?.querySelector('.ie-sl')?.value  || ov.sl_percent || s.sl_percent || 1.0);
  const tf    = typeof s.timeframe === 'string' ? parseInt(s.timeframe, 10) : (s.timeframe || 1);

  const json = {
    id,
    name:            s.name || id,
    enabled:         true,
    symbol:          s.symbol || '',
    timeframe:       tf,
    risk_per_trade:  s.risk_per_trade || 1.0,
    tp_percent:      tp,
    sl_percent:      sl,
    ai_model:        s.ai_model || 'claude-sonnet-4-6',
    description:     desc,
    entry_conditions: entry,
    exit_conditions:  exit_,
  };
  if (s.notes) json.notes = s.notes;

  const blob = new Blob([JSON.stringify(json, null, 2) + '\n'], { type: 'application/json' });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url; a.download = `${id}.json`; a.click();
  URL.revokeObjectURL(url);
  showToast(`${id}.json скачан — сохрани в strategies/`, 'success');
}

// ── Single Strategy View ──────────────────────────────────────────────────────
function renderSingleView(stratId) {
  document.getElementById('allSignalsSection').style.display = 'none';
  document.getElementById('signalSection').style.display  = '';
  document.getElementById('statsSection').style.display   = '';
  document.getElementById('historySection').style.display = '';
  document.getElementById('configSection').style.display  = '';

  const s = strategies[stratId];
  if (!s) return;
  renderSignalBox(s);
  renderStats(s);
  renderTradeHistory(s);
  renderStrategyConfig(s);
}

function renderSignalBox(s) {
  const el = document.getElementById('signalBox');
  if (!el) return;
  const sig = s.currentSignal || {};
  const signal = sig.signal || 'NO_SIGNAL';
  const icon = signal === 'LONG' ? '▲' : signal === 'SHORT' ? '▼' : '◆';
  const conf = sig.confidence || 'LOW';
  el.className = `signal-box ${signal}`;
  el.innerHTML = `
    <div class="signal-icon">${icon}</div>
    <div class="signal-details">
      <div style="display:flex;align-items:center;gap:12px;margin-bottom:6px">
        <div class="signal-direction">${signal}</div>
        <span class="confidence-badge ${conf}">${conf}</span>
      </div>
      ${sig.price ? `<div class="signal-price">Цена: <strong>${fmt(sig.price)}</strong></div>` : ''}
      ${sig.reasoning ? `<div class="signal-reasoning">${sig.reasoning}</div>` : ''}
      <div class="signal-meta">
        ${sig.tp ? `<div class="signal-meta-item tp"><span class="signal-meta-label">TP:</span><span class="signal-meta-value">${fmt(sig.tp)}</span></div>` : ''}
        ${sig.sl ? `<div class="signal-meta-item sl"><span class="signal-meta-label">SL:</span><span class="signal-meta-value">${fmt(sig.sl)}</span></div>` : ''}
        ${sig.generated_at ? `<div class="signal-meta-item"><span class="signal-meta-label">Обновлено:</span><span class="signal-meta-value" style="color:var(--text-muted)">${fmtDT(sig.generated_at)}</span></div>` : ''}
      </div>
    </div>`;
}

function renderStats(s) {
  const el = document.getElementById('statsGrid');
  if (!el) return;
  const st = s.stats || {};
  const wr = st.win_rate || 0; const pnl = st.total_pnl_percent || 0;
  const dd = st.max_drawdown || 0;
  el.innerHTML = `
    <div class="card stat-card"><div class="card-header"><span class="card-title">Win Rate</span><div class="card-icon" style="background:rgba(63,185,80,0.1)">🎯</div></div>
      <div class="card-value" style="color:${wr>=50?'var(--accent-green)':'var(--accent-red)'}">${wr.toFixed(1)}%</div>
      <div class="card-subtitle">${st.winning_trades||0} из ${st.total_trades||0} сделок</div></div>
    <div class="card stat-card"><div class="card-header"><span class="card-title">Сделок</span><div class="card-icon" style="background:rgba(88,166,255,0.1)">📊</div></div>
      <div class="card-value">${st.total_trades||0}</div>
      <div class="card-subtitle">W: ${st.winning_trades||0} / L: ${st.losing_trades||0}</div></div>
    <div class="card stat-card"><div class="card-header"><span class="card-title">PnL</span><div class="card-icon" style="background:rgba(${pnl>=0?'63,185,80':'248,81,73'},0.1)">${pnl>=0?'💹':'📉'}</div></div>
      <div class="card-value" style="color:${pnl>=0?'var(--accent-green)':'var(--accent-red)'}">${pnl>=0?'+':''}${pnl.toFixed(2)}%</div>
      <div class="card-subtitle">Накопленная доходность</div></div>
    <div class="card stat-card"><div class="card-header"><span class="card-title">Просадка</span><div class="card-icon" style="background:rgba(248,81,73,0.1)">⚠️</div></div>
      <div class="card-value" style="color:var(--accent-red)">${dd>0?'-':''}${dd.toFixed(2)}%</div>
      <div class="card-subtitle">Sharpe: ${(st.sharpe_ratio||0).toFixed(2)}</div></div>`;
}

function renderTradeHistory(s) {
  const el = document.getElementById('tradeTableBody');
  if (!el) return;
  const trades = s.trades || [];
  if (trades.length === 0) {
    el.innerHTML = `<tr><td colspan="7"><div class="empty-state"><div class="empty-state-icon">📋</div><div class="empty-state-text">Нет завершённых сделок</div></div></td></tr>`;
    return;
  }
  const sorted = [...trades].sort((a,b) => new Date(b.exit_time||b.entry_time||0) - new Date(a.exit_time||a.entry_time||0));
  el.innerHTML = sorted.slice(0, 100).map(t => {
    const pnl = t.pnl_percent || 0;
    const isLong = (t.direction||'').toUpperCase() === 'LONG';
    return `<tr>
      <td class="cell-muted">${fmtD(t.exit_time||t.entry_time)}</td>
      <td><span class="${isLong?'badge-long':'badge-short'}">${t.direction||'—'}</span></td>
      <td>${fmt(t.entry_price)}</td><td>${fmt(t.exit_price)}</td>
      <td class="${pnl>=0?'cell-positive':'cell-negative'}">${pnl>=0?'+':''}${pnl.toFixed(2)}%</td>
      <td class="cell-muted">${t.exit_reason||'—'}</td>
      <td class="cell-muted" style="font-size:0.75rem">${t.symbol||s.symbol||'—'}</td>
    </tr>`;
  }).join('');
}

function renderStrategyConfig(s) {
  const el = document.getElementById('strategyConfig');
  if (!el) return;
  const ov = getOverrides(s.id);
  const desc  = ov.description || s.description || '';
  const entry = ov.entry_conditions || s.entry_conditions || '';
  const exit_ = ov.exit_conditions  || s.exit_conditions  || '';
  el.innerHTML = `
    <div class="config-grid">
      <div class="config-item"><div class="config-label">ID</div><div class="config-value">${s.id||'—'}</div></div>
      <div class="config-item"><div class="config-label">Символ</div><div class="config-value">${s.symbol||'—'}</div></div>
      <div class="config-item"><div class="config-label">Таймфрейм</div><div class="config-value">${s.timeframe?s.timeframe+'M':'—'}</div></div>
      <div class="config-item"><div class="config-label">TP</div><div class="config-value" style="color:var(--accent-green)">${s.tp_percent!==undefined?s.tp_percent+'%':'—'}</div></div>
      <div class="config-item"><div class="config-label">SL</div><div class="config-value" style="color:var(--accent-red)">${s.sl_percent!==undefined?s.sl_percent+'%':'—'}</div></div>
      <div class="config-item"><div class="config-label">Риск</div><div class="config-value">${s.risk_per_trade!==undefined?s.risk_per_trade+'%':'—'}</div></div>
    </div>
    ${desc  ? `<div class="config-description"><strong>Описание:</strong> ${desc}</div>` : ''}
    ${entry ? `<div class="config-description"><strong>Условия входа:</strong> ${entry}</div>` : ''}
    ${exit_ ? `<div class="config-description"><strong>Условия выхода:</strong> ${exit_}</div>` : ''}`;
}

// ── Chart ─────────────────────────────────────────────────────────────────────
function renderChart() {
  if (!tradesData) return;
  renderPnlChart(tradesData, activeTab === 'all' ? null : activeTab);
}

// ── TradingView Widget ────────────────────────────────────────────────────────
let tvScriptLoaded = false;

function initTradingViewChart() {
  updateTradingViewChart();
}

function updateTradingViewChart() {
  const container = document.getElementById('tv_chart_container');
  if (!container) return;

  const H = 460;

  let sym = selectedSymbol;
  if (!sym) {
    const ids = Object.keys(strategies);
    sym = ids.length > 0 ? (strategies[ids[0]].symbol || 'BINANCE:BTCUSDT.P') : 'BINANCE:BTCUSDT.P';
  }

  const interval = selectedTimeframe === '1440' ? 'D'
                 : selectedTimeframe === '240'  ? '240'
                 : selectedTimeframe || '1';

  const uid = 'tv_' + Date.now();
  container.innerHTML = `<div id="${uid}" style="width:100%;height:${H}px"></div>`;

  const doInit = () => {
    if (!window.TradingView) { setTimeout(doInit, 300); return; }
    new window.TradingView.widget({
      width:             '100%',
      height:            H,
      symbol:            sym,
      interval,
      timezone:          'Etc/UTC',
      theme:             'dark',
      style:             '1',
      locale:            'ru',
      toolbar_bg:        '#161b22',
      enable_publishing: false,
      hide_top_toolbar:  false,
      hide_legend:       false,
      save_image:        false,
      container_id:      uid,
    });
  };
  doInit();
}

// ── Reasoning Logs ────────────────────────────────────────────────────────────
function renderLogs() {
  const container = document.getElementById('logsContainer');
  const countEl   = document.getElementById('logsCount');
  if (!container) return;

  if (logsData.length === 0) {
    container.innerHTML = `<div class="empty-state"><div class="empty-state-icon">📝</div>
      <div class="empty-state-text">Логов пока нет. Запусти <code style="color:var(--accent-orange)">npm run analyze</code></div></div>`;
    if (countEl) countEl.textContent = 'Нет записей';
    return;
  }

  // Filter by active tab (strategy) or selected symbol
  let filtered = logsData;
  if (activeTab !== 'all') {
    filtered = logsData.filter(e => e.strategy_id === activeTab);
  } else if (selectedSymbol) {
    filtered = logsData.filter(e => e.symbol === selectedSymbol);
  }

  if (countEl) countEl.textContent = filtered.length !== logsData.length
    ? `${filtered.length} из ${logsData.length} записей`
    : `${logsData.length} записей`;

  if (filtered.length === 0) {
    container.innerHTML = `<div class="empty-state"><div class="empty-state-icon">🔍</div>
      <div class="empty-state-text">Нет логов для выбранной стратегии</div></div>`;
    return;
  }

  container.innerHTML = filtered.map((entry, idx) => {
    const sig  = entry.signal || 'NO_SIGNAL';
    const dur  = entry.duration_ms ? `${(entry.duration_ms/1000).toFixed(1)}с` : '';
    const conf = entry.confidence || '';
    const confColor = conf === 'HIGH' ? 'var(--accent-green)' : conf === 'MEDIUM' ? 'var(--accent-yellow)' : 'var(--text-muted)';
    const hasSum = !!entry.chart_summary;

    return `<div class="log-entry ${sig}">
      <div class="log-entry-header">
        <span class="log-entry-signal ${sig}">${sig}</span>
        <span class="log-entry-strategy">${entry.strategy_name || entry.strategy_id || ''}</span>
        <span class="log-entry-symbol">${entry.symbol || ''} · ${entry.timeframe || ''}m</span>
        ${conf ? `<span style="font-size:0.72rem;color:${confColor};font-weight:700">${conf}</span>` : ''}
        ${dur  ? `<span class="log-entry-duration">⏱ ${dur}</span>` : ''}
        <span class="log-entry-time">${fmtDT(entry.ts)}</span>
      </div>
      <div class="log-entry-reasoning">${entry.reasoning || 'Нет данных'}</div>
      ${entry.price ? `<div class="log-entry-price">
        <div class="log-price-item"><span class="log-price-label">Цена:</span><span class="log-price-value">${fmt(entry.price)}</span></div>
        ${entry.tp ? `<div class="log-price-item"><span class="log-price-label">TP:</span><span class="log-price-value tp">${fmt(entry.tp)}</span></div>` : ''}
        ${entry.sl ? `<div class="log-price-item"><span class="log-price-label">SL:</span><span class="log-price-value sl">${fmt(entry.sl)}</span></div>` : ''}
      </div>` : ''}
      ${hasSum ? `<button class="log-summary-toggle" data-idx="${idx}">▶ Данные графика (Haiku)</button>
        <div class="log-chart-summary" id="lcs_${idx}">${entry.chart_summary}</div>` : ''}
    </div>`;
  }).join('');

  // Toggle chart summary
  container.querySelectorAll('.log-summary-toggle').forEach(btn => {
    btn.addEventListener('click', () => {
      const idx = btn.dataset.idx;
      const sum = document.getElementById(`lcs_${idx}`);
      sum.classList.toggle('visible');
      btn.textContent = sum.classList.contains('visible') ? '▼ Данные графика (Haiku)' : '▶ Данные графика (Haiku)';
    });
  });
}

// ── Controls ──────────────────────────────────────────────────────────────────
function setupControls() {
  setupSymbolSelector();
  setupTimeframeSelector();

  document.getElementById('refreshBtn')?.addEventListener('click', async () => {
    secondsLeft = REFRESH_INTERVAL;
    await loadData(); renderAll();
  });
}

function setupSymbolSelector() {
  const sel = document.getElementById('symbolSelector');
  if (!sel) return;

  const symbols = new Set();
  Object.values(strategies).forEach(s => { if (s.symbol) symbols.add(s.symbol); });
  const sorted = Array.from(symbols).sort();

  sel.innerHTML = '<option value="">Все символы</option>' +
    sorted.map(s => `<option value="${s}" ${selectedSymbol === s ? 'selected' : ''}>${s}</option>`).join('');

  sel.addEventListener('change', e => {
    selectedSymbol = e.target.value;
    setPref('symbol', selectedSymbol);
    renderMiniSignals();
    updateTradingViewChart();
    renderLogs();
  });
}

function setupTimeframeSelector() {
  const group = document.getElementById('timeframeGroup');
  if (!group) return;

  group.querySelectorAll('.tf-btn').forEach(btn => {
    btn.classList.toggle('active', btn.dataset.tf === selectedTimeframe);
    btn.addEventListener('click', () => {
      selectedTimeframe = btn.dataset.tf;
      setPref('tf', selectedTimeframe);
      group.querySelectorAll('.tf-btn').forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
      updateTradingViewChart();
      showToast(`Таймфрейм: ${btn.textContent}`, 'info');
    });
  });
}

// ── New Strategy Form ─────────────────────────────────────────────────────────
function setupNewStrategyForm() {
  const form = document.getElementById('newStrategyForm');
  document.getElementById('addStrategyBtn')?.addEventListener('click', () => {
    form.style.display = form.style.display === 'none' ? '' : 'none';
  });
  document.getElementById('cancelNewStrategyBtn')?.addEventListener('click', () => {
    form.style.display = 'none';
  });
  document.getElementById('downloadNewStrategyBtn')?.addEventListener('click', () => {
    const name     = document.getElementById('ns_name').value.trim()      || 'Новая стратегия';
    const symbol   = document.getElementById('ns_symbol').value.trim()    || 'BINANCE:BTCUSDT.P';
    const tf       = parseInt(document.getElementById('ns_timeframe').value) || 5;
    const tp       = parseFloat(document.getElementById('ns_tp').value)   || 2.0;
    const sl       = parseFloat(document.getElementById('ns_sl').value)   || 1.0;
    const risk     = parseFloat(document.getElementById('ns_risk').value) || 1.0;
    const desc     = document.getElementById('ns_description').value.trim();
    const entry    = document.getElementById('ns_entry').value.trim();
    const exit_    = document.getElementById('ns_exit').value.trim();

    // Generate sequential ID
    const existing = Object.keys(strategies).length;
    const newNum   = String(existing + 1).padStart(3, '0');
    const id       = `strategy_${newNum}`;

    const json = { id, name, enabled: true, symbol, timeframe: tf,
      risk_per_trade: risk, tp_percent: tp, sl_percent: sl,
      ai_model: 'claude-sonnet-4-6', description: desc,
      entry_conditions: entry, exit_conditions: exit_ };

    const blob = new Blob([JSON.stringify(json, null, 2) + '\n'], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a'); a.href = url; a.download = `${id}.json`; a.click();
    URL.revokeObjectURL(url);
    showToast(`Скачан ${id}.json — сохрани в папку strategies/`, 'success');
    form.style.display = 'none';
  });
}

// ── localStorage overrides ────────────────────────────────────────────────────
function getOverrides(id) {
  try {
    const data = JSON.parse(localStorage.getItem('tradingDashboard') || '{}');
    return data.strategyOverrides?.[id] || {};
  } catch (_) { return {}; }
}

function saveOverrides(id, ov) {
  try {
    const data = JSON.parse(localStorage.getItem('tradingDashboard') || '{}');
    if (!data.strategyOverrides) data.strategyOverrides = {};
    data.strategyOverrides[id] = { ...data.strategyOverrides[id], ...ov };
    localStorage.setItem('tradingDashboard', JSON.stringify(data));
  } catch (_) {}
}

// ── Refresh Loop ──────────────────────────────────────────────────────────────
function startRefreshLoop() {
  secondsLeft = REFRESH_INTERVAL;
  setInterval(async () => {
    secondsLeft--;
    document.getElementById('refreshTimer').textContent = `Обновление через ${secondsLeft}с`;
    const pct = ((REFRESH_INTERVAL - secondsLeft) / REFRESH_INTERVAL) * 100;
    document.getElementById('refreshProgress').style.width = pct + '%';
    if (secondsLeft <= 0) {
      secondsLeft = REFRESH_INTERVAL;
      await loadData(); renderAll();
    }
  }, 1000);
}

// ── Helpers ───────────────────────────────────────────────────────────────────
function updateLastUpdated(iso) {
  const el = document.getElementById('lastUpdated');
  if (el) el.textContent = iso ? 'Обновлено: ' + fmtDT(iso) : 'Нет данных';
}

function fmt(val) {
  if (val === null || val === undefined) return '—';
  const n = parseFloat(val); if (isNaN(n)) return '—';
  if (n > 1000) return n.toLocaleString('ru-RU', { maximumFractionDigits: 2 });
  if (n > 1)    return n.toFixed(4);
  return n.toFixed(6);
}

function fmtDT(iso) {
  if (!iso) return '—';
  try { return new Date(iso).toLocaleString('ru-RU', { day:'2-digit',month:'2-digit',year:'2-digit',hour:'2-digit',minute:'2-digit' }); }
  catch (_) { return iso; }
}

function fmtD(iso) {
  if (!iso) return '—';
  try { return new Date(iso).toLocaleDateString('ru-RU', { day:'2-digit',month:'2-digit',year:'2-digit' }); }
  catch (_) { return iso; }
}

function showToast(msg, type = 'info') {
  const c = document.getElementById('toast-container'); if (!c) return;
  const t = document.createElement('div'); t.className = `toast ${type}`; t.textContent = msg;
  c.appendChild(t);
  setTimeout(() => { t.style.opacity='0'; t.style.transform='translateX(100%)'; t.style.transition='all 0.3s'; setTimeout(()=>t.remove(),300); }, 3000);
}
