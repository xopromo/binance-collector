/**
 * dashboard.js — Main Trading Strategy Dashboard
 * Reads signals and trades from JSON files, renders UI
 * Features: Timeframe selector, Symbol selector, Strategy editor
 */

import { renderPnlChart, destroyChart, STRATEGY_COLORS } from './charts.js';
import {
  openStrategyEditor,
  getStrategyOverrides,
  getSelectedSymbol,
  setSelectedSymbol,
  getSelectedTimeframe,
  setSelectedTimeframe,
} from './strategy-editor.js';

// ── Config ────────────────────────────────────────────────────────────────────
const REFRESH_INTERVAL = 60; // seconds
const BASE_PATH = '';        // relative to index.html in docs/

let signalsData = null;
let tradesData  = null;
let strategies  = {};        // merged from both sources
let activeTab   = 'all';     // 'all' | strategy id
let refreshTimer = null;
let secondsLeft  = REFRESH_INTERVAL;
let selectedSymbolFilter = ''; // filter by selected symbol
let selectedTimeframeFilter = '1'; // default 1m

// ── Bootstrap ─────────────────────────────────────────────────────────────────
document.addEventListener('DOMContentLoaded', async () => {
  // Restore selections from localStorage
  selectedSymbolFilter = getSelectedSymbol();
  selectedTimeframeFilter = getSelectedTimeframe();

  await loadData();
  renderAll();
  startRefreshLoop();
  setupEventListeners();
  setupSelectors();

  // Listen for strategy updates
  window.addEventListener('strategyUpdated', () => {
    renderAll();
  });
});

// ── Data Loading ──────────────────────────────────────────────────────────────
async function loadData() {
  try {
    const [sigResp, trdResp] = await Promise.all([
      fetch(`${BASE_PATH}signals/latest.json?v=${Date.now()}`),
      fetch(`${BASE_PATH}trades/log.json?v=${Date.now()}`),
    ]);

    if (sigResp.ok) signalsData = await sigResp.json();
    if (trdResp.ok) tradesData  = await trdResp.json();

    // Build merged strategies index
    strategies = {};

    // From trades log
    if (tradesData?.strategies) {
      Object.entries(tradesData.strategies).forEach(([id, s]) => {
        strategies[id] = { id, ...s };
      });
    }

    // Overlay signal data
    if (signalsData?.strategies) {
      Object.entries(signalsData.strategies).forEach(([id, sig]) => {
        if (!strategies[id]) strategies[id] = { id };
        strategies[id].currentSignal = sig;
      });
    }

    updateLastUpdated(signalsData?.updated_at);
    showToast('Данные обновлены', 'success');
  } catch (err) {
    console.error('Ошибка загрузки данных:', err);
    showToast('Ошибка загрузки данных', 'error');
  }
}

// ── Render All ────────────────────────────────────────────────────────────────
function renderAll() {
  renderTabs();
  renderView();
}

function renderView() {
  if (activeTab === 'all') {
    renderAllStrategiesView();
  } else {
    renderSingleStrategyView(activeTab);
  }
  renderChart();
}

// ── Tabs ──────────────────────────────────────────────────────────────────────
function renderTabs() {
  const tabsEl = document.getElementById('strategyTabs');
  if (!tabsEl) return;

  const ids = Object.keys(strategies);
  let html = `<button class="tab all ${activeTab === 'all' ? 'active' : ''}" data-id="all">
    <span class="tab-dot" style="background:linear-gradient(135deg,#bc8cff,#58a6ff)"></span>
    Все стратегии
  </button>`;

  ids.forEach((id, i) => {
    const s = strategies[id];
    const color = STRATEGY_COLORS[i % STRATEGY_COLORS.length].line;
    const sig = s.currentSignal?.signal || 'NO_SIGNAL';
    const sigColor = sig === 'LONG' ? '#3fb950' : sig === 'SHORT' ? '#f85149' : '#484f58';
    html += `<button class="tab ${activeTab === id ? 'active' : ''}" data-id="${id}">
      <span class="tab-dot" style="background:${color}"></span>
      ${s.name || id}
      <span style="font-size:0.7rem;color:${sigColor};font-weight:700">${sig}</span>
    </button>`;
  });

  tabsEl.innerHTML = html;

  tabsEl.querySelectorAll('.tab').forEach(btn => {
    btn.addEventListener('click', () => {
      activeTab = btn.dataset.id;
      renderTabs();
      renderView();
    });
  });
}

// ── All Strategies View ───────────────────────────────────────────────────────
function renderAllStrategiesView() {
  const signalSection  = document.getElementById('signalSection');
  const statsSection   = document.getElementById('statsSection');
  const historySection = document.getElementById('historySection');
  const configSection  = document.getElementById('configSection');
  const allSignals     = document.getElementById('allSignalsSection');

  if (allSignals)     allSignals.style.display    = '';
  if (signalSection)  signalSection.style.display  = 'none';
  if (statsSection)   statsSection.style.display   = 'none';
  if (historySection) historySection.style.display = 'none';
  if (configSection)  configSection.style.display  = 'none';

  renderMiniSignals();
}

function renderMiniSignals() {
  const container = document.getElementById('miniSignalsGrid');
  if (!container) return;

  // Filter strategies by symbol if selected
  let ids = Object.keys(strategies);
  if (selectedSymbolFilter) {
    ids = ids.filter(id => strategies[id].symbol === selectedSymbolFilter);
  }

  if (ids.length === 0) {
    container.innerHTML = `<div class="empty-state"><div class="empty-state-icon">📡</div><div class="empty-state-text">Нет стратегий${selectedSymbolFilter ? ' для выбранного символа' : ''}</div></div>`;
    return;
  }

  container.innerHTML = ids.map((id, i) => {
    const s = strategies[id];
    const sig = s.currentSignal || {};
    const signal = sig.signal || 'NO_SIGNAL';
    const stats = s.stats || {};
    const color = STRATEGY_COLORS[i % STRATEGY_COLORS.length].line;

    // Get overrides from localStorage
    const overrides = getStrategyOverrides(id);
    const displayDesc = overrides.description || s.description || '';
    const displayTp = overrides.tp_percent !== undefined ? overrides.tp_percent : s.tp_percent;
    const displaySl = overrides.sl_percent !== undefined ? overrides.sl_percent : s.sl_percent;

    return `<div class="mini-signal" data-id="${id}">
      <div class="mini-signal-header">
        <div>
          <div class="mini-signal-name" style="color:${color}">${s.name || id}</div>
          <div class="mini-signal-symbol">${s.symbol || ''} · ${s.timeframe ? s.timeframe + 'm' : ''}</div>
        </div>
        <div class="mini-signal-direction ${signal}">${signal}</div>
      </div>
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:8px;font-size:0.8rem;margin-bottom:8px">
        <div>
          <div style="color:var(--text-muted);font-size:0.72rem">Win Rate</div>
          <div style="font-weight:700;color:${(stats.win_rate||0) >= 50 ? 'var(--accent-green)' : 'var(--accent-red)'}">
            ${(stats.win_rate||0).toFixed(1)}%
          </div>
        </div>
        <div>
          <div style="color:var(--text-muted);font-size:0.72rem">PnL</div>
          <div style="font-weight:700;color:${(stats.total_pnl_percent||0) >= 0 ? 'var(--accent-green)' : 'var(--accent-red)'}">
            ${(stats.total_pnl_percent||0) >= 0 ? '+' : ''}${(stats.total_pnl_percent||0).toFixed(2)}%
          </div>
        </div>
        <div>
          <div style="color:var(--text-muted);font-size:0.72rem">Сделок</div>
          <div style="font-weight:700">${stats.total_trades || 0}</div>
        </div>
        <div>
          <div style="color:var(--text-muted);font-size:0.72rem">Просадка</div>
          <div style="font-weight:700;color:var(--accent-red)">
            ${(stats.max_drawdown||0) > 0 ? '-' : ''}${(stats.max_drawdown||0).toFixed(2)}%
          </div>
        </div>
      </div>
      ${displayDesc ? `<div style="font-size:0.78rem;color:var(--text-muted);line-height:1.4;margin-bottom:8px">${displayDesc}</div>` : ''}
      ${sig.reasoning ? `<div style="font-size:0.78rem;color:var(--text-muted);line-height:1.4;border-top:1px solid var(--border);padding-top:8px">${sig.reasoning}</div>` : ''}
      ${displayTp || displaySl ? `<div style="font-size:0.75rem;color:var(--accent-blue);margin-top:8px">TP: ${displayTp || '—'}% / SL: ${displaySl || '—'}%</div>` : ''}
      <div class="mini-signal-actions">
        <button class="mini-signal-edit-btn" data-id="${id}">✎ Редактировать</button>
      </div>
    </div>`;
  }).join('');

  // Click to switch tab
  container.querySelectorAll('.mini-signal').forEach(el => {
    el.addEventListener('click', (e) => {
      if (e.target.closest('.mini-signal-edit-btn')) {
        // Don't switch tab if clicking edit button
        return;
      }
      activeTab = el.dataset.id;
      renderTabs();
      renderView();
    });
  });

  // Add edit button handlers
  container.querySelectorAll('.mini-signal-edit-btn').forEach(btn => {
    btn.addEventListener('click', (e) => {
      e.stopPropagation();
      const stratId = btn.dataset.id;
      openStrategyEditor(stratId, strategies[stratId]);
    });
  });
}

// ── Single Strategy View ──────────────────────────────────────────────────────
function renderSingleStrategyView(stratId) {
  const signalSection  = document.getElementById('signalSection');
  const statsSection   = document.getElementById('statsSection');
  const historySection = document.getElementById('historySection');
  const configSection  = document.getElementById('configSection');
  const allSignals     = document.getElementById('allSignalsSection');

  if (allSignals)     allSignals.style.display    = 'none';
  if (signalSection)  signalSection.style.display  = '';
  if (statsSection)   statsSection.style.display   = '';
  if (historySection) historySection.style.display = '';
  if (configSection)  configSection.style.display  = '';

  const s = strategies[stratId];
  if (!s) return;

  renderSignalBox(s);
  renderStats(s);
  renderTradeHistory(s);
  renderStrategyConfig(s);
}

// ── Signal Box ────────────────────────────────────────────────────────────────
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
      ${sig.price ? `<div class="signal-price">Цена: <strong>${formatPrice(sig.price)}</strong></div>` : ''}
      ${sig.reasoning ? `<div class="signal-reasoning">${sig.reasoning}</div>` : ''}
      <div class="signal-meta">
        ${sig.tp ? `<div class="signal-meta-item tp">
          <span class="signal-meta-label">TP:</span>
          <span class="signal-meta-value">${formatPrice(sig.tp)}</span>
        </div>` : ''}
        ${sig.sl ? `<div class="signal-meta-item sl">
          <span class="signal-meta-label">SL:</span>
          <span class="signal-meta-value">${formatPrice(sig.sl)}</span>
        </div>` : ''}
        ${sig.generated_at ? `<div class="signal-meta-item">
          <span class="signal-meta-label">Обновлено:</span>
          <span class="signal-meta-value" style="color:var(--text-muted)">${formatDateTime(sig.generated_at)}</span>
        </div>` : ''}
      </div>
    </div>`;
}

// ── Stats ─────────────────────────────────────────────────────────────────────
function renderStats(s) {
  const el = document.getElementById('statsGrid');
  if (!el) return;

  const st = s.stats || {};
  const wr = st.win_rate || 0;
  const pnl = st.total_pnl_percent || 0;
  const dd = st.max_drawdown || 0;
  const sharpe = st.sharpe_ratio || 0;

  el.innerHTML = `
    <div class="card stat-card">
      <div class="card-header">
        <span class="card-title">Win Rate</span>
        <div class="card-icon" style="background:rgba(63,185,80,0.1)">🎯</div>
      </div>
      <div class="card-value" style="color:${wr >= 50 ? 'var(--accent-green)' : 'var(--accent-red)'}">${wr.toFixed(1)}%</div>
      <div class="card-subtitle">${st.winning_trades || 0} из ${st.total_trades || 0} сделок</div>
    </div>
    <div class="card stat-card">
      <div class="card-header">
        <span class="card-title">Всего сделок</span>
        <div class="card-icon" style="background:rgba(88,166,255,0.1)">📊</div>
      </div>
      <div class="card-value">${st.total_trades || 0}</div>
      <div class="card-subtitle">WIN: ${st.winning_trades || 0} / LOSS: ${st.losing_trades || 0}</div>
    </div>
    <div class="card stat-card">
      <div class="card-header">
        <span class="card-title">Суммарный PnL</span>
        <div class="card-icon" style="background:rgba(${pnl >= 0 ? '63,185,80' : '248,81,73'},0.1)">${pnl >= 0 ? '💹' : '📉'}</div>
      </div>
      <div class="card-value" style="color:${pnl >= 0 ? 'var(--accent-green)' : 'var(--accent-red)'}">
        ${pnl >= 0 ? '+' : ''}${pnl.toFixed(2)}%
      </div>
      <div class="card-subtitle">Накопленная доходность</div>
    </div>
    <div class="card stat-card">
      <div class="card-header">
        <span class="card-title">Макс. просадка</span>
        <div class="card-icon" style="background:rgba(248,81,73,0.1)">⚠️</div>
      </div>
      <div class="card-value" style="color:var(--accent-red)">${dd > 0 ? '-' : ''}${dd.toFixed(2)}%</div>
      <div class="card-subtitle">Sharpe: ${sharpe.toFixed(2)}</div>
    </div>`;
}

// ── Trade History ─────────────────────────────────────────────────────────────
function renderTradeHistory(s) {
  const el = document.getElementById('tradeTableBody');
  if (!el) return;

  const trades = s.trades || [];

  if (trades.length === 0) {
    el.innerHTML = `<tr><td colspan="7"><div class="empty-state">
      <div class="empty-state-icon">📋</div>
      <div class="empty-state-text">Нет завершённых сделок</div>
    </div></td></tr>`;
    return;
  }

  // Sort newest first
  const sorted = [...trades].sort((a, b) => {
    return new Date(b.exit_time || b.entry_time || 0) - new Date(a.exit_time || a.entry_time || 0);
  });

  el.innerHTML = sorted.slice(0, 100).map(t => {
    const pnl = t.pnl_percent || 0;
    const isLong = (t.direction || '').toUpperCase() === 'LONG';
    return `<tr>
      <td class="cell-muted">${formatDate(t.exit_time || t.entry_time)}</td>
      <td><span class="${isLong ? 'badge-long' : 'badge-short'}">${t.direction || '—'}</span></td>
      <td>${formatPrice(t.entry_price)}</td>
      <td>${formatPrice(t.exit_price)}</td>
      <td class="${pnl >= 0 ? 'cell-positive' : 'cell-negative'}">${pnl >= 0 ? '+' : ''}${pnl.toFixed(2)}%</td>
      <td class="cell-muted">${t.exit_reason || '—'}</td>
      <td class="cell-muted" style="font-size:0.75rem">${t.symbol || s.symbol || '—'}</td>
    </tr>`;
  }).join('');
}

// ── Strategy Config ───────────────────────────────────────────────────────────
function renderStrategyConfig(s) {
  const el = document.getElementById('strategyConfig');
  if (!el) return;

  el.innerHTML = `
    <div class="config-grid">
      <div class="config-item">
        <div class="config-label">ID стратегии</div>
        <div class="config-value">${s.id || '—'}</div>
      </div>
      <div class="config-item">
        <div class="config-label">Символ</div>
        <div class="config-value">${s.symbol || '—'}</div>
      </div>
      <div class="config-item">
        <div class="config-label">Таймфрейм</div>
        <div class="config-value">${s.timeframe ? s.timeframe + 'M' : '—'}</div>
      </div>
      <div class="config-item">
        <div class="config-label">Риск/сделка</div>
        <div class="config-value">${s.risk_per_trade !== undefined ? s.risk_per_trade + '%' : '—'}</div>
      </div>
      <div class="config-item">
        <div class="config-label">Take Profit</div>
        <div class="config-value" style="color:var(--accent-green)">${s.tp_percent !== undefined ? s.tp_percent + '%' : '—'}</div>
      </div>
      <div class="config-item">
        <div class="config-label">Stop Loss</div>
        <div class="config-value" style="color:var(--accent-red)">${s.sl_percent !== undefined ? s.sl_percent + '%' : '—'}</div>
      </div>
      <div class="config-item">
        <div class="config-label">AI модель</div>
        <div class="config-value" style="font-size:0.8rem">${s.ai_model || '—'}</div>
      </div>
    </div>
    ${s.description ? `<div class="config-description"><strong>Описание:</strong> ${s.description}</div>` : ''}
    ${s.entry_conditions ? `<div class="config-description"><strong>Условия входа:</strong> ${s.entry_conditions}</div>` : ''}
    ${s.exit_conditions ? `<div class="config-description"><strong>Условия выхода:</strong> ${s.exit_conditions}</div>` : ''}
    ${s.notes ? `<div class="config-description" style="border-left:3px solid var(--accent-yellow)"><strong>Заметки:</strong> ${s.notes}</div>` : ''}`;
}

// ── Chart ─────────────────────────────────────────────────────────────────────
function renderChart() {
  if (!tradesData) return;
  const filter = activeTab === 'all' ? null : activeTab;
  renderPnlChart(tradesData, filter);
}

// ── Refresh Loop ──────────────────────────────────────────────────────────────
function startRefreshLoop() {
  secondsLeft = REFRESH_INTERVAL;
  updateRefreshUI();

  refreshTimer = setInterval(async () => {
    secondsLeft--;
    updateRefreshUI();

    if (secondsLeft <= 0) {
      secondsLeft = REFRESH_INTERVAL;
      await loadData();
      renderAll();
    }
  }, 1000);
}

function updateRefreshUI() {
  const timer = document.getElementById('refreshTimer');
  if (timer) timer.textContent = `Обновление через ${secondsLeft}с`;

  const bar = document.getElementById('refreshProgress');
  if (bar) {
    const pct = ((REFRESH_INTERVAL - secondsLeft) / REFRESH_INTERVAL) * 100;
    bar.style.width = `${pct}%`;
  }
}

// ── Event Listeners ───────────────────────────────────────────────────────────
function setupEventListeners() {
  // Manual refresh button
  const refreshBtn = document.getElementById('refreshBtn');
  if (refreshBtn) {
    refreshBtn.addEventListener('click', async () => {
      secondsLeft = REFRESH_INTERVAL;
      await loadData();
      renderAll();
    });
  }
}

function setupSelectors() {
  setupSymbolSelector();
  setupTimeframeSelector();
}

function setupSymbolSelector() {
  const selector = document.getElementById('symbolSelector');
  if (!selector) return;

  // Collect all unique symbols from strategies
  const symbols = new Set();
  Object.values(strategies).forEach(s => {
    if (s.symbol) symbols.add(s.symbol);
  });

  const sortedSymbols = Array.from(symbols).sort();

  // Build options
  let html = '<option value="">Все символы</option>';
  sortedSymbols.forEach(sym => {
    html += `<option value="${sym}" ${selectedSymbolFilter === sym ? 'selected' : ''}>${sym}</option>`;
  });

  selector.innerHTML = html;

  // Event listener
  selector.addEventListener('change', (e) => {
    selectedSymbolFilter = e.target.value;
    setSelectedSymbol(selectedSymbolFilter);
    renderMiniSignals();
  });
}

function setupTimeframeSelector() {
  const group = document.getElementById('timeframeGroup');
  if (!group) return;

  // Update active button
  group.querySelectorAll('.tf-btn').forEach(btn => {
    btn.classList.remove('active');
    if (btn.dataset.tf === selectedTimeframeFilter) {
      btn.classList.add('active');
    }
  });

  // Event listeners
  group.querySelectorAll('.tf-btn').forEach(btn => {
    btn.addEventListener('click', () => {
      selectedTimeframeFilter = btn.dataset.tf;
      setSelectedTimeframe(selectedTimeframeFilter);

      // Update UI
      group.querySelectorAll('.tf-btn').forEach(b => b.classList.remove('active'));
      btn.classList.add('active');

      // Could add filtering logic here in future
      showToast(`Таймфрейм: ${btn.textContent}`, 'info');
    });
  });
}

// ── Helpers ───────────────────────────────────────────────────────────────────
function updateLastUpdated(iso) {
  const el = document.getElementById('lastUpdated');
  if (!el) return;
  if (iso) {
    el.textContent = 'Обновлено: ' + formatDateTime(iso);
  }
}

function formatPrice(val) {
  if (!val && val !== 0) return '—';
  const n = parseFloat(val);
  if (isNaN(n)) return '—';
  if (n > 1000) return n.toLocaleString('ru-RU', { maximumFractionDigits: 2 });
  if (n > 1)    return n.toFixed(4);
  return n.toFixed(6);
}

function formatDate(iso) {
  if (!iso) return '—';
  try {
    const d = new Date(iso);
    return d.toLocaleDateString('ru-RU', { day: '2-digit', month: '2-digit', year: '2-digit' });
  } catch (_) { return iso; }
}

function formatDateTime(iso) {
  if (!iso) return '—';
  try {
    const d = new Date(iso);
    return d.toLocaleString('ru-RU', {
      day: '2-digit', month: '2-digit', year: '2-digit',
      hour: '2-digit', minute: '2-digit',
    });
  } catch (_) { return iso; }
}

function showToast(message, type = 'info') {
  const container = document.getElementById('toast-container');
  if (!container) return;

  const toast = document.createElement('div');
  toast.className = `toast ${type}`;
  toast.textContent = message;
  container.appendChild(toast);

  setTimeout(() => {
    toast.style.opacity = '0';
    toast.style.transform = 'translateX(100%)';
    toast.style.transition = 'all 0.3s ease';
    setTimeout(() => toast.remove(), 300);
  }, 3000);
}
