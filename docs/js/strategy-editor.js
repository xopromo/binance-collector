/**
 * strategy-editor.js — Strategy Editor Modal
 * Handles opening/closing editor modal and saving strategy overrides to localStorage
 */

let currentEditingStrategyId = null;
let currentStrategyBaseData = null;

// ── Modal Elements ────────────────────────────────────────────────────────────
const editorModal = document.getElementById('editorModal');
const modalOverlay = document.querySelector('.modal-overlay');
const closeModalBtn = document.getElementById('closeModalBtn');
const cancelEditBtn = document.getElementById('cancelEditBtn');
const saveEditBtn = document.getElementById('saveEditBtn');
const strategyEditorForm = document.getElementById('strategyEditorForm');

// ── Form Fields ───────────────────────────────────────────────────────────────
const editorName = document.getElementById('editorName');
const editorSymbol = document.getElementById('editorSymbol');
const editorTimeframe = document.getElementById('editorTimeframe');
const editorDescription = document.getElementById('editorDescription');
const editorEntryConditions = document.getElementById('editorEntryConditions');
const editorExitConditions = document.getElementById('editorExitConditions');
const editorTpPercent = document.getElementById('editorTpPercent');
const editorSlPercent = document.getElementById('editorSlPercent');
const editorTitle = document.getElementById('editorTitle');

// ── Modal Controls ────────────────────────────────────────────────────────────
export function openStrategyEditor(strategyId, strategyData) {
  currentEditingStrategyId = strategyId;
  currentStrategyBaseData = strategyData;

  // Load overrides from localStorage
  const overrides = getStrategyOverrides(strategyId);

  // Populate form with current data
  editorTitle.textContent = `Редактирование: ${strategyData.name || strategyId}`;
  editorName.value = strategyData.name || '';
  editorSymbol.value = strategyData.symbol || '';
  editorTimeframe.value = strategyData.timeframe ? `${strategyData.timeframe}м` : '';

  editorDescription.value = overrides.description || strategyData.description || '';
  editorEntryConditions.value = overrides.entry_conditions || strategyData.entry_conditions || '';
  editorExitConditions.value = overrides.exit_conditions || strategyData.exit_conditions || '';
  editorTpPercent.value = overrides.tp_percent !== undefined ? overrides.tp_percent : (strategyData.tp_percent || '');
  editorSlPercent.value = overrides.sl_percent !== undefined ? overrides.sl_percent : (strategyData.sl_percent || '');

  // Show modal
  editorModal.style.display = 'flex';
  document.body.style.overflow = 'hidden';
}

function closeStrategyEditor() {
  editorModal.style.display = 'none';
  document.body.style.overflow = '';
  currentEditingStrategyId = null;
  strategyEditorForm.reset();
}

function downloadStrategyJson() {
  if (!currentEditingStrategyId || !currentStrategyBaseData) return;

  const base = currentStrategyBaseData;
  const tfRaw = editorTimeframe.value.replace('м', '').trim();
  const timeframe = parseInt(tfRaw, 10) || base.timeframe || 1;

  const strategyJson = {
    id:             currentEditingStrategyId,
    name:           editorName.value || base.name || '',
    enabled:        base.enabled !== undefined ? base.enabled : true,
    symbol:         editorSymbol.value || base.symbol || '',
    timeframe,
    risk_per_trade: base.risk_per_trade !== undefined ? base.risk_per_trade : 1,
    tp_percent:     editorTpPercent.value !== '' ? parseFloat(editorTpPercent.value) : (base.tp_percent || 2.5),
    sl_percent:     editorSlPercent.value !== '' ? parseFloat(editorSlPercent.value) : (base.sl_percent || 1.0),
    description:    editorDescription.value || '',
    entry_conditions: editorEntryConditions.value || '',
    exit_conditions:  editorExitConditions.value || '',
  };

  if (base.notes) strategyJson.notes = base.notes;
  if (base.ai_model) strategyJson.ai_model = base.ai_model;

  const blob = new Blob([JSON.stringify(strategyJson, null, 2) + '\n'], { type: 'application/json' });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = `${currentEditingStrategyId}.json`;
  a.click();
  URL.revokeObjectURL(url);

  showToast(`Скачан ${currentEditingStrategyId}.json — сохрани в папку strategies/`, 'success');
}

// ── Event Listeners ───────────────────────────────────────────────────────────
closeModalBtn.addEventListener('click', closeStrategyEditor);
cancelEditBtn.addEventListener('click', closeStrategyEditor);
modalOverlay.addEventListener('click', closeStrategyEditor);

document.getElementById('downloadJsonBtn')?.addEventListener('click', downloadStrategyJson);

// Prevent closing when clicking inside modal content
document.querySelector('.modal-content')?.addEventListener('click', (e) => {
  e.stopPropagation();
});

saveEditBtn.addEventListener('click', async () => {
  if (!currentEditingStrategyId) return;

  // Build override data
  const overrides = {
    description: editorDescription.value,
    entry_conditions: editorEntryConditions.value,
    exit_conditions: editorExitConditions.value,
  };

  // Only include TP/SL if they have values
  if (editorTpPercent.value) {
    overrides.tp_percent = parseFloat(editorTpPercent.value);
  }
  if (editorSlPercent.value) {
    overrides.sl_percent = parseFloat(editorSlPercent.value);
  }

  // Save to localStorage
  saveStrategyOverride(currentEditingStrategyId, overrides);

  closeStrategyEditor();

  // Show success toast
  showToast('Стратегия сохранена локально', 'success');

  // Dispatch custom event to notify dashboard to re-render
  window.dispatchEvent(new CustomEvent('strategyUpdated', {
    detail: { strategyId: currentEditingStrategyId }
  }));
});

// ── localStorage Management ───────────────────────────────────────────────────
function getStorageData() {
  try {
    const data = localStorage.getItem('tradingDashboard');
    return data ? JSON.parse(data) : {};
  } catch (err) {
    console.error('Error reading localStorage:', err);
    return {};
  }
}

function saveStorageData(data) {
  try {
    localStorage.setItem('tradingDashboard', JSON.stringify(data));
  } catch (err) {
    console.error('Error saving to localStorage:', err);
  }
}

export function getStrategyOverrides(strategyId) {
  const data = getStorageData();
  return data.strategyOverrides?.[strategyId] || {};
}

export function saveStrategyOverride(strategyId, overrides) {
  const data = getStorageData();
  if (!data.strategyOverrides) {
    data.strategyOverrides = {};
  }
  data.strategyOverrides[strategyId] = {
    ...data.strategyOverrides[strategyId],
    ...overrides,
  };
  saveStorageData(data);
}

export function getSelectedSymbol() {
  const data = getStorageData();
  return data.selectedSymbol || '';
}

export function setSelectedSymbol(symbol) {
  const data = getStorageData();
  data.selectedSymbol = symbol;
  saveStorageData(data);
}

export function getSelectedTimeframe() {
  const data = getStorageData();
  return data.selectedTimeframe || '1';
}

export function setSelectedTimeframe(tf) {
  const data = getStorageData();
  data.selectedTimeframe = tf;
  saveStorageData(data);
}

// Toast notification (re-implement if not globally available)
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
