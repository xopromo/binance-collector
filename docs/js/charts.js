/**
 * charts.js — Chart.js PnL chart manager
 * Handles cumulative PnL visualization for all strategies
 */

const STRATEGY_COLORS = [
  { line: '#58a6ff', fill: 'rgba(88,166,255,0.08)' },   // blue
  { line: '#3fb950', fill: 'rgba(63,185,80,0.08)' },    // green
  { line: '#bc8cff', fill: 'rgba(188,140,255,0.08)' },  // purple
  { line: '#ffa657', fill: 'rgba(255,166,87,0.08)' },   // orange
  { line: '#f85149', fill: 'rgba(248,81,73,0.08)' },    // red
  { line: '#d29922', fill: 'rgba(210,153,34,0.08)' },   // yellow
];

let pnlChart = null;

/**
 * Build cumulative PnL array from trades list
 * @param {Array} trades
 * @returns {{ labels: string[], data: number[] }}
 */
function buildCumulativePnl(trades) {
  if (!trades || trades.length === 0) {
    return { labels: [], data: [] };
  }

  const sorted = [...trades].sort((a, b) => {
    const da = new Date(a.exit_time || a.entry_time || 0);
    const db = new Date(b.exit_time || b.entry_time || 0);
    return da - db;
  });

  let cumulative = 0;
  const labels = [];
  const data = [];

  // Add starting point
  labels.push('Старт');
  data.push(0);

  sorted.forEach((trade, idx) => {
    cumulative += (trade.pnl_percent || 0);
    const date = trade.exit_time || trade.entry_time || '';
    let label = `#${idx + 1}`;
    if (date) {
      try {
        const d = new Date(date);
        label = `${d.getDate().toString().padStart(2, '0')}.${(d.getMonth() + 1).toString().padStart(2, '0')}`;
      } catch (_) { /* ignore */ }
    }
    labels.push(label);
    data.push(parseFloat(cumulative.toFixed(3)));
  });

  return { labels, data };
}

/**
 * Create or update the PnL chart
 * @param {Object} tradesData — full trades/log.json content
 * @param {string|null} filterStrategy — show only this strategy id (null = all)
 */
function renderPnlChart(tradesData, filterStrategy = null) {
  const canvas = document.getElementById('pnlChart');
  if (!canvas) return;

  const strategies = tradesData?.strategies || {};
  const ids = Object.keys(strategies);

  const datasets = [];
  const allLabels = new Set(['Старт']);

  // First pass — collect all labels
  ids.forEach(id => {
    if (filterStrategy && id !== filterStrategy) return;
    const trades = strategies[id]?.trades || [];
    const { labels } = buildCumulativePnl(trades);
    labels.forEach(l => allLabels.add(l));
  });

  const labelArr = [...allLabels];

  // Second pass — build datasets aligned to common labels
  ids.forEach((id, colorIdx) => {
    if (filterStrategy && id !== filterStrategy) return;

    const stratData = strategies[id];
    if (!stratData) return;

    const { labels: tradeLabels, data: tradeData } = buildCumulativePnl(stratData.trades || []);

    // Map trade data to common label positions
    const alignedData = labelArr.map((label) => {
      const idx = tradeLabels.indexOf(label);
      return idx >= 0 ? tradeData[idx] : null;
    });

    // Fill nulls with interpolation for display
    let lastVal = 0;
    const filledData = alignedData.map(v => {
      if (v !== null) { lastVal = v; return v; }
      return lastVal;
    });

    const colorSet = STRATEGY_COLORS[colorIdx % STRATEGY_COLORS.length];

    datasets.push({
      label: stratData.name || id,
      data: filledData,
      borderColor: colorSet.line,
      backgroundColor: colorSet.fill,
      borderWidth: 2,
      pointRadius: filledData.length > 50 ? 0 : 3,
      pointHoverRadius: 5,
      tension: 0.3,
      fill: true,
    });
  });

  const chartConfig = {
    type: 'line',
    data: {
      labels: labelArr,
      datasets: datasets,
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      interaction: {
        mode: 'index',
        intersect: false,
      },
      plugins: {
        legend: {
          display: datasets.length > 1,
          position: 'top',
          labels: {
            color: '#8b949e',
            font: { size: 12 },
            boxWidth: 12,
            padding: 16,
          },
        },
        tooltip: {
          backgroundColor: '#1c2128',
          borderColor: '#30363d',
          borderWidth: 1,
          titleColor: '#e6edf3',
          bodyColor: '#8b949e',
          padding: 12,
          callbacks: {
            label: (ctx) => {
              const val = ctx.parsed.y;
              const sign = val >= 0 ? '+' : '';
              return ` ${ctx.dataset.label}: ${sign}${val.toFixed(2)}%`;
            },
          },
        },
        annotation: {},
      },
      scales: {
        x: {
          grid: { color: 'rgba(48,54,61,0.5)', drawBorder: false },
          ticks: {
            color: '#484f58',
            maxTicksLimit: 12,
            maxRotation: 0,
            font: { size: 11 },
          },
        },
        y: {
          grid: { color: 'rgba(48,54,61,0.5)', drawBorder: false },
          ticks: {
            color: '#484f58',
            font: { size: 11 },
            callback: (v) => `${v >= 0 ? '+' : ''}${v.toFixed(1)}%`,
          },
          // Zero line
          afterDataLimits: (axis) => {
            if (axis.min > -0.5) axis.min = -0.5;
          },
        },
      },
    },
  };

  if (pnlChart) {
    pnlChart.data = chartConfig.data;
    pnlChart.options = chartConfig.options;
    pnlChart.update('active');
  } else {
    pnlChart = new Chart(canvas, chartConfig);
  }
}

/**
 * Destroy chart instance (on page unload / tab switch)
 */
function destroyChart() {
  if (pnlChart) {
    pnlChart.destroy();
    pnlChart = null;
  }
}

export { renderPnlChart, destroyChart, STRATEGY_COLORS };
