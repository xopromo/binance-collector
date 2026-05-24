#!/usr/bin/env node
/**
 * run_analysis.js — Multi-strategy analysis runner
 *
 * Usage:
 *   node scripts/run_analysis.js                      # Run all enabled strategies
 *   node scripts/run_analysis.js --strategy 001       # Run only strategy_001
 *   node scripts/run_analysis.js --strategy strategy_002
 *
 * Env vars:
 *   ANTHROPIC_API_KEY   — required
 *   TV_MCP_PORT         — TradingView MCP port (default: 9222)
 *   DRY_RUN=1           — skip git push
 */

import Anthropic from '@anthropic-ai/sdk';
import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { StdioClientTransport } from '@modelcontextprotocol/sdk/client/stdio.js';
import fs from 'fs/promises';
import path from 'path';
import { fileURLToPath } from 'url';
import { execSync, spawn } from 'child_process';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, '..');

// ── CLI flags ─────────────────────────────────────────────────────────────────
const args = process.argv.slice(2);
let strategyFilter = null;
const stratIdx = args.indexOf('--strategy');
if (stratIdx !== -1 && args[stratIdx + 1]) {
  let s = args[stratIdx + 1];
  if (!s.startsWith('strategy_')) s = `strategy_${s}`;
  strategyFilter = s;
}
const DRY_RUN = process.env.DRY_RUN === '1';

// ── Anthropic clients ─────────────────────────────────────────────────────────
const anthropic = new Anthropic({
  apiKey: process.env.ANTHROPIC_API_KEY,
});

const MODEL_ANALYSIS  = 'claude-sonnet-4-6';
const MODEL_FORMATTER = 'claude-haiku-4-5-20251001';

// ── Paths ─────────────────────────────────────────────────────────────────────
const STRATEGIES_DIR  = path.join(ROOT, 'strategies');
const SIGNALS_PATH    = path.join(ROOT, 'docs', 'signals', 'latest.json');
const TRADES_PATH     = path.join(ROOT, 'docs', 'trades', 'log.json');

// ── MCP client ────────────────────────────────────────────────────────────────
let mcpClient = null;

async function startMcpClient() {
  const tvMcpPath = path.join(ROOT, 'tradingview-mcp', 'src', 'server.js');

  console.log('🔌 Подключение к TradingView MCP...');
  const transport = new StdioClientTransport({
    command: 'node',
    args: [tvMcpPath],
    env: {
      ...process.env,
      CDP_PORT: process.env.TV_MCP_PORT || '9222',
    },
  });

  mcpClient = new Client({ name: 'trading-dashboard', version: '1.0.0' });

  try {
    await mcpClient.connect(transport);
    console.log('✅ MCP подключён');
    return true;
  } catch (err) {
    console.warn('⚠️  Не удалось подключиться к TradingView MCP:', err.message);
    console.warn('   Анализ будет выполнен без реальных данных графика');
    mcpClient = null;
    return false;
  }
}

async function callMcpTool(name, args = {}) {
  if (!mcpClient) return null;
  try {
    const result = await mcpClient.callTool({ name, arguments: args });
    return result?.content?.[0]?.text ? JSON.parse(result.content[0].text) : null;
  } catch (err) {
    console.warn(`  ⚠️  MCP ${name} ошибка:`, err.message);
    return null;
  }
}

// ── Chart data gathering ──────────────────────────────────────────────────────
async function getChartData(symbol, timeframe) {
  const result = {
    symbol,
    timeframe,
    quote: null,
    indicators: null,
    ohlcv: null,
    connected: mcpClient !== null,
  };

  if (!mcpClient) return result;

  console.log(`  📊 Получение данных: ${symbol} [${timeframe}m]`);

  // Switch to required chart
  await callMcpTool('chart_set_symbol', { symbol });
  await callMcpTool('chart_set_timeframe', { timeframe: String(timeframe) });

  // Small delay to let chart load
  await new Promise(r => setTimeout(r, 2000));

  // Gather data in parallel
  const [quote, indicators, ohlcv] = await Promise.all([
    callMcpTool('quote_get', {}),
    callMcpTool('data_get_study_values', {}),
    callMcpTool('data_get_ohlcv', { summary: true }),
  ]);

  result.quote      = quote;
  result.indicators = indicators;
  result.ohlcv      = ohlcv;

  return result;
}

// ── Haiku: format chart data into clean text for Sonnet ──────────────────────
async function formatChartDataForAnalysis(chartData, strategy) {
  const prompt = `Ты форматировщик данных. Преобразуй эти сырые данные графика в чёткое краткое резюме для анализа стратегии.

Стратегия: ${strategy.name}
Символ: ${strategy.symbol}
Таймфрейм: ${strategy.timeframe}

Данные графика (сырые):
${JSON.stringify(chartData, null, 2)}

Верни ТОЛЬКО краткое текстовое резюме (максимум 300 слов):
- Текущая цена
- Значения индикаторов (RSI, EMA, MACD, BB и т.д. если доступны)
- Краткий обзор ценового действия (последние бары)
- НЕ делай торговых выводов — только факты

Если данные отсутствуют или MCP не подключён — напиши "Данные недоступны, анализ по общей логике стратегии."`;

  try {
    const resp = await anthropic.messages.create({
      model: MODEL_FORMATTER,
      max_tokens: 500,
      messages: [{ role: 'user', content: prompt }],
    });
    return resp.content[0].text;
  } catch (err) {
    console.warn('  ⚠️  Haiku форматирование ошибка:', err.message);
    return 'Данные недоступны.';
  }
}

// ── Sonnet: analyze and generate signal ──────────────────────────────────────
async function analyzeStrategy(strategy, chartSummary) {
  const systemPrompt = `Ты торговый аналитик. Анализируй рыночные данные и генерируй торговые сигналы строго по правилам стратегии.

ВАЖНО: Отвечай ТОЛЬКО валидным JSON без markdown-блоков и дополнительного текста.

Формат ответа:
{
  "signal": "LONG" | "SHORT" | "NO_SIGNAL",
  "confidence": "HIGH" | "MEDIUM" | "LOW",
  "reasoning": "Краткое объяснение на русском (2-3 предложения)",
  "price": <текущая цена или 0>,
  "tp": <уровень take profit или 0>,
  "sl": <уровень stop loss или 0>
}`;

  const userPrompt = `Стратегия: ${strategy.name}
Символ: ${strategy.symbol} | Таймфрейм: ${strategy.timeframe}M
TP: +${strategy.tp_percent}% | SL: -${strategy.sl_percent}%

Описание стратегии:
${strategy.description}

Условия входа:
${strategy.entry_conditions}

Условия выхода:
${strategy.exit_conditions}

Текущие рыночные данные:
${chartSummary}

Проанализируй данные и выдай торговый сигнал в указанном JSON формате.
Если данных недостаточно для уверенного сигнала — верни NO_SIGNAL с LOW confidence.`;

  try {
    const resp = await anthropic.messages.create({
      model: MODEL_ANALYSIS,
      max_tokens: 600,
      system: systemPrompt,
      messages: [{ role: 'user', content: userPrompt }],
    });

    let text = resp.content[0].text.trim();
    // Strip markdown code blocks if present
    text = text.replace(/^```(?:json)?\s*/i, '').replace(/\s*```$/i, '');

    const parsed = JSON.parse(text);

    // Validate and compute TP/SL if not provided
    const price = parseFloat(parsed.price) || 0;
    if (price > 0) {
      if (!parsed.tp || parsed.tp === 0) {
        const tpMultiplier = parsed.signal === 'LONG'
          ? 1 + strategy.tp_percent / 100
          : 1 - strategy.tp_percent / 100;
        parsed.tp = parseFloat((price * tpMultiplier).toPrecision(6));
      }
      if (!parsed.sl || parsed.sl === 0) {
        const slMultiplier = parsed.signal === 'LONG'
          ? 1 - strategy.sl_percent / 100
          : 1 + strategy.sl_percent / 100;
        parsed.sl = parseFloat((price * slMultiplier).toPrecision(6));
      }
    }

    return {
      signal:       parsed.signal || 'NO_SIGNAL',
      confidence:   parsed.confidence || 'LOW',
      reasoning:    parsed.reasoning || '',
      price:        price,
      tp:           parseFloat(parsed.tp) || 0,
      sl:           parseFloat(parsed.sl) || 0,
      generated_at: new Date().toISOString(),
    };
  } catch (err) {
    console.error('  ❌ Ошибка анализа:', err.message);
    return {
      signal:       'NO_SIGNAL',
      confidence:   'LOW',
      reasoning:    `Ошибка анализа: ${err.message}`,
      price:        0,
      tp:           0,
      sl:           0,
      generated_at: new Date().toISOString(),
    };
  }
}

// ── Load strategies ───────────────────────────────────────────────────────────
async function loadStrategies() {
  const files = await fs.readdir(STRATEGIES_DIR);
  const strategyFiles = files.filter(f => f.endsWith('.json') && f.startsWith('strategy_'));

  const strategies = [];
  for (const file of strategyFiles) {
    try {
      const content = await fs.readFile(path.join(STRATEGIES_DIR, file), 'utf-8');
      const s = JSON.parse(content);
      strategies.push(s);
    } catch (err) {
      console.warn(`⚠️  Не удалось прочитать ${file}:`, err.message);
    }
  }

  return strategies.filter(s => {
    if (!s.enabled) return false;
    if (strategyFilter && s.id !== strategyFilter) return false;
    return true;
  });
}

// ── Read / write JSON helpers ─────────────────────────────────────────────────
async function readJson(filePath, defaultValue = {}) {
  try {
    const content = await fs.readFile(filePath, 'utf-8');
    return JSON.parse(content);
  } catch (_) {
    return defaultValue;
  }
}

async function writeJson(filePath, data) {
  await fs.mkdir(path.dirname(filePath), { recursive: true });
  await fs.writeFile(filePath, JSON.stringify(data, null, 2) + '\n', 'utf-8');
}

// ── Update signals ────────────────────────────────────────────────────────────
async function updateSignals(strategyId, signal) {
  const existing = await readJson(SIGNALS_PATH, { updated_at: '', strategies: {} });
  existing.strategies[strategyId] = signal;
  existing.updated_at = new Date().toISOString();
  await writeJson(SIGNALS_PATH, existing);
}

// ── Update trades log (adds stats entry if missing) ───────────────────────────
async function ensureTradesEntry(strategy) {
  const data = await readJson(TRADES_PATH, { strategies: {} });

  if (!data.strategies[strategy.id]) {
    data.strategies[strategy.id] = {
      name:      strategy.name,
      symbol:    strategy.symbol,
      timeframe: strategy.timeframe,
      trades:    [],
      stats: {
        total_trades:      0,
        winning_trades:    0,
        losing_trades:     0,
        win_rate:          0,
        total_pnl_percent: 0,
        max_drawdown:      0,
        sharpe_ratio:      0,
      },
    };
    await writeJson(TRADES_PATH, data);
  }
}

// ── Git push ──────────────────────────────────────────────────────────────────
function gitPush() {
  if (DRY_RUN) {
    console.log('🚫 DRY_RUN: пропуск git push');
    return;
  }

  try {
    console.log('📤 Git: коммит и пуш...');
    execSync(`git -C "${ROOT}" add docs/signals/latest.json docs/trades/log.json`, { stdio: 'pipe' });
    const timestamp = new Date().toISOString().slice(0, 16).replace('T', ' ');
    execSync(`git -C "${ROOT}" commit -m "signals: ${timestamp} UTC" --allow-empty`, { stdio: 'pipe' });
    execSync(`git -C "${ROOT}" push`, { stdio: 'inherit' });
    console.log('✅ Данные опубликованы на GitHub Pages');
  } catch (err) {
    console.warn('⚠️  Git push ошибка:', err.message);
  }
}

// ── Main ──────────────────────────────────────────────────────────────────────
async function main() {
  console.log('🚀 Запуск анализа торговых стратегий...');
  console.log(`   Время: ${new Date().toISOString()}`);
  if (strategyFilter) console.log(`   Фильтр: ${strategyFilter}`);

  // Check API key
  if (!process.env.ANTHROPIC_API_KEY) {
    console.error('❌ ANTHROPIC_API_KEY не установлен');
    process.exit(1);
  }

  // Load strategies
  const strategies = await loadStrategies();
  if (strategies.length === 0) {
    console.log('ℹ️  Нет активных стратегий для анализа');
    process.exit(0);
  }

  console.log(`📋 Стратегий для анализа: ${strategies.length}`);
  strategies.forEach(s => console.log(`   - ${s.id}: ${s.name}`));

  // Connect to TradingView MCP
  await startMcpClient();

  // Process each strategy
  let anySignal = false;
  for (const strategy of strategies) {
    console.log(`\n📊 Анализ: ${strategy.name} (${strategy.symbol})`);

    try {
      // Ensure trades entry exists
      await ensureTradesEntry(strategy);

      // Get chart data
      const chartData = await getChartData(strategy.symbol, strategy.timeframe);

      // Format with Haiku (low-level formatter)
      const chartSummary = await formatChartDataForAnalysis(chartData, strategy);
      console.log('  📝 Резюме данных готово');

      // Analyze with Sonnet (high-level analyst)
      const signal = await analyzeStrategy(strategy, chartSummary);
      console.log(`  🎯 Сигнал: ${signal.signal} (${signal.confidence}) — ${signal.reasoning.slice(0, 80)}...`);

      // Save signal
      await updateSignals(strategy.id, signal);

      if (signal.signal !== 'NO_SIGNAL') anySignal = true;
    } catch (err) {
      console.error(`  ❌ Ошибка при анализе ${strategy.id}:`, err.message);
    }
  }

  // Disconnect MCP
  if (mcpClient) {
    try { await mcpClient.close(); } catch (_) {}
  }

  // Publish to GitHub Pages
  gitPush();

  console.log(`\n✅ Анализ завершён. ${anySignal ? '🔔 Есть новые сигналы!' : 'Сигналов нет.'}`);
}

main().catch(err => {
  console.error('❌ Критическая ошибка:', err);
  process.exit(1);
});
