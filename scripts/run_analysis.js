#!/usr/bin/env node
/**
 * run_analysis.js — Multi-strategy analysis runner
 *
 * Использует Claude Code CLI (Team подписка, без отдельного API ключа).
 * Модели:
 *   - claude-haiku-4-5-20251001  → форматирование данных (дёшево)
 *   - claude-sonnet-4-6          → торговые решения (точно)
 *
 * Использование:
 *   node scripts/run_analysis.js                    # все стратегии
 *   node scripts/run_analysis.js --strategy 001     # только strategy_001
 *   DRY_RUN=1 node scripts/run_analysis.js          # без git push
 */

import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { StdioClientTransport } from '@modelcontextprotocol/sdk/client/stdio.js';
import fs from 'fs/promises';
import path from 'path';
import { fileURLToPath } from 'url';
import { execSync, execFileSync } from 'child_process';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, '..');

// ── CLI флаги ─────────────────────────────────────────────────────────────────
const args = process.argv.slice(2);
let strategyFilter = null;
const stratIdx = args.indexOf('--strategy');
if (stratIdx !== -1 && args[stratIdx + 1]) {
  let s = args[stratIdx + 1];
  if (!s.startsWith('strategy_')) s = `strategy_${s}`;
  strategyFilter = s;
}
const DRY_RUN = process.env.DRY_RUN === '1';

// ── Модели ────────────────────────────────────────────────────────────────────
const MODEL_FORMATTER = 'claude-haiku-4-5-20251001';  // дешёвый — для обработки данных
const MODEL_ANALYSIS  = 'claude-sonnet-4-6';           // мощный — для торговых решений

// ── Пути ──────────────────────────────────────────────────────────────────────
const STRATEGIES_DIR = path.join(ROOT, 'strategies');
const SIGNALS_PATH   = path.join(ROOT, 'docs', 'signals', 'latest.json');
const TRADES_PATH    = path.join(ROOT, 'docs', 'trades', 'log.json');

// ── Вызов Claude Code CLI ─────────────────────────────────────────────────────
function callClaude(prompt, model) {
  try {
    const result = execFileSync('claude', [
      '--model', model,
      '-p', prompt,
      '--output-format', 'text',
    ], {
      encoding: 'utf-8',
      timeout: 60000,
      maxBuffer: 1024 * 1024,
    });
    return result.trim();
  } catch (err) {
    const msg = err.stdout?.trim() || err.message;
    throw new Error(`Claude CLI ошибка (${model}): ${msg}`);
  }
}

// ── MCP клиент ────────────────────────────────────────────────────────────────
let mcpClient = null;

async function startMcpClient() {
  const tvMcpPath = path.join(ROOT, 'tradingview-mcp', 'src', 'server.js');

  console.log('🔌 Подключение к TradingView MCP...');
  const transport = new StdioClientTransport({
    command: 'node',
    args: [tvMcpPath],
    env: { ...process.env },
  });

  mcpClient = new Client({ name: 'trading-dashboard', version: '1.0.0' });

  try {
    await mcpClient.connect(transport);
    console.log('✅ TradingView MCP подключён');
    return true;
  } catch (err) {
    console.warn('⚠️  TradingView MCP недоступен:', err.message);
    console.warn('   Запусти TradingView с флагом --remote-debugging-port=9222');
    mcpClient = null;
    return false;
  }
}

async function callMcpTool(name, toolArgs = {}) {
  if (!mcpClient) return null;
  try {
    const result = await mcpClient.callTool({ name, arguments: toolArgs });
    const text = result?.content?.[0]?.text;
    return text ? JSON.parse(text) : null;
  } catch (err) {
    console.warn(`  ⚠️  MCP ${name}:`, err.message);
    return null;
  }
}

// ── Сбор данных с TradingView ─────────────────────────────────────────────────
async function getChartData(symbol, timeframe) {
  const result = { symbol, timeframe, quote: null, indicators: null, ohlcv: null };

  if (!mcpClient) return result;

  console.log(`  📊 Получение данных: ${symbol} [${timeframe}m]`);

  await callMcpTool('chart_set_symbol', { symbol });
  await callMcpTool('chart_set_timeframe', { timeframe: String(timeframe) });
  await new Promise(r => setTimeout(r, 2000));

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

// ── Haiku: форматирование данных (экономия токенов) ───────────────────────────
async function formatChartData(chartData, strategy) {
  const prompt = `Ты форматировщик данных. Преобразуй сырые данные графика в краткое резюме.

Стратегия: ${strategy.name} | Символ: ${strategy.symbol} | TF: ${strategy.timeframe}m

Данные:
${JSON.stringify(chartData, null, 2)}

Верни ТОЛЬКО краткое резюме (максимум 200 слов):
- Текущая цена
- Значения индикаторов (RSI, EMA, MACD и др. если есть)
- Последние 3-5 баров: направление, объём
- БЕЗ торговых выводов — только факты

Если данных нет — напиши "Данные недоступны."`;

  try {
    console.log(`  🤖 Haiku форматирует данные...`);
    return callClaude(prompt, MODEL_FORMATTER);
  } catch (err) {
    console.warn('  ⚠️  Haiku недоступен:', err.message);
    return 'Данные недоступны.';
  }
}

// ── Sonnet: торговый анализ и сигнал ──────────────────────────────────────────
async function analyzeStrategy(strategy, chartSummary) {
  const prompt = `Ты торговый аналитик. Генерируй сигналы строго по правилам стратегии.

ВАЖНО: Отвечай ТОЛЬКО валидным JSON без markdown и лишнего текста.

Формат ответа:
{"signal":"LONG"|"SHORT"|"NO_SIGNAL","confidence":"HIGH"|"MEDIUM"|"LOW","reasoning":"2-3 предложения на русском","price":0.0,"tp":0.0,"sl":0.0}

Стратегия: ${strategy.name}
Символ: ${strategy.symbol} | TF: ${strategy.timeframe}m
TP: +${strategy.tp_percent}% | SL: -${strategy.sl_percent}%

Описание: ${strategy.description}
Условия входа: ${strategy.entry_conditions}
Условия выхода: ${strategy.exit_conditions}

Рыночные данные:
${chartSummary}

Выдай торговый сигнал в JSON формате.`;

  try {
    console.log(`  🤖 Sonnet анализирует стратегию...`);
    let text = callClaude(prompt, MODEL_ANALYSIS);

    // Убираем markdown если есть
    text = text.replace(/^```(?:json)?\s*/i, '').replace(/\s*```$/i, '').trim();

    const parsed = JSON.parse(text);
    const price  = parseFloat(parsed.price) || 0;

    // Вычисляем TP/SL если не указаны
    if (price > 0) {
      if (!parsed.tp) {
        parsed.tp = parseFloat((price * (parsed.signal === 'LONG'
          ? 1 + strategy.tp_percent / 100
          : 1 - strategy.tp_percent / 100)).toPrecision(6));
      }
      if (!parsed.sl) {
        parsed.sl = parseFloat((price * (parsed.signal === 'LONG'
          ? 1 - strategy.sl_percent / 100
          : 1 + strategy.sl_percent / 100)).toPrecision(6));
      }
    }

    return {
      signal:       parsed.signal       || 'NO_SIGNAL',
      confidence:   parsed.confidence   || 'LOW',
      reasoning:    parsed.reasoning    || '',
      price,
      tp:           parseFloat(parsed.tp) || 0,
      sl:           parseFloat(parsed.sl) || 0,
      generated_at: new Date().toISOString(),
    };
  } catch (err) {
    console.error('  ❌ Ошибка анализа:', err.message);
    return {
      signal: 'NO_SIGNAL', confidence: 'LOW',
      reasoning: `Ошибка: ${err.message}`,
      price: 0, tp: 0, sl: 0,
      generated_at: new Date().toISOString(),
    };
  }
}

// ── Вспомогательные функции ───────────────────────────────────────────────────
async function readJson(filePath, def = {}) {
  try { return JSON.parse(await fs.readFile(filePath, 'utf-8')); }
  catch (_) { return def; }
}

async function writeJson(filePath, data) {
  await fs.mkdir(path.dirname(filePath), { recursive: true });
  await fs.writeFile(filePath, JSON.stringify(data, null, 2) + '\n', 'utf-8');
}

async function loadStrategies() {
  const files = await fs.readdir(STRATEGIES_DIR);
  const all   = [];
  for (const f of files.filter(f => f.endsWith('.json') && f.startsWith('strategy_'))) {
    try {
      all.push(JSON.parse(await fs.readFile(path.join(STRATEGIES_DIR, f), 'utf-8')));
    } catch (e) { console.warn(`⚠️  Ошибка чтения ${f}:`, e.message); }
  }
  return all.filter(s => s.enabled && (!strategyFilter || s.id === strategyFilter));
}

async function ensureTradesEntry(strategy) {
  const data = await readJson(TRADES_PATH, { strategies: {} });
  if (!data.strategies[strategy.id]) {
    data.strategies[strategy.id] = {
      name: strategy.name, symbol: strategy.symbol, timeframe: strategy.timeframe,
      trades: [],
      stats: { total_trades: 0, winning_trades: 0, losing_trades: 0,
               win_rate: 0, total_pnl_percent: 0, max_drawdown: 0, sharpe_ratio: 0 },
    };
    await writeJson(TRADES_PATH, data);
  }
}

function gitPush() {
  if (DRY_RUN) { console.log('🚫 DRY_RUN: пропуск git push'); return; }
  try {
    console.log('📤 Публикация на GitHub Pages...');
    execSync(`git -C "${ROOT}" add docs/signals/latest.json docs/trades/log.json`, { stdio: 'pipe' });
    const ts = new Date().toISOString().slice(0, 16).replace('T', ' ');
    execSync(`git -C "${ROOT}" commit -m "signals: ${ts} UTC" --allow-empty`, { stdio: 'pipe' });
    execSync(`git -C "${ROOT}" push`, { stdio: 'inherit' });
    console.log('✅ Опубликовано');
  } catch (err) { console.warn('⚠️  Git push ошибка:', err.message); }
}

// ── Главная функция ───────────────────────────────────────────────────────────
async function main() {
  console.log('🚀 Анализ торговых стратегий');
  console.log(`   ${new Date().toISOString()}`);
  console.log(`   Форматирование: ${MODEL_FORMATTER}`);
  console.log(`   Анализ:         ${MODEL_ANALYSIS}`);
  if (strategyFilter) console.log(`   Стратегия:      ${strategyFilter}`);

  // Проверка Claude CLI
  try {
    execSync('claude --version', { stdio: 'pipe' });
  } catch (_) {
    console.error('❌ claude CLI не найден. Установи: npm install -g @anthropic-ai/claude-code');
    process.exit(1);
  }

  const strategies = await loadStrategies();
  if (strategies.length === 0) {
    console.log('ℹ️  Нет активных стратегий');
    process.exit(0);
  }

  console.log(`\n📋 Стратегий: ${strategies.length}`);
  strategies.forEach(s => console.log(`   • ${s.id}: ${s.name}`));

  await startMcpClient();

  let hasSignal = false;
  for (const strategy of strategies) {
    console.log(`\n━━━ ${strategy.name} (${strategy.symbol} ${strategy.timeframe}m) ━━━`);
    try {
      await ensureTradesEntry(strategy);
      const chartData    = await getChartData(strategy.symbol, strategy.timeframe);
      const chartSummary = await formatChartData(chartData, strategy);
      const signal       = await analyzeStrategy(strategy, chartSummary);

      console.log(`  🎯 ${signal.signal} [${signal.confidence}] — ${signal.reasoning.slice(0, 80)}...`);

      const signals = await readJson(SIGNALS_PATH, { updated_at: '', strategies: {} });
      signals.strategies[strategy.id] = signal;
      signals.updated_at = new Date().toISOString();
      await writeJson(SIGNALS_PATH, signals);

      if (signal.signal !== 'NO_SIGNAL') hasSignal = true;
    } catch (err) {
      console.error(`  ❌ ${strategy.id}:`, err.message);
    }
  }

  if (mcpClient) try { await mcpClient.close(); } catch (_) {}

  gitPush();

  console.log(`\n${'━'.repeat(50)}`);
  console.log(`✅ Готово. ${hasSignal ? '🔔 Есть сигналы!' : 'Сигналов нет.'}`);
}

main().catch(err => { console.error('❌', err.message); process.exit(1); });
