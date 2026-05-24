#!/usr/bin/env node
/**
 * collect_data.js — OHLCV data collector via TradingView MCP
 *
 * Usage:
 *   node scripts/collect_data.js
 *   node scripts/collect_data.js --symbol BINANCE:BTCUSDT.P --tf 5
 *
 * Reads all enabled strategies, fetches OHLCV for each unique symbol/timeframe,
 * appends to data/{SYMBOL}_{TF}.json (deduplicates by timestamp).
 *
 * Env vars:
 *   TV_MCP_PORT   — TradingView CDP port (default: 9222)
 *   BARS_COUNT    — how many bars to fetch (default: 500)
 */

import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { StdioClientTransport } from '@modelcontextprotocol/sdk/client/stdio.js';
import fs from 'fs/promises';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, '..');

// ── CLI flags ─────────────────────────────────────────────────────────────────
const args = process.argv.slice(2);
let symbolFilter = null;
let tfFilter     = null;

const symIdx = args.indexOf('--symbol');
if (symIdx !== -1 && args[symIdx + 1]) symbolFilter = args[symIdx + 1];

const tfIdx = args.indexOf('--tf');
if (tfIdx !== -1 && args[tfIdx + 1]) tfFilter = args[tfIdx + 1];

const BARS_COUNT   = parseInt(process.env.BARS_COUNT || '500', 10);
const STRATEGIES_DIR = path.join(ROOT, 'strategies');
const DATA_DIR       = path.join(ROOT, 'data');

// ── MCP client ────────────────────────────────────────────────────────────────
let mcpClient = null;

async function connectMcp() {
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

  mcpClient = new Client({ name: 'data-collector', version: '1.0.0' });

  try {
    await mcpClient.connect(transport);
    console.log('✅ MCP подключён');
    return true;
  } catch (err) {
    console.error('❌ Не удалось подключиться к TradingView MCP:', err.message);
    console.error('   Убедитесь что TradingView Desktop запущен с CDP (порт 9222)');
    return false;
  }
}

async function callTool(name, toolArgs = {}) {
  if (!mcpClient) throw new Error('MCP не подключён');
  const result = await mcpClient.callTool({ name, arguments: toolArgs });
  const text = result?.content?.[0]?.text;
  if (!text) return null;
  return JSON.parse(text);
}

// ── Load strategies → collect unique symbol/TF pairs ─────────────────────────
async function loadSymbolPairs() {
  const files = await fs.readdir(STRATEGIES_DIR);
  const stratFiles = files.filter(f => f.endsWith('.json') && f.startsWith('strategy_'));

  const pairs = new Map(); // key = "SYMBOL_TF"

  for (const file of stratFiles) {
    try {
      const content = await fs.readFile(path.join(STRATEGIES_DIR, file), 'utf-8');
      const s = JSON.parse(content);
      if (!s.enabled) continue;

      // Apply CLI filters
      if (symbolFilter && s.symbol !== symbolFilter) continue;
      if (tfFilter && String(s.timeframe) !== String(tfFilter)) continue;

      const key = `${s.symbol}_${s.timeframe}`;
      if (!pairs.has(key)) {
        pairs.set(key, { symbol: s.symbol, timeframe: String(s.timeframe) });
      }
    } catch (err) {
      console.warn(`⚠️  Не удалось прочитать ${file}:`, err.message);
    }
  }

  return [...pairs.values()];
}

// ── Fetch OHLCV from TradingView ──────────────────────────────────────────────
async function fetchOhlcv(symbol, timeframe) {
  console.log(`  📡 Загрузка ${symbol} [${timeframe}m] — ${BARS_COUNT} баров...`);

  // Switch chart to target symbol/timeframe
  await callTool('chart_set_symbol', { symbol });
  await callTool('chart_set_timeframe', { timeframe });

  // Wait for chart to load
  await new Promise(r => setTimeout(r, 2500));

  // Fetch OHLCV data (no summary = raw bars)
  const result = await callTool('data_get_ohlcv', {
    count:   BARS_COUNT,
    summary: false,
  });

  if (!result || !result.success) {
    throw new Error(`data_get_ohlcv вернул ошибку: ${JSON.stringify(result)}`);
  }

  return result.bars || result.data || [];
}

// ── Read existing data file ────────────────────────────────────────────────────
async function readExistingData(filePath) {
  try {
    const content = await fs.readFile(filePath, 'utf-8');
    const parsed = JSON.parse(content);
    return Array.isArray(parsed.bars) ? parsed.bars : [];
  } catch (_) {
    return [];
  }
}

// ── Deduplicate bars by timestamp ─────────────────────────────────────────────
function deduplicateBars(bars) {
  const seen = new Map();
  for (const bar of bars) {
    const ts = bar.time || bar.timestamp || bar.t;
    if (ts !== undefined) {
      seen.set(ts, bar); // later entries overwrite earlier (more recent data wins)
    }
  }
  // Sort ascending by timestamp
  return [...seen.values()].sort((a, b) => {
    const ta = a.time || a.timestamp || a.t || 0;
    const tb = b.time || b.timestamp || b.t || 0;
    return ta - tb;
  });
}

// ── Build safe filename ───────────────────────────────────────────────────────
function buildFileName(symbol, timeframe) {
  // BINANCE:BTCUSDT.P → BINANCE_BTCUSDT_P
  const safe = symbol.replace(/[:/\.]/g, '_').replace(/_+/g, '_');
  return `${safe}_${timeframe}.json`;
}

// ── Save merged data ──────────────────────────────────────────────────────────
async function saveData(symbol, timeframe, newBars) {
  await fs.mkdir(DATA_DIR, { recursive: true });
  const fileName = buildFileName(symbol, timeframe);
  const filePath = path.join(DATA_DIR, fileName);

  const existing = await readExistingData(filePath);
  const merged = deduplicateBars([...existing, ...newBars]);

  const payload = {
    symbol,
    timeframe,
    updated_at: new Date().toISOString(),
    bars_count: merged.length,
    bars: merged,
  };

  await fs.writeFile(filePath, JSON.stringify(payload, null, 2) + '\n', 'utf-8');

  const newCount = merged.length - existing.length;
  console.log(`  ✅ Сохранено: ${fileName} (всего ${merged.length} баров, +${Math.max(0, newCount)} новых)`);
  return merged.length;
}

// ── Main ──────────────────────────────────────────────────────────────────────
async function main() {
  console.log('🚀 Запуск сборщика OHLCV данных...');
  console.log(`   Время: ${new Date().toISOString()}`);

  const pairs = await loadSymbolPairs();
  if (pairs.length === 0) {
    console.log('ℹ️  Нет символов для сбора данных');
    process.exit(0);
  }

  console.log(`📋 Символов для сбора: ${pairs.length}`);
  pairs.forEach(p => console.log(`   - ${p.symbol} [${p.timeframe}m]`));

  // Connect to TradingView MCP
  const connected = await connectMcp();
  if (!connected) {
    process.exit(1);
  }

  let successCount = 0;
  let errorCount   = 0;
  let totalBars    = 0;

  for (const pair of pairs) {
    console.log(`\n📊 Обработка: ${pair.symbol} [${pair.timeframe}m]`);
    try {
      const bars   = await fetchOhlcv(pair.symbol, pair.timeframe);
      const count  = await saveData(pair.symbol, pair.timeframe, bars);
      totalBars   += count;
      successCount++;
    } catch (err) {
      console.error(`  ❌ Ошибка: ${err.message}`);
      errorCount++;
    }
  }

  // Disconnect
  if (mcpClient) {
    try { await mcpClient.close(); } catch (_) {}
  }

  console.log(`\n✅ Сбор завершён:`);
  console.log(`   Успешно: ${successCount}/${pairs.length}`);
  console.log(`   Ошибок:  ${errorCount}`);
  console.log(`   Итого баров в хранилище: ${totalBars}`);

  if (errorCount > 0) process.exit(1);
}

main().catch(err => {
  console.error('❌ Критическая ошибка:', err);
  process.exit(1);
});
