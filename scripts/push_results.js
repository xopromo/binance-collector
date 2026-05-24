#!/usr/bin/env node
/**
 * push_results.js — Git commit & push helper
 *
 * Usage:
 *   node scripts/push_results.js [--message "custom commit message"]
 *   node scripts/push_results.js --all     # stage all tracked changes
 *
 * By default stages: docs/signals/, docs/trades/
 */

import { execSync } from 'child_process';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, '..');

const args = process.argv.slice(2);
const stageAll = args.includes('--all');

const msgIdx = args.indexOf('--message');
const customMsg = msgIdx !== -1 && args[msgIdx + 1] ? args[msgIdx + 1] : null;

function run(cmd, opts = {}) {
  return execSync(cmd, { cwd: ROOT, stdio: 'pipe', ...opts }).toString().trim();
}

function runVisible(cmd) {
  return execSync(cmd, { cwd: ROOT, stdio: 'inherit' });
}

try {
  // Stage files
  if (stageAll) {
    run('git add -u');
    console.log('📁 Добавлены все изменённые файлы');
  } else {
    const paths = [
      'docs/signals/latest.json',
      'docs/trades/log.json',
      'docs/index.html',
      'docs/css/',
      'docs/js/',
    ];
    for (const p of paths) {
      try {
        run(`git add "${p}"`);
      } catch (_) {}
    }
    console.log('📁 Добавлены файлы дашборда');
  }

  // Check if there's anything to commit
  const status = run('git status --porcelain');
  if (!status) {
    console.log('ℹ️  Нет изменений для коммита');
    process.exit(0);
  }

  // Commit
  const timestamp = new Date().toISOString().slice(0, 16).replace('T', ' ');
  const message = customMsg || `dashboard: обновление ${timestamp} UTC`;

  run(`git commit -m "${message.replace(/"/g, '\\"')}"`);
  console.log(`✅ Создан коммит: ${message}`);

  // Push
  console.log('📤 Пуш на GitHub...');
  runVisible('git push');
  console.log('✅ Опубликовано на GitHub Pages');

} catch (err) {
  console.error('❌ Ошибка:', err.message);
  process.exit(1);
}
