#!/usr/bin/env node
/**
 * ICE Database — Interactive REPL (JavaScript)
 *
 * Usage:
 *   node ice.js                    — in-memory database
 *   node ice.js --dir ./mydb       — file-backed database
 *
 * Commands:
 *   .tables      — list all tables
 *   .schema T    — show schema for table T
 *   .quit        — exit
 *   Any SQL      — execute SQL statement
 */
import { createInterface } from 'readline';
import { Runner } from './src/resolution/Runner.js';
import { MemoryStore } from './src/persistence/Store.js';
import { MemoryRefs } from './src/persistence/Refs.js';
import { Event } from './src/core/Event.js';
import { registerDatabaseGates } from './src/gates/database/register.js';
import { registerSQLGates } from './src/gates/query/sql/register.js';

// Parse args
const args = process.argv.slice(2);
let useFile = false;
let dataDir = null;
for (let i = 0; i < args.length; i++) {
  if (args[i] === '--dir' && args[i + 1]) {
    useFile = true;
    dataDir = args[i + 1];
    i++;
  }
}

// Initialize
let store, refs;
if (useFile && dataDir) {
  // Dynamic import for file-backed persistence
  const { FileStore } = await import('./src/persistence/FileStore.js');
  const { FileRefs } = await import('./src/persistence/FileRefs.js');
  const { mkdirSync } = await import('fs');
  mkdirSync(dataDir, { recursive: true });
  store = new FileStore(dataDir);
  refs = new FileRefs(dataDir);
  console.log(`ICE Database (file: ${dataDir})`);
} else {
  store = new MemoryStore();
  refs = new MemoryRefs();
  console.log('ICE Database (in-memory)');
}

const runner = new Runner({ store, refs });
registerDatabaseGates(runner);
registerSQLGates(runner);

function sql(query) {
  runner.clearPending();
  try {
    runner.emit(new Event('sql', { sql: query }));
  } catch (e) {
    console.log(`Error: ${e.message}`);
    return;
  }
  const pending = runner.sample().pending;
  for (const event of pending) {
    if (event.type === 'query_result') {
      const rows = event.data.rows;
      if (rows.length === 0) {
        console.log('(empty result set)');
      } else {
        printTable(rows);
      }
    } else if (event.type === 'error') {
      console.log(`Error: ${event.data.message}`);
    } else if (event.type === 'table_created') {
      console.log(`Table '${event.data.table}' created.`);
    } else if (event.type === 'table_dropped') {
      console.log(`Table '${event.data.table}' dropped.`);
    } else if (event.type === 'row_inserted') {
      console.log(`Row inserted (id: ${event.data.id}).`);
    } else if (event.type === 'rows_inserted') {
      console.log(`${event.data.count} row(s) inserted into '${event.data.table}'.`);
    } else if (event.type === 'row_updated') {
      console.log(`${event.data.ids.length} row(s) updated.`);
    } else if (event.type === 'row_deleted') {
      console.log(`${event.data.ids.length} row(s) deleted.`);
    } else if (event.type === 'transaction_begun') {
      console.log('Transaction started.');
    } else if (event.type === 'transaction_committed') {
      console.log('Transaction committed.');
    } else if (event.type === 'transaction_rolled_back') {
      console.log('Transaction rolled back.');
    } else if (event.type === 'table_exists') {
      console.log(`Table '${event.data.table}' already exists (IF NOT EXISTS).`);
    }
  }
}

function printTable(rows) {
  if (rows.length === 0) return;
  const cols = Object.keys(rows[0]);
  const widths = cols.map(c => c.length);
  const strRows = rows.map(r => cols.map((c, i) => {
    const v = r[c] === null || r[c] === undefined ? 'NULL' : String(r[c]);
    if (v.length > widths[i]) widths[i] = v.length;
    return v;
  }));
  const sep = widths.map(w => '-'.repeat(w + 2)).join('+');
  console.log(cols.map((c, i) => ` ${c.padEnd(widths[i])} `).join('|'));
  console.log(sep);
  for (const row of strRows) {
    console.log(row.map((v, i) => ` ${v.padEnd(widths[i])} `).join('|'));
  }
  console.log(`(${rows.length} row${rows.length !== 1 ? 's' : ''})`);
}

function handleDotCommand(cmd) {
  const parts = cmd.trim().split(/\s+/);
  const command = parts[0].toLowerCase();

  if (command === '.quit' || command === '.exit') {
    process.exit(0);
  }

  if (command === '.tables') {
    sql("SELECT name FROM (SELECT 'tables' AS source) WHERE 1 = 0");
    // Direct state inspection
    const state = runner.sample().state || {};
    const tables = [];
    for (const key of Object.keys(state)) {
      const match = key.match(/^db\/tables\/([^/]+)\/schema$/);
      if (match) tables.push(match[1]);
    }
    if (tables.length === 0) {
      console.log('(no tables)');
    } else {
      for (const t of tables.sort()) console.log(`  ${t}`);
    }
    return;
  }

  if (command === '.schema') {
    const tableName = parts[1];
    if (!tableName) {
      console.log('Usage: .schema <table_name>');
      return;
    }
    runner.clearPending();
    // Read schema ref directly
    const refs = runner.sample().refs || {};
    const schema = refs[`db/tables/${tableName}/schema`];
    if (!schema) {
      console.log(`Table '${tableName}' not found.`);
      return;
    }
    console.log(`Table: ${schema.name}`);
    console.log(`Columns:`);
    for (const col of schema.columns || []) {
      const nullable = col.nullable ? 'NULL' : 'NOT NULL';
      const def = col.default !== null && col.default !== undefined ? ` DEFAULT ${col.default}` : '';
      console.log(`  ${col.name} ${col.type.toUpperCase()} ${nullable}${def}`);
    }
    return;
  }

  console.log(`Unknown command: ${command}`);
  console.log('Commands: .tables, .schema <table>, .quit');
}

// REPL loop
const rl = createInterface({ input: process.stdin, output: process.stdout, prompt: 'ice> ' });

rl.prompt();
let buffer = '';

rl.on('line', (line) => {
  const trimmed = line.trim();

  if (trimmed.startsWith('.')) {
    handleDotCommand(trimmed);
    rl.prompt();
    return;
  }

  buffer += (buffer ? ' ' : '') + line;

  if (buffer.trim().endsWith(';') || buffer.trim() === '') {
    if (buffer.trim()) {
      const query = buffer.trim().replace(/;$/, '');
      sql(query);
    }
    buffer = '';
    rl.prompt();
  } else {
    process.stdout.write('  -> ');
  }
});

rl.on('close', () => {
  console.log('\nBye!');
  process.exit(0);
});
