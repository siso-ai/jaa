/**
 * JoinGate — joins two tables.
 * Supports inner, left, right, full join types.
 *
 * Exports joinRows() for direct use by QueryPlanGate (Phase 5).
 */
import { StateGate } from '../../protocol/StateGate.js';
import { ReadSet } from '../../protocol/ReadSet.js';
import { MutationBatch } from '../../protocol/MutationBatch.js';
import { Event } from '../../core/Event.js';

export function joinRows(leftRows, rightRows, on, type = 'inner', leftTable = '', rightTable = '') {
  const results = [];
  const rightMatched = new Set();

  // CROSS JOIN — cartesian product
  if (type === 'cross' || (on === null && type === 'inner')) {
    for (const left of leftRows) {
      for (const right of rightRows) {
        results.push(mergeJoinRow(left, right, leftTable, rightTable));
      }
    }
    return results;
  }

  for (const left of leftRows) {
    let matched = false;
    for (let i = 0; i < rightRows.length; i++) {
      const right = rightRows[i];
      if (matchesJoin(left, right, on)) {
        results.push(mergeJoinRow(left, right, leftTable, rightTable));
        rightMatched.add(i);
        matched = true;
      }
    }
    if (!matched && (type === 'left' || type === 'full')) {
      results.push(mergeJoinRow(left, nullRow(rightRows), leftTable, rightTable));
    }
  }

  if (type === 'right' || type === 'full') {
    for (let i = 0; i < rightRows.length; i++) {
      if (!rightMatched.has(i)) {
        results.push(mergeJoinRow(nullRow(leftRows), rightRows[i], leftTable, rightTable));
      }
    }
  }

  return results;
}

function resolveCol(row, col) {
  if (col in row) return row[col];
  const dot = col.indexOf('.');
  if (dot !== -1) {
    const short = col.substring(dot + 1);
    if (short in row) return row[short];
  }
  return undefined;
}

function matchesJoin(left, right, on) {
  if (Array.isArray(on)) {
    return on.every(o => resolveCol(left, o.left) === resolveCol(right, o.right));
  }
  return resolveCol(left, on.left) === resolveCol(right, on.right);
}

function mergeJoinRow(left, right, leftTable, rightTable) {
  const merged = {};
  // Add all left columns with qualified names
  for (const [key, value] of Object.entries(left)) {
    merged[key] = value;
    if (leftTable) merged[`${leftTable}.${key}`] = value;
  }
  // Add right columns with qualified names; on conflict, don't overwrite base key
  for (const [key, value] of Object.entries(right)) {
    if (rightTable) merged[`${rightTable}.${key}`] = value;
    if (!(key in merged)) {
      merged[key] = value;
    }
  }
  return merged;
}

function nullRow(sampleRows) {
  if (sampleRows.length === 0) return {};
  const row = {};
  for (const key of Object.keys(sampleRows[0])) {
    row[key] = null;
  }
  return row;
}

export class JoinGate extends StateGate {
  constructor() { super('join'); }

  reads(event) {
    return new ReadSet()
      .pattern(`db/tables/${event.data.left.table}/rows/`)
      .pattern(`db/tables/${event.data.right.table}/rows/`);
  }

  transform(event, state) {
    const { left, right, on, type } = event.data;
    const leftRows = Object.values(state.patterns[`db/tables/${left.table}/rows/`] || {});
    const rightRows = Object.values(state.patterns[`db/tables/${right.table}/rows/`] || {});
    const rows = joinRows(leftRows, rightRows, on, type || 'inner');
    return new MutationBatch()
      .emit(new Event('join_result', { rows }));
  }
}
