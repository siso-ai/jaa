/**
 * UpdateExecuteGate — updates rows in a table.
 * Supports update by id or by where clause.
 * Phase 12: supports expression-based SET values (e.g. SET price = price + 1).
 */
import { StateGate } from '../../protocol/StateGate.js';
import { ReadSet } from '../../protocol/ReadSet.js';
import { MutationBatch } from '../../protocol/MutationBatch.js';
import { Event } from '../../core/Event.js';
import { evaluateCondition, evaluateExpression } from './expression.js';
import { scanConditionSubqueries, resolveConditionSubqueries } from '../query/sql/QueryPlanGate.js';

export class UpdateExecuteGate extends StateGate {
  constructor() { super('update_execute'); }

  reads(event) {
    const table = event.data.table;
    const rs = new ReadSet()
      .ref(`db/tables/${table}/schema`)
      .pattern(`db/tables/${table}/rows/`)
      .pattern(`db/tables/${table}/indexes/`);
    if (event.data.fromTable) rs.pattern(`db/tables/${event.data.fromTable}/rows/`);
    if (event.data.where) scanConditionSubqueries(event.data.where, rs);
    return rs;
  }

  transform(event, state) {
    const { table, changes, id, changesExprs } = event.data;
    let where = event.data.where || null;
    if (where) where = resolveConditionSubqueries(where, state);
    const schema = state.refs[`db/tables/${table}/schema`];
    const fromTable = event.data.fromTable || null;
    const fromAlias = event.data.fromAlias || fromTable;

    if (schema === null) {
      return new MutationBatch()
        .emit(new Event('error', { message: `Table '${table}' does not exist`, source: 'update_execute' }));
    }

    const allRows = state.patterns[`db/tables/${table}/rows/`] || {};

    const targets = [];
    if (fromTable) {
      const fromRows = Object.values(state.patterns[`db/tables/${fromTable}/rows/`] || {});
      for (const [refName, row] of Object.entries(allRows)) {
        for (const fromRow of fromRows) {
          const merged = { ...row };
          for (const [k, v] of Object.entries(fromRow)) {
            merged[`${fromAlias}.${k}`] = v;
            merged[`${fromTable}.${k}`] = v;
            if (!(k in merged)) merged[k] = v;
          }
          for (const [k, v] of Object.entries(row)) {
            merged[`${table}.${k}`] = v;
          }
          if (!where || evaluateCondition(where, merged)) {
            targets.push({ refName, row, context: merged });
          }
        }
      }
    } else {
      for (const [refName, row] of Object.entries(allRows)) {
        if (id !== undefined && row.id === id) {
          targets.push({ refName, row, context: row });
        } else if (where && evaluateCondition(where, row)) {
          targets.push({ refName, row, context: row });
        } else if (id === undefined && !where) {
          targets.push({ refName, row, context: row });
        }
      }
    }

    if (targets.length === 0) {
      return new MutationBatch()
        .emit(new Event('row_updated', { table, ids: [], changes }));
    }

    const batch = new MutationBatch();
    const updatedIds = [];
    const updatedRows = [];
    let putIdx = 0;

    const targetChangeMaps = new Map();
    for (const { refName, row, context } of targets) {
      const ctx = context || row;
      const rowChanges = { ...changes };
      if (changesExprs) {
        for (const [col, expr] of Object.entries(changesExprs)) {
          rowChanges[col] = evaluateExpression(expr, ctx);
        }
      }
      targetChangeMaps.set(refName, rowChanges);
      const newRow = { ...row, ...rowChanges };
      batch.put('row', newRow);
      batch.refSet(refName, putIdx++);
      updatedIds.push(row.id);
      updatedRows.push(newRow);
    }

    const indexes = state.patterns[`db/tables/${table}/indexes/`] || {};
    for (const [idxRef, index] of Object.entries(indexes)) {
      const allUpdated = {};
      for (const [refName, row] of Object.entries(allRows)) {
        const rowChanges = targetChangeMaps.get(refName);
        allUpdated[refName] = rowChanges ? { ...row, ...rowChanges } : row;
      }
      const rebuilt = rebuildIndex(index, Object.values(allUpdated));
      batch.put('btree', rebuilt);
      batch.refSet(idxRef, putIdx++);
    }

    batch.emit(new Event('row_updated', { table, ids: updatedIds, changes }));

    // RETURNING
    const returning = event.data.returning || null;
    if (returning && updatedRows.length > 0) {
      const returnedRows = updatedRows.map(row => {
        if (returning[0] === '*') return row;
        const out = {};
        for (const c of returning) out[c] = row[c] ?? null;
        return out;
      });
      batch.emit(new Event('query_result', { rows: returnedRows }));
    }

    return batch;
  }
}

export function rebuildIndex(index, rows) {
  const entries = [];
  for (const row of rows) {
    const key = row[index.column];
    const existing = entries.find(e => e.key === key);
    if (existing) {
      existing.row_ids.push(row.id);
    } else {
      entries.push({ key, row_ids: [row.id] });
    }
  }
  return { ...index, entries };
}
