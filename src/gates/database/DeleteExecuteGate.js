/**
 * DeleteExecuteGate — deletes rows from a table.
 * Supports delete by id or by where clause.
 */
import { StateGate } from '../../protocol/StateGate.js';
import { ReadSet } from '../../protocol/ReadSet.js';
import { MutationBatch } from '../../protocol/MutationBatch.js';
import { Event } from '../../core/Event.js';
import { evaluateCondition } from './expression.js';
import { scanConditionSubqueries, resolveConditionSubqueries } from '../query/sql/QueryPlanGate.js';

export class DeleteExecuteGate extends StateGate {
  constructor() { super('delete_execute'); }

  reads(event) {
    const table = event.data.table;
    const rs = new ReadSet()
      .pattern(`db/tables/${table}/rows/`)
      .pattern(`db/tables/${table}/indexes/`);
    if (event.data.where) scanConditionSubqueries(event.data.where, rs);
    return rs;
  }

  transform(event, state) {
    const { table, id } = event.data;
    let where = event.data.where || null;
    if (where) where = resolveConditionSubqueries(where, state);
    const allRows = state.patterns[`db/tables/${table}/rows/`] || {};

    // Find rows to delete
    const targets = [];
    for (const [refName, row] of Object.entries(allRows)) {
      if (id !== undefined && row.id === id) {
        targets.push({ refName, row });
      } else if (where && evaluateCondition(where, row)) {
        targets.push({ refName, row });
      } else if (id === undefined && !where) {
        targets.push({ refName, row }); // delete all
      }
    }

    const batch = new MutationBatch();
    const deletedIds = [];

    for (const { refName, row } of targets) {
      batch.refDelete(refName);
      deletedIds.push(row.id);
    }

    // Rebuild indexes without deleted rows
    const indexes = state.patterns[`db/tables/${table}/indexes/`] || {};
    const targetRefNames = new Set(targets.map(t => t.refName));
    let putIdx = 0;

    for (const [idxRef, index] of Object.entries(indexes)) {
      const remaining = Object.entries(allRows)
        .filter(([refName]) => !targetRefNames.has(refName))
        .map(([, row]) => row);
      const rebuilt = rebuildIndex(index, remaining);
      batch.put('btree', rebuilt);
      batch.refSet(idxRef, putIdx++);
    }

    batch.emit(new Event('row_deleted', { table, ids: deletedIds }));

    // RETURNING
    const returning = event.data.returning || null;
    if (returning && targets.length > 0) {
      const returnedRows = targets.map(t => {
        const row = t.row;
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

function rebuildIndex(index, rows) {
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
