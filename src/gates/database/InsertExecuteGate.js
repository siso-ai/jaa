/**
 * InsertExecuteGate — inserts a row into a table.
 * Validates against schema, assigns id, updates counter.
 */
import { StateGate } from '../../protocol/StateGate.js';
import { ReadSet } from '../../protocol/ReadSet.js';
import { MutationBatch } from '../../protocol/MutationBatch.js';
import { Event } from '../../core/Event.js';
import { evaluateExpression } from './expression.js';
import { rebuildIndex } from './UpdateExecuteGate.js';

export class InsertExecuteGate extends StateGate {
  constructor() { super('insert_execute'); }

  reads(event) {
    const table = event.data.table;
    const rs = new ReadSet()
      .ref(`db/tables/${table}/schema`)
      .ref(`db/tables/${table}/next_id`)
      .pattern(`db/tables/${table}/indexes/`);
    if (event.data.onConflict) {
      rs.pattern(`db/tables/${table}/rows/`);
    }
    return rs;
  }

  transform(event, state) {
    const { table, row } = event.data;
    const schema = state.refs[`db/tables/${table}/schema`];
    const onConflict = event.data.onConflict || null;
    const returning = event.data.returning || null;

    if (schema === null) {
      return new MutationBatch()
        .emit(new Event('error', { message: `Table '${table}' does not exist`, source: 'insert_execute' }));
    }

    // UPSERT: check for conflict
    if (onConflict && onConflict.column) {
      const conflictCol = onConflict.column;
      const conflictVal = row[conflictCol] ?? null;
      const allRows = state.patterns[`db/tables/${table}/rows/`] || {};

      for (const [refName, existingRow] of Object.entries(allRows)) {
        if ((existingRow[conflictCol] ?? null) === conflictVal) {
          if (onConflict.action === 'nothing') {
            const batch = new MutationBatch()
              .emit(new Event('row_inserted', { table, id: existingRow.id, row: existingRow, conflict: 'skipped' }));
            if (returning) batch.emit(new Event('query_result', { rows: [applyReturning(existingRow, returning)] }));
            return batch;
          }
          // DO UPDATE SET
          const newRow = { ...existingRow };
          for (const [col, expr] of Object.entries(onConflict.updates)) {
            newRow[col] = evaluateExpression(expr, existingRow);
          }
          const batch = new MutationBatch()
            .put('row', newRow)
            .refSet(refName, 0);

          const indexes = state.patterns[`db/tables/${table}/indexes/`] || {};
          let putIdx = 1;
          for (const [idxRef, index] of Object.entries(indexes)) {
            const remaining = Object.entries(allRows).map(([rn, r]) => rn === refName ? newRow : r);
            batch.put('btree', rebuildIndex(index, remaining));
            batch.refSet(idxRef, putIdx++);
          }

          batch.emit(new Event('row_inserted', { table, id: newRow.id, row: newRow, conflict: 'updated' }));
          if (returning) batch.emit(new Event('query_result', { rows: [applyReturning(newRow, returning)] }));
          return batch;
        }
      }
      // No conflict → proceed with normal insert
    }

    // Compute next id
    const counter = parseInt(state.refs[`db/tables/${table}/next_id`]) || 0;
    const id = counter + 1;

    // Build complete row with defaults
    const completeRow = { id };
    for (const col of schema.columns) {
      if (col.name === 'id') continue;
      if (row[col.name] !== undefined) {
        completeRow[col.name] = row[col.name];
      } else if (col.default !== undefined && col.default !== null) {
        completeRow[col.name] = col.default;
      } else if (col.nullable === false) {
        return new MutationBatch()
          .emit(new Event('error', {
            message: `Column '${col.name}' cannot be null`,
            source: 'insert_execute'
          }));
      } else {
        completeRow[col.name] = null;
      }
    }

    const batch = new MutationBatch()
      .put('row', completeRow)
      .refSet(`db/tables/${table}/rows/${id}`, 0)
      .put('counter', String(id))
      .refSet(`db/tables/${table}/next_id`, 1);

    // Update indexes
    const indexes = state.patterns[`db/tables/${table}/indexes/`] || {};
    let putIdx = 2; // next available puts index
    for (const [refName, index] of Object.entries(indexes)) {
      const updated = addToIndex(index, completeRow);
      batch.put('btree', updated);
      batch.refSet(refName, putIdx++);
    }

    batch.emit(new Event('row_inserted', { table, id, row: completeRow }));

    // RETURNING
    if (returning) {
      batch.emit(new Event('query_result', { rows: [applyReturning(completeRow, returning)] }));
    }

    return batch;
  }
}

function addToIndex(index, row) {
  const key = row[index.column];
  const entries = [...(index.entries || [])];
  const existing = entries.find(e => e.key === key);
  if (existing) {
    existing.row_ids = [...existing.row_ids, row.id];
  } else {
    entries.push({ key, row_ids: [row.id] });
  }
  return { ...index, entries };
}

function applyReturning(row, cols) {
  if (cols[0] === '*') return row;
  const out = {};
  for (const c of cols) out[c] = row[c] ?? null;
  return out;
}
