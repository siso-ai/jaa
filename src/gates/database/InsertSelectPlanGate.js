/**
 * InsertSelectPlanGate — executes INSERT...SELECT and CREATE TABLE AS SELECT.
 * StateGate: reads source tables via pipeline, inserts into target table.
 */
import { StateGate } from '../../protocol/StateGate.js';
import { ReadSet } from '../../protocol/ReadSet.js';
import { MutationBatch } from '../../protocol/MutationBatch.js';
import { Event } from '../../core/Event.js';
import { filterRows } from './FilterGate.js';
import { projectRows } from './ProjectionGate.js';
import { orderByRows } from './OrderByGate.js';
import { limitRows } from './LimitGate.js';
import { distinctRows } from './DistinctGate.js';
import { aggregateRows } from './AggregateGate.js';
import { joinRows } from './JoinGate.js';

export class InsertSelectPlanGate extends StateGate {
  constructor() { super('insert_select_plan'); }

  reads(event) {
    const rs = new ReadSet();
    const table = event.data.table;
    rs.ref(`db/tables/${table}/schema`);
    rs.ref(`db/tables/${table}/next_id`);
    scanPipelineReads(event.data.pipeline, rs);
    return rs;
  }

  transform(event, state) {
    const { table, columns, pipeline, createTable, ifNotExists } = event.data;
    const sourceRows = executePipeline(pipeline, state);

    if (sourceRows.length === 0) {
      if (createTable) {
        return new MutationBatch()
          .put('schema', { name: table, columns: [] })
          .refSet(`db/tables/${table}/schema`, 0)
          .put('counter', '0')
          .refSet(`db/tables/${table}/next_id`, 1)
          .emit(new Event('table_created', { table }));
      }
      return new MutationBatch().emit(new Event('rows_inserted', { table, count: 0 }));
    }

    const batch = new MutationBatch();
    let putIdx = 0;

    if (createTable) {
      const existing = state.refs[`db/tables/${table}/schema`];
      if (existing !== null && existing !== undefined) {
        if (ifNotExists) {
          return new MutationBatch().emit(new Event('table_exists', { table }));
        }
        return new MutationBatch()
          .emit(new Event('error', { message: `Table '${table}' already exists`, source: 'insert_select_plan' }));
      }
      const colNames = Object.keys(sourceRows[0]).filter(c => c !== 'id');
      const schemaCols = colNames.map(c => ({ name: c, type: 'text', nullable: true, default: null }));
      batch.put('schema', { name: table, columns: schemaCols });
      batch.refSet(`db/tables/${table}/schema`, putIdx++);
    } else {
      const schema = state.refs[`db/tables/${table}/schema`];
      if (!schema) {
        return new MutationBatch()
          .emit(new Event('error', { message: `Table '${table}' does not exist`, source: 'insert_select_plan' }));
      }
    }

    let counter = parseInt(state.refs[`db/tables/${table}/next_id`] || '0', 10);

    for (const srcRow of sourceRows) {
      counter++;
      const newRow = { id: counter };
      if (columns) {
        const srcValues = Object.values(srcRow);
        for (let i = 0; i < columns.length; i++) {
          newRow[columns[i]] = srcValues[i] !== undefined ? srcValues[i] : null;
        }
      } else {
        for (const [k, v] of Object.entries(srcRow)) {
          if (k !== 'id') newRow[k] = v;
        }
      }
      batch.put('row', newRow);
      batch.refSet(`db/tables/${table}/rows/${counter}`, putIdx++);
    }

    batch.put('counter', String(counter));
    batch.refSet(`db/tables/${table}/next_id`, putIdx++);

    return batch.emit(new Event('rows_inserted', { table, count: sourceRows.length }));
  }
}

function scanPipelineReads(pipeline, rs) {
  for (const step of pipeline) {
    if (step.type === 'table_scan') rs.pattern(`db/tables/${step.data.table}/rows/`);
    if (step.type === 'index_scan') {
      rs.ref(`db/tables/${step.data.table}/indexes/${step.data.index}`);
      rs.pattern(`db/tables/${step.data.table}/rows/`);
    }
    if (step.type === 'join') rs.pattern(`db/tables/${step.data.right.table}/rows/`);
    if (step.type === 'union') {
      scanPipelineReads(step.data.left, rs);
      scanPipelineReads(step.data.right, rs);
    }
  }
}

function executePipeline(pipeline, state) {
  let rows = [];
  for (const step of pipeline) {
    switch (step.type) {
      case 'table_scan':
        rows = Object.values(state.patterns[`db/tables/${step.data.table}/rows/`] || {});
        break;
      case 'filter':
        rows = filterRows(rows, step.data.where);
        break;
      case 'project':
        rows = projectRows(rows, step.data.columns);
        break;
      case 'order_by':
        rows = orderByRows(rows, step.data.order);
        break;
      case 'limit':
        rows = limitRows(rows, step.data.limit, step.data.offset || 0);
        break;
      case 'distinct':
        rows = distinctRows(rows, step.data.columns || null);
        break;
      case 'aggregate':
        rows = aggregateRows(rows, step.data.aggregates, step.data.groupBy || null);
        break;
      case 'join': {
        const rightRows = Object.values(state.patterns[`db/tables/${step.data.right.table}/rows/`] || {});
        rows = joinRows(rows, rightRows, step.data.on, step.data.type || 'inner');
        break;
      }
      case 'union': {
        const leftRows = executePipeline(step.data.left, state);
        const rightRows = executePipeline(step.data.right, state);
        rows = [...leftRows, ...rightRows];
        if (!step.data.all) rows = distinctRows(rows, null);
        break;
      }
    }
  }
  return rows;
}
