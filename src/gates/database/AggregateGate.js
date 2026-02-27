/**
 * AggregateGate — computes aggregations (COUNT, SUM, AVG, MIN, MAX)
 * with optional GROUP BY.
 * PureGate: rows in, aggregated rows out. No state access.
 *
 * Exports aggregateRows() for direct use by QueryPlanGate (Phase 5).
 */
import { PureGate } from '../../protocol/PureGate.js';
import { Event } from '../../core/Event.js';

export function aggregateRows(rows, aggregates, groupBy) {
  // Group rows
  const groups = groupRows(rows, groupBy);

  // Compute aggregates per group
  return groups.map(({ key, rows: groupedRows }) => {
    const result = {};

    // Include group-by columns
    if (groupBy && groupBy.length > 0 && groupedRows.length > 0) {
      for (const col of groupBy) {
        // Strip table prefix for output: s.region → region
        const outKey = col.includes('.') ? col.split('.').pop() : col;
        result[outKey] = resolveCol(groupedRows[0], col);
      }
    }

    // Compute each aggregate
    for (const agg of aggregates) {
      const alias = agg.alias || `${agg.fn}_${agg.column}`;
      const val = computeAggregate(
        agg.fn, agg.column, groupedRows, agg.distinct || false, agg.separator || ','
      );
      result[alias] = val;
      // Add synthetic key for HAVING: e.g. "SUM(amount)"
      const synKey = `${agg.fn.toUpperCase()}(${agg.column})`;
      if (synKey !== alias) result[synKey] = val;
    }

    return result;
  });
}

/** Resolve a column value from a row, handling qualified names (table.col → col fallback) */
function resolveCol(row, col) {
  if (col in row) return row[col];
  if (col.includes('.')) {
    const bare = col.split('.').pop();
    if (bare in row) return row[bare];
  }
  return undefined;
}

function groupRows(rows, groupBy) {
  if (!groupBy || groupBy.length === 0) {
    return [{ key: null, rows }];
  }

  const groups = new Map();
  for (const row of rows) {
    const key = JSON.stringify(groupBy.map(c => resolveCol(row, c)));
    if (!groups.has(key)) groups.set(key, []);
    groups.get(key).push(row);
  }

  return [...groups.entries()].map(([key, rows]) => ({ key, rows }));
}

function computeAggregate(fn, column, rows, distinct = false, separator = ',') {
  switch (fn.toUpperCase()) {
    case 'COUNT': {
      if (column === '*') return rows.length;
      let vals = rows.filter(r => { const v = resolveCol(r, column); return v !== null && v !== undefined; });
      if (distinct) {
        const seen = new Set();
        vals = vals.filter(r => { const v = resolveCol(r, column); if (seen.has(v)) return false; seen.add(v); return true; });
      }
      return vals.length;
    }

    case 'SUM': {
      const vals = numericValues(rows, column);
      return vals.length === 0 ? 0 : vals.reduce((a, b) => a + b, 0);
    }

    case 'AVG': {
      const vals = numericValues(rows, column);
      return vals.length === 0 ? null : vals.reduce((a, b) => a + b, 0) / vals.length;
    }

    case 'MIN': {
      const vals = nonNullValues(rows, column);
      if (vals.length === 0) return null;
      return vals.reduce((min, v) => v < min ? v : min, vals[0]);
    }

    case 'MAX': {
      const vals = nonNullValues(rows, column);
      if (vals.length === 0) return null;
      return vals.reduce((max, v) => v > max ? v : max, vals[0]);
    }

    case 'GROUP_CONCAT': {
      let vals = rows.map(r => resolveCol(r, column)).filter(v => v !== null && v !== undefined).map(String);
      if (distinct) vals = [...new Set(vals)];
      return vals.length > 0 ? vals.join(separator) : null;
    }

    default:
      return null;
  }
}

function numericValues(rows, column) {
  return rows
    .map(r => resolveCol(r, column))
    .filter(v => v !== null && v !== undefined && typeof v === 'number');
}

function nonNullValues(rows, column) {
  return rows
    .map(r => resolveCol(r, column))
    .filter(v => v !== null && v !== undefined);
}

export class AggregateGate extends PureGate {
  constructor() { super('aggregate'); }

  transform(event) {
    const result = aggregateRows(
      event.data.rows,
      event.data.aggregates,
      event.data.groupBy
    );
    return new Event('aggregate_result', { rows: result });
  }
}
