/**
 * QueryPlanGate — executes a query pipeline.
 *
 * StateGate: reads tables referenced in scan steps,
 * then runs filter/project/order/limit/distinct/aggregate.
 * Phase 13: resolves subqueries in filter/project expressions.
 */
import { StateGate } from '../../../protocol/StateGate.js';
import { ReadSet } from '../../../protocol/ReadSet.js';
import { MutationBatch } from '../../../protocol/MutationBatch.js';
import { Event } from '../../../core/Event.js';
import { filterRows } from '../../database/FilterGate.js';
import { projectRows } from '../../database/ProjectionGate.js';
import { orderByRows } from '../../database/OrderByGate.js';
import { limitRows } from '../../database/LimitGate.js';
import { distinctRows } from '../../database/DistinctGate.js';
import { aggregateRows } from '../../database/AggregateGate.js';
import { joinRows } from '../../database/JoinGate.js';
import { SelectParseGate } from './SelectParseGate.js';
import { kw } from './parser-utils.js';

export class QueryPlanGate extends StateGate {
  constructor() { super('query_plan'); }

  reads(event) {
    const rs = new ReadSet();
    const ctes = event.data.ctes || {};
    scanPipelineReads(event.data.pipeline, rs, ctes);
    return rs;
  }

  transform(event, state) {
    const ctes = event.data.ctes || {};
    const rows = executePipeline(event.data.pipeline, state, ctes);
    return new MutationBatch()
      .emit(new Event('query_result', { rows }));
  }
}

function scanPipelineReads(pipeline, rs, ctes = {}) {
  for (const step of pipeline) {
    if (step.type === 'table_scan') rs.pattern(`db/tables/${step.data.table}/rows/`);
    if (step.type === 'index_scan') {
      rs.ref(`db/tables/${step.data.table}/indexes/${step.data.index}`);
      rs.pattern(`db/tables/${step.data.table}/rows/`);
    }
    if (step.type === 'join') {
      const rightTable = step.data.right.table;
      if (ctes[rightTable]) {
        scanSubqueryTokensForReads(ctes[rightTable], rs);
      } else {
        rs.pattern(`db/tables/${rightTable}/rows/`);
      }
    }
    if (step.type === 'union') {
      scanPipelineReads(step.data.left, rs, ctes);
      scanPipelineReads(step.data.right, rs, ctes);
    }
    // Scan subqueries in filter/project
    if (step.type === 'filter') scanConditionSubqueries(step.data.where, rs);
    if (step.type === 'project') {
      for (const col of step.data.columns || []) {
        if (typeof col === 'object' && col.expr) scanExprSubqueries(col.expr, rs);
      }
    }
    if (step.type === 'derived_scan') {
      scanSubqueryTokensForReads(step.data.subquery, rs);
    }
  }
}

export function scanConditionSubqueries(cond, rs) {
  if (!cond) return;
  if (cond.and) { for (const c of cond.and) scanConditionSubqueries(c, rs); return; }
  if (cond.or) { for (const c of cond.or) scanConditionSubqueries(c, rs); return; }
  if (cond.not) { scanConditionSubqueries(cond.not, rs); return; }
  if (cond.subquery) scanSubqueryTokensForReads(cond.subquery, rs);
  if (cond.leftExpr) scanExprSubqueries(cond.leftExpr, rs);
  if (cond.rightExpr) scanExprSubqueries(cond.rightExpr, rs);
}

function scanExprSubqueries(expr, rs) {
  if (!expr || typeof expr !== 'object') return;
  if (expr.subquery) scanSubqueryTokensForReads(expr.subquery, rs);
  if (expr.left) scanExprSubqueries(expr.left, rs);
  if (expr.right) scanExprSubqueries(expr.right, rs);
  if (expr.args) for (const a of expr.args) scanExprSubqueries(a, rs);
}

function scanSubqueryTokensForReads(tokens, rs) {
  for (let i = 0; i < tokens.length; i++) {
    if ((tokens[i].value || '') === 'FROM' && tokens[i + 1]) {
      const table = tokens[i + 1].value || '';
      if (table) rs.pattern(`db/tables/${table}/rows/`);
    }
  }
}

function executePipeline(pipeline, state, ctes = {}) {
  let rows = [];
  let leftTable = '';

  for (const step of pipeline) {
    switch (step.type) {
      case 'virtual_row':
        rows = [{}]; // Single empty row for SELECT without FROM
        break;
      case 'table_scan': {
        const pattern = `db/tables/${step.data.table}/rows/`;
        rows = Object.values(state.patterns[pattern] || {});
        leftTable = step.data.alias || step.data.table;
        break;
      }
      case 'derived_scan': {
        if (step.data.recursive) {
          rows = executeRecursiveCTE(step.data, state);
        } else {
          rows = executeSubquery(step.data.subquery, state);
        }
        leftTable = step.data.alias || 'derived';
        break;
      }
      case 'window': {
        rows = computeWindowFunctions(rows, step.data.windows);
        break;
      }
      case 'index_scan': {
        const index = state.refs[`db/tables/${step.data.table}/indexes/${step.data.index}`];
        if (!index) break;
        const matchingIds = new Set();
        for (const entry of (index.entries || [])) {
          if (matchesIndexOp(entry.key, step.data.op, step.data.value)) {
            for (const id of entry.row_ids) matchingIds.add(id);
          }
        }
        const allRows = Object.values(state.patterns[`db/tables/${step.data.table}/rows/`] || {});
        rows = allRows.filter(r => matchingIds.has(r.id));
        leftTable = step.data.table;
        break;
      }
      case 'filter': {
        const where = resolveConditionSubqueries(step.data.where, state);
        rows = filterRows(rows, where);
        break;
      }
      case 'project': {
        const cols = resolveProjectSubqueries(step.data.columns, state);
        rows = projectRows(rows, cols);
        break;
      }
      case 'order_by': {
        const order = resolveOrderByNumbers(step.data.order, step.data.selectCols || []);
        rows = orderByRows(rows, order);
        break;
      }
      case 'limit':
        rows = limitRows(rows, step.data.limit, step.data.offset);
        break;
      case 'distinct':
        rows = distinctRows(rows, step.data.columns);
        break;
      case 'aggregate':
        rows = aggregateRows(rows, step.data.aggregates, step.data.groupBy);
        break;
      case 'join': {
        const rightTable = step.data.right.table;
        let rightRows;
        if (ctes[rightTable]) {
          rightRows = executeSubquery(ctes[rightTable], state);
        } else {
          const rightPattern = `db/tables/${rightTable}/rows/`;
          rightRows = Object.values(state.patterns[rightPattern] || {});
        }
        const rightAlias = step.data.right.alias || rightTable;
        const leftAlias = step.data.leftAlias || leftTable;
        rows = joinRows(rows, rightRows, step.data.on, step.data.type || 'inner', leftAlias, rightAlias);
        break;
      }
      case 'union': {
        const leftRows = executePipeline(step.data.left, state, ctes);
        const rightRows = executePipeline(step.data.right, state, ctes);
        const setOp = step.data.setOp || 'union';
        const all = step.data.all || false;

        if (setOp === 'union') {
          rows = [...leftRows, ...rightRows];
          if (!all) rows = distinctRows(rows, null);
        } else if (setOp === 'except') {
          const rightKeys = new Set(rightRows.map(r => JSON.stringify(r)));
          rows = leftRows.filter(r => !rightKeys.has(JSON.stringify(r)));
          if (!all) rows = distinctRows(rows, null);
        } else if (setOp === 'intersect') {
          const rightKeys = new Set(rightRows.map(r => JSON.stringify(r)));
          rows = leftRows.filter(r => rightKeys.has(JSON.stringify(r)));
          if (!all) rows = distinctRows(rows, null);
        }
        break;
      }
    }
  }

  return rows;
}

function matchesIndexOp(key, op, value) {
  switch (op) {
    case 'eq':  return key === value;
    case 'neq': return key !== value;
    case 'gt':  return key > value;
    case 'lt':  return key < value;
    case 'gte': return key >= value;
    case 'lte': return key <= value;
    default: return false;
  }
}

// ── Subquery resolution ─────────────────────────────────

function resolveOrderByNumbers(order, selectCols) {
  return order.map(o => {
    if (typeof o.column === 'number' || (typeof o.column === 'string' && /^\d+$/.test(o.column))) {
      const idx = parseInt(o.column) - 1;
      if (idx >= 0 && idx < selectCols.length) {
        const col = selectCols[idx];
        return { ...o, column: typeof col === 'string' ? col : (col.alias || col.name || col.column || o.column) };
      }
    }
    return o;
  });
}

function executeRecursiveCTE(data, state) {
  const tokens = data.subquery;
  const cteName = data.cteName;
  const cteColumns = data.cteColumns;
  const maxIterations = 1000;

  // Find UNION ALL split point
  let unionPos = null;
  let depth = 0;
  for (let i = 0; i < tokens.length; i++) {
    if (tokens[i].type === 'SYMBOL' && tokens[i].value === '(') depth++;
    if (tokens[i].type === 'SYMBOL' && tokens[i].value === ')') depth--;
    if (depth === 0 && kw(tokens, i, 'UNION')) { unionPos = i; break; }
  }

  if (unionPos === null) return executeSubquery(tokens, state);

  const baseTokens = tokens.slice(0, unionPos);
  let pos = unionPos + 1; // skip UNION
  if (kw(tokens, pos, 'ALL')) pos++;
  const recursiveTokens = tokens.slice(pos);

  // Execute base case
  let allRows = executeSubquery(baseTokens, state).map(row => {
    const vals = Object.values(row);
    const out = {};
    cteColumns.forEach((col, i) => { out[col] = vals[i] !== undefined ? vals[i] : null; });
    return out;
  });

  let currentRows = allRows;
  for (let iter = 0; iter < maxIterations && currentRows.length > 0; iter++) {
    const virtualState = { ...state, patterns: { ...state.patterns }, refs: { ...state.refs } };
    const virtualPattern = `db/tables/${cteName}/rows/`;
    virtualState.patterns[virtualPattern] = {};
    currentRows.forEach((row, i) => {
      virtualState.patterns[virtualPattern][`db/tables/${cteName}/rows/${i}`] = { __rowid__: i + 1, ...row };
    });
    virtualState.refs[`db/tables/${cteName}/schema`] = {
      columns: cteColumns.map(c => ({ name: c, type: 'text' }))
    };

    const newRows = executeSubquery(recursiveTokens, virtualState).map(row => {
      const vals = Object.values(row);
      const out = {};
      cteColumns.forEach((col, i) => { out[col] = vals[i] !== undefined ? vals[i] : null; });
      return out;
    });

    if (newRows.length === 0) break;
    allRows = [...allRows, ...newRows];
    currentRows = newRows;
  }

  return allRows;
}

function executeSubquery(tokens, state) {
  const parseGate = new SelectParseGate();
  const result = parseGate.transform(new Event('select_parse', { tokens, sql: '' }));
  if (!result || result.type !== 'query_plan') return [];
  return executePipeline(result.data.pipeline, state);
}

export function resolveConditionSubqueries(cond, state) {
  if (!cond) return null;
  if (cond.and) return { and: cond.and.map(c => resolveConditionSubqueries(c, state)) };
  if (cond.or) return { or: cond.or.map(c => resolveConditionSubqueries(c, state)) };
  if (cond.not) return { not: resolveConditionSubqueries(cond.not, state) };

  // EXISTS subquery
  if (cond.exists !== undefined && cond.subquery) {
    const rows = executeSubquery(cond.subquery, state);
    return { exists: true, resolved: rows.length > 0 };
  }

  // IN subquery
  if (cond.subquery && cond.op === 'in') {
    const rows = executeSubquery(cond.subquery, state);
    const values = rows.map(r => Object.values(r)[0] ?? null);
    const resolved = { ...cond };
    delete resolved.subquery;
    resolved.value = values;
    return resolved;
  }

  // Scalar subquery in leftExpr/rightExpr
  const result = { ...cond };
  if (cond.leftExpr) result.leftExpr = resolveExprSubqueries(cond.leftExpr, state);
  if (cond.rightExpr) result.rightExpr = resolveExprSubqueries(cond.rightExpr, state);
  return result;
}

function resolveExprSubqueries(expr, state) {
  if (!expr || typeof expr !== 'object') return expr;

  if (expr.subquery && !('resolved' in expr)) {
    const rows = executeSubquery(expr.subquery, state);
    if (rows.length > 0) {
      return { ...expr, resolved: Object.values(rows[0])[0] ?? null };
    }
    return { ...expr, resolved: null };
  }

  const result = { ...expr };
  if (expr.left) result.left = resolveExprSubqueries(expr.left, state);
  if (expr.right) result.right = resolveExprSubqueries(expr.right, state);
  if (expr.args) result.args = expr.args.map(a => resolveExprSubqueries(a, state));
  if (expr.case) {
    result.case = expr.case.map(b => ({
      when: resolveConditionSubqueries(b.when, state),
      then: resolveExprSubqueries(b.then, state),
    }));
  }
  if (expr.else !== undefined) result.else = resolveExprSubqueries(expr.else, state);
  return result;
}

function resolveProjectSubqueries(columns, state) {
  return columns.map(col => {
    if (typeof col === 'object' && col.expr) {
      return { ...col, expr: resolveExprSubqueries(col.expr, state) };
    }
    return col;
  });
}

function computeWindowFunctions(rows, windows) {
  if (rows.length === 0) return rows;

  for (const win of windows) {
    const fn = win.fn.toUpperCase();
    const col = win.column || '*';
    const alias = win.alias;
    const partitionBy = win.over.partitionBy;
    const orderBy = win.over.orderBy;

    // Group into partitions
    const partitions = {};
    rows.forEach((row, idx) => {
      const key = partitionBy
        ? JSON.stringify(partitionBy.map(c => row[c] ?? null))
        : '__all__';
      if (!partitions[key]) partitions[key] = [];
      partitions[key].push(idx);
    });

    for (const indices of Object.values(partitions)) {
      // Sort within partition
      if (orderBy) {
        indices.sort((a, b) => {
          for (const spec of orderBy) {
            const va = rows[a][spec.column] ?? null;
            const vb = rows[b][spec.column] ?? null;
            const dir = (spec.direction || 'asc') === 'desc' ? -1 : 1;
            if (va === null && vb === null) continue;
            if (va === null) return 1;
            if (vb === null) return -1;
            if (va < vb) return -1 * dir;
            if (va > vb) return 1 * dir;
          }
          return 0;
        });
      }

      const partitionRows = indices.map(i => rows[i]);
      let rank = 0, denseRank = 0, prevVals = null;

      indices.forEach((rowIdx, posInPart) => {
        switch (fn) {
          case 'ROW_NUMBER':
            rows[rowIdx][alias] = posInPart + 1;
            break;

          case 'RANK':
            if (orderBy) {
              const curVals = orderBy.map(s => rows[rowIdx][s.column] ?? null);
              if (prevVals === null || JSON.stringify(curVals) !== JSON.stringify(prevVals)) {
                rank = posInPart + 1;
                prevVals = curVals;
              }
              rows[rowIdx][alias] = rank;
            } else {
              rows[rowIdx][alias] = 1;
            }
            break;

          case 'DENSE_RANK':
            if (orderBy) {
              const curVals = orderBy.map(s => rows[rowIdx][s.column] ?? null);
              if (prevVals === null || JSON.stringify(curVals) !== JSON.stringify(prevVals)) {
                denseRank++;
                prevVals = curVals;
              }
              rows[rowIdx][alias] = denseRank;
            } else {
              rows[rowIdx][alias] = 1;
            }
            break;

          case 'SUM': {
            const vals = partitionRows.map(r => r[col]).filter(v => v !== null && v !== undefined && typeof v === 'number');
            rows[rowIdx][alias] = vals.reduce((a, b) => a + b, 0);
            break;
          }
          case 'AVG': {
            const vals = partitionRows.map(r => r[col]).filter(v => v !== null && v !== undefined && typeof v === 'number');
            rows[rowIdx][alias] = vals.length > 0 ? vals.reduce((a, b) => a + b, 0) / vals.length : null;
            break;
          }
          case 'COUNT':
            rows[rowIdx][alias] = col === '*'
              ? partitionRows.length
              : partitionRows.filter(r => r[col] !== null && r[col] !== undefined).length;
            break;

          case 'MIN': {
            const vals = partitionRows.map(r => r[col]).filter(v => v !== null && v !== undefined);
            rows[rowIdx][alias] = vals.length > 0 ? Math.min(...vals) : null;
            break;
          }
          case 'MAX': {
            const vals = partitionRows.map(r => r[col]).filter(v => v !== null && v !== undefined);
            rows[rowIdx][alias] = vals.length > 0 ? Math.max(...vals) : null;
            break;
          }
        }
      });
    }
  }
  return rows;
}
