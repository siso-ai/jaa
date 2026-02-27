/**
 * Phase 12 Parse Gates — EXPLAIN, INSERT...SELECT, CREATE TABLE AS SELECT.
 */
import { PureGate } from '../../../protocol/PureGate.js';
import { Event } from '../../../core/Event.js';
import { SelectParseGate } from './SelectParseGate.js';

/**
 * ExplainGate — returns the query plan as rows rather than executing.
 */
export class ExplainGate extends PureGate {
  constructor() { super('explain'); }

  transform(event) {
    const tokens = event.data.tokens;
    if (!tokens.length || tokens[0].value !== 'SELECT') {
      return new Event('error', { message: 'EXPLAIN only supports SELECT', source: 'explain' });
    }
    const parseGate = new SelectParseGate();
    const result = parseGate.transform(new Event('select_parse', { tokens, sql: '' }));
    if (!result || result.type !== 'query_plan') return result;
    return new Event('query_result', { rows: describePipeline(result.data.pipeline) });
  }
}

function describePipeline(pipeline, depth = 0) {
  const rows = [];
  for (let i = 0; i < pipeline.length; i++) {
    const step = pipeline[i];
    const prefix = '  '.repeat(depth);
    let desc;
    switch (step.type) {
      case 'table_scan': desc = `SCAN ${step.data.table}`; break;
      case 'index_scan': desc = `INDEX SCAN ${step.data.table}.${step.data.index}`; break;
      case 'filter': desc = 'FILTER'; break;
      case 'project': desc = 'PROJECT'; break;
      case 'order_by': desc = 'ORDER BY'; break;
      case 'limit': desc = `LIMIT ${step.data.limit}${step.data.offset ? ` OFFSET ${step.data.offset}` : ''}`; break;
      case 'distinct': desc = 'DISTINCT'; break;
      case 'aggregate': desc = 'AGGREGATE'; break;
      case 'join': desc = `${(step.data.type || 'inner').toUpperCase()} JOIN ${step.data.right.table}`; break;
      case 'union': desc = `${(step.data.setOp || 'union').toUpperCase()}${step.data.all ? ' ALL' : ''}`; break;
      default: desc = step.type.toUpperCase(); break;
    }
    rows.push({ step: i + 1, operation: prefix + desc });
    if (step.type === 'union') {
      rows.push({ step: null, operation: prefix + '  LEFT:' });
      rows.push(...describePipeline(step.data.left, depth + 2));
      rows.push({ step: null, operation: prefix + '  RIGHT:' });
      rows.push(...describePipeline(step.data.right, depth + 2));
    }
  }
  return rows;
}

/**
 * InsertSelectGate — transforms INSERT...SELECT into insert_select_plan event.
 */
export class InsertSelectGate extends PureGate {
  constructor() { super('insert_select'); }

  transform(event) {
    const { table, columns, selectTokens } = event.data;
    const parseGate = new SelectParseGate();
    const result = parseGate.transform(new Event('select_parse', { tokens: selectTokens, sql: '' }));
    if (!result || result.type !== 'query_plan') {
      return result || new Event('error', { message: 'Invalid SELECT in INSERT...SELECT', source: 'insert_select' });
    }
    return new Event('insert_select_plan', { table, columns, pipeline: result.data.pipeline });
  }
}

/**
 * CreateTableAsSelectGate — transforms CREATE TABLE AS SELECT into insert_select_plan.
 */
export class CreateTableAsSelectGate extends PureGate {
  constructor() { super('create_table_as_select'); }

  transform(event) {
    const { table, ifNotExists, selectTokens } = event.data;
    const parseGate = new SelectParseGate();
    const result = parseGate.transform(new Event('select_parse', { tokens: selectTokens, sql: '' }));
    if (!result || result.type !== 'query_plan') {
      return result || new Event('error', { message: 'Invalid SELECT in CREATE TABLE AS', source: 'create_table_as_select' });
    }
    return new Event('insert_select_plan', {
      table, columns: null, pipeline: result.data.pipeline,
      createTable: true, ifNotExists: ifNotExists || false,
    });
  }
}
