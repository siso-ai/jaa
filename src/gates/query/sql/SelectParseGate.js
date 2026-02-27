import { PureGate } from '../../../protocol/PureGate.js';
import { Event } from '../../../core/Event.js';
import { kw, sym, parseColumnList, parseWhereClause, parseOrderBy, parseTableRef, isSubquery, parseSubquery } from './parser-utils.js';

export class SelectParseGate extends PureGate {
  constructor() { super('select_parse'); }

  transform(event) {
    const tokens = event.data.tokens;
    let pos = 1; // skip SELECT
    const pipeline = [];
    const ctes = event.data.ctes || {};
    const recursive = event.data.recursive || false;
    const cteColumns = event.data.cteColumns || {};

    // DISTINCT
    let distinct = false;
    if (kw(tokens, pos, 'DISTINCT')) {
      distinct = true;
      pos++;
    }

    // Column list
    const colResult = parseColumnList(tokens, pos);
    const columns = colResult.columns;
    pos = colResult.pos;

    // Extract aggregates, window functions from columns
    const aggregates = [];
    const projectCols = [];
    const allSelectCols = []; // Tracks all columns in original order for ORDER BY number
    let hasAggregates = false;
    const windowFns = [];

    for (const col of columns) {
      if (col === '*') {
        projectCols.push('*');
        allSelectCols.push('*');
      } else if (typeof col === 'object' && col.aggregate) {
        hasAggregates = true;
        const agg = { fn: col.aggregate.fn, column: col.aggregate.column, alias: col.alias };
        if (col.aggregate.distinct) agg.distinct = true;
        if (col.aggregate.separator !== undefined) agg.separator = col.aggregate.separator;
        aggregates.push(agg);
        allSelectCols.push({ alias: col.alias });
      } else if (typeof col === 'object' && col.window) {
        windowFns.push({
          fn: col.window.fn, column: col.window.column,
          distinct: col.window.distinct || false,
          over: col.window.over, alias: col.alias,
        });
        allSelectCols.push({ alias: col.alias });
      } else if (typeof col === 'object' && col.expr) {
        projectCols.push(col);
        allSelectCols.push(col);
      } else {
        projectCols.push(col);
        allSelectCols.push(col);
      }
    }

    // FROM (optional — SELECT without FROM returns one row)
    let mainAlias;

    if (!kw(tokens, pos, 'FROM')) {
      pipeline.push({ type: 'virtual_row', data: {} });
    } else {
    pos++;

    // Derived table: FROM (SELECT ...) [AS] alias
    if (isSubquery(tokens, pos)) {
      const sub = parseSubquery(tokens, pos);
      pos = sub.pos;
      let alias = 'derived';
      if (kw(tokens, pos, 'AS')) pos++;
      if (tokens[pos]?.type === 'IDENTIFIER') { alias = tokens[pos].value; pos++; }
      pipeline.push({ type: 'derived_scan', data: { subquery: sub.tokens, alias } });
      mainAlias = alias;
    } else {
      const tableRef = parseTableRef(tokens, pos);
      const mainTable = tableRef.table;
      mainAlias = tableRef.alias || mainTable;
      pos = tableRef.pos;

      if (ctes[mainTable]) {
        const scanData = { subquery: ctes[mainTable], alias: mainAlias };
        if (recursive && cteColumns[mainTable]) {
          scanData.recursive = true;
          scanData.cteName = mainTable;
          scanData.cteColumns = cteColumns[mainTable];
        }
        pipeline.push({ type: 'derived_scan', data: scanData });
      } else {
        pipeline.push({ type: 'table_scan', data: { table: mainTable, alias: mainAlias } });
      }
    }
    } // end FROM else

    // JOIN clauses
    while (pos < tokens.length && isJoinKeyword(tokens, pos)) {
      const join = parseJoin(tokens, pos, mainAlias);
      pipeline.push(join.step);
      pos = join.pos;
    }

    // WHERE
    if (kw(tokens, pos, 'WHERE')) {
      pos++;
      const whereResult = parseWhereClause(tokens, pos);
      pipeline.push({ type: 'filter', data: { where: whereResult.condition } });
      pos = whereResult.pos;
    }

    // GROUP BY
    let groupBy = null;
    if (kw(tokens, pos, 'GROUP') && kw(tokens, pos + 1, 'BY')) {
      pos += 2;
      groupBy = [];
      while (pos < tokens.length && !kw(tokens, pos, 'HAVING') && !kw(tokens, pos, 'ORDER') && !kw(tokens, pos, 'LIMIT') && !sym(tokens, pos, ';')) {
        let col = tokens[pos].value;
        pos++;
        // Handle qualified: table.column
        if (sym(tokens, pos, '.')) { pos++; col = col + '.' + tokens[pos].value; pos++; }
        groupBy.push(col);
        if (sym(tokens, pos, ',')) pos++;
      }
    }

    // Aggregates
    if (hasAggregates || groupBy) {
      const aggList = aggregates.length > 0 ? aggregates : [{ fn: 'COUNT', column: '*', alias: 'count' }];
      pipeline.push({ type: 'aggregate', data: { aggregates: aggList, groupBy } });
    }

    // HAVING
    if (kw(tokens, pos, 'HAVING')) {
      pos++;
      const havingResult = parseWhereClause(tokens, pos);
      pipeline.push({ type: 'filter', data: { where: havingResult.condition } });
      pos = havingResult.pos;
    }

    // WINDOW FUNCTIONS
    if (windowFns.length > 0) {
      pipeline.push({ type: 'window', data: { windows: windowFns } });
      for (const wf of windowFns) projectCols.push(wf.alias);
    }

    // PROJECT
    const needsProject = !(projectCols.length === 1 && projectCols[0] === '*') &&
                         !(projectCols.length === 0 && hasAggregates) &&
                         !hasAggregates;
    if (needsProject && projectCols.length > 0) {
      pipeline.push({ type: 'project', data: { columns: projectCols } });
    }

    // DISTINCT
    if (distinct) {
      pipeline.push({ type: 'distinct', data: { columns: null } });
    }

    // ORDER BY
    if (kw(tokens, pos, 'ORDER') && kw(tokens, pos + 1, 'BY')) {
      pos += 2;
      const orderResult = parseOrderBy(tokens, pos);
      pipeline.push({ type: 'order_by', data: { order: orderResult.order, selectCols: allSelectCols } });
      pos = orderResult.pos;
    }

    // LIMIT / OFFSET
    if (kw(tokens, pos, 'LIMIT')) {
      pos++;
      const limit = tokens[pos].value;
      pos++;
      let offset = 0;
      if (kw(tokens, pos, 'OFFSET')) { pos++; offset = tokens[pos].value; pos++; }
      pipeline.push({ type: 'limit', data: { limit, offset } });
    }

    // UNION / EXCEPT / INTERSECT
    if (kw(tokens, pos, 'UNION') || kw(tokens, pos, 'EXCEPT') || kw(tokens, pos, 'INTERSECT')) {
      const setOp = tokens[pos].value;
      pos++;
      let all = false;
      if (kw(tokens, pos, 'ALL')) { all = true; pos++; }
      const rightTokens = tokens.slice(pos);
      const rightEvent = new Event('select_parse', { tokens: rightTokens, sql: '' });
      const rightResult = this.transform(rightEvent);
      if (rightResult && rightResult.type === 'query_plan') {
        let rightPipeline = rightResult.data.pipeline;
        // Hoist ORDER BY / LIMIT from rightmost branch to post-union level
        const hoistTypes = ['order_by', 'limit', 'distinct'];
        const postSteps = [];
        // Walk to deepest right leaf of nested unions
        let leaf = rightPipeline;
        while (leaf.length === 1 && leaf[0].type === 'union') {
          leaf = leaf[0].data.right;
        }
        while (leaf.length > 0 && hoistTypes.includes(leaf[leaf.length - 1].type)) {
          postSteps.push(leaf.pop());
        }
        postSteps.reverse();

        const finalPipeline = [
          { type: 'union', data: { left: pipeline, right: rightPipeline, setOp: setOp.toLowerCase(), all } },
          ...postSteps
        ];
        return new Event('query_plan', { pipeline: finalPipeline, ctes });
      }
      return rightResult;
    }

    return new Event('query_plan', { pipeline, ctes });
  }
}

function isJoinKeyword(tokens, pos) {
  if (kw(tokens, pos, 'JOIN')) return true;
  if (kw(tokens, pos, 'INNER') || kw(tokens, pos, 'LEFT') ||
      kw(tokens, pos, 'RIGHT') || kw(tokens, pos, 'FULL') ||
      kw(tokens, pos, 'CROSS')) return true;
  return false;
}

function parseJoin(tokens, pos, leftAlias) {
  let type = 'inner';
  if (kw(tokens, pos, 'LEFT')) { type = 'left'; pos++; if (kw(tokens, pos, 'OUTER')) pos++; }
  else if (kw(tokens, pos, 'RIGHT')) { type = 'right'; pos++; if (kw(tokens, pos, 'OUTER')) pos++; }
  else if (kw(tokens, pos, 'FULL')) { type = 'full'; pos++; if (kw(tokens, pos, 'OUTER')) pos++; }
  else if (kw(tokens, pos, 'CROSS')) { type = 'cross'; pos++; }
  else if (kw(tokens, pos, 'INNER')) { pos++; }

  pos++; // skip JOIN

  const tableRef = parseTableRef(tokens, pos);
  const rightAlias = tableRef.alias || tableRef.table;
  pos = tableRef.pos;

  let on = null;
  if (kw(tokens, pos, 'ON')) {
    pos++;
    const conditions = [];
    do {
      const firstAlias = tokens[pos].value; pos++;
      let firstField = firstAlias;
      if (sym(tokens, pos, '.')) { pos++; firstField = tokens[pos].value; pos++; }

      pos++; // skip operator (=, <, >, etc.)

      const secondAlias = tokens[pos].value; pos++;
      let secondField = secondAlias;
      if (sym(tokens, pos, '.')) { pos++; secondField = tokens[pos].value; pos++; }

      let cond;
      if (firstAlias === rightAlias) {
        const leftRef = (secondAlias !== secondField) ? `${secondAlias}.${secondField}` : secondField;
        cond = { left: leftRef, right: firstField };
      } else if (secondAlias === rightAlias) {
        const leftRef = (firstAlias !== firstField) ? `${firstAlias}.${firstField}` : firstField;
        cond = { left: leftRef, right: secondField };
      } else {
        cond = { left: firstField, right: secondField };
      }
      conditions.push(cond);
    } while (kw(tokens, pos, 'AND') && ++pos);

    on = conditions.length === 1 ? conditions[0] : conditions;
  }

  return {
    step: { type: 'join', data: { right: { table: tableRef.table, alias: rightAlias }, on, type, leftAlias } },
    pos
  };
}
