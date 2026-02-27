/**
 * SQL Parser Utilities — shared parsing functions.
 * Every function takes (tokens, pos) and returns { result, pos }.
 * Pure. Stateless. Used by all parse gates.
 */

/** Check if token at pos matches type and optionally value */
export function match(tokens, pos, type, value) {
  if (pos >= tokens.length) return false;
  const t = tokens[pos];
  if (t.type !== type) return false;
  if (value !== undefined && t.value !== value) return false;
  return true;
}

/** Check if token at pos is a keyword with given value */
export function kw(tokens, pos, value) {
  return match(tokens, pos, 'KEYWORD', value);
}

/** Check if token at pos is a symbol with given value */
export function sym(tokens, pos, value) {
  return match(tokens, pos, 'SYMBOL', value);
}

/** Expect a specific token, throw if not found */
export function expect(tokens, pos, type, value) {
  if (!match(tokens, pos, type, value)) {
    const actual = pos < tokens.length ? `${tokens[pos].type}:${tokens[pos].value}` : 'EOF';
    throw new Error(`Expected ${type}:${value}, got ${actual} at position ${pos}`);
  }
  return pos + 1;
}

/** Get value at pos */
export function val(tokens, pos) {
  return tokens[pos]?.value;
}

/** Parse a column list: col1, col2, col3 or * */
export function parseColumnList(tokens, pos) {
  const columns = [];

  if (sym(tokens, pos, '*')) {
    return { columns: ['*'], pos: pos + 1 };
  }

  while (pos < tokens.length) {
    const col = parseSelectColumn(tokens, pos);
    columns.push(col.column);
    pos = col.pos;

    if (sym(tokens, pos, ',')) {
      pos++; // skip comma
    } else {
      break;
    }
  }

  return { columns, pos };
}

/** Parse a single column expression in SELECT */
function parseSelectColumn(tokens, pos) {
  // Star
  if (sym(tokens, pos, '*')) {
    return { column: '*', pos: pos + 1 };
  }

  // Scalar subquery: (SELECT ...)
  if (isSubquery(tokens, pos)) {
    const sub = parseSubquery(tokens, pos);
    let alias = 'subquery';
    if (kw(tokens, sub.pos, 'AS')) { sub.pos++; alias = tokens[sub.pos].value; sub.pos++; }
    return { column: { expr: { subquery: sub.tokens }, alias }, pos: sub.pos };
  }

  // Aggregate/Window functions
  const aggFns = ['COUNT', 'SUM', 'AVG', 'MIN', 'MAX'];
  const winFns = ['ROW_NUMBER', 'RANK', 'DENSE_RANK', 'NTILE', 'LAG', 'LEAD'];
  const allSpecialFns = [...aggFns, ...winFns, 'GROUP_CONCAT'];
  const tokVal = tokens[pos]?.value || '';

  if (allSpecialFns.includes(tokVal) && sym(tokens, pos + 1, '(')) {
    const fn = tokVal;
    pos++; // skip fn name
    pos = expect(tokens, pos, 'SYMBOL', '(');

    let column = null;
    let distinct = false;
    let separator = ',';

    if (sym(tokens, pos, '*')) { column = '*'; pos++; }
    else if (sym(tokens, pos, ')')) { column = '*'; } // No args: ROW_NUMBER()
    else {
      if (kw(tokens, pos, 'DISTINCT')) { distinct = true; pos++; }
      column = tokens[pos].value; pos++;
      // Handle qualified column: table.column
      if (sym(tokens, pos, '.')) { pos++; column = column + '.' + tokens[pos].value; pos++; }
      if (fn === 'GROUP_CONCAT' && kw(tokens, pos, 'SEPARATOR')) { pos++; separator = tokens[pos].value; pos++; }
    }
    pos = expect(tokens, pos, 'SYMBOL', ')');

    // Check for OVER → window function
    if (kw(tokens, pos, 'OVER')) {
      pos++;
      const over = parseWindowSpec(tokens, pos);
      pos = over.pos;
      let alias = fn.toLowerCase();
      if (kw(tokens, pos, 'AS')) { pos++; alias = tokens[pos].value; pos++; }
      return { column: { window: { fn, column, distinct, over: over.spec }, alias }, pos };
    }

    // Regular aggregate
    let alias = `${fn.toLowerCase()}_${column}`;
    if (kw(tokens, pos, 'AS')) { pos++; alias = tokens[pos].value; pos++; }

    if (fn === 'GROUP_CONCAT') {
      return { column: { aggregate: { fn: 'GROUP_CONCAT', column, distinct, separator }, alias }, pos };
    }
    return { column: { aggregate: { fn, column, distinct }, alias }, pos };
  }

  // General expression
  const exprResult = parseExpression(tokens, pos);
  let expr = exprResult.expr;
  pos = exprResult.pos;

  let alias = null;
  if (kw(tokens, pos, 'AS')) { pos++; alias = tokens[pos].value; pos++; }

  if (typeof expr === 'string' && alias === null) return { column: expr, pos };
  if (alias === null) alias = typeof expr === 'string' ? expr : `expr_${pos}`;
  return { column: { expr, alias }, pos };
}

/** Parse a WHERE clause into a condition tree */
export function parseWhereClause(tokens, pos) {
  return parseOrExpr(tokens, pos);
}

function parseOrExpr(tokens, pos) {
  let result = parseAndExpr(tokens, pos);
  let left = result.condition;
  pos = result.pos;

  while (kw(tokens, pos, 'OR')) {
    pos++;
    const right = parseAndExpr(tokens, pos);
    left = { or: [left, right.condition] };
    pos = right.pos;
  }

  return { condition: left, pos };
}

function parseAndExpr(tokens, pos) {
  let result = parseNotExpr(tokens, pos);
  let left = result.condition;
  pos = result.pos;

  while (kw(tokens, pos, 'AND')) {
    pos++;
    const right = parseNotExpr(tokens, pos);
    left = { and: [left, right.condition] };
    pos = right.pos;
  }

  return { condition: left, pos };
}

function parseNotExpr(tokens, pos) {
  if (kw(tokens, pos, 'NOT')) {
    pos++;
    // NOT EXISTS
    if (kw(tokens, pos, 'EXISTS') && isSubquery(tokens, pos + 1)) {
      pos++; // skip EXISTS
      const sub = parseSubquery(tokens, pos);
      return { condition: { not: { exists: true, subquery: sub.tokens } }, pos: sub.pos };
    }
    const result = parseComparison(tokens, pos);
    return { condition: { not: result.condition }, pos: result.pos };
  }
  // EXISTS (SELECT ...)
  if (kw(tokens, pos, 'EXISTS') && isSubquery(tokens, pos + 1)) {
    pos++; // skip EXISTS
    const sub = parseSubquery(tokens, pos);
    return { condition: { exists: true, subquery: sub.tokens }, pos: sub.pos };
  }
  return parseComparison(tokens, pos);
}

function parseComparison(tokens, pos) {
  // Parenthesized condition or subquery
  if (sym(tokens, pos, '(')) {
    if (kw(tokens, pos + 1, 'SELECT')) {
      // Scalar subquery comparison: (SELECT ...) = value
      const sub = parseSubquery(tokens, pos);
      if (tokens[sub.pos] && tokens[sub.pos].type === 'OPERATOR') {
        const op = tokens[sub.pos].value;
        pos = sub.pos + 1;
        const rightResult = parseExpression(tokens, pos);
        return {
          condition: { leftExpr: { subquery: sub.tokens }, op, rightExpr: rightResult.expr },
          pos: rightResult.pos,
        };
      }
      // Standalone scalar subquery in boolean context
      return { condition: { exists: true, subquery: sub.tokens }, pos: sub.pos };
    }
    pos++;
    const result = parseOrExpr(tokens, pos);
    if (sym(tokens, result.pos, ')')) {
      return { condition: result.condition, pos: result.pos + 1 };
    }
    return result;
  }

  // Parse left side as expression
  const leftResult = parseExpression(tokens, pos);
  const leftExpr = leftResult.expr;
  pos = leftResult.pos;
  const isSimpleColumn = typeof leftExpr === 'string';
  const column = isSimpleColumn ? leftExpr : null;

  // IS NULL / IS NOT NULL
  if (kw(tokens, pos, 'IS')) {
    pos++;
    if (kw(tokens, pos, 'NOT')) {
      pos++; pos++; // skip NOT NULL
      return isSimpleColumn
        ? { condition: { column, op: 'is_not_null' }, pos }
        : { condition: { leftExpr, op: 'is_not_null' }, pos };
    }
    pos++; // skip NULL
    return isSimpleColumn
      ? { condition: { column, op: 'is_null' }, pos }
      : { condition: { leftExpr, op: 'is_null' }, pos };
  }

  // NOT IN / NOT LIKE / NOT ILIKE / NOT BETWEEN
  if (kw(tokens, pos, 'NOT')) {
    pos++;
    if (kw(tokens, pos, 'IN')) {
      pos++;
      if (isSubquery(tokens, pos)) {
        const sub = parseSubquery(tokens, pos);
        const cond = isSimpleColumn
          ? { column, op: 'in', subquery: sub.tokens }
          : { leftExpr, op: 'in', subquery: sub.tokens };
        return { condition: { not: cond }, pos: sub.pos };
      }
      const list = parseInList(tokens, pos);
      const cond = isSimpleColumn
        ? { column, op: 'in', value: list.values }
        : { leftExpr, op: 'in', value: list.values };
      return { condition: { not: cond }, pos: list.pos };
    }
    if (kw(tokens, pos, 'LIKE') || kw(tokens, pos, 'ILIKE')) {
      const opType = tokens[pos].value.toLowerCase();
      pos++;
      const pattern = tokens[pos].value; pos++;
      const cond = isSimpleColumn
        ? { column, op: opType, value: pattern }
        : { leftExpr, op: opType, value: pattern };
      return { condition: { not: cond }, pos };
    }
    if (kw(tokens, pos, 'BETWEEN')) {
      pos++;
      const low = parseLiteralValue(tokens, pos); pos = low.pos;
      pos++; // skip AND
      const high = parseLiteralValue(tokens, pos); pos = high.pos;
      if (isSimpleColumn) {
        return { condition: { not: { and: [
          { column, op: '>=', value: low.value },
          { column, op: '<=', value: high.value }
        ]}}, pos };
      }
      return { condition: { not: { and: [
        { leftExpr, op: '>=', rightExpr: { literal: low.value } },
        { leftExpr, op: '<=', rightExpr: { literal: high.value } }
      ]}}, pos };
    }
  }

  // IN
  if (kw(tokens, pos, 'IN')) {
    pos++;
    if (isSubquery(tokens, pos)) {
      const sub = parseSubquery(tokens, pos);
      const cond = isSimpleColumn
        ? { column, op: 'in', subquery: sub.tokens }
        : { leftExpr, op: 'in', subquery: sub.tokens };
      return { condition: cond, pos: sub.pos };
    }
    const list = parseInList(tokens, pos);
    const cond = isSimpleColumn
      ? { column, op: 'in', value: list.values }
      : { leftExpr, op: 'in', value: list.values };
    return { condition: cond, pos: list.pos };
  }

  // LIKE
  // LIKE / ILIKE
  if (kw(tokens, pos, 'LIKE') || kw(tokens, pos, 'ILIKE')) {
    const opType = tokens[pos].value.toLowerCase();
    pos++;
    const pattern = tokens[pos].value; pos++;
    const cond = isSimpleColumn
      ? { column, op: opType, value: pattern }
      : { leftExpr, op: opType, value: pattern };
    return { condition: cond, pos };
  }

  // BETWEEN
  if (kw(tokens, pos, 'BETWEEN')) {
    pos++;
    const low = parseLiteralValue(tokens, pos); pos = low.pos;
    pos++; // skip AND
    const high = parseLiteralValue(tokens, pos); pos = high.pos;
    if (isSimpleColumn) {
      return { condition: { and: [
        { column, op: '>=', value: low.value },
        { column, op: '<=', value: high.value }
      ]}, pos };
    }
    return { condition: { and: [
      { leftExpr, op: '>=', rightExpr: { literal: low.value } },
      { leftExpr, op: '<=', rightExpr: { literal: high.value } }
    ]}, pos };
  }

  // Standard comparison
  const op = tokens[pos].value; pos++;
  const rightResult = parseExpression(tokens, pos);
  const rightExpr = rightResult.expr;
  pos = rightResult.pos;

  // Backward-compatible format for simple cases
  const isSimpleRight = typeof rightExpr === 'object' && rightExpr !== null && rightExpr.literal !== undefined;
  if (isSimpleColumn && isSimpleRight) {
    return { condition: { column, op, value: rightExpr.literal }, pos };
  }

  return { condition: { leftExpr: isSimpleColumn ? column : leftExpr, op, rightExpr }, pos };
}

function parseInList(tokens, pos) {
  pos = expect(tokens, pos, 'SYMBOL', '(');
  const values = [];
  while (!sym(tokens, pos, ')')) {
    const v = parseLiteralValue(tokens, pos);
    values.push(v.value);
    pos = v.pos;
    if (sym(tokens, pos, ',')) pos++;
  }
  pos++; // skip )
  return { values, pos };
}

/** Parse a literal value: number, string, boolean, null */
export function parseLiteralValue(tokens, pos) {
  const t = tokens[pos];
  if (t.type === 'NUMBER') return { value: t.value, pos: pos + 1 };
  if (t.type === 'STRING') return { value: t.value, pos: pos + 1 };
  if (t.type === 'BOOLEAN') return { value: t.value, pos: pos + 1 };
  if (t.type === 'NULL') return { value: null, pos: pos + 1 };
  if (t.type === 'KEYWORD' && t.value === 'DEFAULT') return { value: undefined, pos: pos + 1 };
  // Negative number
  if (t.type === 'OPERATOR' && t.value === '-' && tokens[pos + 1]?.type === 'NUMBER') {
    return { value: -tokens[pos + 1].value, pos: pos + 2 };
  }
  throw new Error(`Expected literal value, got ${t.type}:${t.value} at position ${pos}`);
}

/** Parse ORDER BY clause */
export function parseOrderBy(tokens, pos) {
  const order = [];
  while (pos < tokens.length) {
    let column = tokens[pos].value;
    pos++;
    // Handle qualified: table.column
    if (sym(tokens, pos, '.')) { pos++; column = column + '.' + tokens[pos].value; pos++; }
    let direction = 'asc';
    if (kw(tokens, pos, 'ASC')) { direction = 'asc'; pos++; }
    else if (kw(tokens, pos, 'DESC')) { direction = 'desc'; pos++; }
    const entry = { column, direction };
    if (kw(tokens, pos, 'NULLS')) {
      pos++;
      if (kw(tokens, pos, 'FIRST')) { entry.nulls = 'first'; pos++; }
      else if (kw(tokens, pos, 'LAST')) { entry.nulls = 'last'; pos++; }
    }
    order.push(entry);
    if (sym(tokens, pos, ',')) { pos++; } else { break; }
  }
  return { order, pos };
}

/** Parse a table reference with optional alias */
export function parseTableRef(tokens, pos) {
  const table = tokens[pos].value;
  pos++;
  let alias = null;
  if (kw(tokens, pos, 'AS')) {
    pos++;
    alias = tokens[pos].value;
    pos++;
  } else if (tokens[pos]?.type === 'IDENTIFIER' && !kw(tokens, pos, 'WHERE') && !kw(tokens, pos, 'ON') && !kw(tokens, pos, 'SET') && !kw(tokens, pos, 'ORDER') && !kw(tokens, pos, 'GROUP') && !kw(tokens, pos, 'LIMIT') && !kw(tokens, pos, 'JOIN') && !kw(tokens, pos, 'INNER') && !kw(tokens, pos, 'LEFT') && !kw(tokens, pos, 'RIGHT') && !kw(tokens, pos, 'FULL') && !kw(tokens, pos, 'CROSS') && !kw(tokens, pos, 'HAVING')) {
    alias = tokens[pos].value;
    pos++;
  }
  return { table, alias, pos };
}

/** Parse a comma-separated value list: (val1, val2, ...) */
export function parseValueList(tokens, pos) {
  pos = expect(tokens, pos, 'SYMBOL', '(');
  const values = [];
  const exprs = [];
  let hasExprs = false;
  while (!sym(tokens, pos, ')')) {
    const exprResult = parseExpression(tokens, pos);
    const expr = exprResult.expr;
    pos = exprResult.pos;
    if (typeof expr === 'object' && expr !== null && expr.literal !== undefined) {
      values.push(expr.literal);
      exprs.push(expr);
    } else {
      values.push(null);
      exprs.push(expr);
      hasExprs = true;
    }
    if (sym(tokens, pos, ',')) pos++;
  }
  pos++; // skip )
  const result = { values, pos };
  if (hasExprs) result.exprs = exprs;
  return result;
}

/** Parse a parenthesized identifier list: (col1, col2) */
export function parseIdentList(tokens, pos) {
  pos = expect(tokens, pos, 'SYMBOL', '(');
  const idents = [];
  while (!sym(tokens, pos, ')')) {
    idents.push(tokens[pos].value);
    pos++;
    if (sym(tokens, pos, ',')) pos++;
  }
  pos++; // skip )
  return { idents, pos };
}

/** Map SQL type names to normalized types */
export function normalizeType(typeName) {
  const upper = typeName.toUpperCase();
  const map = {
    'INTEGER': 'integer', 'INT': 'integer', 'BIGINT': 'integer', 'SMALLINT': 'integer',
    'TEXT': 'text', 'VARCHAR': 'text', 'CHAR': 'text', 'STRING': 'text',
    'REAL': 'real', 'FLOAT': 'real', 'DOUBLE': 'real', 'NUMERIC': 'real', 'DECIMAL': 'real',
    'BOOLEAN': 'boolean', 'BOOL': 'boolean',
    'BLOB': 'blob',
    'DATE': 'date', 'TIMESTAMP': 'timestamp',
  };
  return map[upper] || 'text';
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// SQL Expression Parser
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

const SQL_FUNCTIONS = ['UPPER','LOWER','LENGTH','ABS','ROUND','CONCAT','SUBSTR','SUBSTRING',
  'IFNULL','NULLIF','REPLACE','TRIM','LTRIM','RTRIM','COALESCE','CAST',
  'LEFT','RIGHT','REVERSE','REPEAT','LPAD','RPAD','POSITION','INSTR',
  'CHAR_LENGTH','CHARACTER_LENGTH','STARTS_WITH','ENDS_WITH',
  'CEIL','CEILING','FLOOR','POWER','POW','SQRT','MOD','SIGN',
  'LOG','LN','EXP','PI','RANDOM','RAND',
  'TYPEOF','GREATEST','LEAST',
  'IIF','DATE','TIME','DATETIME','STRFTIME','NOW',
  'CURRENT_TIMESTAMP','CURRENT_DATE','CURRENT_TIME',
  'MAX','MIN','SUM','AVG','COUNT'];

function isSQLFunction(name) {
  return SQL_FUNCTIONS.includes(name.toUpperCase());
}

export function parseExpression(tokens, pos) {
  let result = parseAdditiveExpr(tokens, pos);
  let expr = result.expr;
  pos = result.pos;
  while (pos < tokens.length && tokens[pos]?.value === '||') {
    pos++;
    const right = parseAdditiveExpr(tokens, pos);
    expr = { fn: 'CONCAT', args: [expr, right.expr] };
    pos = right.pos;
  }
  return { expr, pos };
}

function parseAdditiveExpr(tokens, pos) {
  let result = parseMultiplicativeExpr(tokens, pos);
  let expr = result.expr;
  pos = result.pos;
  while (pos < tokens.length && ['+', '-'].includes(tokens[pos]?.value) && tokens[pos]?.type === 'OPERATOR') {
    const op = tokens[pos].value; pos++;
    const right = parseMultiplicativeExpr(tokens, pos);
    expr = { op, left: expr, right: right.expr };
    pos = right.pos;
  }
  return { expr, pos };
}

function parseMultiplicativeExpr(tokens, pos) {
  let result = parseUnaryExpr(tokens, pos);
  let expr = result.expr;
  pos = result.pos;
  while (pos < tokens.length && ['*', '/', '%'].includes(tokens[pos]?.value)) {
    if (tokens[pos].type === 'SYMBOL' && tokens[pos].value === '*') {
      if (pos > 0) {
        const prevType = tokens[pos - 1]?.type || '';
        if (!['NUMBER', 'IDENTIFIER', 'SYMBOL'].includes(prevType)) break;
        if (prevType === 'SYMBOL' && tokens[pos - 1].value !== ')') break;
      } else break;
    }
    const op = tokens[pos].value; pos++;
    const right = parseUnaryExpr(tokens, pos);
    expr = { op, left: expr, right: right.expr };
    pos = right.pos;
  }
  return { expr, pos };
}

function parseUnaryExpr(tokens, pos) {
  if (pos < tokens.length && tokens[pos]?.value === '-' && tokens[pos]?.type === 'OPERATOR') {
    pos++;
    const result = parseUnaryExpr(tokens, pos);
    if (typeof result.expr === 'object' && result.expr?.literal !== undefined && typeof result.expr.literal === 'number') {
      return { expr: { literal: -result.expr.literal }, pos: result.pos };
    }
    return { expr: { op: '-', left: { literal: 0 }, right: result.expr }, pos: result.pos };
  }
  return parseAtomExpr(tokens, pos);
}

function parseAtomExpr(tokens, pos) {
  if (pos >= tokens.length) throw new Error(`Unexpected end of expression at position ${pos}`);
  const token = tokens[pos];

  if (kw(tokens, pos, 'CASE')) return parseCaseExpr(tokens, pos);

  if (sym(tokens, pos, '(')) {
    // Scalar subquery: (SELECT ...)
    if (isSubquery(tokens, pos)) {
      const sub = parseSubquery(tokens, pos);
      return { expr: { subquery: sub.tokens }, pos: sub.pos };
    }
    pos++;
    const result = parseExpression(tokens, pos);
    if (sym(tokens, result.pos, ')')) return { expr: result.expr, pos: result.pos + 1 };
    return result;
  }

  if (token.type === 'NUMBER') {
    // Tokenizer already converts to proper numeric type
    const val = typeof token.value === 'string'
      ? (token.value.includes('.') ? parseFloat(token.value) : parseInt(token.value, 10))
      : token.value;
    return { expr: { literal: val }, pos: pos + 1 };
  }
  if (token.type === 'STRING') return { expr: { literal: token.value }, pos: pos + 1 };
  if (token.type === 'BOOLEAN') return { expr: { literal: token.value === 'TRUE' || token.value === true }, pos: pos + 1 };
  if (token.type === 'NULL') return { expr: { literal: null }, pos: pos + 1 };

  // IIF(condition, then, else) → CASE WHEN condition THEN then ELSE else END
  if ((token.value || '').toUpperCase() === 'IIF' && sym(tokens, pos + 1, '(')) {
    pos += 2; // skip IIF(
    const condResult = parseWhereClause(tokens, pos);
    const cond = condResult.condition;
    pos = condResult.pos;
    pos++; // skip comma
    const thenResult = parseExpression(tokens, pos);
    pos = thenResult.pos;
    pos++; // skip comma
    const elseResult = parseExpression(tokens, pos);
    pos = elseResult.pos;
    pos++; // skip )
    return { expr: { case: [{ when: cond, then: thenResult.expr }], else: elseResult.expr }, pos };
  }

  if ((token.type === 'KEYWORD' || token.type === 'IDENTIFIER') && isSQLFunction(token.value) && sym(tokens, pos + 1, '(')) {
    return parseFunctionCall(token.value, tokens, pos + 1);
  }

  if (token.type === 'IDENTIFIER' || token.type === 'KEYWORD') {
    if (sym(tokens, pos + 1, '.')) {
      return { expr: `${token.value}.${tokens[pos + 2]?.value || ''}`, pos: pos + 3 };
    }
    return { expr: token.value, pos: pos + 1 };
  }

  if (sym(tokens, pos, '*')) return { expr: '*', pos: pos + 1 };

  throw new Error(`Unexpected token '${token.value}' (${token.type}) at position ${pos}`);
}

function parseCaseExpr(tokens, pos) {
  pos++; // skip CASE
  const branches = [];
  let elseExpr = undefined;
  while (kw(tokens, pos, 'WHEN')) {
    pos++;
    const condition = parseWhereClause(tokens, pos);
    pos = condition.pos;
    if (kw(tokens, pos, 'THEN')) pos++;
    const thenResult = parseExpression(tokens, pos);
    pos = thenResult.pos;
    branches.push({ when: condition.condition, then: thenResult.expr });
  }
  if (kw(tokens, pos, 'ELSE')) {
    pos++;
    const elseResult = parseExpression(tokens, pos);
    elseExpr = elseResult.expr;
    pos = elseResult.pos;
  }
  if (kw(tokens, pos, 'END')) pos++;
  const result = { case: branches };
  if (elseExpr !== undefined) result.else = elseExpr;
  return { expr: result, pos };
}

function parseFunctionCall(fn, tokens, pos) {
  pos = expect(tokens, pos, 'SYMBOL', '(');
  const args = [];
  if (fn.toUpperCase() === 'CAST') {
    const exprResult = parseExpression(tokens, pos);
    args.push(exprResult.expr);
    pos = exprResult.pos;
    if (kw(tokens, pos, 'AS')) { pos++; args.push({ literal: normalizeType(tokens[pos].value) }); pos++; }
    pos = expect(tokens, pos, 'SYMBOL', ')');
    return { expr: { fn: fn.toUpperCase(), args }, pos };
  }
  if (fn.toUpperCase() === 'COUNT' && sym(tokens, pos, '*')) {
    pos++;
    pos = expect(tokens, pos, 'SYMBOL', ')');
    return { expr: { fn: 'COUNT', args: [{ literal: '*' }] }, pos };
  }
  if (!sym(tokens, pos, ')')) {
    while (true) {
      const argResult = parseExpression(tokens, pos);
      args.push(argResult.expr);
      pos = argResult.pos;
      if (sym(tokens, pos, ',')) { pos++; } else break;
    }
  }
  pos = expect(tokens, pos, 'SYMBOL', ')');
  return { expr: { fn: fn.toUpperCase(), args }, pos };
}

export function isSubquery(tokens, pos) {
  return sym(tokens, pos, '(') && kw(tokens, pos + 1, 'SELECT');
}

export function parseSubquery(tokens, pos) {
  pos++; // skip (
  let depth = 1;
  const subTokens = [];
  while (pos < tokens.length && depth > 0) {
    if (sym(tokens, pos, '(')) depth++;
    if (sym(tokens, pos, ')')) { depth--; if (depth === 0) break; }
    subTokens.push(tokens[pos]);
    pos++;
  }
  pos++; // skip )
  return { tokens: subTokens, pos };
}

/** Parse OVER (PARTITION BY ... ORDER BY ...) window specification */
function parseWindowSpec(tokens, pos) {
  const spec = { partitionBy: null, orderBy: null };
  pos = expect(tokens, pos, 'SYMBOL', '(');

  // Empty window: OVER ()
  if (sym(tokens, pos, ')')) return { spec, pos: pos + 1 };

  // PARTITION BY
  if (kw(tokens, pos, 'PARTITION') && kw(tokens, pos + 1, 'BY')) {
    pos += 2;
    spec.partitionBy = [];
    while (pos < tokens.length && !kw(tokens, pos, 'ORDER') && !sym(tokens, pos, ')')) {
      spec.partitionBy.push(tokens[pos].value);
      pos++;
      if (sym(tokens, pos, ',')) pos++;
    }
  }

  // ORDER BY
  if (kw(tokens, pos, 'ORDER') && kw(tokens, pos + 1, 'BY')) {
    pos += 2;
    spec.orderBy = [];
    while (pos < tokens.length && !sym(tokens, pos, ')')) {
      const col = tokens[pos].value; pos++;
      let dir = 'asc';
      if (kw(tokens, pos, 'ASC')) { dir = 'asc'; pos++; }
      else if (kw(tokens, pos, 'DESC')) { dir = 'desc'; pos++; }
      spec.orderBy.push({ column: col, direction: dir });
      if (sym(tokens, pos, ',')) pos++;
    }
  }

  pos = expect(tokens, pos, 'SYMBOL', ')');
  return { spec, pos };
}
