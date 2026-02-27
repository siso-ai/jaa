/**
 * Expression evaluator for database gates.
 *
 * Evaluates conditions and expressions against row data.
 * Used by FilterGate, ProjectionGate, UpdateExecuteGate.
 * Pure functions — no state access, no side effects.
 */

/**
 * Evaluate a where condition against a row.
 * Returns true/false.
 *
 * Condition shapes:
 *   { column, op, value }           — comparison
 *   { and: [conditions] }           — logical AND
 *   { or: [conditions] }            — logical OR
 *   { not: condition }              — logical NOT
 *   { column, op: "in", value: [] } — IN list
 *   { column, op: "like", value }   — LIKE pattern
 *   { column, op: "is_null" }       — IS NULL
 *   { column, op: "is_not_null" }   — IS NOT NULL
 */
export function evaluateCondition(condition, row) {
  if (!condition) return true;

  // Logical operators
  if (condition.and) {
    return condition.and.every(c => evaluateCondition(c, row));
  }
  if (condition.or) {
    return condition.or.some(c => evaluateCondition(c, row));
  }
  if (condition.not) {
    return !evaluateCondition(condition.not, row);
  }
  if (condition.exists !== undefined) {
    // exists is pre-resolved to a boolean by QueryPlanGate
    return !!condition.resolved;
  }

  // Expression-based comparison: {leftExpr, op, rightExpr}
  if (condition.leftExpr !== undefined) {
    const val = evaluateExpression(condition.leftExpr, row);
    const op = condition.op;

    if (op === 'is_null') return val === null || val === undefined;
    if (op === 'is_not_null') return val !== null && val !== undefined;
    if (op === 'in') {
      const target = condition.value || [];
      return Array.isArray(target) && target.includes(val);
    }
    if (op === 'like') return matchLike(val, condition.value || '', false);
    if (op === 'ilike') return matchLike(val, condition.value || '', true);

    const right = condition.rightExpr !== undefined ? evaluateExpression(condition.rightExpr, row) : null;
    switch (op) {
      case '=': case '==': return val === right;
      case '!=': case '<>': return val !== right;
      case '<': return val < right;
      case '>': return val > right;
      case '<=': return val <= right;
      case '>=': return val >= right;
      default: return false;
    }
  }

  // Classic column-based comparison: {column, op, value}
  const colName = condition.column;
  let val = row[colName];
  // alias.column fallback
  if (val === undefined && colName && colName.indexOf('.') !== -1) {
    const shortCol = colName.substring(colName.indexOf('.') + 1);
    val = row[shortCol];
  }
  const target = condition.value;

  switch (condition.op) {
    case '=':
    case '==':
      return val === target;
    case '!=':
    case '<>':
      return val !== target;
    case '<':
      return val < target;
    case '>':
      return val > target;
    case '<=':
      return val <= target;
    case '>=':
      return val >= target;
    case 'in':
      return Array.isArray(target) && target.includes(val);
    case 'like':
      return matchLike(val, target, false);
    case 'ilike':
      return matchLike(val, target, true);
    case 'is_null':
      return val === null || val === undefined;
    case 'is_not_null':
      return val !== null && val !== undefined;
    default:
      return false;
  }
}

/**
 * Evaluate an expression against a row.
 * Returns a value.
 *
 * Expression shapes:
 *   "column_name"                        — column reference (string)
 *   { literal: value }                   — literal value
 *   { op, left, right }                  — arithmetic/comparison
 *   { fn, args }                         — function call
 *   { case: [{ when, then }], else }     — CASE WHEN
 *   { coalesce: [exprs] }               — COALESCE
 */
export function evaluateExpression(expr, row) {
  // String → column reference
  if (typeof expr === 'string') {
    if (expr in row) return row[expr];
    // alias.column fallback
    const dot = expr.indexOf('.');
    if (dot !== -1) {
      const col = expr.substring(dot + 1);
      if (col in row) return row[col];
    }
    return undefined;
  }

  // Number/boolean/null → literal
  if (typeof expr === 'number' || typeof expr === 'boolean' || expr === null) {
    return expr;
  }

  // Literal wrapper
  if (expr.literal !== undefined) {
    return expr.literal;
  }

  // Pre-resolved scalar subquery
  if (expr.subquery && 'resolved' in expr) {
    return expr.resolved;
  }

  // Arithmetic / comparison
  if (expr.op && expr.left !== undefined && expr.right !== undefined) {
    const left = evaluateExpression(expr.left, row);
    const right = evaluateExpression(expr.right, row);
    switch (expr.op) {
      case '+': return left + right;
      case '-': return left - right;
      case '*': return left * right;
      case '/': return right !== 0 ? left / right : null;
      case '%': return right !== 0 ? left % right : null;
      default: return null;
    }
  }

  // Function calls
  if (expr.fn) {
    const fnUpper = expr.fn.toUpperCase();
    // For aggregate functions, check if pre-computed in row (HAVING support)
    const aggFns = ['SUM', 'COUNT', 'AVG', 'MIN', 'MAX', 'GROUP_CONCAT'];
    if (aggFns.includes(fnUpper)) {
      const argStr = expr.args && expr.args[0] ? (typeof expr.args[0] === 'string' ? expr.args[0] : '*') : '*';
      const synKey = `${fnUpper}(${argStr})`;
      if (synKey in row) return row[synKey];
    }
    const args = (expr.args || []).map(a => evaluateExpression(a, row));
    const fn = expr.fn.toUpperCase();
    const s0 = typeof args[0] === 'string' ? args[0] : null;
    const n0 = typeof args[0] === 'number' ? args[0] : null;
    switch (fn) {
      // String functions
      case 'UPPER': return s0 !== null ? s0.toUpperCase() : null;
      case 'LOWER': return s0 !== null ? s0.toLowerCase() : null;
      case 'LENGTH':
      case 'CHAR_LENGTH':
      case 'CHARACTER_LENGTH': return s0 !== null ? s0.length : null;
      case 'CONCAT': return args.map(a => a ?? '').join('');
      case 'SUBSTR':
      case 'SUBSTRING': return s0 !== null
        ? s0.substring((args[1] || 1) - 1, args[2] !== undefined ? (args[1] || 1) - 1 + args[2] : undefined)
        : null;
      case 'REPLACE': return s0 !== null && typeof args[1] === 'string'
        ? s0.split(args[1]).join(args[2] ?? '')
        : null;
      case 'TRIM': return s0 !== null ? s0.trim() : null;
      case 'LTRIM': return s0 !== null ? s0.trimStart() : null;
      case 'RTRIM': return s0 !== null ? s0.trimEnd() : null;
      case 'LEFT': return s0 !== null && typeof args[1] === 'number'
        ? s0.substring(0, args[1]) : null;
      case 'RIGHT': return s0 !== null && typeof args[1] === 'number'
        ? s0.substring(s0.length - args[1]) : null;
      case 'REVERSE': return s0 !== null ? s0.split('').reverse().join('') : null;
      case 'REPEAT': return s0 !== null && typeof args[1] === 'number'
        ? s0.repeat(Math.max(0, args[1])) : null;
      case 'LPAD': return s0 !== null && typeof args[1] === 'number'
        ? s0.padStart(args[1], args[2] ?? ' ') : null;
      case 'RPAD': return s0 !== null && typeof args[1] === 'number'
        ? s0.padEnd(args[1], args[2] ?? ' ') : null;
      case 'POSITION':
      case 'INSTR': return s0 !== null && typeof args[1] === 'string'
        ? (s0.indexOf(args[1]) + 1) : 0;
      case 'STARTS_WITH': return s0 !== null && typeof args[1] === 'string'
        ? (s0.startsWith(args[1]) ? 1 : 0) : 0;
      case 'ENDS_WITH': return s0 !== null && typeof args[1] === 'string'
        ? (s0.endsWith(args[1]) ? 1 : 0) : 0;

      // Math functions
      case 'ABS': return n0 !== null ? Math.abs(n0) : null;
      case 'ROUND': return n0 !== null
        ? (args[1] !== undefined ? parseFloat(n0.toFixed(args[1])) : Math.round(n0))
        : null;
      case 'CEIL':
      case 'CEILING': return n0 !== null ? Math.ceil(n0) : null;
      case 'FLOOR': return n0 !== null ? Math.floor(n0) : null;
      case 'POWER':
      case 'POW': return n0 !== null && typeof args[1] === 'number'
        ? Math.pow(n0, args[1]) : null;
      case 'SQRT': return n0 !== null && n0 >= 0 ? Math.sqrt(n0) : null;
      case 'MOD': return n0 !== null && typeof args[1] === 'number' && args[1] !== 0
        ? n0 % args[1] : null;
      case 'SIGN': return n0 !== null ? Math.sign(n0) : null;
      case 'LOG': return n0 !== null && n0 > 0
        ? (args[1] !== undefined ? Math.log(n0) / Math.log(args[1]) : Math.log10(n0))
        : null;
      case 'LN': return n0 !== null && n0 > 0 ? Math.log(n0) : null;
      case 'EXP': return n0 !== null ? Math.exp(n0) : null;
      case 'PI': return Math.PI;
      case 'RANDOM':
      case 'RAND': return Math.random();

      // Null/type functions
      case 'IFNULL': return (args[0] !== null && args[0] !== undefined) ? args[0] : (args[1] ?? null);
      case 'NULLIF': return args[0] === args[1] ? null : args[0];
      case 'COALESCE': return args.find(a => a !== null && a !== undefined) ?? null;
      case 'CAST': return castValue(args[0], args[1]);
      case 'TYPEOF': {
        const v = args[0];
        if (v === null || v === undefined) return 'null';
        if (typeof v === 'number') return Number.isInteger(v) ? 'integer' : 'real';
        return 'text';
      }
      case 'GREATEST': {
        const valid = args.filter(a => a !== null && a !== undefined);
        return valid.length > 0 ? Math.max(...valid) : null;
      }
      case 'LEAST': {
        const valid = args.filter(a => a !== null && a !== undefined);
        return valid.length > 0 ? Math.min(...valid) : null;
      }
      case 'IIF':
        return (args[0]) ? (args[1] ?? null) : (args[2] ?? null);
      case 'IFNULL':
        return (args[0] !== null && args[0] !== undefined) ? args[0] : (args[1] ?? null);
      case 'DATE': {
        const input = args[0] || 'now';
        const d = input.toLowerCase() === 'now' ? new Date() : new Date(input);
        return d.toISOString().slice(0, 10);
      }
      case 'TIME': {
        const input = args[0] || 'now';
        const d = input.toLowerCase() === 'now' ? new Date() : new Date(input);
        return d.toISOString().slice(11, 19);
      }
      case 'DATETIME': {
        const input = args[0] || 'now';
        const d = input.toLowerCase() === 'now' ? new Date() : new Date(input);
        return d.toISOString().slice(0, 19).replace('T', ' ');
      }
      case 'STRFTIME': {
        const fmt = args[0] || '%Y-%m-%d';
        const input = args[1] || 'now';
        const d = input.toLowerCase() === 'now' ? new Date() : new Date(input);
        const pad = n => String(n).padStart(2, '0');
        return fmt.replace(/%Y/g, d.getUTCFullYear())
          .replace(/%m/g, pad(d.getUTCMonth() + 1))
          .replace(/%d/g, pad(d.getUTCDate()))
          .replace(/%H/g, pad(d.getUTCHours()))
          .replace(/%M/g, pad(d.getUTCMinutes()))
          .replace(/%S/g, pad(d.getUTCSeconds()))
          .replace(/%w/g, d.getUTCDay())
          .replace(/%j/g, Math.floor((d - new Date(d.getUTCFullYear(), 0, 0)) / 86400000));
      }
      case 'NOW':
      case 'CURRENT_TIMESTAMP':
        return new Date().toISOString().slice(0, 19).replace('T', ' ');
      case 'CURRENT_DATE':
        return new Date().toISOString().slice(0, 10);
      case 'CURRENT_TIME':
        return new Date().toISOString().slice(11, 19);
      default: return null;
    }
  }

  // COALESCE shorthand
  if (expr.coalesce) {
    for (const e of expr.coalesce) {
      const val = evaluateExpression(e, row);
      if (val !== null && val !== undefined) return val;
    }
    return null;
  }

  // CASE WHEN
  if (expr.case) {
    for (const branch of expr.case) {
      if (evaluateCondition(branch.when, row)) {
        return evaluateExpression(branch.then, row);
      }
    }
    return expr.else !== undefined ? evaluateExpression(expr.else, row) : null;
  }

  return null;
}

/**
 * LIKE pattern matching.
 * % matches any sequence of characters.
 * _ matches any single character.
 */
function matchLike(value, pattern, caseInsensitive = false) {
  if (value === null || value === undefined) return false;
  if (typeof value !== 'string') value = String(value);
  if (typeof pattern !== 'string') return false;

  // Convert LIKE pattern to regex
  let regex = '^';
  for (const ch of pattern) {
    if (ch === '%') regex += '.*';
    else if (ch === '_') regex += '.';
    else regex += ch.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  }
  regex += '$';
  return new RegExp(regex, caseInsensitive ? 'i' : '').test(value);
}

function castValue(val, type) {
  if (val === null || val === undefined) return null;
  const t = (typeof type === 'string') ? type.toLowerCase() : 'text';
  switch (t) {
    case 'integer': case 'int': return parseInt(val, 10) || 0;
    case 'real': case 'float': case 'double': return parseFloat(val) || 0;
    case 'text': case 'string': case 'varchar': return String(val);
    case 'boolean': case 'bool': return Boolean(val);
    default: return val;
  }
}
