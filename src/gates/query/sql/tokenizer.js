/**
 * SQL Tokenizer — breaks SQL strings into typed tokens.
 *
 * Token types: KEYWORD, IDENTIFIER, NUMBER, STRING, OPERATOR, SYMBOL, BOOLEAN, NULL
 * Case-insensitive keyword matching. Identifiers preserve case.
 * Quoted identifiers: "col name" or `col name`.
 */

const KEYWORDS = new Set([
  'SELECT', 'FROM', 'WHERE', 'AND', 'OR', 'NOT', 'IN', 'LIKE', 'ILIKE', 'IS',
  'NULL', 'TRUE', 'FALSE', 'AS', 'ON', 'JOIN', 'INNER', 'LEFT', 'RIGHT',
  'FULL', 'OUTER', 'CROSS', 'INSERT', 'INTO', 'VALUES', 'UPDATE', 'SET',
  'DELETE', 'CREATE', 'DROP', 'TABLE', 'INDEX', 'VIEW', 'TRIGGER',
  'ALTER', 'ADD', 'CONSTRAINT', 'UNIQUE', 'CHECK', 'FOREIGN', 'KEY',
  'REFERENCES', 'PRIMARY', 'DEFAULT', 'IF', 'EXISTS', 'ORDER', 'BY',
  'ASC', 'DESC', 'LIMIT', 'OFFSET', 'GROUP', 'HAVING', 'DISTINCT',
  'COUNT', 'SUM', 'AVG', 'MIN', 'MAX', 'BETWEEN', 'CASE', 'WHEN',
  'THEN', 'ELSE', 'END', 'INTEGER', 'INT', 'TEXT', 'VARCHAR', 'REAL',
  'FLOAT', 'DOUBLE', 'BOOLEAN', 'BOOL', 'BLOB', 'DATE', 'TIMESTAMP',
  'AFTER', 'BEFORE', 'FOR', 'EACH', 'ROW', 'BEGIN',
  'COMMIT', 'ROLLBACK', 'RENAME', 'TO', 'COLUMN',
  'UNION', 'ALL', 'CAST', 'EXCEPT', 'INTERSECT',
  'EXPLAIN',
  'WITH', 'RECURSIVE', 'OVER', 'PARTITION', 'ROWS', 'RANGE', 'UNBOUNDED', 'PRECEDING', 'FOLLOWING', 'CURRENT',
  'NULLS', 'FIRST', 'LAST',
  'ROW_NUMBER', 'RANK', 'DENSE_RANK', 'NTILE', 'LAG', 'LEAD',
  'GROUP_CONCAT', 'SEPARATOR',
  'CONFLICT', 'DO', 'NOTHING', 'RETURNING', 'TRUNCATE',
]);

const OPERATORS = ['>=', '<=', '<>', '!=', '=', '<', '>', '||', '+', '/', '%'];

export function tokenize(sql) {
  const tokens = [];
  let i = 0;

  while (i < sql.length) {
    // Skip whitespace
    if (/\s/.test(sql[i])) { i++; continue; }

    // Skip comments
    if (sql[i] === '-' && sql[i + 1] === '-') {
      while (i < sql.length && sql[i] !== '\n') i++;
      continue;
    }

    // String literal (single quotes)
    if (sql[i] === "'") {
      let val = '';
      i++; // skip opening quote
      while (i < sql.length) {
        if (sql[i] === "'" && sql[i + 1] === "'") {
          val += "'"; i += 2; // escaped quote
        } else if (sql[i] === "'") {
          break; // closing quote
        } else {
          val += sql[i]; i++;
        }
      }
      i++; // skip closing quote
      tokens.push({ type: 'STRING', value: val });
      continue;
    }

    // Quoted identifier (double quotes)
    if (sql[i] === '"') {
      let val = '';
      i++;
      while (i < sql.length && sql[i] !== '"') { val += sql[i]; i++; }
      i++;
      tokens.push({ type: 'IDENTIFIER', value: val });
      continue;
    }

    // Backtick-quoted identifier
    if (sql[i] === '`') {
      let val = '';
      i++;
      while (i < sql.length && sql[i] !== '`') { val += sql[i]; i++; }
      i++;
      tokens.push({ type: 'IDENTIFIER', value: val });
      continue;
    }

    // Operators (check multi-char first)
    let matchedOp = null;
    for (const op of OPERATORS) {
      if (sql.substring(i, i + op.length) === op) {
        matchedOp = op; break;
      }
    }
    if (matchedOp) {
      tokens.push({ type: 'OPERATOR', value: matchedOp });
      i += matchedOp.length;
      continue;
    }

    // Symbols
    if ('(),*.;'.includes(sql[i])) {
      tokens.push({ type: 'SYMBOL', value: sql[i] });
      i++;
      continue;
    }

    // Numbers
    if (/[0-9]/.test(sql[i]) || (sql[i] === '-' && i + 1 < sql.length && /[0-9]/.test(sql[i + 1]) && (tokens.length === 0 || ['OPERATOR', 'SYMBOL', 'KEYWORD'].includes(tokens[tokens.length - 1].type)))) {
      let num = '';
      if (sql[i] === '-') { num += '-'; i++; }
      while (i < sql.length && /[0-9.]/.test(sql[i])) { num += sql[i]; i++; }
      tokens.push({ type: 'NUMBER', value: num.includes('.') ? parseFloat(num) : parseInt(num, 10) });
      continue;
    }

    // Words (keywords or identifiers)
    if (/[a-zA-Z_]/.test(sql[i])) {
      let word = '';
      while (i < sql.length && /[a-zA-Z0-9_]/.test(sql[i])) { word += sql[i]; i++; }
      const upper = word.toUpperCase();

      if (upper === 'TRUE' || upper === 'FALSE') {
        tokens.push({ type: 'BOOLEAN', value: upper === 'TRUE' });
      } else if (upper === 'NULL') {
        tokens.push({ type: 'NULL', value: null });
      } else if (KEYWORDS.has(upper)) {
        tokens.push({ type: 'KEYWORD', value: upper });
      } else {
        tokens.push({ type: 'IDENTIFIER', value: word });
      }
      continue;
    }

    // Minus sign not consumed as negative number — emit as operator
    if (sql[i] === '-') {
      tokens.push({ type: 'OPERATOR', value: '-' });
      i++;
      continue;
    }

    // Unknown character — skip
    i++;
  }

  return tokens;
}
