/**
 * SQLDispatchGate — routes SQL to the appropriate parse gate.
 * Examines first tokens to determine statement type.
 */
import { PureGate } from '../../../protocol/PureGate.js';
import { Event } from '../../../core/Event.js';
import { tokenize } from './tokenizer.js';
import { kw, sym, parseSubquery, parseIdentList } from './parser-utils.js';

export class SQLDispatchGate extends PureGate {
  constructor() { super('sql'); }

  transform(event) {
    const sql = event.data.sql.trim();
    const tokens = tokenize(sql);
    if (tokens.length === 0) {
      return new Event('error', { message: 'Empty SQL statement', source: 'sql' });
    }

    const first = tokens[0].value;
    const second = tokens[1]?.value;

    if (first === 'CREATE' && second === 'TABLE') return new Event('create_table_parse', { sql, tokens });
    if (first === 'DROP' && second === 'TABLE') return new Event('drop_table_parse', { sql, tokens });
    if (first === 'INSERT') return new Event('insert_parse', { sql, tokens });
    if (first === 'SELECT') return new Event('select_parse', { sql, tokens });
    if (first === 'UPDATE') return new Event('update_parse', { sql, tokens });
    if (first === 'DELETE') return new Event('delete_parse', { sql, tokens });
    if (first === 'CREATE' && second === 'UNIQUE') return new Event('index_create_parse', { sql, tokens });
    if (first === 'CREATE' && second === 'INDEX') return new Event('index_create_parse', { sql, tokens });
    if (first === 'DROP' && second === 'INDEX') return new Event('index_drop_parse', { sql, tokens });
    if (first === 'CREATE' && second === 'VIEW') return new Event('view_create_parse', { sql, tokens });
    if (first === 'DROP' && second === 'VIEW') return new Event('view_drop_parse', { sql, tokens });
    if (first === 'CREATE' && second === 'TRIGGER') return new Event('trigger_create_parse', { sql, tokens });
    if (first === 'DROP' && second === 'TRIGGER') return new Event('trigger_drop_parse', { sql, tokens });
    if (first === 'ALTER' && second === 'TABLE') {
      let addIsConstraint = false, dropIsConstraint = false;
      let addIsColumn = false, dropIsColumn = false;
      let hasRename = false;
      for (let i = 0; i < tokens.length - 1; i++) {
        if (tokens[i].value === 'ADD' && tokens[i + 1]?.value === 'CONSTRAINT') addIsConstraint = true;
        if (tokens[i].value === 'DROP' && tokens[i + 1]?.value === 'CONSTRAINT') dropIsConstraint = true;
        if (tokens[i].value === 'ADD' && !addIsConstraint) addIsColumn = true;
        if (tokens[i].value === 'DROP' && !dropIsConstraint) dropIsColumn = true;
        if (tokens[i].value === 'RENAME') hasRename = true;
      }
      if (addIsConstraint) return new Event('constraint_create_parse', { sql, tokens });
      if (dropIsConstraint) return new Event('constraint_drop_parse', { sql, tokens });
      if (hasRename) return new Event('rename_table_parse', { sql, tokens });
      if (addIsColumn) return new Event('alter_table_add_column_parse', { sql, tokens });
      if (dropIsColumn) return new Event('alter_table_drop_column_parse', { sql, tokens });
    }

    // Transaction support
    if (first === 'BEGIN') return new Event('transaction_begin', {});
    if (first === 'COMMIT') return new Event('transaction_commit', {});
    if (first === 'ROLLBACK') return new Event('transaction_rollback', {});

    // TRUNCATE TABLE
    if (first === 'TRUNCATE') {
      let pos = 1;
      if (kw(tokens, pos, 'TABLE')) pos++;
      const table = tokens[pos]?.value || '';
      return new Event('delete_execute', { table, where: null });
    }

    // WITH ... AS (Common Table Expressions)
    if (first === 'WITH') {
      const ctes = {};
      let pos = 1;
      let recursive = false;
      if (kw(tokens, pos, 'RECURSIVE')) { recursive = true; pos++; }
      const cteColumns = {};
      while (pos < tokens.length) {
        const cteName = tokens[pos].value;
        pos++;
        // Optional column list: name(col1, col2, ...)
        let cols = null;
        if (sym(tokens, pos, '(')) {
          const identResult = parseIdentList(tokens, pos);
          cols = identResult.idents;
          pos = identResult.pos;
        }
        if (cols) cteColumns[cteName] = cols;
        if (kw(tokens, pos, 'AS')) pos++;
        if (sym(tokens, pos, '(')) {
          const sub = parseSubquery(tokens, pos);
          ctes[cteName] = sub.tokens;
          pos = sub.pos;
        }
        if (sym(tokens, pos, ',')) { pos++; continue; }
        break;
      }
      const mainTokens = tokens.slice(pos);
      if (mainTokens.length > 0 && mainTokens[0].value === 'SELECT') {
        const data = { sql, tokens: mainTokens, ctes };
        if (recursive) data.recursive = true;
        if (Object.keys(cteColumns).length > 0) data.cteColumns = cteColumns;
        return new Event('select_parse', data);
      }
      return new Event('error', { message: 'WITH must be followed by SELECT', source: 'sql' });
    }

    // EXPLAIN
    if (first === 'EXPLAIN') {
      const innerTokens = tokens.slice(1);
      return new Event('explain', { sql: innerTokens.map(t => t.value).join(' '), tokens: innerTokens });
    }

    return new Event('error', { message: `Unrecognized SQL: ${sql}`, source: 'sql' });
  }
}
