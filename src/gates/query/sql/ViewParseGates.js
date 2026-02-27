import { PureGate } from '../../../protocol/PureGate.js';
import { Event } from '../../../core/Event.js';
import { kw } from './parser-utils.js';
import { tokenize } from './tokenizer.js';

export class ViewCreateParseGate extends PureGate {
  constructor() { super('view_create_parse'); }

  transform(event) {
    const tokens = event.data.tokens;
    let pos = 2; // skip CREATE VIEW

    const name = tokens[pos].value;
    pos++;

    // AS SELECT ...
    pos++; // skip AS

    // Grab remaining tokens as the sub-select
    const subTokens = tokens.slice(pos);
    // Wrap as a select_parse event to be parsed separately
    // But we need the pipeline — re-parse the sub-select inline
    // For simplicity, store the raw SQL and tokens for the sub-query
    const subSql = event.data.sql.substring(event.data.sql.toUpperCase().indexOf(' AS ') + 4).trim();

    return new Event('view_create_execute', {
      name,
      query: { sql: subSql, tokens: subTokens },
      columns: null
    });
  }
}

export class ViewDropParseGate extends PureGate {
  constructor() { super('view_drop_parse'); }

  transform(event) {
    const tokens = event.data.tokens;
    return new Event('view_drop_execute', { name: tokens[2].value });
  }
}
