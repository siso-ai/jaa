import { PureGate } from '../../../protocol/PureGate.js';
import { Event } from '../../../core/Event.js';
import { kw } from './parser-utils.js';

export class TriggerCreateParseGate extends PureGate {
  constructor() { super('trigger_create_parse'); }

  transform(event) {
    const tokens = event.data.tokens;
    let pos = 2; // skip CREATE TRIGGER

    const name = tokens[pos].value;
    pos++;

    // BEFORE|AFTER
    const timing = tokens[pos].value.toLowerCase();
    pos++;

    // INSERT|UPDATE|DELETE
    const triggerEvent = tokens[pos].value.toLowerCase();
    pos++;

    // ON table
    pos++; // ON
    const table = tokens[pos].value;
    pos++;

    // Rest is the action body — store as raw tokens
    const action = { tokens: tokens.slice(pos) };

    return new Event('trigger_create_execute', { name, table, timing, event: triggerEvent, action });
  }
}

export class TriggerDropParseGate extends PureGate {
  constructor() { super('trigger_drop_parse'); }

  transform(event) {
    return new Event('trigger_drop_execute', { name: event.data.tokens[2].value });
  }
}
