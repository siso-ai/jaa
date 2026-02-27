/**
 * Trigger gates — create and drop triggers.
 */
import { StateGate } from '../../protocol/StateGate.js';
import { ReadSet } from '../../protocol/ReadSet.js';
import { MutationBatch } from '../../protocol/MutationBatch.js';
import { Event } from '../../core/Event.js';

export class TriggerCreateExecuteGate extends StateGate {
  constructor() { super('trigger_create_execute'); }

  reads(event) {
    return new ReadSet().ref(`db/triggers/${event.data.name}`);
  }

  transform(event, state) {
    const { name, table, timing, event: triggerEvent, action } = event.data;
    const existing = state.refs[`db/triggers/${name}`];

    if (existing !== null) {
      return new MutationBatch()
        .emit(new Event('error', { message: `Trigger '${name}' already exists`, source: 'trigger_create_execute' }));
    }

    return new MutationBatch()
      .put('trigger', { name, table, timing, event: triggerEvent, action })
      .refSet(`db/triggers/${name}`, 0)
      .emit(new Event('trigger_created', { name }));
  }
}

export class TriggerDropExecuteGate extends StateGate {
  constructor() { super('trigger_drop_execute'); }

  reads(event) {
    return new ReadSet().ref(`db/triggers/${event.data.name}`);
  }

  transform(event, state) {
    const { name } = event.data;
    if (state.refs[`db/triggers/${name}`] === null) {
      return new MutationBatch()
        .emit(new Event('error', { message: `Trigger '${name}' does not exist`, source: 'trigger_drop_execute' }));
    }

    return new MutationBatch()
      .refDelete(`db/triggers/${name}`)
      .emit(new Event('trigger_dropped', { name }));
  }
}
