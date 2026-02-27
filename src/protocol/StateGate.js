/**
 * StateGate — event + resolved state in, MutationBatch out.
 *
 * Declares what state it needs via reads(event) → ReadSet.
 * Receives resolved state in transform(event, state) → MutationBatch.
 * Never touches persistence directly. Pure function of its inputs.
 *
 * Execute gates — insert, update, delete, create table — are StateGates.
 *
 * Extends Gate so the Stream can register it.
 * The resolution layer (Phase 3) recognizes StateGate, resolves
 * the ReadSet, calls transform with the resolved state, and
 * applies the returned MutationBatch.
 */
import { Gate } from '../core/Gate.js';
import { ReadSet } from './ReadSet.js';
import { MutationBatch } from './MutationBatch.js';

export class StateGate extends Gate {
  constructor(signature) {
    super(signature);
  }

  /**
   * Declare what state this gate needs to read.
   * Examines the event to determine refs and patterns.
   * Pure function of the event. Override in subclass.
   */
  reads(event) {
    return new ReadSet();
  }

  /**
   * Transform the event given resolved state.
   * Returns a MutationBatch describing all mutations
   * and follow-up events. Override in subclass.
   */
  transform(event, state) {
    return new MutationBatch();
  }
}
