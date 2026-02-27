/**
 * PureGate — event in, event out.
 *
 * No state access. No side effects. A pure function of its input.
 * Parse gates, filter gates, projection gates — all PureGates.
 *
 * transform(event) → Event | null
 *   - Returns an Event to continue processing
 *   - Returns null to consume the event and emit nothing
 *
 * Extends Gate so the Stream can register it.
 * The resolution layer (Phase 3) recognizes PureGate and
 * calls transform(event) without state resolution.
 */
import { Gate } from '../core/Gate.js';

export class PureGate extends Gate {
  constructor(signature) {
    super(signature);
  }

  /**
   * Transform the event. Override in subclass.
   * Returns an Event or null. No stream reference.
   */
  transform(event) {
    return null;
  }
}
