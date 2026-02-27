/**
 * Gate — a shape that recognizes one type of event
 * and transforms it into another.
 *
 * The signature is a unique key. No two gates in a
 * stream may share one. The stream uses it for
 * direct lookup — O(1), no scanning.
 *
 * This is the shape of the arrow in →E→E→.
 */
export class Gate {
  constructor(signature) {
    this.signature = signature;
  }

  /**
   * Transform the event. May call stream.emit()
   * zero or more times. The original event is consumed.
   */
  transform(event, stream) {
    // Override in subclass.
  }
}
