/**
 * Event — a datum flowing through the stream.
 * Has a type (its signature) and arbitrary data.
 * This is E.
 */
export class Event {
  constructor(type, data = {}) {
    this.type = type;
    this.data = data;
  }
}
