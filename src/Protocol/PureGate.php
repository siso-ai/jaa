<?php
namespace Ice\Protocol;

use Ice\Core\Gate;
use Ice\Core\Event;

abstract class PureGate extends Gate {
    public function __construct(string $signature) {
        parent::__construct($signature);
    }

    /**
     * Transform the event. Override in subclass.
     * Returns Event, array of Events, or null.
     */
    abstract public function transform(Event $event): Event|array|null;

    public function process(Event $event): Event|array|null {
        return $this->transform($event);
    }
}
