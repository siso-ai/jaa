<?php
namespace Ice\Protocol;

use Ice\Core\Gate;
use Ice\Core\Event;

abstract class StateGate extends Gate {
    public function __construct(string $signature) {
        parent::__construct($signature);
    }

    /**
     * Declare what state this gate needs.
     */
    abstract public function reads(Event $event): ReadSet;

    /**
     * Transform event given resolved state. Returns MutationBatch.
     */
    abstract public function transformEvent(Event $event, array $state): MutationBatch;

    /**
     * StateGate must be run through Runner.
     */
    public function process(Event $event): Event|array|null {
        throw new \RuntimeException('StateGate must be run through Runner');
    }
}
