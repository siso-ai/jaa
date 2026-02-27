<?php
namespace Ice\Core;

abstract class Gate {
    public readonly string $signature;

    public function __construct(string $signature) {
        $this->signature = $signature;
    }

    abstract public function process(Event $event): Event|array|null;
}
