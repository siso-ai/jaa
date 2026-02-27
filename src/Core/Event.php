<?php
namespace Ice\Core;

class Event {
    public readonly string $type;
    public readonly array $data;

    public function __construct(string $type, array $data = []) {
        $this->type = $type;
        $this->data = $data;
    }
}
