<?php
namespace Ice\Protocol;

use Ice\Core\Event;

class MutationBatch {
    /** @var array{type: string, content: mixed}[] */
    private array $puts = [];
    /** @var array{name: string, putIndex?: int, hash?: string}[] */
    private array $refSets = [];
    /** @var string[] */
    private array $refDeletes = [];
    /** @var Event[] */
    private array $events = [];

    /**
     * Add an object to store. Returns $this for chaining.
     */
    public function put(string $type, mixed $content): self {
        $this->puts[] = ['type' => $type, 'content' => $content];
        return $this;
    }

    /**
     * Set a ref to point at a put's hash (by index into puts array).
     */
    public function refSet(string $name, int $putIndex): self {
        if ($putIndex >= count($this->puts)) {
            throw new \RuntimeException(
                "refSet index $putIndex out of range (" . count($this->puts) . " puts)"
            );
        }
        $this->refSets[] = ['name' => $name, 'putIndex' => $putIndex];
        return $this;
    }

    /**
     * Set a ref to a known hash directly.
     */
    public function refSetHash(string $name, string $hash): self {
        $this->refSets[] = ['name' => $name, 'hash' => $hash];
        return $this;
    }

    /**
     * Delete a ref by name.
     */
    public function refDelete(string $name): self {
        $this->refDeletes[] = $name;
        return $this;
    }

    /**
     * Add a follow-up event to emit after mutations.
     */
    public function emit(Event $event): self {
        $this->events[] = $event;
        return $this;
    }

    public function getPuts(): array { return $this->puts; }
    public function getRefSets(): array { return $this->refSets; }
    public function getRefDeletes(): array { return $this->refDeletes; }
    public function getEvents(): array { return $this->events; }
}
