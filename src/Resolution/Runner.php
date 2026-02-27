<?php
namespace Ice\Resolution;

use Ice\Core\Event;
use Ice\Core\Gate;
use Ice\Core\Stream;
use Ice\Protocol\PureGate;
use Ice\Protocol\StateGate;
use Ice\Protocol\MutationBatch;

class Runner {
    private Stream $stream;
    private object $store;
    private object $refs;
    private ?\Ice\Core\StreamLog $log;
    /** @var array<string, array{reads: callable, execute: callable}> */
    private array $stepHandlers = [];

    public function __construct(object $store, object $refs, ?\Ice\Core\StreamLog $log = null) {
        $this->store = $store;
        $this->refs = $refs;
        $this->log = $log;
        $this->stream = new Stream($log);
    }

    public function register(Gate $gate): void {
        $runner = $this;

        if ($gate instanceof StateGate) {
            $wrapper = new class($gate->signature) extends Gate {
                public \Closure $processFunc;
                public function __construct(string $sig) { parent::__construct($sig); }
                public function process(Event $event): Event|array|null {
                    return ($this->processFunc)($event);
                }
            };
            $stateGate = $gate;
            $wrapper->processFunc = function (Event $event) use ($runner, $stateGate): Event|array|null {
                try {
                    $readSet = $stateGate->reads($event);
                    $state = $runner->resolve($readSet);
                    $batch = $stateGate->transformEvent($event, $state);
                    return $runner->apply($batch);
                } catch (\Throwable $e) {
                    return new Event('error', [
                        'message' => $e->getMessage(),
                        'source' => $stateGate->signature,
                    ]);
                }
            };
            $this->stream->register($wrapper);

        } elseif ($gate instanceof PureGate) {
            $wrapper = new class($gate->signature) extends Gate {
                public \Closure $processFunc;
                public function __construct(string $sig) { parent::__construct($sig); }
                public function process(Event $event): Event|array|null {
                    return ($this->processFunc)($event);
                }
            };
            $pureGate = $gate;
            $wrapper->processFunc = function (Event $event) use ($pureGate): Event|array|null {
                try {
                    return $pureGate->transform($event);
                } catch (\Throwable $e) {
                    return new Event('error', [
                        'message' => $e->getMessage(),
                        'source' => $pureGate->signature,
                    ]);
                }
            };
            $this->stream->register($wrapper);

        } else {
            $this->stream->register($gate);
        }
    }

    public function emit(Event $event): void {
        $this->stream->emit($event);
    }

    public function sample(): array {
        return $this->stream->sample();
    }

    /**
     * Resolve a ReadSet against persistence.
     */
    public function resolve(\Ice\Protocol\ReadSet $readSet): array {
        $state = ['refs' => [], 'patterns' => []];

        foreach ($readSet->getRefs() as $name) {
            $hash = $this->refs->get($name);
            if ($hash === null) {
                $state['refs'][$name] = null;
            } else {
                $state['refs'][$name] = $this->store->get($hash);
            }
        }

        foreach ($readSet->getPatterns() as $pattern) {
            $state['patterns'][$pattern] = [];
            $names = $this->refs->list($pattern);
            foreach ($names as $name) {
                $hash = $this->refs->get($name);
                if ($hash !== null) {
                    $state['patterns'][$pattern][$name] = $this->store->get($hash);
                }
            }
        }

        return $state;
    }

    /**
     * Apply a MutationBatch. Returns array of follow-up events.
     */
    public function apply(MutationBatch $batch): array|Event|null {
        // 1. Store all puts, collect hashes
        $hashes = [];
        foreach ($batch->getPuts() as $put) {
            $hashes[] = $this->store->put($put['content']);
        }

        // 2. Apply ref sets
        foreach ($batch->getRefSets() as $refSet) {
            $hash = isset($refSet['putIndex'])
                ? $hashes[$refSet['putIndex']]
                : $refSet['hash'];
            $this->refs->set($refSet['name'], $hash);
        }

        // 3. Apply ref deletes
        foreach ($batch->getRefDeletes() as $name) {
            $this->refs->delete($name);
        }

        // 4. Return follow-up events (Stream handles emission)
        $events = $batch->getEvents();
        if (count($events) === 0) return null;
        if (count($events) === 1) return $events[0];
        return $events;
    }

    /**
     * Snapshot persistence state for rollback.
     */
    public function snapshot(): array {
        return [
            'store' => $this->store->getObjects(),
            'refs' => $this->refs->getRefs(),
        ];
    }

    /**
     * Restore persistence from snapshot.
     */
    public function restore(array $snapshot): void {
        $this->store->setObjects($snapshot['store']);
        $this->refs->setRefs($snapshot['refs']);
    }

    // Step handler registry for extensible pipeline
    public function registerStepHandler(string $type, array $handler): void {
        $this->stepHandlers[$type] = $handler;
    }

    public function getStepHandlers(): array {
        return $this->stepHandlers;
    }

    public function getStore(): object { return $this->store; }
    public function getRefs(): object { return $this->refs; }

    /** Clear pending events (for REPL use). */
    public function clearPending(): void {
        $this->stream->pending = [];
    }
}
