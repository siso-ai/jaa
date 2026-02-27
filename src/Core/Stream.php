<?php
namespace Ice\Core;

class Stream {
    /** @var array<string, Gate> */
    private array $gates = [];
    /** @var Event[] */
    public array $pending = [];
    private int $eventCount = 0;
    private ?StreamLog $log;
    private ?int $streamId;
    private ?int $parentStreamId;

    public function __construct(?StreamLog $log = null, ?int $parentStreamId = null) {
        $this->log = $log;
        $this->streamId = $log ? $log->nextStreamId() : null;
        $this->parentStreamId = $parentStreamId;
    }

    public function register(Gate $gate): void {
        if (isset($this->gates[$gate->signature])) {
            throw new \RuntimeException("Signature collision: '{$gate->signature}'");
        }
        $this->gates[$gate->signature] = $gate;
    }

    public function emit(Event $event): void {
        $this->eventCount++;
        $gate = $this->gates[$event->type] ?? null;

        if ($this->log) {
            $this->log->record([
                'streamId' => $this->streamId,
                'parentStreamId' => $this->parentStreamId,
                'eventType' => $event->type,
                'gateClaimed' => $gate ? $gate->signature : null,
                'eventData' => $event->data,
            ]);
        }

        if ($gate) {
            $result = $gate->process($event);
            if ($result instanceof Event) {
                $this->emit($result);
            } elseif (is_array($result)) {
                foreach ($result as $e) {
                    if ($e instanceof Event) $this->emit($e);
                }
            }
            // null → consumed
        } else {
            $this->pending[] = $event;
        }
    }

    public function sample(): array {
        return [
            'pending' => $this->pending,
            'eventCount' => $this->eventCount,
            'gateCount' => count($this->gates),
        ];
    }
}
