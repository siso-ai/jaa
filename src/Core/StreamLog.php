<?php
namespace Ice\Core;

class StreamLog {
    private array $entries = [];
    private int $seq = 0;
    public string $level;

    private static int $streamIdCounter = 0;

    public function __construct(string $level = 'EVENTS') {
        $this->level = $level;
    }

    public function record(array $data): void {
        $levels = ['OFF' => 0, 'EVENTS' => 1, 'DEEP' => 2, 'DATA' => 3];
        $lvl = $levels[$this->level] ?? 0;
        if ($lvl === 0) return;

        $entry = [
            'seq' => $this->seq++,
            'time' => (int)(microtime(true) * 1000),
            'type' => $data['eventType'],
            'claimed' => $data['gateClaimed'],
        ];

        if ($lvl >= 2) {
            $entry['streamId'] = $data['streamId'];
            if ($data['parentStreamId'] !== null) {
                $entry['parentStreamId'] = $data['parentStreamId'];
            }
        }

        if ($lvl >= 3) {
            $entry['data'] = $data['eventData'];
        }

        $this->entries[] = $entry;
    }

    public function nextStreamId(): int {
        return ++self::$streamIdCounter;
    }

    public function sample(): array {
        return [
            'level' => $this->level,
            'count' => count($this->entries),
            'entries' => $this->entries,
        ];
    }

    public function clear(): void {
        $this->entries = [];
        $this->seq = 0;
    }

    // Additional methods for simple log access
    public function append(Event $event): int {
        $idx = count($this->entries);
        $this->entries[] = [
            'seq' => $this->seq++,
            'time' => (int)(microtime(true) * 1000),
            'type' => $event->type,
            'data' => $event->data,
        ];
        return $idx;
    }

    public function get(int $index): array {
        return $this->entries[$index] ?? throw new \RuntimeException("Log entry not found: $index");
    }

    public function since(int $index): array {
        return array_slice($this->entries, $index);
    }

    public function length(): int {
        return count($this->entries);
    }

    public function all(): array {
        return $this->entries;
    }
}
