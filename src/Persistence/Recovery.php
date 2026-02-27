<?php
namespace Ice\Persistence;

class Recovery {
    private string $walDir;
    private string $pendingPath;

    public function __construct(string $basePath) {
        $this->walDir = $basePath . '/wal';
        $this->pendingPath = $this->walDir . '/pending.json';
    }

    public function check(): array {
        if (!file_exists($this->pendingPath)) {
            return ['clean' => true, 'pending' => null];
        }
        $raw = file_get_contents($this->pendingPath);
        $pending = json_decode($raw, true);
        return ['clean' => false, 'pending' => $pending];
    }

    public function begin(array $puts, array $refSets, array $refDeletes): array {
        if (!is_dir($this->walDir)) {
            mkdir($this->walDir, 0777, true);
        }

        $batch = [
            'timestamp' => (int)(microtime(true) * 1000),
            'puts' => array_map(fn($p) => [
                'hash' => $p['hash'], 'content' => $p['content'], 'applied' => false
            ], $puts),
            'refSets' => array_map(fn($r) => [
                'name' => $r['name'], 'hash' => $r['hash'], 'applied' => false
            ], $refSets),
            'refDeletes' => array_map(fn($r) => [
                'name' => $r, 'applied' => false
            ], $refDeletes),
        ];

        file_put_contents($this->pendingPath, json_encode($batch, JSON_PRETTY_PRINT));
        return $batch;
    }

    public function commit(): void {
        if (file_exists($this->pendingPath)) {
            unlink($this->pendingPath);
        }
    }

    /**
     * Recover from crash — replay unapplied operations.
     * @param MemoryStore|FileStore $store
     * @param MemoryRefs|FileRefs $refs
     */
    public function recover(object $store, object $refs): void {
        $status = $this->check();
        if ($status['clean']) return;

        $batch = $status['pending'];

        foreach ($batch['puts'] as $put) {
            if (!$put['applied'] && isset($put['content'])) {
                $content = is_string($put['content'])
                    ? json_decode($put['content'], true)
                    : $put['content'];
                $store->put($content);
            }
        }

        foreach ($batch['refSets'] as $refSet) {
            if (!$refSet['applied']) {
                $refs->set($refSet['name'], $refSet['hash']);
            }
        }

        foreach ($batch['refDeletes'] as $refDel) {
            if (!$refDel['applied']) {
                $refs->delete($refDel['name']);
            }
        }

        $this->commit();
    }
}
