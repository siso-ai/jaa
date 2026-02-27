<?php
/**
 * ICE Database — Phase 7 Test Suite
 * Run: php test/test.php
 */
require __DIR__ . '/runner.php';
require __DIR__ . '/../ice/autoload.php';

use Ice\Core\Event;
use Ice\Core\Gate;
use Ice\Core\Stream;
use Ice\Core\StreamLog;
use Ice\Persistence\MemoryStore;
use Ice\Persistence\MemoryRefs;
use Ice\Persistence\FileStore;
use Ice\Persistence\FileRefs;
use Ice\Persistence\Recovery;
use Ice\Protocol\ReadSet;
use Ice\Protocol\MutationBatch;
use Ice\Protocol\PureGate;
use Ice\Protocol\StateGate;
use Ice\Resolution\Runner;

use function Ice\Persistence\canonicalize;
use function Ice\Gates\Database\registerDatabaseGates;
use function Ice\Gates\Database\evaluateCondition;
use function Ice\Gates\Database\evaluateExpression;
use function Ice\Gates\Database\filterRows;
use function Ice\Gates\Database\projectRows;
use function Ice\Gates\Database\orderByRows;
use function Ice\Gates\Database\limitRows;
use function Ice\Gates\Database\distinctRows;
use function Ice\Gates\Database\aggregateRows;
use function Ice\Gates\Database\joinRows;

echo "ICE Database — Phase 7: PHP Engine Port\n";

// ─── Helpers ──────────────────────────────────────────────
function freshRunner(): Runner {
    $runner = new Runner(new MemoryStore(), new MemoryRefs());
    registerDatabaseGates($runner);
    return $runner;
}

function createTable(Runner $r, string $name, array $cols): void {
    $r->emit(new Event('create_table_execute', ['table' => $name, 'columns' => $cols]));
}

function insertRow(Runner $r, string $table, array $row): void {
    $r->emit(new Event('insert_execute', ['table' => $table, 'row' => $row]));
}

function getPending(Runner $r): array { return $r->sample()['pending']; }

function lastPending(Runner $r): ?Event {
    $p = getPending($r);
    return count($p) > 0 ? $p[count($p) - 1] : null;
}

$tmpDir = sys_get_temp_dir() . '/ice_test_' . getmypid();

function cleanTmp(): void {
    global $tmpDir;
    if (is_dir($tmpDir)) {
        $it = new RecursiveDirectoryIterator($tmpDir, RecursiveDirectoryIterator::SKIP_DOTS);
        $files = new RecursiveIteratorIterator($it, RecursiveIteratorIterator::CHILD_FIRST);
        foreach ($files as $f) { $f->isDir() ? rmdir($f->getRealPath()) : unlink($f->getRealPath()); }
        rmdir($tmpDir);
    }
}

// ─── CORE TESTS ──────────────────────────────────────────
section('Core / Event');

test('Event construction and readonly properties', function () {
    $e = new Event('test', ['key' => 'val']);
    assertEqual($e->type, 'test');
    assertEqual($e->data['key'], 'val');
});

test('Event data is accessible', function () {
    $e = new Event('x', ['a' => 1, 'b' => 2]);
    assertEqual($e->data['a'], 1);
    assertEqual($e->data['b'], 2);
});

test('Event with empty data', function () {
    $e = new Event('empty');
    assertEqual($e->data, []);
});

section('Core / Gate');

test('Gate signature assignment', function () {
    $g = new class('test_sig') extends Gate {
        public function process(Event $event): Event|array|null { return null; }
    };
    assertEqual($g->signature, 'test_sig');
});

section('Core / Stream');

test('Stream register and emit', function () {
    $s = new Stream();
    $called = false;
    $g = new class('ping') extends Gate {
        public $called = false;
        public function process(Event $event): Event|array|null { $this->called = true; return null; }
    };
    $s->register($g);
    $s->emit(new Event('ping'));
    assertTrue($g->called);
});

test('Stream routes to correct gate', function () {
    $s = new Stream();
    $g1 = new class('a') extends Gate {
        public $called = false;
        public function process(Event $event): Event|array|null { $this->called = true; return null; }
    };
    $g2 = new class('b') extends Gate {
        public $called = false;
        public function process(Event $event): Event|array|null { $this->called = true; return null; }
    };
    $s->register($g1);
    $s->register($g2);
    $s->emit(new Event('a'));
    assertTrue($g1->called);
    assertFalse($g2->called);
});

test('Stream signature collision throws', function () {
    $s = new Stream();
    $g1 = new class('dup') extends Gate { public function process(Event $event): Event|array|null { return null; } };
    $g2 = new class('dup') extends Gate { public function process(Event $event): Event|array|null { return null; } };
    $s->register($g1);
    assertThrows(fn() => $s->register($g2));
});

test('Stream pending collects unclaimed events', function () {
    $s = new Stream();
    $s->emit(new Event('unclaimed'));
    assertCount(1, $s->sample()['pending']);
    assertEqual($s->sample()['pending'][0]->type, 'unclaimed');
});

test('Stream gate returns null (swallow)', function () {
    $s = new Stream();
    $g = new class('eat') extends Gate { public function process(Event $event): Event|array|null { return null; } };
    $s->register($g);
    $s->emit(new Event('eat'));
    assertCount(0, $s->sample()['pending']);
});

test('Stream gate returns single Event', function () {
    $s = new Stream();
    $g = new class('transform') extends Gate {
        public function process(Event $event): Event|array|null { return new Event('output', ['v' => 1]); }
    };
    $s->register($g);
    $s->emit(new Event('transform'));
    assertCount(1, $s->sample()['pending']);
    assertEqual($s->sample()['pending'][0]->type, 'output');
});

test('Stream gate returns array (fan-out)', function () {
    $s = new Stream();
    $g = new class('fan') extends Gate {
        public function process(Event $event): Event|array|null {
            return [new Event('out1'), new Event('out2')];
        }
    };
    $s->register($g);
    $s->emit(new Event('fan'));
    assertCount(2, $s->sample()['pending']);
});

test('Stream emit to no matching gate', function () {
    $s = new Stream();
    $s->emit(new Event('nothing'));
    assertCount(1, $s->sample()['pending']);
});

test('Stream chain (gate A emits event that gate B claims)', function () {
    $s = new Stream();
    $gA = new class('step1') extends Gate {
        public function process(Event $event): Event|array|null { return new Event('step2'); }
    };
    $gB = new class('step2') extends Gate {
        public function process(Event $event): Event|array|null { return new Event('done'); }
    };
    $s->register($gA);
    $s->register($gB);
    $s->emit(new Event('step1'));
    assertCount(1, $s->sample()['pending']);
    assertEqual($s->sample()['pending'][0]->type, 'done');
});

test('Stream sample returns eventCount and gateCount', function () {
    $s = new Stream();
    $g = new class('x') extends Gate { public function process(Event $event): Event|array|null { return null; } };
    $s->register($g);
    $s->emit(new Event('x'));
    $s->emit(new Event('unclaimed'));
    assertEqual($s->sample()['eventCount'], 2);
    assertEqual($s->sample()['gateCount'], 1);
});

section('Core / StreamLog');

test('StreamLog append and get', function () {
    $log = new StreamLog();
    $idx = $log->append(new Event('test', ['x' => 1]));
    assertEqual($idx, 0);
    assertEqual($log->get(0)['type'], 'test');
});

test('StreamLog since returns slice', function () {
    $log = new StreamLog();
    $log->append(new Event('a'));
    $log->append(new Event('b'));
    $log->append(new Event('c'));
    $since = $log->since(1);
    assertCount(2, $since);
    assertEqual($since[0]['type'], 'b');
});

test('StreamLog length', function () {
    $log = new StreamLog();
    assertEqual($log->length(), 0);
    $log->append(new Event('x'));
    assertEqual($log->length(), 1);
});

test('StreamLog all', function () {
    $log = new StreamLog();
    $log->append(new Event('a'));
    $log->append(new Event('b'));
    assertCount(2, $log->all());
});

// ─── CANONICALIZE TESTS ──────────────────────────────────
section('Persistence / Canonicalize');

test('null', function () { assertEqual(canonicalize(null), 'null'); });
test('boolean true', function () { assertEqual(canonicalize(true), 'true'); });
test('boolean false', function () { assertEqual(canonicalize(false), 'false'); });
test('integer', function () { assertEqual(canonicalize(42), '42'); });
test('float', function () { assertTrue(str_contains(canonicalize(3.14), '3.14')); });
test('string', function () { assertEqual(canonicalize('hello'), '"hello"'); });
test('empty string', function () { assertEqual(canonicalize(''), '""'); });
test('string with special chars', function () { assertEqual(canonicalize("a\"b"), '"a\\"b"'); });
test('sequential array', function () { assertEqual(canonicalize([1, 2, 3]), '[1,2,3]'); });
test('associative array — key ordering', function () {
    assertEqual(canonicalize(['b' => 2, 'a' => 1]), '{"a":1,"b":2}');
});
test('nested objects', function () {
    $r = canonicalize(['x' => ['b' => 2, 'a' => 1]]);
    assertEqual($r, '{"x":{"a":1,"b":2}}');
});
test('deterministic (same content → same string)', function () {
    $a = canonicalize(['z' => 1, 'a' => 2]);
    $b = canonicalize(['a' => 2, 'z' => 1]);
    assertEqual($a, $b);
});

// ─── MEMORY STORE TESTS ─────────────────────────────────
section('Persistence / MemoryStore');

test('put returns hash', function () {
    $s = new MemoryStore();
    $h = $s->put(['x' => 1]);
    assertTrue(strlen($h) === 64);
});

test('put deterministic', function () {
    $s = new MemoryStore();
    $h1 = $s->put(['a' => 1]);
    $h2 = $s->put(['a' => 1]);
    assertEqual($h1, $h2);
});

test('get returns content', function () {
    $s = new MemoryStore();
    $h = $s->put(['name' => 'Alice']);
    $obj = $s->get($h);
    assertEqual($obj['name'], 'Alice');
});

test('get returns clone', function () {
    $s = new MemoryStore();
    $h = $s->put(['x' => 1]);
    $a = $s->get($h);
    $b = $s->get($h);
    $a['x'] = 99;
    assertEqual($b['x'], 1);
});

test('get throws on missing', function () {
    $s = new MemoryStore();
    assertThrows(fn() => $s->get('nonexistent'));
});

test('has true/false', function () {
    $s = new MemoryStore();
    $h = $s->put('test');
    assertTrue($s->has($h));
    assertFalse($s->has('nope'));
});

test('all value types', function () {
    $s = new MemoryStore();
    $s->put(null); $s->put(true); $s->put(42); $s->put('hello'); $s->put([1, 2]);
    // No throw = success
    assertTrue(true);
});

test('key ordering canonical', function () {
    $s = new MemoryStore();
    $h1 = $s->put(['b' => 2, 'a' => 1]);
    $h2 = $s->put(['a' => 1, 'b' => 2]);
    assertEqual($h1, $h2);
});

// ─── MEMORY REFS TESTS ──────────────────────────────────
section('Persistence / MemoryRefs');

test('set and get', function () {
    $r = new MemoryRefs();
    $r->set('x', 'hash123');
    assertEqual($r->get('x'), 'hash123');
});

test('get null for missing', function () {
    $r = new MemoryRefs();
    assertNull($r->get('nope'));
});

test('overwrite', function () {
    $r = new MemoryRefs();
    $r->set('x', 'old');
    $r->set('x', 'new');
    assertEqual($r->get('x'), 'new');
});

test('delete', function () {
    $r = new MemoryRefs();
    $r->set('x', 'hash');
    $r->delete('x');
    assertNull($r->get('x'));
});

test('delete missing no-op', function () {
    $r = new MemoryRefs();
    $r->delete('nope'); // no throw
    assertTrue(true);
});

test('list with prefix, sorted', function () {
    $r = new MemoryRefs();
    $r->set('db/tables/users/schema', 'h1');
    $r->set('db/tables/users/rows/1', 'h2');
    $r->set('db/tables/orders/schema', 'h3');
    $list = $r->list('db/tables/users/');
    assertCount(2, $list);
    assertEqual($list[0], 'db/tables/users/rows/1');
    assertEqual($list[1], 'db/tables/users/schema');
});

// ─── FILE STORE TESTS ────────────────────────────────────
section('Persistence / FileStore');

test('put creates file', function () {
    global $tmpDir; cleanTmp();
    $s = new FileStore($tmpDir);
    $h = $s->put(['file' => 'test']);
    assertTrue($s->has($h));
});

test('file content canonical', function () {
    global $tmpDir; cleanTmp();
    $s = new FileStore($tmpDir);
    $h = $s->put(['b' => 2, 'a' => 1]);
    $obj = $s->get($h);
    assertEqual($obj['a'], 1);
});

test('dedup', function () {
    global $tmpDir; cleanTmp();
    $s = new FileStore($tmpDir);
    $h1 = $s->put('same');
    $h2 = $s->put('same');
    assertEqual($h1, $h2);
});

test('get reads back', function () {
    global $tmpDir; cleanTmp();
    $s = new FileStore($tmpDir);
    $h = $s->put(['name' => 'Bob']);
    assertEqual($s->get($h)['name'], 'Bob');
});

test('get clone', function () {
    global $tmpDir; cleanTmp();
    $s = new FileStore($tmpDir);
    $h = $s->put(['x' => 1]);
    $a = $s->get($h);
    $a['x'] = 99;
    assertEqual($s->get($h)['x'], 1);
});

test('get throws missing', function () {
    global $tmpDir; cleanTmp();
    $s = new FileStore($tmpDir);
    assertThrows(fn() => $s->get('nonexistent'));
});

test('has', function () {
    global $tmpDir; cleanTmp();
    $s = new FileStore($tmpDir);
    $h = $s->put('x');
    assertTrue($s->has($h));
    assertFalse($s->has('nope'));
});

test('hash matches MemoryStore', function () {
    global $tmpDir; cleanTmp();
    $ms = new MemoryStore();
    $fs = new FileStore($tmpDir);
    $content = ['same' => 'content', 'number' => 42];
    assertEqual($ms->put($content), $fs->put($content));
});

// ─── FILE REFS TESTS ─────────────────────────────────────
section('Persistence / FileRefs');

test('set creates file', function () {
    global $tmpDir; cleanTmp();
    $r = new FileRefs($tmpDir);
    $r->set('x', 'hash123');
    assertEqual($r->get('x'), 'hash123');
});

test('get reads hash', function () {
    global $tmpDir; cleanTmp();
    $r = new FileRefs($tmpDir);
    $r->set('test/ref', 'abc');
    assertEqual($r->get('test/ref'), 'abc');
});

test('get null missing', function () {
    global $tmpDir; cleanTmp();
    $r = new FileRefs($tmpDir);
    assertNull($r->get('nope'));
});

test('overwrite', function () {
    global $tmpDir; cleanTmp();
    $r = new FileRefs($tmpDir);
    $r->set('x', 'old');
    $r->set('x', 'new');
    assertEqual($r->get('x'), 'new');
});

test('delete removes', function () {
    global $tmpDir; cleanTmp();
    $r = new FileRefs($tmpDir);
    $r->set('x', 'hash');
    $r->delete('x');
    assertNull($r->get('x'));
});

test('delete no-op', function () {
    global $tmpDir; cleanTmp();
    $r = new FileRefs($tmpDir);
    $r->delete('nope');
    assertTrue(true);
});

test('list directory walk', function () {
    global $tmpDir; cleanTmp();
    $r = new FileRefs($tmpDir);
    $r->set('db/a', 'h1');
    $r->set('db/b', 'h2');
    $r->set('other/c', 'h3');
    $list = $r->list('db/');
    assertCount(2, $list);
});

test('nested paths', function () {
    global $tmpDir; cleanTmp();
    $r = new FileRefs($tmpDir);
    $r->set('db/tables/users/schema', 'h1');
    $r->set('db/tables/users/rows/1', 'h2');
    $list = $r->list('db/tables/users/');
    assertCount(2, $list);
});

test('cleanup empty dirs', function () {
    global $tmpDir; cleanTmp();
    $r = new FileRefs($tmpDir);
    $r->set('a/b/c/d', 'h');
    $r->delete('a/b/c/d');
    // Parent dirs should be cleaned
    assertNull($r->get('a/b/c/d'));
});

// ─── RECOVERY TESTS ──────────────────────────────────────
section('Persistence / Recovery');

test('clean state', function () {
    global $tmpDir; cleanTmp();
    $rec = new Recovery($tmpDir);
    $status = $rec->check();
    assertTrue($status['clean']);
});

test('pending found', function () {
    global $tmpDir; cleanTmp();
    $rec = new Recovery($tmpDir);
    $rec->begin(
        [['hash' => 'h1', 'content' => '{"x":1}']],
        [['name' => 'ref1', 'hash' => 'h1']],
        []
    );
    $status = $rec->check();
    assertFalse($status['clean']);
});

test('recover replays', function () {
    global $tmpDir; cleanTmp();
    $store = new MemoryStore();
    $refs = new MemoryRefs();
    $rec = new Recovery($tmpDir);

    // Simulate crash: begin but don't commit
    $content = ['recovered' => true];
    $hash = hash('sha256', canonicalize($content));
    $rec->begin(
        [['hash' => $hash, 'content' => json_encode($content)]],
        [['name' => 'test/ref', 'hash' => $hash]],
        []
    );

    // Recover
    $rec->recover($store, $refs);
    assertEqual($refs->get('test/ref'), $hash);
    assertTrue($rec->check()['clean']);
});

test('idempotent', function () {
    global $tmpDir; cleanTmp();
    $store = new MemoryStore();
    $refs = new MemoryRefs();
    $rec = new Recovery($tmpDir);

    $hash = $store->put(['data' => 1]);
    $refs->set('existing', $hash);

    // Begin with an already-applied-like state, recover is safe
    $rec->begin(
        [['hash' => $hash, 'content' => json_encode(['data' => 1])]],
        [['name' => 'existing', 'hash' => $hash]],
        []
    );
    $rec->recover($store, $refs);
    assertEqual($refs->get('existing'), $hash);
});

test('mixed applied/unapplied', function () {
    global $tmpDir; cleanTmp();
    $store = new MemoryStore();
    $refs = new MemoryRefs();
    $rec = new Recovery($tmpDir);

    $content = ['mixed' => true];
    $hash = hash('sha256', canonicalize($content));
    $batch = $rec->begin(
        [['hash' => $hash, 'content' => json_encode($content)]],
        [['name' => 'applied_ref', 'hash' => $hash], ['name' => 'unapplied_ref', 'hash' => $hash]],
        ['delete_me']
    );

    // Manually mark first refSet as applied
    $batch['refSets'][0]['applied'] = true;
    file_put_contents($tmpDir . '/wal/pending.json', json_encode($batch));

    $rec->recover($store, $refs);
    assertEqual($refs->get('unapplied_ref'), $hash);
});

// ─── PROTOCOL TESTS ──────────────────────────────────────
section('Protocol / ReadSet');

test('ref adds name', function () {
    $rs = (new ReadSet())->ref('test');
    assertEqual($rs->getRefs(), ['test']);
});

test('pattern adds prefix', function () {
    $rs = (new ReadSet())->pattern('db/');
    assertEqual($rs->getPatterns(), ['db/']);
});

test('isEmpty true when empty', function () { assertTrue((new ReadSet())->isEmpty()); });
test('isEmpty false when populated', function () { assertFalse((new ReadSet())->ref('x')->isEmpty()); });

test('chaining', function () {
    $rs = (new ReadSet())->ref('a')->ref('b')->pattern('c/');
    assertCount(2, $rs->getRefs());
    assertCount(1, $rs->getPatterns());
});

test('multiple refs and patterns', function () {
    $rs = (new ReadSet())->ref('a')->ref('b')->pattern('c/')->pattern('d/');
    assertCount(2, $rs->getRefs());
    assertCount(2, $rs->getPatterns());
});

section('Protocol / MutationBatch');

test('put stores content', function () {
    $mb = (new MutationBatch())->put('row', ['x' => 1]);
    assertCount(1, $mb->getPuts());
    assertEqual($mb->getPuts()[0]['content']['x'], 1);
});

test('refSet', function () {
    $mb = (new MutationBatch())->put('row', [])->refSet('ref', 0);
    assertCount(1, $mb->getRefSets());
});

test('deleteRef', function () {
    $mb = (new MutationBatch())->refDelete('x');
    assertEqual($mb->getRefDeletes(), ['x']);
});

test('emit stores event', function () {
    $mb = (new MutationBatch())->emit(new Event('test'));
    assertCount(1, $mb->getEvents());
});

test('chaining', function () {
    $mb = (new MutationBatch())->put('a', [])->refSet('r', 0)->refDelete('d')->emit(new Event('e'));
    assertCount(1, $mb->getPuts());
    assertCount(1, $mb->getRefSets());
    assertCount(1, $mb->getRefDeletes());
    assertCount(1, $mb->getEvents());
});

test('getPuts/getRefSets/getRefDeletes/getEvents', function () {
    $mb = new MutationBatch();
    assertTrue(is_array($mb->getPuts()));
    assertTrue(is_array($mb->getRefSets()));
    assertTrue(is_array($mb->getRefDeletes()));
    assertTrue(is_array($mb->getEvents()));
});

test('multiple operations', function () {
    $mb = (new MutationBatch())->put('a', 1)->put('b', 2)->refSet('r1', 0)->refSet('r2', 1);
    assertCount(2, $mb->getPuts());
    assertCount(2, $mb->getRefSets());
});

test('empty batch', function () {
    $mb = new MutationBatch();
    assertCount(0, $mb->getPuts());
    assertCount(0, $mb->getEvents());
});

section('Protocol / PureGate');

test('transform returns single event', function () {
    $g = new class('echo') extends PureGate {
        public function transform(Event $event): Event|array|null {
            return new Event('echoed', $event->data);
        }
    };
    $result = $g->process(new Event('echo', ['v' => 1]));
    assertTrue($result instanceof Event);
    assertEqual($result->type, 'echoed');
});

test('transform returns array', function () {
    $g = new class('multi') extends PureGate {
        public function transform(Event $event): Event|array|null {
            return [new Event('a'), new Event('b')];
        }
    };
    $result = $g->process(new Event('multi'));
    assertTrue(is_array($result));
    assertCount(2, $result);
});

test('process delegates to transform', function () {
    $g = new class('del') extends PureGate {
        public function transform(Event $event): Event|array|null { return null; }
    };
    assertNull($g->process(new Event('del')));
});

section('Protocol / StateGate');

test('reads returns ReadSet', function () {
    $g = new class('sg') extends StateGate {
        public function reads(Event $event): ReadSet { return (new ReadSet())->ref('test'); }
        public function transformEvent(Event $event, array $state): MutationBatch { return new MutationBatch(); }
    };
    $rs = $g->reads(new Event('sg'));
    assertTrue($rs instanceof ReadSet);
    assertEqual($rs->getRefs(), ['test']);
});

test('transform receives state', function () {
    $g = new class('sg2') extends StateGate {
        public function reads(Event $event): ReadSet { return new ReadSet(); }
        public function transformEvent(Event $event, array $state): MutationBatch {
            return (new MutationBatch())->emit(new Event('ok', ['got_state' => true]));
        }
    };
    $mb = $g->transformEvent(new Event('sg2'), ['refs' => [], 'patterns' => []]);
    assertCount(1, $mb->getEvents());
});

test('process throws without Runner', function () {
    $g = new class('sg3') extends StateGate {
        public function reads(Event $event): ReadSet { return new ReadSet(); }
        public function transformEvent(Event $event, array $state): MutationBatch { return new MutationBatch(); }
    };
    assertThrows(fn() => $g->process(new Event('sg3')));
});

// ─── RUNNER TESTS ────────────────────────────────────────
section('Resolution / Runner');

test('register and emit PureGate', function () {
    $r = new Runner(new MemoryStore(), new MemoryRefs());
    $g = new class('ping') extends PureGate {
        public function transform(Event $event): Event|array|null { return new Event('pong'); }
    };
    $r->register($g);
    $r->emit(new Event('ping'));
    assertEqual(getPending($r)[0]->type, 'pong');
});

test('register and emit StateGate', function () {
    $r = new Runner(new MemoryStore(), new MemoryRefs());
    $g = new class('store_it') extends StateGate {
        public function reads(Event $event): ReadSet { return new ReadSet(); }
        public function transformEvent(Event $event, array $state): MutationBatch {
            return (new MutationBatch())
                ->put('data', ['stored' => true])
                ->refSet('test/ref', 0)
                ->emit(new Event('stored'));
        }
    };
    $r->register($g);
    $r->emit(new Event('store_it'));
    assertEqual(lastPending($r)->type, 'stored');
});

test('resolves ReadSet refs', function () {
    $store = new MemoryStore();
    $refs = new MemoryRefs();
    $h = $store->put(['val' => 42]);
    $refs->set('myref', $h);

    $r = new Runner($store, $refs);
    $state = $r->resolve((new ReadSet())->ref('myref'));
    assertEqual($state['refs']['myref']['val'], 42);
});

test('resolves ReadSet patterns', function () {
    $store = new MemoryStore();
    $refs = new MemoryRefs();
    $h1 = $store->put(['a' => 1]);
    $h2 = $store->put(['b' => 2]);
    $refs->set('db/x', $h1);
    $refs->set('db/y', $h2);

    $r = new Runner($store, $refs);
    $state = $r->resolve((new ReadSet())->pattern('db/'));
    assertCount(2, $state['patterns']['db/']);
});

test('applies MutationBatch puts', function () {
    $store = new MemoryStore();
    $refs = new MemoryRefs();
    $r = new Runner($store, $refs);

    $mb = (new MutationBatch())->put('data', ['x' => 1])->refSet('test', 0);
    $r->apply($mb);
    $h = $refs->get('test');
    assertEqual($store->get($h)['x'], 1);
});

test('applies MutationBatch refSets', function () {
    $store = new MemoryStore();
    $refs = new MemoryRefs();
    $h = $store->put('existing');
    $r = new Runner($store, $refs);
    $mb = (new MutationBatch())->refSetHash('ptr', $h);
    $r->apply($mb);
    assertEqual($refs->get('ptr'), $h);
});

test('applies MutationBatch refDeletes', function () {
    $store = new MemoryStore();
    $refs = new MemoryRefs();
    $refs->set('gone', 'hash');
    $r = new Runner($store, $refs);
    $mb = (new MutationBatch())->refDelete('gone');
    $r->apply($mb);
    assertNull($refs->get('gone'));
});

test('queues emitted events', function () {
    $r = new Runner(new MemoryStore(), new MemoryRefs());
    $g = new class('emit_test') extends StateGate {
        public function reads(Event $event): ReadSet { return new ReadSet(); }
        public function transformEvent(Event $event, array $state): MutationBatch {
            return (new MutationBatch())->emit(new Event('emitted_a'))->emit(new Event('emitted_b'));
        }
    };
    $r->register($g);
    $r->emit(new Event('emit_test'));
    $types = array_map(fn($e) => $e->type, getPending($r));
    assertTrue(in_array('emitted_a', $types));
    assertTrue(in_array('emitted_b', $types));
});

test('chain (gate A → event → gate B)', function () {
    $r = new Runner(new MemoryStore(), new MemoryRefs());
    $gA = new class('chain_a') extends PureGate {
        public function transform(Event $event): Event|array|null { return new Event('chain_b'); }
    };
    $gB = new class('chain_b') extends PureGate {
        public function transform(Event $event): Event|array|null { return new Event('chain_done'); }
    };
    $r->register($gA);
    $r->register($gB);
    $r->emit(new Event('chain_a'));
    assertEqual(lastPending($r)->type, 'chain_done');
});

test('error event from gate', function () {
    $r = new Runner(new MemoryStore(), new MemoryRefs());
    $g = new class('boom') extends PureGate {
        public function transform(Event $event): Event|array|null { throw new \RuntimeException('kaboom'); }
    };
    $r->register($g);
    $r->emit(new Event('boom'));
    assertEqual(lastPending($r)->type, 'error');
    assertEqual(lastPending($r)->data['message'], 'kaboom');
});

test('snapshot captures refs', function () {
    $r = new Runner(new MemoryStore(), new MemoryRefs());
    $r->getRefs()->set('x', 'h1');
    $snap = $r->snapshot();
    assertTrue(isset($snap['refs']['x']));
});

test('restore reverts refs', function () {
    $r = new Runner(new MemoryStore(), new MemoryRefs());
    $r->getRefs()->set('x', 'h1');
    $snap = $r->snapshot();
    $r->getRefs()->set('x', 'h2');
    $r->restore($snap);
    assertEqual($r->getRefs()->get('x'), 'h1');
});

test('log records events', function () {
    $r = new Runner(new MemoryStore(), new MemoryRefs());
    $r->emit(new Event('logged'));
    $pending = getPending($r);
    assertCount(1, $pending);
});

test('multiple StateGates', function () {
    $r = freshRunner();
    $r->emit(new Event('create_table_execute', ['table' => 'a', 'columns' => []]));
    $r->emit(new Event('create_table_execute', ['table' => 'b', 'columns' => []]));
    $types = array_map(fn($e) => $e->type, getPending($r));
    assertEqual(count(array_filter($types, fn($t) => $t === 'table_created')), 2);
});

test('PureGate + StateGate mixed', function () {
    $r = new Runner(new MemoryStore(), new MemoryRefs());
    $pg = new class('pure_step') extends PureGate {
        public function transform(Event $event): Event|array|null { return new Event('result', ['v' => 1]); }
    };
    $sg = new class('state_step') extends StateGate {
        public function reads(Event $event): ReadSet { return new ReadSet(); }
        public function transformEvent(Event $event, array $state): MutationBatch {
            return (new MutationBatch())->emit(new Event('state_result'));
        }
    };
    $r->register($pg);
    $r->register($sg);
    $r->emit(new Event('pure_step'));
    $r->emit(new Event('state_step'));
    $types = array_map(fn($e) => $e->type, getPending($r));
    assertTrue(in_array('result', $types));
    assertTrue(in_array('state_result', $types));
});

test('registerStepHandler stores handler', function () {
    $r = new Runner(new MemoryStore(), new MemoryRefs());
    $r->registerStepHandler('test_step', [
        'reads' => fn($step) => new ReadSet(),
        'execute' => fn($step, $rows, $state) => $rows,
    ]);
    assertTrue(isset($r->getStepHandlers()['test_step']));
});

test('getStepHandlers returns all', function () {
    $r = new Runner(new MemoryStore(), new MemoryRefs());
    $r->registerStepHandler('a', ['reads' => fn($s) => new ReadSet(), 'execute' => fn($s, $r, $st) => $r]);
    $r->registerStepHandler('b', ['reads' => fn($s) => new ReadSet(), 'execute' => fn($s, $r, $st) => $r]);
    assertCount(2, $r->getStepHandlers());
});

test('step handler callable', function () {
    $r = new Runner(new MemoryStore(), new MemoryRefs());
    $r->registerStepHandler('transform', [
        'reads' => fn($step) => new ReadSet(),
        'execute' => fn($step, $rows, $state) => array_map(fn($r) => array_merge($r, ['added' => true]), $rows),
    ]);
    $handler = $r->getStepHandlers()['transform'];
    $result = ($handler['execute'])(['type' => 'transform'], [['id' => 1]], []);
    assertEqual($result[0]['added'], true);
});

test('FileStore + FileRefs integration', function () {
    global $tmpDir; cleanTmp();
    $r = new Runner(new FileStore($tmpDir), new FileRefs($tmpDir));
    registerDatabaseGates($r);
    createTable($r, 'test', [['name' => 'val', 'type' => 'TEXT']]);
    insertRow($r, 'test', ['val' => 'hello']);
    $types = array_map(fn($e) => $e->type, getPending($r));
    assertTrue(in_array('row_inserted', $types));
});

test('cross-Runner persistence (file)', function () {
    global $tmpDir; cleanTmp();
    // Runner 1: create and insert
    $r1 = new Runner(new FileStore($tmpDir), new FileRefs($tmpDir));
    registerDatabaseGates($r1);
    createTable($r1, 'persist', [['name' => 'x', 'type' => 'INTEGER']]);
    insertRow($r1, 'persist', ['x' => 42]);

    // Runner 2: read back
    $r2 = new Runner(new FileStore($tmpDir), new FileRefs($tmpDir));
    registerDatabaseGates($r2);
    $r2->emit(new Event('table_scan', ['table' => 'persist']));
    $last = lastPending($r2);
    assertEqual($last->type, 'scan_result');
    assertCount(1, $last->data['rows']);
    assertEqual($last->data['rows'][0]['x'], 42);
});

// ─── EXPRESSION TESTS ────────────────────────────────────
section('Database Gates / Expression');

test('evaluateCondition: =', function () {
    assertTrue(evaluateCondition(['column' => 'x', 'op' => '=', 'value' => 1], ['x' => 1]));
    assertFalse(evaluateCondition(['column' => 'x', 'op' => '=', 'value' => 2], ['x' => 1]));
});

test('evaluateCondition: !=', function () {
    assertTrue(evaluateCondition(['column' => 'x', 'op' => '!=', 'value' => 2], ['x' => 1]));
});

test('evaluateCondition: < > <= >=', function () {
    assertTrue(evaluateCondition(['column' => 'x', 'op' => '<', 'value' => 10], ['x' => 5]));
    assertTrue(evaluateCondition(['column' => 'x', 'op' => '>', 'value' => 3], ['x' => 5]));
    assertTrue(evaluateCondition(['column' => 'x', 'op' => '<=', 'value' => 5], ['x' => 5]));
    assertTrue(evaluateCondition(['column' => 'x', 'op' => '>=', 'value' => 5], ['x' => 5]));
});

test('evaluateCondition: AND', function () {
    $cond = ['and' => [
        ['column' => 'x', 'op' => '>', 'value' => 0],
        ['column' => 'x', 'op' => '<', 'value' => 10],
    ]];
    assertTrue(evaluateCondition($cond, ['x' => 5]));
    assertFalse(evaluateCondition($cond, ['x' => 15]));
});

test('evaluateCondition: OR', function () {
    $cond = ['or' => [
        ['column' => 'x', 'op' => '=', 'value' => 1],
        ['column' => 'x', 'op' => '=', 'value' => 2],
    ]];
    assertTrue(evaluateCondition($cond, ['x' => 2]));
    assertFalse(evaluateCondition($cond, ['x' => 3]));
});

test('evaluateCondition: NOT', function () {
    $cond = ['not' => ['column' => 'x', 'op' => '=', 'value' => 1]];
    assertTrue(evaluateCondition($cond, ['x' => 2]));
});

test('evaluateCondition: IN', function () {
    assertTrue(evaluateCondition(['column' => 'x', 'op' => 'in', 'value' => [1, 2, 3]], ['x' => 2]));
    assertFalse(evaluateCondition(['column' => 'x', 'op' => 'in', 'value' => [1, 2, 3]], ['x' => 4]));
});

test('evaluateCondition: LIKE', function () {
    assertTrue(evaluateCondition(['column' => 'name', 'op' => 'like', 'value' => 'Al%'], ['name' => 'Alice']));
    assertFalse(evaluateCondition(['column' => 'name', 'op' => 'like', 'value' => 'Bo%'], ['name' => 'Alice']));
});

test('evaluateCondition: IS NULL / IS NOT NULL', function () {
    assertTrue(evaluateCondition(['column' => 'x', 'op' => 'is_null'], ['x' => null]));
    assertTrue(evaluateCondition(['column' => 'x', 'op' => 'is_not_null'], ['x' => 1]));
});

test('evaluateCondition: null returns true', function () {
    assertTrue(evaluateCondition(null, ['x' => 1]));
});

test('evaluateExpression: column ref', function () {
    assertEqual(evaluateExpression('name', ['name' => 'Alice']), 'Alice');
});

test('evaluateExpression: literal', function () {
    assertEqual(evaluateExpression(['literal' => 42], []), 42);
    assertEqual(evaluateExpression(42, []), 42);
});

test('evaluateExpression: arithmetic', function () {
    assertEqual(evaluateExpression(['op' => '+', 'left' => 'a', 'right' => 'b'], ['a' => 3, 'b' => 4]), 7);
    assertEqual(evaluateExpression(['op' => '*', 'left' => 'x', 'right' => ['literal' => 2]], ['x' => 5]), 10);
});

test('evaluateExpression: functions', function () {
    assertEqual(evaluateExpression(['fn' => 'UPPER', 'args' => ['name']], ['name' => 'alice']), 'ALICE');
    assertEqual(evaluateExpression(['fn' => 'LENGTH', 'args' => ['name']], ['name' => 'hello']), 5);
    assertEqual(evaluateExpression(['fn' => 'ABS', 'args' => ['x']], ['x' => -3]), 3);
});

test('evaluateExpression: COALESCE', function () {
    assertEqual(evaluateExpression(['fn' => 'COALESCE', 'args' => ['a', 'b']], ['a' => null, 'b' => 5]), 5);
});

test('evaluateExpression: division by zero returns null', function () {
    assertNull(evaluateExpression(['op' => '/', 'left' => ['literal' => 10], 'right' => ['literal' => 0]], []));
});

test('evaluateExpression: CASE WHEN', function () {
    $expr = [
        'case' => [['when' => ['column' => 'x', 'op' => '>', 'value' => 5], 'then' => ['literal' => 'big']]],
        'else' => ['literal' => 'small'],
    ];
    assertEqual(evaluateExpression($expr, ['x' => 10]), 'big');
    assertEqual(evaluateExpression($expr, ['x' => 2]), 'small');
});

// ─── PURE GATE TESTS ────────────────────────────────────
section('Database Gates / Pure Gates');

test('filterRows basic', function () {
    $rows = [['id' => 1, 'x' => 10], ['id' => 2, 'x' => 20], ['id' => 3, 'x' => 30]];
    $result = filterRows($rows, ['column' => 'x', 'op' => '>', 'value' => 15]);
    assertCount(2, $result);
});

test('filterRows null where returns all', function () {
    $rows = [['id' => 1], ['id' => 2]];
    assertCount(2, filterRows($rows, null));
});

test('projectRows specific columns', function () {
    $rows = [['id' => 1, 'name' => 'Alice', 'age' => 30]];
    $result = projectRows($rows, ['name']);
    assertEqual($result[0], ['name' => 'Alice']);
});

test('projectRows * returns all', function () {
    $rows = [['id' => 1, 'name' => 'Alice']];
    $result = projectRows($rows, ['*']);
    assertEqual($result[0]['id'], 1);
});

test('projectRows with expression alias', function () {
    $rows = [['x' => 5]];
    $result = projectRows($rows, [['expr' => ['op' => '*', 'left' => 'x', 'right' => ['literal' => 2]], 'alias' => 'doubled']]);
    assertEqual($result[0]['doubled'], 10);
});

test('orderByRows ascending', function () {
    $rows = [['n' => 3], ['n' => 1], ['n' => 2]];
    $result = orderByRows($rows, [['column' => 'n', 'direction' => 'asc']]);
    assertEqual($result[0]['n'], 1);
    assertEqual($result[2]['n'], 3);
});

test('orderByRows descending', function () {
    $rows = [['n' => 1], ['n' => 3], ['n' => 2]];
    $result = orderByRows($rows, [['column' => 'n', 'direction' => 'desc']]);
    assertEqual($result[0]['n'], 3);
});

test('orderByRows multiple columns', function () {
    $rows = [['a' => 1, 'b' => 2], ['a' => 1, 'b' => 1], ['a' => 2, 'b' => 1]];
    $result = orderByRows($rows, [['column' => 'a', 'direction' => 'asc'], ['column' => 'b', 'direction' => 'asc']]);
    assertEqual($result[0]['b'], 1);
    assertEqual($result[1]['b'], 2);
});

test('orderByRows nulls last', function () {
    $rows = [['n' => null], ['n' => 1], ['n' => 2]];
    $result = orderByRows($rows, [['column' => 'n', 'direction' => 'asc']]);
    assertNull($result[2]['n']);
});

test('limitRows', function () {
    $rows = [['id' => 1], ['id' => 2], ['id' => 3], ['id' => 4]];
    $result = limitRows($rows, 2);
    assertCount(2, $result);
    assertEqual($result[0]['id'], 1);
});

test('limitRows with offset', function () {
    $rows = [['id' => 1], ['id' => 2], ['id' => 3]];
    $result = limitRows($rows, 1, 1);
    assertCount(1, $result);
    assertEqual($result[0]['id'], 2);
});

test('distinctRows', function () {
    $rows = [['x' => 1], ['x' => 2], ['x' => 1]];
    $result = distinctRows($rows);
    assertCount(2, $result);
});

test('distinctRows by column', function () {
    $rows = [['a' => 1, 'b' => 'x'], ['a' => 1, 'b' => 'y'], ['a' => 2, 'b' => 'x']];
    $result = distinctRows($rows, ['a']);
    assertCount(2, $result);
});

test('aggregateRows COUNT(*)', function () {
    $rows = [['id' => 1], ['id' => 2], ['id' => 3]];
    $result = aggregateRows($rows, [['fn' => 'COUNT', 'column' => '*', 'alias' => 'cnt']]);
    assertEqual($result[0]['cnt'], 3);
});

test('aggregateRows SUM', function () {
    $rows = [['v' => 10], ['v' => 20], ['v' => 30]];
    $result = aggregateRows($rows, [['fn' => 'SUM', 'column' => 'v', 'alias' => 'total']]);
    assertEqual($result[0]['total'], 60);
});

test('aggregateRows AVG', function () {
    $rows = [['v' => 10], ['v' => 20]];
    $result = aggregateRows($rows, [['fn' => 'AVG', 'column' => 'v', 'alias' => 'avg']]);
    assertTrue($result[0]['avg'] == 15.0, "AVG should be 15.0, got {$result[0]['avg']}");
});

test('aggregateRows MIN/MAX', function () {
    $rows = [['v' => 5], ['v' => 15], ['v' => 10]];
    $result = aggregateRows($rows, [
        ['fn' => 'MIN', 'column' => 'v', 'alias' => 'mn'],
        ['fn' => 'MAX', 'column' => 'v', 'alias' => 'mx'],
    ]);
    assertEqual($result[0]['mn'], 5);
    assertEqual($result[0]['mx'], 15);
});

test('aggregateRows GROUP BY', function () {
    $rows = [['dept' => 'eng', 'x' => 1], ['dept' => 'eng', 'x' => 2], ['dept' => 'sales', 'x' => 3]];
    $result = aggregateRows($rows, [['fn' => 'COUNT', 'column' => '*', 'alias' => 'cnt']], ['dept']);
    assertCount(2, $result);
});

test('joinRows inner', function () {
    $left = [['id' => 1, 'name' => 'Alice'], ['id' => 2, 'name' => 'Bob']];
    $right = [['user_id' => 1, 'order' => 'A'], ['user_id' => 3, 'order' => 'C']];
    $result = joinRows($left, $right, ['left' => 'id', 'right' => 'user_id'], 'inner');
    assertCount(1, $result);
    assertEqual($result[0]['name'], 'Alice');
});

test('joinRows left', function () {
    $left = [['id' => 1], ['id' => 2]];
    $right = [['uid' => 1, 'v' => 'x']];
    $result = joinRows($left, $right, ['left' => 'id', 'right' => 'uid'], 'left');
    assertCount(2, $result);
    assertNull($result[1]['v']);
});

// ─── DDL GATE TESTS ──────────────────────────────────────
section('Database Gates / DDL');

test('CREATE TABLE', function () {
    $r = freshRunner();
    createTable($r, 'users', [['name' => 'name', 'type' => 'TEXT'], ['name' => 'age', 'type' => 'INTEGER']]);
    $types = array_map(fn($e) => $e->type, getPending($r));
    assertTrue(in_array('table_created', $types));
});

test('CREATE TABLE duplicate error', function () {
    $r = freshRunner();
    createTable($r, 'dup', []);
    createTable($r, 'dup', []);
    $errors = array_filter(getPending($r), fn($e) => $e->type === 'error');
    assertTrue(count($errors) > 0);
});

test('DROP TABLE', function () {
    $r = freshRunner();
    createTable($r, 'temp', []);
    $r->emit(new Event('drop_table_execute', ['table' => 'temp']));
    $types = array_map(fn($e) => $e->type, getPending($r));
    assertTrue(in_array('table_dropped', $types));
});

test('DROP TABLE IF EXISTS', function () {
    $r = freshRunner();
    $r->emit(new Event('drop_table_execute', ['table' => 'nope', 'ifExists' => true]));
    $errors = array_filter(getPending($r), fn($e) => $e->type === 'error');
    assertCount(0, $errors);
});

test('DROP TABLE nonexistent error', function () {
    $r = freshRunner();
    $r->emit(new Event('drop_table_execute', ['table' => 'nope']));
    $last = lastPending($r);
    assertEqual($last->type, 'error');
});

// ─── DML GATE TESTS ─────────────────────────────────────
section('Database Gates / DML');

test('INSERT row', function () {
    $r = freshRunner();
    createTable($r, 'users', [['name' => 'name', 'type' => 'TEXT']]);
    insertRow($r, 'users', ['name' => 'Alice']);
    $inserted = array_filter(getPending($r), fn($e) => $e->type === 'row_inserted');
    $ins = array_values($inserted)[0];
    assertEqual($ins->data['row']['name'], 'Alice');
    assertEqual($ins->data['row']['id'], 1);
});

test('INSERT auto-increment', function () {
    $r = freshRunner();
    createTable($r, 't', [['name' => 'x', 'type' => 'INTEGER']]);
    insertRow($r, 't', ['x' => 1]);
    insertRow($r, 't', ['x' => 2]);
    $inserts = array_values(array_filter(getPending($r), fn($e) => $e->type === 'row_inserted'));
    assertEqual($inserts[0]->data['id'], 1);
    assertEqual($inserts[1]->data['id'], 2);
});

test('INSERT with defaults', function () {
    $r = freshRunner();
    createTable($r, 't', [['name' => 'status', 'type' => 'TEXT', 'default' => 'active']]);
    insertRow($r, 't', []);
    $ins = array_values(array_filter(getPending($r), fn($e) => $e->type === 'row_inserted'))[0];
    assertEqual($ins->data['row']['status'], 'active');
});

test('INSERT into nonexistent table', function () {
    $r = freshRunner();
    insertRow($r, 'nope', ['x' => 1]);
    assertEqual(lastPending($r)->type, 'error');
});

test('UPDATE rows', function () {
    $r = freshRunner();
    createTable($r, 't', [['name' => 'x', 'type' => 'INTEGER']]);
    insertRow($r, 't', ['x' => 1]);
    $r->emit(new Event('update_execute', ['table' => 't', 'changes' => ['x' => 99], 'where' => ['column' => 'id', 'op' => '=', 'value' => 1]]));
    $updated = array_values(array_filter(getPending($r), fn($e) => $e->type === 'row_updated'));
    assertTrue(count($updated) > 0);
    assertTrue(in_array(1, $updated[0]->data['ids']));
});

test('UPDATE nonexistent table', function () {
    $r = freshRunner();
    $r->emit(new Event('update_execute', ['table' => 'nope', 'changes' => ['x' => 1]]));
    assertEqual(lastPending($r)->type, 'error');
});

test('DELETE rows', function () {
    $r = freshRunner();
    createTable($r, 't', [['name' => 'x', 'type' => 'INTEGER']]);
    insertRow($r, 't', ['x' => 1]);
    insertRow($r, 't', ['x' => 2]);
    $r->emit(new Event('delete_execute', ['table' => 't', 'where' => ['column' => 'x', 'op' => '=', 'value' => 1]]));
    $deleted = array_values(array_filter(getPending($r), fn($e) => $e->type === 'row_deleted'));
    assertTrue(count($deleted) > 0);
});

test('TABLE SCAN returns rows', function () {
    $r = freshRunner();
    createTable($r, 't', [['name' => 'v', 'type' => 'TEXT']]);
    insertRow($r, 't', ['v' => 'a']);
    insertRow($r, 't', ['v' => 'b']);
    $r->emit(new Event('table_scan', ['table' => 't']));
    $scan = array_values(array_filter(getPending($r), fn($e) => $e->type === 'scan_result'));
    assertCount(2, $scan[0]->data['rows']);
});

test('TABLE SCAN empty table', function () {
    $r = freshRunner();
    createTable($r, 'empty', []);
    $r->emit(new Event('table_scan', ['table' => 'empty']));
    $scan = array_values(array_filter(getPending($r), fn($e) => $e->type === 'scan_result'));
    assertCount(0, $scan[0]->data['rows']);
});

// ─── QUERY GATE TESTS ────────────────────────────────────
section('Database Gates / Query');

test('INDEX CREATE', function () {
    $r = freshRunner();
    createTable($r, 't', [['name' => 'x', 'type' => 'INTEGER']]);
    insertRow($r, 't', ['x' => 10]);
    $r->emit(new Event('index_create_execute', ['table' => 't', 'index' => 'idx_x', 'column' => 'x']));
    $created = array_filter(getPending($r), fn($e) => $e->type === 'index_created');
    assertTrue(count($created) > 0);
});

test('INDEX SCAN eq', function () {
    $r = freshRunner();
    createTable($r, 't', [['name' => 'x', 'type' => 'INTEGER']]);
    insertRow($r, 't', ['x' => 10]);
    insertRow($r, 't', ['x' => 20]);
    $r->emit(new Event('index_create_execute', ['table' => 't', 'index' => 'idx_x', 'column' => 'x']));
    $r->emit(new Event('index_scan', ['table' => 't', 'index' => 'idx_x', 'op' => 'eq', 'value' => 10]));
    $scans = array_values(array_filter(getPending($r), fn($e) => $e->type === 'scan_result'));
    $lastScan = end($scans);
    assertCount(1, $lastScan->data['rows']);
    assertEqual($lastScan->data['rows'][0]['x'], 10);
});

test('INDEX DROP', function () {
    $r = freshRunner();
    createTable($r, 't', [['name' => 'x', 'type' => 'INTEGER']]);
    $r->emit(new Event('index_create_execute', ['table' => 't', 'index' => 'idx', 'column' => 'x']));
    $r->emit(new Event('index_drop_execute', ['table' => 't', 'index' => 'idx']));
    $dropped = array_filter(getPending($r), fn($e) => $e->type === 'index_dropped');
    assertTrue(count($dropped) > 0);
});

test('JOIN via gate', function () {
    $r = freshRunner();
    createTable($r, 'users', [['name' => 'name', 'type' => 'TEXT']]);
    createTable($r, 'orders', [['name' => 'user_id', 'type' => 'INTEGER'], ['name' => 'item', 'type' => 'TEXT']]);
    insertRow($r, 'users', ['name' => 'Alice']);
    insertRow($r, 'orders', ['user_id' => 1, 'item' => 'Book']);
    $r->emit(new Event('join', [
        'left' => ['table' => 'users'],
        'right' => ['table' => 'orders'],
        'on' => ['left' => 'id', 'right' => 'user_id'],
        'type' => 'inner',
    ]));
    $joins = array_values(array_filter(getPending($r), fn($e) => $e->type === 'join_result'));
    assertCount(1, $joins[0]->data['rows']);
});

test('FILTER via gate', function () {
    $r = new Runner(new MemoryStore(), new MemoryRefs());
    registerDatabaseGates($r);
    $r->emit(new Event('filter', [
        'rows' => [['x' => 1], ['x' => 2], ['x' => 3]],
        'where' => ['column' => 'x', 'op' => '>', 'value' => 1],
    ]));
    $result = array_values(array_filter(getPending($r), fn($e) => $e->type === 'filter_result'));
    assertCount(2, $result[0]->data['rows']);
});

test('PROJECT via gate', function () {
    $r = new Runner(new MemoryStore(), new MemoryRefs());
    registerDatabaseGates($r);
    $r->emit(new Event('project', [
        'rows' => [['a' => 1, 'b' => 2, 'c' => 3]],
        'columns' => ['a', 'c'],
    ]));
    $result = array_values(array_filter(getPending($r), fn($e) => $e->type === 'project_result'));
    assertEqual(array_keys($result[0]->data['rows'][0]), ['a', 'c']);
});

test('ORDER BY via gate', function () {
    $r = new Runner(new MemoryStore(), new MemoryRefs());
    registerDatabaseGates($r);
    $r->emit(new Event('order_by', [
        'rows' => [['n' => 3], ['n' => 1], ['n' => 2]],
        'order' => [['column' => 'n', 'direction' => 'asc']],
    ]));
    $result = array_values(array_filter(getPending($r), fn($e) => $e->type === 'ordered_result'));
    assertEqual($result[0]->data['rows'][0]['n'], 1);
});

test('AGGREGATE via gate', function () {
    $r = new Runner(new MemoryStore(), new MemoryRefs());
    registerDatabaseGates($r);
    $r->emit(new Event('aggregate', [
        'rows' => [['v' => 10], ['v' => 20]],
        'aggregates' => [['fn' => 'SUM', 'column' => 'v', 'alias' => 'total']],
    ]));
    $result = array_values(array_filter(getPending($r), fn($e) => $e->type === 'aggregate_result'));
    assertEqual($result[0]->data['rows'][0]['total'], 30);
});

// ─── META GATE TESTS ─────────────────────────────────────
section('Database Gates / Meta');

test('VIEW CREATE', function () {
    $r = freshRunner();
    $r->emit(new Event('view_create_execute', ['name' => 'v1', 'query' => ['pipeline' => []], 'columns' => []]));
    $created = array_filter(getPending($r), fn($e) => $e->type === 'view_created');
    assertTrue(count($created) > 0);
});

test('VIEW CREATE duplicate error', function () {
    $r = freshRunner();
    $r->emit(new Event('view_create_execute', ['name' => 'v1', 'query' => [], 'columns' => []]));
    $r->emit(new Event('view_create_execute', ['name' => 'v1', 'query' => [], 'columns' => []]));
    $errors = array_filter(getPending($r), fn($e) => $e->type === 'error');
    assertTrue(count($errors) > 0);
});

test('VIEW DROP', function () {
    $r = freshRunner();
    $r->emit(new Event('view_create_execute', ['name' => 'v1', 'query' => [], 'columns' => []]));
    $r->emit(new Event('view_drop_execute', ['name' => 'v1']));
    $dropped = array_filter(getPending($r), fn($e) => $e->type === 'view_dropped');
    assertTrue(count($dropped) > 0);
});

test('TRIGGER CREATE', function () {
    $r = freshRunner();
    $r->emit(new Event('trigger_create_execute', ['name' => 'tr1', 'table' => 't', 'timing' => 'BEFORE', 'event' => 'INSERT', 'action' => 'SET x = 1']));
    $created = array_filter(getPending($r), fn($e) => $e->type === 'trigger_created');
    assertTrue(count($created) > 0);
});

test('TRIGGER CREATE duplicate error', function () {
    $r = freshRunner();
    $r->emit(new Event('trigger_create_execute', ['name' => 'tr1', 'table' => 't', 'timing' => 'BEFORE', 'event' => 'INSERT', 'action' => '']));
    $r->emit(new Event('trigger_create_execute', ['name' => 'tr1', 'table' => 't', 'timing' => 'BEFORE', 'event' => 'INSERT', 'action' => '']));
    $errors = array_filter(getPending($r), fn($e) => $e->type === 'error');
    assertTrue(count($errors) > 0);
});

test('TRIGGER DROP', function () {
    $r = freshRunner();
    $r->emit(new Event('trigger_create_execute', ['name' => 'tr1', 'table' => 't', 'timing' => 'BEFORE', 'event' => 'INSERT', 'action' => '']));
    $r->emit(new Event('trigger_drop_execute', ['name' => 'tr1']));
    $dropped = array_filter(getPending($r), fn($e) => $e->type === 'trigger_dropped');
    assertTrue(count($dropped) > 0);
});

test('TRIGGER DROP nonexistent error', function () {
    $r = freshRunner();
    $r->emit(new Event('trigger_drop_execute', ['name' => 'nope']));
    assertEqual(lastPending($r)->type, 'error');
});

test('CONSTRAINT CREATE', function () {
    $r = freshRunner();
    createTable($r, 't', [['name' => 'x', 'type' => 'INTEGER']]);
    $r->emit(new Event('constraint_create_execute', ['table' => 't', 'name' => 'c1', 'type' => 'unique', 'params' => ['column' => 'x']]));
    $created = array_filter(getPending($r), fn($e) => $e->type === 'constraint_created');
    assertTrue(count($created) > 0);
});

test('CONSTRAINT CREATE duplicate error', function () {
    $r = freshRunner();
    createTable($r, 't', [['name' => 'x', 'type' => 'INTEGER']]);
    $r->emit(new Event('constraint_create_execute', ['table' => 't', 'name' => 'c1', 'type' => 'unique', 'params' => []]));
    $r->emit(new Event('constraint_create_execute', ['table' => 't', 'name' => 'c1', 'type' => 'unique', 'params' => []]));
    $errors = array_filter(getPending($r), fn($e) => $e->type === 'error');
    assertTrue(count($errors) > 0);
});

test('CONSTRAINT DROP', function () {
    $r = freshRunner();
    createTable($r, 't', [['name' => 'x', 'type' => 'INTEGER']]);
    $r->emit(new Event('constraint_create_execute', ['table' => 't', 'name' => 'c1', 'type' => 'unique', 'params' => []]));
    $r->emit(new Event('constraint_drop_execute', ['table' => 't', 'name' => 'c1']));
    $dropped = array_filter(getPending($r), fn($e) => $e->type === 'constraint_dropped');
    assertTrue(count($dropped) > 0);
});

test('CONSTRAINT DROP nonexistent error', function () {
    $r = freshRunner();
    $r->emit(new Event('constraint_drop_execute', ['table' => 't', 'name' => 'nope']));
    assertEqual(lastPending($r)->type, 'error');
});

test('CONSTRAINT CREATE on nonexistent table', function () {
    $r = freshRunner();
    $r->emit(new Event('constraint_create_execute', ['table' => 'nope', 'name' => 'c1', 'type' => 'unique', 'params' => []]));
    assertEqual(lastPending($r)->type, 'error');
});

// ─── LIFECYCLE TESTS ─────────────────────────────────────
section('Database Gates / Lifecycle');

test('full CRUD lifecycle', function () {
    $r = freshRunner();
    createTable($r, 'users', [['name' => 'name', 'type' => 'TEXT'], ['name' => 'age', 'type' => 'INTEGER']]);
    insertRow($r, 'users', ['name' => 'Alice', 'age' => 30]);
    insertRow($r, 'users', ['name' => 'Bob', 'age' => 25]);

    // Scan
    $r->emit(new Event('table_scan', ['table' => 'users']));
    $scans = array_values(array_filter(getPending($r), fn($e) => $e->type === 'scan_result'));
    $lastScan = end($scans);
    assertCount(2, $lastScan->data['rows']);

    // Update
    $r->emit(new Event('update_execute', ['table' => 'users', 'changes' => ['age' => 31], 'where' => ['column' => 'name', 'op' => '=', 'value' => 'Alice']]));

    // Scan again to verify
    $r->emit(new Event('table_scan', ['table' => 'users']));
    $scans = array_values(array_filter(getPending($r), fn($e) => $e->type === 'scan_result'));
    $lastScan = end($scans);
    $alice = array_values(array_filter($lastScan->data['rows'], fn($row) => $row['name'] === 'Alice'));
    assertEqual($alice[0]['age'], 31);

    // Delete
    $r->emit(new Event('delete_execute', ['table' => 'users', 'where' => ['column' => 'name', 'op' => '=', 'value' => 'Bob']]));

    // Scan to verify
    $r->emit(new Event('table_scan', ['table' => 'users']));
    $scans = array_values(array_filter(getPending($r), fn($e) => $e->type === 'scan_result'));
    $lastScan = end($scans);
    assertCount(1, $lastScan->data['rows']);
    assertEqual($lastScan->data['rows'][0]['name'], 'Alice');
});

test('snapshot and restore', function () {
    $r = freshRunner();
    createTable($r, 'users', [['name' => 'name', 'type' => 'TEXT']]);
    insertRow($r, 'users', ['name' => 'Alice']);
    $snap = $r->snapshot();

    insertRow($r, 'users', ['name' => 'Bob']);
    $r->emit(new Event('table_scan', ['table' => 'users']));
    $scans = array_values(array_filter(getPending($r), fn($e) => $e->type === 'scan_result'));
    $last = end($scans);
    assertCount(2, $last->data['rows']);

    $r->restore($snap);
    $r->emit(new Event('table_scan', ['table' => 'users']));
    $scans = array_values(array_filter(getPending($r), fn($e) => $e->type === 'scan_result'));
    $last = end($scans);
    assertCount(1, $last->data['rows']);
});

test('DROP TABLE cleans everything', function () {
    $r = freshRunner();
    createTable($r, 'users', [['name' => 'x', 'type' => 'INTEGER']]);
    insertRow($r, 'users', ['x' => 1]);
    insertRow($r, 'users', ['x' => 2]);
    $r->emit(new Event('index_create_execute', ['table' => 'users', 'index' => 'idx', 'column' => 'x']));
    $r->emit(new Event('drop_table_execute', ['table' => 'users']));

    // Verify table is gone
    $r->emit(new Event('table_scan', ['table' => 'users']));
    $scans = array_values(array_filter(getPending($r), fn($e) => $e->type === 'scan_result'));
    $last = end($scans);
    assertCount(0, $last->data['rows']);
});

// ─── DONE ────────────────────────────────────────────────
cleanTmp();
report();
