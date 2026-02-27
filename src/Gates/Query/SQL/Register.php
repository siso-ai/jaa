<?php
namespace Ice\Gates\Query\SQL;

use Ice\Resolution\Runner;

require_once __DIR__ . '/Phase12Gates.php';

function registerSQLGates(Runner $runner): void {
    $runner->register(new SQLDispatchGate());
    $runner->register(new CreateTableParseGate());
    $runner->register(new DropTableParseGate());
    $runner->register(new InsertParseGate());
    $runner->register(new SelectParseGate());
    $runner->register(new UpdateParseGate());
    $runner->register(new DeleteParseGate());
    $runner->register(new IndexCreateParseGate());
    $runner->register(new IndexDropParseGate());
    $runner->register(new ViewCreateParseGate());
    $runner->register(new ViewDropParseGate());
    $runner->register(new TriggerCreateParseGate());
    $runner->register(new TriggerDropParseGate());
    $runner->register(new ConstraintCreateParseGate());
    $runner->register(new ConstraintDropParseGate());
    $runner->register(new QueryPlanGate());
    $runner->register(new AlterTableAddColumnParseGate());
    $runner->register(new AlterTableDropColumnParseGate());
    $runner->register(new RenameTableParseGate());

    // Phase 12
    $runner->register(new ExplainGate());
    $runner->register(new InsertSelectGate());
    $runner->register(new CreateTableAsSelectGate());
}
