<?php
namespace Ice\Gates\Database;

use Ice\Resolution\Runner;

// Require multi-class files that PSR-4 can't autoload by class name
require_once __DIR__ . '/ViewGates.php';
require_once __DIR__ . '/TriggerGates.php';
require_once __DIR__ . '/ConstraintGates.php';
require_once __DIR__ . '/TransactionGates.php';
require_once __DIR__ . '/AlterTableGates.php';
require_once __DIR__ . '/InsertSelectPlanGate.php';

function registerDatabaseGates(Runner $runner): void {
    // DDL
    $runner->register(new CreateTableExecuteGate());
    $runner->register(new DropTableExecuteGate());

    // DML
    $runner->register(new InsertExecuteGate());
    $runner->register(new UpdateExecuteGate());
    $runner->register(new DeleteExecuteGate());

    // Query
    $runner->register(new TableScanGate());
    $runner->register(new IndexScanGate());
    $runner->register(new FilterGate());
    $runner->register(new ProjectionGate());
    $runner->register(new OrderByGate());
    $runner->register(new LimitGate());
    $runner->register(new DistinctGate());
    $runner->register(new AggregateGate());
    $runner->register(new JoinGate());

    // Index management
    $runner->register(new IndexCreateExecuteGate());
    $runner->register(new IndexDropExecuteGate());

    // View management
    $runner->register(new ViewCreateExecuteGate());
    $runner->register(new ViewDropExecuteGate());
    $runner->register(new ViewExpansionGate());

    // Trigger management
    $runner->register(new TriggerCreateExecuteGate());
    $runner->register(new TriggerDropExecuteGate());

    // Constraint management
    $runner->register(new ConstraintCreateExecuteGate());
    $runner->register(new ConstraintDropExecuteGate());

    // ALTER TABLE
    $runner->register(new AlterTableAddColumnGate());
    $runner->register(new AlterTableDropColumnGate());
    $runner->register(new RenameTableGate());

    // INSERT...SELECT / CREATE TABLE AS SELECT
    $runner->register(new InsertSelectPlanGate());

    // Transactions
    $txn = new TransactionManager();
    $runner->register(new TransactionBeginGate($txn, fn() => $runner->snapshot()));
    $runner->register(new TransactionCommitGate($txn));
    $runner->register(new TransactionRollbackGate($txn, fn($snap) => $runner->restore($snap)));
}
