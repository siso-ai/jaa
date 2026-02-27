/**
 * registerDatabaseGates — registers all database gates on a runner.
 */
import { CreateTableExecuteGate } from './CreateTableExecuteGate.js';
import { DropTableExecuteGate } from './DropTableExecuteGate.js';
import { InsertExecuteGate } from './InsertExecuteGate.js';
import { UpdateExecuteGate } from './UpdateExecuteGate.js';
import { DeleteExecuteGate } from './DeleteExecuteGate.js';
import { TableScanGate } from './TableScanGate.js';
import { IndexScanGate } from './IndexScanGate.js';
import { FilterGate } from './FilterGate.js';
import { ProjectionGate } from './ProjectionGate.js';
import { OrderByGate } from './OrderByGate.js';
import { LimitGate } from './LimitGate.js';
import { DistinctGate } from './DistinctGate.js';
import { AggregateGate } from './AggregateGate.js';
import { JoinGate } from './JoinGate.js';
import { IndexCreateExecuteGate } from './IndexCreateExecuteGate.js';
import { IndexDropExecuteGate } from './IndexDropExecuteGate.js';
import { ViewCreateExecuteGate, ViewDropExecuteGate, ViewExpansionGate } from './ViewGates.js';
import { TriggerCreateExecuteGate, TriggerDropExecuteGate } from './TriggerGates.js';
import { ConstraintCreateExecuteGate, ConstraintDropExecuteGate } from './ConstraintGates.js';
import { TransactionManager, TransactionBeginGate, TransactionCommitGate, TransactionRollbackGate } from './TransactionGates.js';
import { AlterTableAddColumnGate, AlterTableDropColumnGate, RenameTableGate } from './AlterTableGates.js';
import { InsertSelectPlanGate } from './InsertSelectPlanGate.js';

export function registerDatabaseGates(runner) {
  // DDL
  runner.register(new CreateTableExecuteGate());
  runner.register(new DropTableExecuteGate());

  // DML
  runner.register(new InsertExecuteGate());
  runner.register(new UpdateExecuteGate());
  runner.register(new DeleteExecuteGate());

  // Query
  runner.register(new TableScanGate());
  runner.register(new IndexScanGate());
  runner.register(new FilterGate());
  runner.register(new ProjectionGate());
  runner.register(new OrderByGate());
  runner.register(new LimitGate());
  runner.register(new DistinctGate());
  runner.register(new AggregateGate());
  runner.register(new JoinGate());

  // Index management
  runner.register(new IndexCreateExecuteGate());
  runner.register(new IndexDropExecuteGate());

  // View management
  runner.register(new ViewCreateExecuteGate());
  runner.register(new ViewDropExecuteGate());
  runner.register(new ViewExpansionGate());

  // Trigger management
  runner.register(new TriggerCreateExecuteGate());
  runner.register(new TriggerDropExecuteGate());

  // Constraint management
  runner.register(new ConstraintCreateExecuteGate());
  runner.register(new ConstraintDropExecuteGate());

  // ALTER TABLE
  runner.register(new AlterTableAddColumnGate());
  runner.register(new AlterTableDropColumnGate());
  runner.register(new RenameTableGate());

  // INSERT...SELECT / CREATE TABLE AS SELECT
  runner.register(new InsertSelectPlanGate());

  // Transactions
  const txn = new TransactionManager();
  runner.register(new TransactionBeginGate(txn, () => runner.snapshot()));
  runner.register(new TransactionCommitGate(txn));
  runner.register(new TransactionRollbackGate(txn, (snap) => runner.restore(snap)));
}
