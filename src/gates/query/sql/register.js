import { SQLDispatchGate } from './SQLDispatchGate.js';
import { CreateTableParseGate } from './CreateTableParseGate.js';
import { DropTableParseGate } from './DropTableParseGate.js';
import { InsertParseGate } from './InsertParseGate.js';
import { SelectParseGate } from './SelectParseGate.js';
import { UpdateParseGate } from './UpdateParseGate.js';
import { DeleteParseGate } from './DeleteParseGate.js';
import { IndexCreateParseGate, IndexDropParseGate } from './IndexParseGates.js';
import { ViewCreateParseGate, ViewDropParseGate } from './ViewParseGates.js';
import { TriggerCreateParseGate, TriggerDropParseGate } from './TriggerParseGates.js';
import { ConstraintCreateParseGate, ConstraintDropParseGate } from './ConstraintParseGates.js';
import { QueryPlanGate } from './QueryPlanGate.js';
import { AlterTableAddColumnParseGate, AlterTableDropColumnParseGate, RenameTableParseGate } from './AlterTableParseGates.js';
import { ExplainGate, InsertSelectGate, CreateTableAsSelectGate } from './Phase12Gates.js';

export function registerSQLGates(runner) {
  runner.register(new SQLDispatchGate());
  runner.register(new CreateTableParseGate());
  runner.register(new DropTableParseGate());
  runner.register(new InsertParseGate());
  runner.register(new SelectParseGate());
  runner.register(new UpdateParseGate());
  runner.register(new DeleteParseGate());
  runner.register(new IndexCreateParseGate());
  runner.register(new IndexDropParseGate());
  runner.register(new ViewCreateParseGate());
  runner.register(new ViewDropParseGate());
  runner.register(new TriggerCreateParseGate());
  runner.register(new TriggerDropParseGate());
  runner.register(new ConstraintCreateParseGate());
  runner.register(new ConstraintDropParseGate());
  runner.register(new QueryPlanGate());
  runner.register(new AlterTableAddColumnParseGate());
  runner.register(new AlterTableDropColumnParseGate());
  runner.register(new RenameTableParseGate());

  // Phase 12
  runner.register(new ExplainGate());
  runner.register(new InsertSelectGate());
  runner.register(new CreateTableAsSelectGate());
}
