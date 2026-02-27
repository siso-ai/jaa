<?php
/**
 * ICE Database — PSR-4 autoloader.
 * Maps Ice\Core\Event → ice/Core/Event.php
 */
spl_autoload_register(function ($class) {
    $prefix = 'Ice\\';
    if (!str_starts_with($class, $prefix)) return;
    $relative = str_replace('\\', '/', substr($class, strlen($prefix)));
    $file = __DIR__ . '/' . $relative . '.php';
    if (file_exists($file)) require $file;
});

// Load function files (not autoloadable)
require_once __DIR__ . '/Persistence/Canonicalize.php';
require_once __DIR__ . '/Gates/Database/Expression.php';
require_once __DIR__ . '/Gates/Database/FilterGate.php';
require_once __DIR__ . '/Gates/Database/ProjectionGate.php';
require_once __DIR__ . '/Gates/Database/OrderByGate.php';
require_once __DIR__ . '/Gates/Database/LimitGate.php';
require_once __DIR__ . '/Gates/Database/DistinctGate.php';
require_once __DIR__ . '/Gates/Database/AggregateGate.php';
require_once __DIR__ . '/Gates/Database/InsertExecuteGate.php';
require_once __DIR__ . '/Gates/Database/UpdateExecuteGate.php';
require_once __DIR__ . '/Gates/Database/DeleteExecuteGate.php';
require_once __DIR__ . '/Gates/Database/IndexScanGate.php';
require_once __DIR__ . '/Gates/Database/JoinGate.php';
require_once __DIR__ . '/Gates/Database/ViewGates.php';
require_once __DIR__ . '/Gates/Database/TriggerGates.php';
require_once __DIR__ . '/Gates/Database/ConstraintGates.php';
require_once __DIR__ . '/Gates/Database/Register.php';

// SQL Query/Parse layer (Phase 8)
require_once __DIR__ . '/Gates/Query/SQL/Tokenizer.php';
require_once __DIR__ . '/Gates/Query/SQL/ParserUtils.php';
require_once __DIR__ . '/Gates/Query/SQL/SQLDispatchGate.php';
require_once __DIR__ . '/Gates/Query/SQL/CreateTableParseGate.php';
require_once __DIR__ . '/Gates/Query/SQL/DropTableParseGate.php';
require_once __DIR__ . '/Gates/Query/SQL/InsertParseGate.php';
require_once __DIR__ . '/Gates/Query/SQL/SelectParseGate.php';
require_once __DIR__ . '/Gates/Query/SQL/UpdateParseGate.php';
require_once __DIR__ . '/Gates/Query/SQL/DeleteParseGate.php';
require_once __DIR__ . '/Gates/Query/SQL/IndexParseGates.php';
require_once __DIR__ . '/Gates/Query/SQL/ViewParseGates.php';
require_once __DIR__ . '/Gates/Query/SQL/TriggerParseGates.php';
require_once __DIR__ . '/Gates/Query/SQL/ConstraintParseGates.php';
require_once __DIR__ . '/Gates/Query/SQL/QueryPlanGate.php';
require_once __DIR__ . '/Gates/Query/SQL/AlterTableParseGates.php';
require_once __DIR__ . '/Gates/Query/SQL/Register.php';
