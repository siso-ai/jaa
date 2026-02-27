# Jaa Database Engine

**A complete relational database engine built on the SISO framework.**

SQL string in, query results out. The entire database is a set of gates — SQL parsing is PureGates, table scans and mutations are StateGates, the Runner wires them to content-addressable persistence. File-backed or in-memory. Interactive REPL. Dual implementation in JavaScript and PHP from the same architecture.

Built with the [SISO Framework](https://siso-framework.org).

## Quick Start

```bash
# In-memory database
node jaa.js

# File-backed database
node jaa.js --dir ./mydb
```

```
jaa> CREATE TABLE users (name TEXT NOT NULL, email TEXT, age INTEGER);
Table 'users' created.

jaa> INSERT INTO users (name, email, age) VALUES ('Alice', 'alice@example.com', 30);
Row inserted (id: 1).

jaa> SELECT name, age FROM users WHERE age > 25 ORDER BY name;
 name  | age
-------+-----
 Alice | 30
(1 row)

jaa> .tables
  users

jaa> .quit
```

## SQL Support

### DDL

- `CREATE TABLE` with column types, NOT NULL, DEFAULT, IF NOT EXISTS
- `DROP TABLE`, `TRUNCATE TABLE`
- `ALTER TABLE` — ADD COLUMN, DROP COLUMN, RENAME TABLE
- `CREATE INDEX`, `CREATE UNIQUE INDEX`, `DROP INDEX`
- `CREATE VIEW`, `DROP VIEW`
- `CREATE TRIGGER`, `DROP TRIGGER`
- `ADD CONSTRAINT`, `DROP CONSTRAINT`

### DML

- `INSERT INTO` with expressions, defaults, multi-row
- `INSERT INTO ... SELECT`
- `UPDATE ... SET ... WHERE` with complex expressions
- `UPDATE ... FROM` (update from subquery)
- `DELETE FROM ... WHERE`
- `UPSERT` — `ON CONFLICT DO NOTHING / DO UPDATE SET`
- `RETURNING` clause

### Queries

- `SELECT` with expressions, aliases, `*`
- `WHERE` with AND, OR, NOT, IN, LIKE, BETWEEN, IS NULL, IS NOT NULL
- `JOIN` — INNER, LEFT, RIGHT, CROSS
- `GROUP BY` with `HAVING`
- `ORDER BY` (columns, expressions, numeric positions, ASC/DESC, NULLS FIRST/LAST)
- `LIMIT` / `OFFSET`
- `DISTINCT`
- `UNION` / `UNION ALL`
- Subqueries (scalar, IN, EXISTS, FROM clause)
- Common Table Expressions (`WITH ... AS`)
- Recursive CTEs (`WITH RECURSIVE`)
- `CASE WHEN ... THEN ... ELSE ... END`
- `EXPLAIN`

### Functions & Operators

- Aggregates: `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`
- String: `UPPER`, `LOWER`, `LENGTH`, `SUBSTR`, `TRIM`, `REPLACE`, `CONCAT`, `||`
- Math: `ABS`, `ROUND`, `CEIL`, `FLOOR`, `MOD`, `POWER`, `SQRT`
- Null: `COALESCE`, `NULLIF`, `IFNULL`, `IIF`
- Date/Time: `DATE`, `TIME`, `DATETIME`, `STRFTIME`, `JULIANDAY`
- Type: `CAST`, `TYPEOF`
- Arithmetic: `+`, `-`, `*`, `/`, `%`
- Comparison: `=`, `!=`, `<>`, `<`, `>`, `<=`, `>=`

### Transactions

- `BEGIN`, `COMMIT`, `ROLLBACK`
- Snapshot isolation via content-addressable store

## Architecture

The event chain for a SELECT query:

```
sql → sql_dispatch → select_parse → query_plan → table_scan
    → filter → projection → order_by → limit → query_result
```

Every arrow is a gate. Every gate is independently testable.

- **PureGates** handle parsing, filtering, projection, ordering — no state needed
- **StateGates** handle table scans, inserts, updates, deletes — declare reads via ReadSet, return MutationBatch
- **Runner** resolves state and applies mutations. The only impure component

### Persistence

Content-addressable store using SHA-256 hashes of canonical JSON. Named refs point at hashes.

```
db/tables/users/schema     → sha256:a1b2c3...
db/tables/users/rows/1     → sha256:d4e5f6...
db/tables/users/rows/2     → sha256:g7h8i9...
db/tables/users/next_id    → sha256:j0k1l2...
db/tables/users/indexes/   → (prefix pattern)
```

Same content always produces the same hash. The store doesn't know it's holding a database.

## Tests

### JavaScript

```bash
# Run all tests
for f in test/*.test.js; do node "$f"; done
```

```
core: 18/18 passed
streamlog: 15/15 passed
phase10-parity: 39/39 passed
phase11-expressions: 34/34 passed
phase12-sql-completeness: 24/24 passed
phase13-subqueries: 16/16 passed
phase14-advanced: 18/18 passed
phase15-dml-advanced: 18/18 passed
phase16-operators-functions: 30/30 passed
phase17-crossjoin-having: 15/15 passed
phase18-features: 16/16 passed
phase19-features: 19/19 passed
phase20-features: 14/14 passed
...
587 tests passing
```

### PHP

```bash
php test/test.php
# 536 tests passing
```

## Polyglot

The JavaScript and PHP implementations share the same architecture, the same gate signatures, the same persistence model, and the same test coverage. The contract is the shape, not the language.

```
src/core/          ↔  ice/Core/
src/protocol/      ↔  ice/Protocol/
src/resolution/    ↔  ice/Resolution/
src/persistence/   ↔  ice/Persistence/
src/gates/         ↔  ice/Gates/
```

## Requirements

- Node.js 16+ (JavaScript) or PHP 8.1+ (PHP)
- Zero external dependencies

## License

GPL-3.0. See [LICENSE](LICENSE) for details.

## Outro
...?
