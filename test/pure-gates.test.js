import { test, assert, assertEqual, report } from './runner.js';
import { filterRows } from '../src/gates/database/FilterGate.js';
import { projectRows } from '../src/gates/database/ProjectionGate.js';
import { orderByRows } from '../src/gates/database/OrderByGate.js';
import { limitRows } from '../src/gates/database/LimitGate.js';
import { distinctRows } from '../src/gates/database/DistinctGate.js';
import { aggregateRows } from '../src/gates/database/AggregateGate.js';
import { joinRows } from '../src/gates/database/JoinGate.js';

const rows = [
  { id: 1, name: 'Alice', age: 30, dept: 'eng' },
  { id: 2, name: 'Bob', age: 25, dept: 'sales' },
  { id: 3, name: 'Carol', age: 35, dept: 'eng' },
  { id: 4, name: 'Dave', age: 25, dept: 'eng' },
  { id: 5, name: 'Eve', age: 30, dept: 'sales' },
];

// ── filterRows ──────────────────────────────────

test('filterRows: by equality', () => {
  const r = filterRows(rows, { column: 'dept', op: '=', value: 'eng' });
  assertEqual(r.length, 3);
});

test('filterRows: by comparison', () => {
  const r = filterRows(rows, { column: 'age', op: '>', value: 28 });
  assertEqual(r.length, 3);
});

test('filterRows: null condition returns all', () => {
  assertEqual(filterRows(rows, null).length, 5);
});

test('filterRows: AND condition', () => {
  const r = filterRows(rows, {
    and: [
      { column: 'dept', op: '=', value: 'eng' },
      { column: 'age', op: '>', value: 28 }
    ]
  });
  assertEqual(r.length, 2);
  assert(r.some(row => row.name === 'Alice'));
  assert(r.some(row => row.name === 'Carol'));
});

// ── projectRows ─────────────────────────────────

test('projectRows: select columns', () => {
  const r = projectRows(rows, ['name', 'age']);
  assertEqual(Object.keys(r[0]).length, 2);
  assertEqual(r[0].name, 'Alice');
  assertEqual(r[0].age, 30);
  assertEqual(r[0].dept, undefined);
});

test('projectRows: star returns all', () => {
  const r = projectRows(rows, ['*']);
  assertEqual(Object.keys(r[0]).length, 4);
});

test('projectRows: empty/null returns all', () => {
  assertEqual(projectRows(rows, null).length, 5);
  assertEqual(projectRows(rows, []).length, 5);
});

test('projectRows: expression alias', () => {
  const r = projectRows(rows, [
    { expr: { op: '+', left: 'age', right: 1 }, alias: 'next_age' }
  ]);
  assertEqual(r[0].next_age, 31);
});

// ── orderByRows ─────────────────────────────────

test('orderByRows: ascending', () => {
  const r = orderByRows(rows, [{ column: 'age', direction: 'asc' }]);
  assertEqual(r[0].age, 25);
  assertEqual(r[r.length - 1].age, 35);
});

test('orderByRows: descending', () => {
  const r = orderByRows(rows, [{ column: 'age', direction: 'desc' }]);
  assertEqual(r[0].age, 35);
  assertEqual(r[r.length - 1].age, 25);
});

test('orderByRows: multi-column', () => {
  const r = orderByRows(rows, [
    { column: 'age', direction: 'asc' },
    { column: 'name', direction: 'asc' }
  ]);
  // age 25: Bob, Dave
  assertEqual(r[0].name, 'Bob');
  assertEqual(r[1].name, 'Dave');
});

test('orderByRows: does not mutate original', () => {
  const original = [...rows];
  orderByRows(rows, [{ column: 'age', direction: 'desc' }]);
  assertEqual(rows[0].name, original[0].name);
});

// ── limitRows ───────────────────────────────────

test('limitRows: basic limit', () => {
  assertEqual(limitRows(rows, 2).length, 2);
});

test('limitRows: offset', () => {
  const r = limitRows(rows, 2, 1);
  assertEqual(r.length, 2);
  assertEqual(r[0].name, 'Bob');
});

test('limitRows: limit exceeds length', () => {
  assertEqual(limitRows(rows, 100).length, 5);
});

test('limitRows: null limit returns all', () => {
  assertEqual(limitRows(rows, null).length, 5);
});

// ── distinctRows ────────────────────────────────

test('distinctRows: by column', () => {
  const r = distinctRows(rows, ['dept']);
  assertEqual(r.length, 2);
});

test('distinctRows: by age', () => {
  const r = distinctRows(rows, ['age']);
  assertEqual(r.length, 3); // 25, 30, 35
});

test('distinctRows: full row', () => {
  const duped = [...rows, { id: 1, name: 'Alice', age: 30, dept: 'eng' }];
  const r = distinctRows(duped, null);
  assertEqual(r.length, 5);
});

// ── aggregateRows ───────────────────────────────

test('aggregateRows: COUNT', () => {
  const r = aggregateRows(rows,
    [{ fn: 'COUNT', column: '*', alias: 'total' }],
    null
  );
  assertEqual(r.length, 1);
  assertEqual(r[0].total, 5);
});

test('aggregateRows: SUM, AVG', () => {
  const r = aggregateRows(rows,
    [
      { fn: 'SUM', column: 'age', alias: 'sum_age' },
      { fn: 'AVG', column: 'age', alias: 'avg_age' }
    ],
    null
  );
  assertEqual(r[0].sum_age, 145);
  assertEqual(r[0].avg_age, 29);
});

test('aggregateRows: MIN, MAX', () => {
  const r = aggregateRows(rows,
    [
      { fn: 'MIN', column: 'age', alias: 'min_age' },
      { fn: 'MAX', column: 'age', alias: 'max_age' }
    ],
    null
  );
  assertEqual(r[0].min_age, 25);
  assertEqual(r[0].max_age, 35);
});

test('aggregateRows: GROUP BY', () => {
  const r = aggregateRows(rows,
    [{ fn: 'COUNT', column: '*', alias: 'count' }],
    ['dept']
  );
  assertEqual(r.length, 2);
  const eng = r.find(g => g.dept === 'eng');
  const sales = r.find(g => g.dept === 'sales');
  assertEqual(eng.count, 3);
  assertEqual(sales.count, 2);
});

test('aggregateRows: GROUP BY with SUM', () => {
  const r = aggregateRows(rows,
    [{ fn: 'SUM', column: 'age', alias: 'total_age' }],
    ['dept']
  );
  const eng = r.find(g => g.dept === 'eng');
  assertEqual(eng.total_age, 90); // 30 + 35 + 25
});

// ── joinRows ────────────────────────────────────

const users = [
  { id: 1, name: 'Alice', dept_id: 10 },
  { id: 2, name: 'Bob', dept_id: 20 },
  { id: 3, name: 'Carol', dept_id: null },
];
const depts = [
  { id: 10, dept_name: 'Engineering' },
  { id: 20, dept_name: 'Sales' },
  { id: 30, dept_name: 'HR' },
];

test('joinRows: inner join', () => {
  const r = joinRows(users, depts, { left: 'dept_id', right: 'id' }, 'inner');
  assertEqual(r.length, 2);
  assert(r.some(row => row.name === 'Alice' && row.dept_name === 'Engineering'));
  assert(r.some(row => row.name === 'Bob' && row.dept_name === 'Sales'));
});

test('joinRows: left join', () => {
  const r = joinRows(users, depts, { left: 'dept_id', right: 'id' }, 'left');
  assertEqual(r.length, 3);
  const carol = r.find(row => row.name === 'Carol');
  assertEqual(carol.dept_name, null);
});

test('joinRows: right join', () => {
  const r = joinRows(users, depts, { left: 'dept_id', right: 'id' }, 'right');
  assertEqual(r.length, 3); // Alice+Eng, Bob+Sales, null+HR
  const hr = r.find(row => row.dept_name === 'HR');
  assertEqual(hr.name, null);
});

test('joinRows: full join', () => {
  const r = joinRows(users, depts, { left: 'dept_id', right: 'id' }, 'full');
  assertEqual(r.length, 4); // Alice+Eng, Bob+Sales, Carol+null, null+HR
});

const exitCode = report('pure-gates');
process.exit(exitCode);
