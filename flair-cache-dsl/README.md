# flair-cache-dsl — Query DSL

SQL-like filtering, joining, grouping, and aggregating over any in-memory Java collection.

[![Java](https://img.shields.io/badge/Java-11%2B-orange.svg)](https://openjdk.org/)
[![Zero Dependencies](https://img.shields.io/badge/Dependencies-Zero-brightgreen.svg)]()

---

## Overview

`flair-cache-dsl` is a standalone, zero-dependency Java library for building fluent, composable
queries over in-memory data. It accepts any `Map` or `Collection` as input — it has no knowledge
of the FLAIR cache store and can be used in any Java project that needs predicate-driven queries
over in-memory collections.

```java
List<Product> cheapElectronics = QueryEngine.over(productMap)
    .from("products", Product.class, Decoder.identity())
    .where(p -> p.category().equals("electronics"))
    .and(p -> p.price() < 500)
    .orderBy(Product::price, Order.ASC)
    .limit(10)
    .fetch();
```

**Capabilities at a glance**

- Filter with predicate chains (`where` / `and` / `or`)
- Sort, paginate (`orderBy` / `limit` / `offset`)
- Count, find, and summarize with numeric statistics
- Hash-join or nested-loop join across two named sources
- Group by key and aggregate (sum, avg, min, max, count, minBy, maxBy)
- Opt into parallel execution on a dedicated, isolated `ForkJoinPool`
- Immutable query builder — reuse templates safely across threads

---

## Dependency

```xml
<dependency>
    <groupId>com.simplj.flair</groupId>
    <artifactId>flair-cache-dsl</artifactId>
    <version>${flair.version}</version>
</dependency>
```

Zero transitive dependencies. JDK 11+ bytecode. No external libraries.

---

## Getting Started

### 1. Register your data

Wrap one or more `Map` or `Collection` instances in a `DataRegistry`, then create an engine.

```java
Map<String, Product>  products  = /* your map */;
Map<String, Customer> customers = /* your map */;

DataRegistry registry = new DataRegistry()
    .register("products",  products)
    .register("customers", customers);

QueryEngine engine = QueryEngine.over(registry);
```

If you only have one source, skip the registry:

```java
QueryEngine engine = QueryEngine.over(productMap);
```

### 2. Build and execute a query

```java
List<Product> results = engine
    .from("products", Product.class, Decoder.identity())
    .where(p -> p.category().equals("books"))
    .orderBy(Product::price, Order.ASC)
    .limit(20)
    .fetch();
```

---

## Core Concepts

### `QueryEngine`

The stateless entry point. It is thread-safe and should be reused across calls.

| Factory method | Description |
|---|---|
| `QueryEngine.over(Map<K,V>)` | Single-source mode. Join is not supported. |
| `QueryEngine.over(DataRegistry)` | Multi-source mode. Supports joins. |

`engine.from(name, Class<T>, Decoder<T>)` starts a query against the named source.
The `name` is used as a documentation label in single-source mode.

---

### `Decoder<T>`

Controls how raw `Object` values from the source are converted to the typed query element `T`.

| Decoder | Behaviour |
|---|---|
| `Decoder.identity()` | Unchecked cast — use when the map's value type matches `T`. Fast, no allocation. |
| `Decoder.typed(Class<T>)` | Checked cast — throws a descriptive `ClassCastException` early if a value is the wrong type or `null`. |
| Custom lambda | `raw -> (T) myConvert(raw)` — implement `Decoder<T>` for any transformation. |

**When to prefer `typed()`:**

```java
// identity() silently succeeds, CCE surfaces later at a call site you may not expect
engine.from("items", Product.class, Decoder.identity())

// typed() throws immediately at decode time with "Expected Product but got String"
engine.from("items", Product.class, Decoder.typed(Product.class))
```

---

### `DataRegistry`

Holds named `Map` or `Collection` sources for multi-source queries.

```java
DataRegistry registry = new DataRegistry();

registry.register("products", productMap);          // throws if "products" already registered
registry.registerOrReplace("products", productMap); // silently overwrites

boolean exists = registry.contains("products");     // true
```

**`register()` vs `registerOrReplace()`**

Use `register()` for initial setup — it prevents accidental overwrites by throwing
`IllegalStateException` on duplicate names. Use `registerOrReplace()` for intentional
live data reloads (e.g., refreshing a source after a database read).

**Live-view semantics**

`register(name, Map)` stores a live view of `Map.values()`. Mutations to the original
map after registration are immediately visible in subsequent query results. Pass a defensive
copy if you need snapshot semantics:

```java
registry.register("snapshot", new HashMap<>(liveMap));
```

---

## Filtering

All filter methods return a new, independent `SingleBlockQuery` — the original is unchanged.

```java
SingleBlockQuery<Product> base = engine
    .from("products", Product.class, Decoder.identity())
    .where(p -> p.stock() > 0);            // base: in-stock products

List<Product> electronics = base
    .where(p -> p.category().equals("electronics"))   // AND-ed with base predicate
    .fetch();

List<Product> expensive = base
    .where(p -> p.price() > 1000)                     // independent of `electronics` query
    .fetch();
```

| Method | Description |
|---|---|
| `where(Predicate<T>)` | Adds a filter (ANDed with any existing predicate). |
| `and(Predicate<T>)` | Alias for `where` — improves readability when chaining. |
| `or(Predicate<T>)` | ORs the new predicate onto the current filter. |

---

## Ordering and Pagination

```java
List<Product> page2 = engine
    .from("products", Product.class, Decoder.identity())
    .where(p -> p.stock() > 0)
    .orderBy(Product::price, Order.ASC)   // sort ascending by price
    .offset(20)                           // skip first 20
    .limit(10)                            // take next 10
    .fetch();
```

| Method | Description |
|---|---|
| `orderBy(Function<T,U>, Order)` | Sort by a `Comparable` key. `Order.ASC` or `Order.DESC`. |
| `limit(int)` | Maximum number of results to return. `limit(0)` returns empty. |
| `offset(int)` | Number of entries to skip before collecting results. `offset > size` returns empty. |

`orderBy` materialises the full filtered stream to sort. Always pair it with `limit` in production
to cap the sort cost.

---

## Count and Find

```java
// Count respecting offset + limit (e.g. for current-page size)
long pageCount = query.count();

// Total matches regardless of offset/limit (e.g. for "showing 10 of N" UIs)
long total = query.countMatching();

// First element in encounter order (respects orderBy, offset, limit)
Optional<Product> first = query.orderBy(Product::price, Order.ASC).findFirst();

// Any matching element — faster parallel alternative when order does not matter
Optional<Product> any = query.parallel().where(p -> p.active()).findAny();
```

| Method | Respects predicate | Respects pagination | Parallel-aware |
|---|---|---|---|
| `count()` | ✅ | ✅ | ✅ |
| `countMatching()` | ✅ | ❌ (ignores limit/offset) | ✅ |
| `findFirst()` | ✅ | ✅ | ❌ (always sequential) |
| `findAny()` | ✅ | ❌ | ✅ |

`findFirst()` always executes sequentially to preserve encounter order.
`findAny()` routes through the dedicated DSL pool when `.parallel()` was called —
the returned element may not be the first in encounter order.

---

## Summary Statistics

Compute numeric statistics over the paginated, filtered result set in one pass.

```java
SummaryStatistics stats = engine
    .from("products", Product.class, Decoder.identity())
    .where(p -> p.category().equals("electronics"))
    .summarize(Product::price);

stats.count();   // number of matching entries
stats.sum();     // sum of the field
stats.min();     // minimum value
stats.max();     // maximum value
stats.avg();     // arithmetic average
```

Returns `SummaryStatistics.EMPTY` (sentinel with `count=0`, `min/max = NaN`) when no entries match.
Respects `orderBy`, `offset`, and `limit`. Routes through the dedicated DSL pool when `.parallel()`
was called.

---

## Query Result with Metadata

`fetchResult()` returns a `QueryResult<T>` containing the items and wall-clock execution time.

```java
QueryResult<Product> result = engine
    .from("products", Product.class, Decoder.identity())
    .where(p -> p.price() < 1000)
    .fetchResult();

List<Product> items        = result.items();
int           count        = result.size();
long          nanos        = result.executionNanos();
boolean       empty        = result.isEmpty();
```

---

## Parallel Execution

Call `.parallel()` anywhere in the chain to opt into parallel execution on a dedicated
`ForkJoinPool` (workers named `flaircache-user-{n}`). The pool is shared across all `QueryEngine`
instances and loaded lazily on first use.

```java
List<Product> results = engine
    .from("products", Product.class, Decoder.identity())
    .parallel()
    .where(p -> p.price() < 500)
    .orderBy(Product::price, Order.ASC)
    .fetch();
```

**What parallel() affects**

| Terminal | Parallel-aware |
|---|---|
| `fetch()` | ✅ |
| `fetchResult()` | ✅ (delegates to `fetch`) |
| `count()` | ✅ |
| `countMatching()` | ✅ |
| `summarize()` | ✅ |
| `findAny()` | ✅ |
| `findFirst()` | ❌ — always sequential to preserve encounter order |
| `groupBy().aggregate()` | ❌ — always sequential in V1 (see [Limitations](#known-limitations)) |

Results are identical to sequential execution. `parallel()` is a performance hint for large
datasets — overhead can outweigh benefit below ~50,000 entries
(`QueryEngine.DEFAULT_PARALLEL_THRESHOLD`).

---

## Joining Two Sources

Join pairs entries from two named sources. Requires a `DataRegistry` engine (not the single-map
`over(Map)` variant).

### Hash join — preferred for equality conditions

Builds a hash index over the right-side source for O(n + m) execution.

```java
List<OrderSummary> results = engine
    .from("orders", Order.class, Decoder.identity())
    .<Customer>join("customers", Customer.class, Decoder.identity())
    .on(Order::customerId, Customer::id)           // left-key, right-key extractors
    .select((order, customer) -> new OrderSummary(order.id(), customer.name()))
    .fetch();
```

### Nested-loop join — for non-equality or complex conditions

O(n × m) — use when the join condition cannot be expressed as equality on a shared key.

```java
List<Match> results = engine
    .from("products", Product.class, Decoder.identity())
    .<Discount>join("discounts", Discount.class, Decoder.identity())
    .on((p, d) -> p.category().equals(d.category()) && p.price() > d.minPrice())
    .select((p, d) -> new Match(p, d))
    .fetch();
```

### Filtering the left side before joining

```java
engine.from("orders", Order.class, Decoder.identity())
    .<Customer>join("customers", Customer.class, Decoder.identity())
    .on(Order::customerId, Customer::id)
    .where(o -> o.total() > 500)           // applied to left side only
    .select((o, c) -> new OrderSummary(o.id(), c.name()))
    .fetch();
```

Alternatively, chain `.where()` on the `SingleBlockQuery` before calling `.join()` — the
predicate is automatically forwarded as the initial left-side filter.

### Join API reference

| Method | Description |
|---|---|
| `join(String, Class<R>, Decoder<R>)` | Starts a join against a named right-side source. |
| `on(Function<L,K>, Function<R,K>)` | Hash join strategy — equality on shared key. |
| `on(BiPredicate<L,R>)` | Nested-loop join strategy — arbitrary condition. |
| `where(Predicate<L>)` | Adds a filter on the left side. ANDed with any prior filter. |
| `select(BiFunction<L,R,S>)` | Defines the output projection. Returns `JoinSelectQuery<L,R,S>`. |
| `JoinSelectQuery.fetch()` | Executes the join and returns `List<S>`. Throws `IllegalStateException` if `on()` was never called. |

Calling `on()` a second time replaces the previous strategy entirely. The two strategies
(`BiPredicate` and key-extractor) are mutually exclusive.

**Null key semantics (SQL-consistent):** keys extracted as `null` never match. Left rows with a
`null` key are silently dropped in the join output, consistent with SQL `NULL` behaviour.

---

## GroupBy and Aggregation

Partition the result set by a key, then reduce each partition with an aggregator.

```java
Map<String, Long> countByCategory = engine
    .from("products", Product.class, Decoder.identity())
    .groupBy(Product::category)
    .aggregate(Aggregators.count());

Map<String, Double> avgPriceByCategory = engine
    .from("products", Product.class, Decoder.identity())
    .where(p -> p.stock() > 0)
    .groupBy(Product::category)
    .aggregate(Aggregators.avg(Product::price));
```

### Built-in aggregators

| Aggregator | Return type | Description |
|---|---|---|
| `Aggregators.count()` | `Long` | Number of entries in the group. |
| `Aggregators.sum(ToDoubleFunction<T>)` | `Double` | Sum of the field across the group. |
| `Aggregators.avg(ToDoubleFunction<T>)` | `Double` | Average of the field. Returns `NaN` for empty group. |
| `Aggregators.min(ToDoubleFunction<T>)` | `Double` | Minimum value. Returns `NaN` for empty group. |
| `Aggregators.max(ToDoubleFunction<T>)` | `Double` | Maximum value. Returns `NaN` for empty group. |
| `Aggregators.minBy(Function<T,R>)` | `R` (nullable) | Element with the smallest value of the key field. Returns `null` for empty group. |
| `Aggregators.maxBy(Function<T,R>)` | `R` (nullable) | Element with the largest value of the key field. Returns `null` for empty group. |

### Custom aggregator

`Aggregator<T, R>` is a `@FunctionalInterface` over `Stream<T>`:

```java
Aggregator<Product, String> csvIds = stream ->
    stream.map(Product::id).collect(Collectors.joining(","));

Map<String, String> idsByCategory = engine
    .from("products", Product.class, Decoder.identity())
    .groupBy(Product::category)
    .aggregate(csvIds);
```

---

## Immutable Query Builder

Every chaining method returns a new `SingleBlockQuery` — the original is unchanged. This makes
queries safe to share across threads and reuse as templates.

```java
SingleBlockQuery<Product> base = engine
    .from("products", Product.class, Decoder.identity())
    .where(p -> p.stock() > 0);

// Two independent queries — neither modifies `base`
List<Product> electronics = base.where(p -> p.category().equals("electronics")).fetch();
List<Product> books        = base.where(p -> p.category().equals("books")).fetch();

long totalInStock = base.count();   // still 4 — unaffected by the two above
```

---

## Using with FLAIR CacheBlock

`flair-cache-dsl` has no dependency on the FLAIR store. Integration is a thin adapter in
`flair-cache-store` that wraps `CacheBlock<K,V>` as a `Map` and passes it to `DataRegistry`.
From the DSL's perspective, it is just another `Map`.

```java
// flair-cache-store adapter wires this in automatically
DataRegistry registry = new DataRegistry()
    .register("products",  cache.block("products").asMap())
    .register("customers", cache.block("customers").asMap());

QueryEngine engine = QueryEngine.over(registry);
```

If you are using `flair-cache-dsl` outside of FLAIR, pass any `java.util.Map` or
`java.util.Collection` directly — no FLAIR classes required.

---

## Known Limitations

**`groupBy()` always executes sequentially**
When `.parallel()` is chained before `.groupBy()`, the parallel hint is intentionally ignored.
Running a parallel stream inside `GroupByQuery.aggregate()` without submitting the work to the
dedicated DSL pool would consume `ForkJoinPool.commonPool()`, violating the pool-isolation
guarantee. A `WARNING` is logged when this combination is used. Parallel groupBy on the
dedicated pool is planned for V2.

**`findFirst()` is always sequential**
`findFirst()` always executes on the calling thread to preserve encounter order.
Use `findAny()` with `.parallel()` when any matching element is acceptable and
order does not matter.

**`Decoder.identity()` does not type-check**
`Decoder.identity()` performs an unchecked cast with no runtime verification. If the source
contains values of unexpected types, the `ClassCastException` surfaces at the call site rather
than at the point of decoding — making the root cause hard to trace. Use `Decoder.typed(Class<T>)`
when the source may contain mixed value types.

**Join is limited to two sources**
A single query chain can join exactly two named sources. Joining three or more sources requires
running multiple queries in sequence and feeding intermediate results into the next query
manually. Multi-source join in a single chain is planned for V2.

**Join returns only matched rows (inner join)**
The DSL join always behaves like a SQL `INNER JOIN` — rows with no match on the other side are
dropped. Left, right, and full outer join are planned for V2.

---

## Thread Safety

`QueryEngine` is stateless with respect to query results and safe to share across threads.

`SingleBlockQuery` is an immutable value — every chaining method returns a new instance.
A query object may be safely stored, passed to other threads, and used as a reusable template.

`DataRegistry` uses a non-thread-safe `HashMap` internally. Concurrent `register()` /
`registerOrReplace()` calls from multiple threads require external synchronisation.
Queries executed concurrently against a stable registry are safe.

---

## Module at a Glance

| Class | Role |
|---|---|
| `QueryEngine` | Entry point. `over(Map)` or `over(DataRegistry)`. |
| `DataRegistry` | Named registry of `Map` and `Collection` sources. |
| `Decoder<T>` | Converts raw source values to `T`. `identity()`, `typed()`, or custom lambda. |
| `SingleBlockQuery<T>` | Immutable fluent query builder. Terminals: `fetch`, `fetchResult`, `count`, `countMatching`, `findFirst`, `findAny`, `summarize`. |
| `QueryResult<T>` | Record: `items`, `executionNanos`. |
| `SummaryStatistics` | Record: `count`, `sum`, `min`, `max`, `avg`. `EMPTY` sentinel for empty results. |
| `JoinQuery<L,R>` | Join builder. `on()`, `where()`, `select()`. |
| `JoinSelectQuery<L,R,S>` | Type-safe join terminal. `fetch()` returns `List<S>`. |
| `GroupByQuery<T,K>` | GroupBy terminal. `aggregate(Aggregator<T,R>)` returns `Map<K,R>`. |
| `Aggregator<T,R>` | Functional interface over `Stream<T>` → `R`. |
| `Aggregators` | Factory: `count`, `sum`, `avg`, `min`, `max`, `minBy`, `maxBy`. |
| `Order` | Enum: `ASC`, `DESC`. |
| `QueryPredicate<T>` | Sealed predicate tree (`SimplePredicate`, `AndPredicate`, `OrPredicate`, `NotPredicate`). Internal. |

---

*Part of the [FLAIR Cache](../README.md) project.*
