# flair-cache-serial

Fast, zero-dependency binary serialization for Java.

Hand-rolled `ByteBuffer`-based encoding with no reflection overhead on the hot path, no external libraries, and a compact wire format.
Part of the [FlairCache](https://github.com/simplj/flair-cache) family, but fully usable as a standalone JAR in any Java project that needs efficient binary serialization without third-party dependencies.

---

## Contents

- [What this module does](#what-this-module-does)
- [Requirements](#requirements)
- [Installation](#installation)
- [Quick start](#quick-start)
- [Core concepts](#core-concepts)
- [Three-tier API](#three-tier-api)
  - [Tier 1 — Scalar methods (ByteBuffer, no registry)](#tier-1--scalar-methods-bytebuffer-no-registry)
  - [Tier 2 — Registered types via ByteBuffer](#tier-2--registered-types-via-bytebuffer)
  - [Tier 3 — Registered types via byte array](#tier-3--registered-types-via-byte-array)
- [Built-in types](#built-in-types)
- [Wire format reference](#wire-format-reference)
- [Container codecs](#container-codecs)
- [Writing a hand-crafted codec with CompositeCodec](#writing-a-hand-crafted-codec-with-compositecodec)
- [Auto-generated codecs with ReflectiveCodec](#auto-generated-codecs-with-reflectivecodec)
- [Enum codecs](#enum-codecs)
- [Implementing Codec directly](#implementing-codec-directly)
- [FrameBuffer — NIO frame construction](#framebuffer--nio-frame-construction)
- [CodecRegistry — managing custom types](#codecregistry--managing-custom-types)
- [Null handling contracts](#null-handling-contracts)
- [Thread safety](#thread-safety)
- [Unsupported types](#unsupported-types)
- [Package structure](#package-structure)

---

## What this module does

`flair-cache-serial` gives you a single entry point — `Codecs` — through which you can:

- Serialize any registered Java type into a `ByteBuffer` or a `byte[]`.
- Deserialize back from either.
- Compute exact sizes before writing, so buffers are never over-allocated.
- Write mixed-type wire frames efficiently with zero intermediate allocation.

Encoding is always **big-endian**. There is no type tag, no schema overhead, no versioning envelope — just the raw fields in the declared order. You own the wire contract.

### Why use this instead of …

| Alternative | Why it doesn't fit here |
|---|---|
| Java's `ObjectOutputStream` | ~10× slower, produces unreadable class-graph blobs, breaks on refactoring |
| Protobuf / Avro / Thrift | External compile-time dependency + generated code; adds a build step |
| Kryo / FST | Runtime third-party JAR required |
| Jackson / Gson (JSON) | 4–8× larger on wire, GC pressure from String allocation |

---

## Requirements

- **Java 17** or later (source and target bytecode are both Java 17)
- **Zero runtime dependencies** — JDK classes only

---

## Installation

### Maven

```xml
<dependency>
    <groupId>com.simplj.flair</groupId>
    <artifactId>flair-cache-serial</artifactId>
    <version>0.1.0-SNAPSHOT</version>
</dependency>
```

### Gradle

```groovy
implementation 'com.simplj.flair:flair-cache-serial:0.1.0-SNAPSHOT'
```

---

## Quick start

```java
// Obtain a Codecs instance pre-loaded with all built-in type codecs.
Codecs codecs = Codecs.standard();

// --- byte[] tier (simplest) ---
byte[] bytes = codecs.encode(String.class, "hello world");
String back  = codecs.decode(String.class, bytes);  // "hello world"

// --- Register a custom type, then encode/decode ---
codecs.register(Point.class, pointCodec);
byte[] encoded = codecs.encode(Point.class, new Point(3, 7));
Point  decoded = codecs.decode(Point.class, encoded);

// --- ByteBuffer tier (frame construction with multiple fields) ---
int frameSize = codecs.sizeOfString("order-42") + 4 + 16;
ByteBuffer buf = ByteBuffer.allocate(frameSize);
codecs.serializeString("order-42", buf);
codecs.serializeInt(100, buf);
codecs.serializeUuid(orderId, buf);

buf.flip();
String key      = codecs.deserializeString(buf);
int    quantity = codecs.deserializeInt(buf);
UUID   id       = codecs.deserializeUuid(buf);
```

---

## Core concepts

### `Codec<T>`

```java
public interface Codec<T> extends Serializer<T>, Deserializer<T> {}

public interface Serializer<T> {
    void serialize(T obj, ByteBuffer buf);   // write obj into buf at current position
    int  sizeOf(T obj);                      // exact number of bytes serialize() will write
}

public interface Deserializer<T> {
    T deserialize(ByteBuffer buf);           // read and advance buf; return the decoded value
}
```

`sizeOf` must always be consistent with `serialize`: the number of bytes advanced by `serialize` must equal the value returned by `sizeOf` for the same object.

### `Codecs`

The single entry point. Wraps a `CodecRegistry` and exposes the three-tier API described below.
Obtain an instance via `Codecs.standard()`. Each call to `standard()` returns a fresh, independent instance with its own registry.

### `CodecRegistry`

A `ConcurrentHashMap<Class<?>, Codec<?>>` that maps types to their codecs.
Exposed via `Codecs.register`, `Codecs.lookup`, and `Codecs.reflect`.
Advanced users can operate on it directly; see [CodecRegistry](#codecregistry--managing-custom-types).

---

## Three-tier API

### Tier 1 — Scalar methods (ByteBuffer, no registry)

Named methods on `Codecs` that write a single primitive or well-known scalar directly into a caller-owned `ByteBuffer`, bypassing the registry entirely. No lookup, no boxing.

```java
Codecs c = Codecs.standard();

c.serializeByte(b, buf);          c.deserializeByte(buf);
c.serializeShort(s, buf);         c.deserializeShort(buf);
c.serializeInt(i, buf);           c.deserializeInt(buf);
c.serializeLong(l, buf);          c.deserializeLong(buf);
c.serializeFloat(f, buf);         c.deserializeFloat(buf);
c.serializeDouble(d, buf);        c.deserializeDouble(buf);
c.serializeBoolean(flag, buf);    c.deserializeBoolean(buf);
c.serializeChar(ch, buf);         c.deserializeChar(buf);

int sz = c.sizeOfString(str);
c.serializeString(str, buf);      c.deserializeString(buf);

int sz = c.sizeOfBytes(bytes);
c.serializeBytes(bytes, buf);     c.deserializeBytes(buf);

c.serializeUuid(uuid, buf);       c.deserializeUuid(buf);
c.serializeInstant(instant, buf); c.deserializeInstant(buf);
```

Use this tier when building wire frames from scratch (e.g. network frames, file headers).

### Tier 2 — Registered types via ByteBuffer

Useful when a frame contains fields of many different types and you want to build it in one pass without intermediate allocation.

```java
Codecs c = Codecs.standard();

// Ask for the size first, allocate once, then serialize.
int size = c.sizeOf(Product.class, product)
         + c.sizeOf(Integer.class, count);

ByteBuffer buf = ByteBuffer.allocate(size);
c.serialize(Product.class, product, buf);
c.serialize(Integer.class, count,   buf);

buf.flip();
Product p = c.deserialize(Product.class, buf);
int     n = c.deserialize(Integer.class, buf);
```

### Tier 3 — Registered types via byte array

Simplest API. Good for standalone payloads and testing. Creates one allocation per call.

```java
Codecs c = Codecs.standard();

byte[]  bytes   = c.encode(LocalDate.class, LocalDate.of(2024, 6, 1));
LocalDate date  = c.decode(LocalDate.class, bytes);
```

---

## Built-in types

`Codecs.standard()` pre-registers codecs for the following 28 types. No `register()` call required.

### Boxed primitives and scalars

| Java type | Wire size |
|---|---|
| `Byte` | 1 byte |
| `Short` | 2 bytes |
| `Integer` | 4 bytes |
| `Long` | 8 bytes |
| `Float` | 4 bytes |
| `Double` | 8 bytes |
| `Boolean` | 1 byte (`1` = true, `0` = false) |
| `Character` | 2 bytes |
| `String` | 2 + UTF-8 byte count (max 65,535 UTF-8 bytes) |
| `byte[]` | 4 + length bytes |
| `UUID` | 16 bytes |
| `Instant` | 12 bytes |
| `BigInteger` | 4 + two's-complement byte length (variable) |
| `BigDecimal` | BigInteger (unscaled value) + 4 (scale) |

### `java.time` types

| Java type | Wire size |
|---|---|
| `LocalDate` | 6 bytes |
| `LocalTime` | 8 bytes |
| `LocalDateTime` | 14 bytes |
| `Duration` | 12 bytes |
| `OffsetDateTime` | 16 bytes |
| `ZonedDateTime` | 12 + zone ID string (variable) |
| `Period` | 12 bytes |

### Primitive arrays

| Java type | Wire size |
|---|---|
| `int[]` | 4 + n × 4 bytes |
| `long[]` | 4 + n × 8 bytes |
| `double[]` | 4 + n × 8 bytes |
| `float[]` | 4 + n × 4 bytes |
| `short[]` | 4 + n × 2 bytes |
| `char[]` | 4 + n × 2 bytes |
| `boolean[]` | 4 + n × 1 bytes |

---

## Wire format reference

All encoding is **big-endian**. There are no type tags, no length envelopes beyond those documented here, and no padding.

### Primitives and scalars

```
byte      → 1B  raw
short     → 2B  big-endian signed
int       → 4B  big-endian signed
long      → 8B  big-endian signed
float     → 4B  IEEE 754 big-endian
double    → 8B  IEEE 754 big-endian
boolean   → 1B  (0x01 = true, 0x00 = false)
char      → 2B  big-endian unsigned (UTF-16 code unit)
```

### String

```
[  len(2B uint16)  ][  UTF-8 bytes (len bytes)  ]
```

Empty or `null` strings encode as `len=0` with no following bytes, and decode as `""`.
Maximum serializable string is 65,535 UTF-8 bytes (not characters — a surrogate pair takes 4 bytes).

### byte[]

```
[  len(4B uint32)  ][  raw bytes (len bytes)  ]
```

`null` and empty arrays both encode as `len=0` and decode as `new byte[0]`.

### UUID

```
[  most-significant 8B  ][  least-significant 8B  ]   = 16 bytes total
```

### Instant

```
[  epochSecond(8B)  ][  nanos(4B int)  ]   = 12 bytes
```

### BigInteger

```
[  len(4B int)  ][  two's-complement bytes (len bytes)  ]
```

`null` encodes as `len=0` and decodes as `BigInteger.ZERO`.
Note: `BigInteger.ZERO.toByteArray()` returns `[0x00]` (length 1), so ZERO itself serializes as 5 bytes (`len=1, data=[0x00]`). Only `len=0` is the null sentinel.

### BigDecimal

```
[  BigInteger (unscaledValue — variable)  ][  scale(4B int)  ]
```

`null` encodes as `BigInteger(null) + scale=0` and decodes as `BigDecimal.ZERO`.

### LocalDate

```
[  year(4B int)  ][  month(1B)  ][  day(1B)  ]   = 6 bytes
```

Supports the full range: `LocalDate.MIN` (year -999,999,999) to `LocalDate.MAX` (year +999,999,999).

### LocalTime

```
[  nanoOfDay(8B long)  ]   = 8 bytes
```

### LocalDateTime

```
[  year(4B int)  ][  month(1B)  ][  day(1B)  ][  nanoOfDay(8B long)  ]   = 14 bytes
```

### Duration

```
[  seconds(8B long)  ][  nanos(4B int)  ]   = 12 bytes
```

Nanos are always in `[0, 999_999_999]`; the sign of the duration is carried by seconds.
Negative durations round toward negative infinity (e.g. `-1.5s` → `seconds=-2, nanos=500_000_000`).

### OffsetDateTime

```
[  epochSecond(8B)  ][  nanos(4B)  ][  totalOffsetSeconds(4B int)  ]   = 16 bytes
```

Both the point-in-time and the UTC offset are preserved, so local time components round-trip exactly.

### ZonedDateTime

```
[  epochSecond(8B)  ][  nanos(4B)  ][  zoneId(String — variable)  ]
```

The zone ID string is the IANA region name (e.g. `"America/New_York"`) or an offset string (e.g. `"+05:30"`).
Both point-in-time and zone identity round-trip exactly.

### Period

```
[  years(4B int)  ][  months(4B int)  ][  days(4B int)  ]   = 12 bytes
```

### Primitive arrays

```
[  count(4B int)  ][  element_0  ][  element_1  ] …
```

Each element is encoded at its native width (same as the scalar above).
`null` arrays encode as `count=0` and decode as a zero-length array.

---

## Container codecs

Container codecs are not registered in `standard()` — they are parameterized and must be instantiated manually.

### `ListCodec<T>`

```java
Codec<List<String>> codec = new ListCodec<>(StringCodec.INSTANCE);

// Nested generics are composed the same way:
Codec<List<List<Integer>>> nested =
    new ListCodec<>(new ListCodec<>(PrimitiveCodecs.INT));
```

Wire: `count(4B int)` + elements. `null` and empty lists encode as `count=0` and decode as an empty `ArrayList`.

### `SetCodec<E>`

```java
Codec<Set<UUID>> codec = new SetCodec<>(UuidCodec.INSTANCE);
```

Wire format is identical to `ListCodec`. Deserializes as a `LinkedHashSet` (insertion order is preserved).

### `MapCodec<K, V>`

```java
Codec<Map<String, Integer>> codec =
    new MapCodec<>(StringCodec.INSTANCE, PrimitiveCodecs.INT);
```

Wire: `count(4B int)` + key₀ + value₀ + key₁ + value₁ + …
`null` and empty maps encode as `count=0` and decode as an empty `HashMap`.
Iteration order of the source map determines wire order. Deserializes as a `HashMap`.

### `OptionalCodec<T>`

```java
Codec<Optional<LocalDate>> codec =
    new OptionalCodec<>(LocalDateCodec.INSTANCE);
```

Wire: `present(1B)` (0 = absent, 1 = present). If present, the value follows.
`null` and `Optional.empty()` both encode as `present=0` and decode as `Optional.empty()`.

---

## Writing a hand-crafted codec with CompositeCodec

`CompositeCodec` is the recommended approach for production types. Wire order matches the order you declare fields — this is your binary contract and must not change once deployed.

```java
// Example type with a builder
public final class Product {
    private final String id;
    private final String name;
    private final double price;
    private final int    stock;

    // ... constructor, getters, builder ...
}

// Build the codec once (typically at startup) and keep it.
Codec<Product> productCodec = CompositeCodec.of(Product.class)
    .field(StringCodec.INSTANCE,   Product::getId,    Product.Builder::id)
    .field(StringCodec.INSTANCE,   Product::getName,  Product.Builder::name)
    .field(PrimitiveCodecs.DOUBLE, Product::getPrice, Product.Builder::price)
    .field(PrimitiveCodecs.INT,    Product::getStock, Product.Builder::stock)
    .build(Product.Builder::new, Product.Builder::build);

// Register and use.
Codecs codecs = Codecs.standard();
codecs.register(Product.class, productCodec);

byte[]  encoded = codecs.encode(Product.class, product);
Product decoded = codecs.decode(Product.class, encoded);
```

### `.field(codec, getter, setter)` signature

```java
<B, F> Accumulator<T> field(
    Codec<F>              codec,    // codec for this field's type
    Function<T, F>        getter,   // extract the field value from T
    BiFunction<B, F, B>   setter    // apply the field value to the builder B, return B
)
```

The setter is a `BiFunction<Builder, FieldValue, Builder>` — a chainable accumulator pattern.
If your builder uses void setters instead of fluent returns, adapt it:

```java
.field(StringCodec.INSTANCE, Product::getId, (b, v) -> { b.id(v); return b; })
```

### Composing with container codecs

```java
// A type that contains a List<String> tags field:
CompositeCodec.of(Article.class)
    .field(StringCodec.INSTANCE,                      Article::getTitle, Article.Builder::title)
    .field(new ListCodec<>(StringCodec.INSTANCE),     Article::getTags,  Article.Builder::tags)
    .field(InstantCodec.INSTANCE,                     Article::getPublishedAt, Article.Builder::publishedAt)
    .build(Article.Builder::new, Article.Builder::build);
```

---

## Auto-generated codecs with ReflectiveCodec

`ReflectiveCodec` inspects a class at runtime and generates a codec automatically.
**Use this for prototyping and internal tooling only.** Production binary protocols should use `CompositeCodec` so the wire format is explicit and stable.

```java
Codecs codecs = Codecs.standard();

// Generate and cache a codec for Point. Subsequent calls return the cached instance.
codecs.reflect(Point.class);

byte[] bytes = codecs.encode(Point.class, new Point(3, 7));
Point  p     = codecs.decode(Point.class, bytes);
```

### Supported types

| Kind | Requirements |
|---|---|
| **Records** | Any record. Uses canonical constructor and component declaration order. No no-arg constructor needed. |
| **Regular classes** | Must have a reachable no-arg constructor. All non-static, non-transient, non-final fields are encoded in alphabetical order, starting from the topmost ancestor down. |

### Wire ordering contract

- **Records:** fields are encoded in component declaration order.
- **Regular classes:** fields are encoded alphabetically within each class-hierarchy level (parent fields before child fields).

Any rename, addition, or removal of a field silently changes the binary layout and corrupts previously encoded data. This is why `CompositeCodec` is preferred for production — it makes the wire contract explicit in code.

### Supported field types

`ReflectiveCodec` recursively resolves codecs for field types using the registry. The following field types are supported:

- All primitive scalars (`int`, `long`, `double`, `boolean`, `char`, etc.)
- All boxed primitives and scalars registered in `standard()`: `String`, `UUID`, `Instant`, all `java.time` types, `BigInteger`, `BigDecimal`
- `byte[]` and all primitive array types (`int[]`, `long[]`, `double[]`, etc.)
- `List<T>`, `Set<T>`, `Map<K,V>`, `Optional<T>` where the type argument is itself a supported type
- Any other class — `ReflectiveCodec` is generated for it recursively and cached in the registry

### Circular references

`ReflectiveCodec` registers itself in the registry before resolving its own fields. If a field type references the same class being constructed, the registry returns the partially-built codec rather than recursing infinitely. This handles direct and transitive circular references.

### Nested auto-registration

When `reflect(Outer.class)` is called, any inner class types encountered during field resolution are also auto-registered as a side effect. Subsequent calls to `reflect(Inner.class)` return the cached instance.

```java
codecs.reflect(Order.class);

// Order contains a field of type Address — Address is now also cached.
Codec<Address> addrCodec = codecs.lookup(Address.class); // returns the cached codec
```

### Caveats

```java
// Wire format is fragile — this rename silently breaks all existing encoded bytes:
record Point(int x, int y) {}    // encoded as [x(4B)][y(4B)]
record Point(int x, int z) {}    // now encodes  [x(4B)][z(4B)] — silently wrong on decode
```

- Requires `--add-opens` under strong module encapsulation (JPMS) for non-exported packages.
- Do not call `reflect()` from multiple threads simultaneously for the same type for the first time. Call it once at startup before concurrent access begins.

---

## Enum codecs

`EnumCodec` encodes an enum constant as its ordinal in a single byte.

```java
enum Status { PENDING, ACTIVE, CLOSED }

Codec<Status> statusCodec = new EnumCodec<>(Status.class);
```

Wire: 1 byte (unsigned ordinal). Maximum 256 constants per enum. Construction throws `IllegalArgumentException` for enums with more than 256 constants.

For use with `ReflectiveCodec`, enum fields are resolved automatically — no manual registration needed.
For use with `CompositeCodec`, pass `new EnumCodec<>(Status.class)` as the field codec.

---

## Implementing Codec directly

For types that don't fit the fluent builder pattern, implement `Codec<T>` directly.

```java
public final class PointCodec implements Codec<Point> {

    public static final PointCodec INSTANCE = new PointCodec();

    private PointCodec() {}

    @Override
    public void serialize(Point p, ByteBuffer buf) {
        buf.putInt(p.x());
        buf.putInt(p.y());
    }

    @Override
    public int sizeOf(Point p) {
        return 8;
    }

    @Override
    public Point deserialize(ByteBuffer buf) {
        return new Point(buf.getInt(), buf.getInt());
    }
}
```

Guidelines:
- Use `INSTANCE` singleton pattern for stateless codecs.
- `sizeOf` must match `serialize` exactly — the buffer allocated in `Codecs.encode` is sized by `sizeOf`.
- `serialize` must not call `buf.flip()` or reset the buffer — it only writes and advances position.
- `deserialize` must consume exactly as many bytes as `sizeOf` would return for the encoded value.

---

## FrameBuffer — NIO frame construction

`FrameBuffer` is a thin wrapper around a pre-allocated direct `ByteBuffer`. It is designed for
frame construction in the NIO write path: allocate once per writer thread, reuse across frames.

```java
// Allocate once (e.g. 64KB per writer thread).
FrameBuffer fb = FrameBuffer.allocateDirect(65_536);

// Build a frame.
ByteBuffer buf = fb.buffer();
codecs.serializeInt(frameType, buf);
codecs.serializeString(key, buf);
codecs.serializeBytes(value, buf);

// Snapshot without disturbing the write position.
byte[] frame = fb.toBytes();

// Reset for the next frame.
fb.reset();
```

| Method | Description |
|---|---|
| `FrameBuffer.allocateDirect(int n)` | Creates a new direct `ByteBuffer` of `n` bytes |
| `buffer()` | Returns the underlying `ByteBuffer` for writing |
| `toBytes()` | Snapshot of bytes written so far; does not alter the buffer position |
| `reset()` | Clears the buffer (position → 0, limit → capacity) |

Never allocate a `FrameBuffer` per frame. Allocate once per thread and reuse.

---

## CodecRegistry — managing custom types

`CodecRegistry` backs `Codecs` and is exposed indirectly through `Codecs.register`, `Codecs.lookup`, and `Codecs.reflect`.

```java
Codecs codecs = Codecs.standard();

// Register a codec.
codecs.register(Point.class, PointCodec.INSTANCE);

// Look up a codec. Throws IllegalArgumentException if not found.
Codec<Point> c = codecs.lookup(Point.class);

// Generate and cache via reflection.
codecs.reflect(Product.class);
```

`CodecRegistry` is thread-safe (backed by `ConcurrentHashMap`). The lookup used internally by `encode`/`decode`/`serialize`/`deserialize` is a single `ConcurrentHashMap.get` — no locking on the read path.

---

## Null handling contracts

Null behavior differs by codec family:

| Family | `serialize(null)` | `decode` of null-sentinel |
|---|---|---|
| Primitive scalars (`byte`, `int`, …) | `NullPointerException` (auto-unboxing) | n/a |
| `String` | encodes as length=0; decodes as `""` | `""` |
| `byte[]` | encodes as length=0; decodes as `new byte[0]` | `new byte[0]` |
| Primitive arrays (`int[]`, etc.) | encodes as count=0; decodes as zero-length array | zero-length array |
| `BigInteger` | encodes as length=0 sentinel; decodes as `BigInteger.ZERO` | `BigInteger.ZERO` |
| `BigDecimal` | encodes as zero sentinel; decodes as `BigDecimal.ZERO` | `BigDecimal.ZERO` |
| `List` | encodes as count=0; decodes as empty `ArrayList` | empty `ArrayList` |
| `Set` | encodes as count=0; decodes as empty `LinkedHashSet` | empty `LinkedHashSet` |
| `Map` | encodes as count=0; decodes as empty `HashMap` | empty `HashMap` |
| `Optional` | treats null same as `Optional.empty()`; decodes as `Optional.empty()` | `Optional.empty()` |
| Fixed-size value types (`UUID`, `Instant`, all `java.time.*`) | `NullPointerException` | n/a |

If you need to distinguish `null` from zero/empty for a type in the nullable column, wrap it with `OptionalCodec`:

```java
Codec<Optional<BigDecimal>> nullable =
    new OptionalCodec<>(BigDecimalCodec.INSTANCE);
```

---

## Thread safety

| Component | Thread safety |
|---|---|
| `Codecs` | Safe for concurrent reads after registration is complete. Do not call `register()` or `reflect()` concurrently for the same type. |
| `CodecRegistry` | Read operations (`lookup`) are lock-free. Write operations (`register`, `deregister`) use `ConcurrentHashMap` semantics. |
| `StringCodec` | Uses a `ThreadLocal<CharsetEncoder>` — each thread gets its own encoder; no contention. |
| All built-in `Codec` instances | Stateless singletons; safe for concurrent use from any number of threads. |
| `FrameBuffer` | Not thread-safe. Allocate one instance per writer thread. |
| `ReflectiveCodec` construction | Not safe to call `reflect()` for the same type concurrently from multiple threads for the first time. Safe to call after the codec is cached. |

The recommended pattern: call `Codecs.standard()`, register all custom codecs, call `reflect()` for any reflective types, then share the resulting `Codecs` instance as a read-only singleton.

---

## Unsupported types

Some types are explicitly rejected with a clear `IllegalArgumentException` at codec-construction time. Attempting to use them with `ReflectiveCodec` produces an error message that points you to the correct alternative.

### Object arrays (`String[]`, `Integer[]`, `MyPojo[]`)

Object arrays are not supported. Use `List<T>` instead.

**Why:** Java arrays are unsoundly covariant — `String[]` is assignable to `Object[]`, which allows inserting a non-`String` element at runtime and produces an `ArrayStoreException` the compiler cannot catch. `List<T>` carries the same information without that pitfall, and is fully supported via `ListCodec`.

```java
// ❌ Not supported:
record Batch(String[] ids) {}

// ✓ Correct:
record Batch(List<String> ids) {}
```

### Atomic and accumulator types (`AtomicInteger`, `LongAdder`, etc.)

Atomic and accumulator types are not supported. Replace with the plain primitive equivalent.

**Why:** These are concurrency primitives, not data containers. Their only serializable value is the snapshot of the wrapped primitive — the atomicity guarantee is meaningless once the value crosses a wire or JVM boundary. A class that holds an `AtomicInteger` field has a design issue at the serialization layer.

```java
// ❌ Not supported:
record Counter(AtomicInteger count) {}

// ✓ Correct:
record Counter(int count) {}
```

### Raw `Collection<T>` and `Iterable<T>`

These interfaces are not supported as field types in `ReflectiveCodec` because they are ambiguous at deserialization time.

```java
// ❌ Not supported — which concrete collection should deserialize produce?
record Bag(Collection<String> items) {}

// ✓ Correct — declare the concrete interface:
record Bag(List<String> items) {}    // or Set<String>
```

### Wildcard-parameterized types (`List<? extends Number>`, `Map<String, ? super T>`)

Wildcard bounds are not supported. Use a concrete type argument.

```java
// ❌ Not supported:
record Range(List<? extends Number> values) {}

// ✓ Correct:
record Range(List<Double> values) {}
```

### Unbound type variables

A class with an unresolved type parameter cannot be reflectively encoded because the field type is not known at runtime.

```java
// ❌ Not supported:
record Box<T>(T value) {}   // T is unknown at codec construction time

// ✓ Correct — use a concrete parameterization:
record Box(String value) {}
// or register a hand-crafted codec for each concrete Box<T> you need
```

---

## Package structure

```
com.simplj.flair.cache.serial
│
├── Codec<T>               Interface — serialize + sizeOf + deserialize
├── Serializer<T>          Interface — serialize + sizeOf
├── Deserializer<T>        Interface — deserialize
├── Codecs                 Entry point — three-tier API + codec registry wrapper
├── CodecRegistry          ConcurrentHashMap<Class<?>, Codec<?>>
└── FrameBuffer            Pre-allocated direct ByteBuffer for NIO frame construction

com.simplj.flair.cache.serial.codecs
│
├── PrimitiveCodecs        BYTE, SHORT, INT, LONG, FLOAT, DOUBLE, BOOLEAN, CHAR
├── PrimitiveArrayCodecs   INT, LONG, DOUBLE, FLOAT, SHORT, CHAR, BOOLEAN
├── StringCodec            ThreadLocal encoder; uint16 length prefix; UTF-8
├── ByteArrayCodec         uint32 length prefix
├── UuidCodec              16 bytes (MSB 8B + LSB 8B)
├── InstantCodec           epochSecond(8) + nanos(4)
├── BigIntegerCodec        length(4) + two's-complement bytes
├── BigDecimalCodec        BigInteger (unscaled) + scale(4)
├── LocalDateCodec         year(4) + month(1) + day(1)
├── LocalTimeCodec         nanoOfDay(8)
├── LocalDateTimeCodec     year(4) + month(1) + day(1) + nanoOfDay(8)
├── DurationCodec          seconds(8) + nanos(4)
├── OffsetDateTimeCodec    epochSecond(8) + nanos(4) + totalOffsetSeconds(4)
├── ZonedDateTimeCodec     epochSecond(8) + nanos(4) + zoneId(String)
├── PeriodCodec            years(4) + months(4) + days(4)
├── ListCodec<T>           count(4) + elements
├── SetCodec<E>            count(4) + elements → LinkedHashSet
├── MapCodec<K,V>          count(4) + key+value pairs → HashMap
├── OptionalCodec<T>       present(1) + value?
├── EnumCodec<E>           ordinal(1) — max 256 constants
├── CompositeCodec<T,B>    Fluent builder for hand-crafted POJO codecs
└── ReflectiveCodec<T>     Reflection-generated codec — prototyping only
```

---

*Part of [FlairCache](https://github.com/simplj/flair-cache) — Fast Local Access with In-memory Replication.*
*Apache License 2.0.*
