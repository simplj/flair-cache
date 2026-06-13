# flair-cache-transport

**Non-blocking NIO TCP server and client — zero dependencies, raw `byte[]` only.**

Part of the [FLAIR Cache](../README.md) project, but fully standalone. You can embed this module in any Java project that needs a high-performance TCP transport layer built entirely on JDK NIO — no Netty, no external libraries, nothing.

---

## What it does

- Persistent, non-blocking TCP connections via `ServerSocketChannel` / `SocketChannel` + `Selector`
- Single selector thread multiplexes all connections — server accepts + client channels share one event loop
- Per-connection write queues with batched flushing (64 frames or 2 ms, whichever comes first)
- Frame-boundary reassembly across TCP segment splits
- TLS 1.3 with optional mutual authentication (`javax.net.ssl` only)
- Three backpressure policies: block, drop-oldest, drop-newest

**What it does not do:**
Transport passes raw `byte[]` only. It knows nothing about frame semantics, serialization formats, or application protocols. Encoding and decoding frame payloads is the caller's responsibility.

---

## Maven

```xml
<dependency>
    <groupId>com.simplj.flair</groupId>
    <artifactId>flair-cache-transport</artifactId>
    <version>${flair.version}</version>
</dependency>
```

Zero transitive dependencies. JDK 11+ only.

---

## Quick start

### Server

```java
TcpServer server = TcpServer.builder()
    .port(7890)
    .handler((conn, frame) -> {
        // frame.type()    — application-defined byte code
        // frame.payload() — raw byte[]
        System.out.println("received " + frame.payload().length + " bytes from " + conn.id());
    })
    .build();

server.start();
```

### Client

```java
TcpClient client = TcpClient.builder()
    .remoteAddress("10.0.0.1")
    .remotePort(7890)
    .handler((conn, frame) -> { /* handle inbound frames */ })
    .build();

Connection conn = client.connect();        // blocks until connected
conn.send(new RawFrame((byte) 0x01, data));
```

### Server push (reply from server to client)

```java
TcpServer server = TcpServer.builder()
    .port(7890)
    .handler((conn, frame) -> {
        // echo back
        conn.send(new RawFrame(frame.type(), frame.payload()));
    })
    .build();
server.start();
```

---

## Wire format

Every frame on the wire is prefixed with an 8-byte big-endian header:

```
┌─ MAGIC(2) ─┬─ VER(1) ─┬─ TYPE(1) ───┬─ LEN(4) ─┬─ PAYLOAD(LEN bytes) ──┐
│  0xCA 0xFE │  0x01    │ app-defined │ uint32   │ raw bytes             │
└────────────┴──────────┴─────────────┴──────────┴───────────────────────┘
```

| Field | Size | Description |
|---|---|---|
| MAGIC | 2 bytes | `0xCA 0xFE` — protocol identifier |
| VER | 1 byte | Protocol version (`0x01`) |
| TYPE | 1 byte | Application-defined frame type code |
| LEN | 4 bytes | Unsigned 32-bit payload length (big-endian) |
| PAYLOAD | LEN bytes | Raw bytes — transport is agnostic to content |

**Limits:** default max payload 1 MB; configurable up to 64 MB via `maxPayloadBytes()`.

`RawFrame` encodes and the `FrameAssembler` decodes this format. Frames that arrive split across multiple TCP segments are automatically reassembled before delivery to the `FrameHandler`.

---

## API reference

### `TcpServer`

```java
TcpServer server = TcpServer.builder()
    .bindAddress("0.0.0.0")       // default
    .port(7890)                    // 0 = OS-assigned (use localPort() after build)
    .handler(frameHandler)         // required — called for every inbound frame
    .selectorThreads(1)            // NIO selector thread count (default 1 — see below)
    .workerThreads(4)              // frame dispatch pool per selector thread (default 4)
    .readBufferBytes(65536)        // per-connection read buffer (default 64 KB)
    .queueCapacity(4096)           // per-connection write queue depth (default 4096)
    .backpressurePolicy(BackpressurePolicy.DROP_OLDEST) // default
    .maxPayloadBytes(1 << 20)      // max inbound payload (default 1 MB)
    .tls(TlsConfig.disabled())     // default — see TLS section
    .build();                      // throws IOException if bind fails

server.start();                    // starts the selector thread(s)
server.localPort();                // actual bound port (useful when port=0)
server.eventLoop();                // NioEventLoop — round-robins across the pool
server.onConnect(conn -> { });     // called when a new connection is accepted
server.shutdown();                 // closes all connections and stops all selectors
```

#### `selectorThreads`

Controls how many NIO selector threads share the connection load. The default of `1` is correct for most use cases — a single `select()` call batches all ready channels and is often faster than multiple threads due to better CPU cache locality and no cross-thread contention.

**When to increase:** when managing a large number of simultaneous persistent connections (e.g. 20+ peers in `flair-cache-replication`) and profiling shows the selector thread as a CPU bottleneck. Rule of thumb: **1 selector per ~30 connections, capped at CPU core count**. Total I/O thread count = `selectorThreads × workerThreads`.

**When to keep at 1:** fewer than ~20 connections, or any scenario where write throughput does not saturate the selector. Do not increase `selectorThreads` to fix slow `FrameHandler` callbacks — those run on worker threads; increase `workerThreads` instead.

### `TcpClient`

```java
TcpClient client = TcpClient.builder()
    .remoteAddress("10.0.0.1")     // required
    .remotePort(7890)              // required
    .handler(frameHandler)         // required unless sharing an event loop
    .connectTimeoutMs(3000)        // default 3 s
    .workerThreads(2)              // only used when no eventLoop() is set (default 2)
    .readBufferBytes(65536)        // default 64 KB
    .queueCapacity(4096)           // default 4096
    .backpressurePolicy(BackpressurePolicy.DROP_OLDEST) // default
    .maxPayloadBytes(1 << 20)      // default 1 MB
    .tls(TlsConfig.disabled())     // default
    .eventLoop(server.eventLoop()) // optional — share server's selector thread
    .build();

Connection conn = client.connect(); // blocks until connected (or timeout)
client.shutdown();                  // shuts down owned event loop (no-op if shared)
```

### `Connection`

Returned by `TcpClient.connect()` and passed to the `FrameHandler` on every inbound frame.

```java
conn.id()             // UUID — unique per connection instance
conn.remoteAddress()  // InetAddress of the peer
conn.send(frame)      // enqueue a frame for async delivery; no-op if not alive
conn.isAlive()        // false after close() or a write failure
conn.close()          // initiates an orderly close
```

`send()` is non-blocking. It enqueues the frame into the per-connection write queue and returns immediately. The write is flushed by a dedicated writer thread.

### `RawFrame`

```java
new RawFrame(byte type, byte[] payload)

frame.type()           // application-defined type byte
frame.payload()        // raw payload bytes
frame.encodedLength()  // header (8) + payload.length
frame.encodeTo(buf)    // write the full framed representation into a ByteBuffer
```

### `FrameHandler`

```java
@FunctionalInterface
public interface FrameHandler {
    void onFrame(Connection source, RawFrame frame);
}
```

Called on a worker thread from the transport's internal pool. Not called on the selector thread — it is safe to do blocking work, but keep handlers short to avoid stalling the worker pool.

---

## Sharing one event loop across server and clients

All TCP channels — incoming (accepted by the server) and outgoing (opened by clients) — can share a single `NioEventLoop` and one selector thread. This is the pattern used by `flair-cache-replication` where each node runs one server and N peer clients.

```java
TcpServer server = TcpServer.builder()
    .port(7890)
    .handler(inboundHandler)
    .build();
server.start();

// All clients multiplex through the server's selector
TcpClient peerA = TcpClient.builder()
    .remoteAddress("10.0.0.2").remotePort(7890)
    .eventLoop(server.eventLoop())  // no handler needed — handler comes from the shared loop
    .build();

TcpClient peerB = TcpClient.builder()
    .remoteAddress("10.0.0.3").remotePort(7890)
    .eventLoop(server.eventLoop())
    .build();

Connection connA = peerA.connect();
Connection connB = peerB.connect();
```

When a client shares an event loop, `client.shutdown()` is a no-op — only the server owns and controls the loop's lifecycle.

---

## Backpressure

The write queue between `Connection.send()` and the TCP socket has a bounded capacity. When the queue is full, the configured `BackpressurePolicy` determines what happens:

| Policy | `send()` behavior when queue is full |
|---|---|
| `DROP_OLDEST` | Evicts the oldest queued frame to make room, then enqueues the new frame. Never blocks. (default) |
| `DROP_NEWEST` | Silently drops the new frame. Never blocks. |
| `BLOCK` | Blocks the calling thread until space is available. Use when zero frame loss is required. |

```java
TcpClient.builder()
    .queueCapacity(8192)
    .backpressurePolicy(BackpressurePolicy.BLOCK)
    ...
```

---

## TLS

TLS 1.3 is supported via `javax.net.ssl`. No Bouncy Castle, no external crypto library.

### Server TLS (one-way)

```java
SSLContext ctx = TlsContextFactory.serverContext(
    getClass().getResourceAsStream("/server.jks"), "keystorePassword".toCharArray(),
    getClass().getResourceAsStream("/truststore.jks"), "truststorePassword".toCharArray()
);

TcpServer server = TcpServer.builder()
    .port(7890)
    .tls(TlsConfig.of(ctx))
    .handler(handler)
    .build();
```

### Server TLS with mutual authentication (mTLS)

```java
TcpServer server = TcpServer.builder()
    .tls(TlsConfig.withMutualAuth(ctx))  // requires client certificate
    ...
```

### Client TLS

```java
SSLContext clientCtx = TlsContextFactory.clientContext(
    getClass().getResourceAsStream("/truststore.jks"), "truststorePassword".toCharArray()
);

TcpClient client = TcpClient.builder()
    .tls(TlsConfig.of(clientCtx))
    ...
```

### Client TLS with certificate (mTLS)

```java
SSLContext clientCtx = TlsContextFactory.clientContextWithCert(
    getClass().getResourceAsStream("/client.jks"), "clientKeystorePassword".toCharArray(),
    getClass().getResourceAsStream("/truststore.jks"), "truststorePassword".toCharArray()
);

TcpClient client = TcpClient.builder()
    .tls(TlsConfig.of(clientCtx))
    ...
```

`TlsConfig.disabled()` is the default for both server and client.

The TLS handshake is driven entirely on the selector thread. Delegated tasks (certificate validation) are off-loaded to the worker pool. `TcpClient.connect()` blocks until the full handshake completes — the returned `Connection` is always ready to send application data.

---

## Thread model

| Thread name | Count | Role |
|---|---|---|
| `flaircache-nio-selector` | 1 per event loop | Select loop — accept, read, TLS handshake |
| `flaircache-writer-{uuid}` | 1 per connection | Drain write queue, batch-flush to socket |
| `flaircache-transport-worker-{n}` | configurable | Dispatch received frames to `FrameHandler` |

All threads are daemon threads created by `FlairCacheThreadFactory`. `new Thread(...)` is never used directly.

The selector thread never calls `FrameHandler` directly. It hands off to a worker thread immediately after frame assembly completes, so a slow handler cannot stall the selector.

---

## Performance notes

- Write buffer: 512 KB `ByteBuffer.allocateDirect()` pre-allocated per writer thread — never allocated per frame or per batch.
- Batch flush: up to 64 frames are coalesced into one `channel.write()` call, or flushed after 2 ms, whichever comes first.
- Read buffer: `ByteBuffer.allocateDirect()` per connection, sized via `readBufferBytes()`.
- No boxing, no allocation in the send/receive hot path after connection setup.
- TLS wrap buffers are pooled across handshakes (`ByteBufferPool`).

---

## Internal components

These types are package-private implementation details. They are not part of the public API.

| Type | Role |
|---|---|
| `NioEventLoop` | Single-threaded `Selector` loop — accepts, reads, drives TLS handshake |
| `ConnectionImpl` | Holds per-connection state: read buffer, `FrameAssembler`, `WriteQueue`, optional `SSLEngine` |
| `WriteQueue` | Bounded queue + dedicated writer thread, batches frames before flushing |
| `FrameAssembler` | Stateful reassembler — accumulates bytes across reads, emits complete `RawFrame`s |
| `ByteBufferPool` | Pool of pre-allocated direct `ByteBuffer`s for TLS wrap operations |
| `FlairCacheThreadFactory` | Creates named daemon threads |

---

## Design constraints

- **No frame encoding in transport.** `RawFrame` carries a `type` byte and a raw `byte[]` payload. The meaning of those bytes is defined by the caller. In `flair-cache-replication`, `FrameEncoder` and `FrameDecoder` translate between `RawFrame` and cache protocol messages — but that logic lives in `replication`, not here.
- **No dependency on other FLAIR modules.** This module is usable in any Java project, unrelated to caching.
- **No external dependencies.** `ServerSocketChannel`, `SocketChannel`, `Selector`, `SSLEngine` — pure JDK.

---

## License

Apache License, Version 2.0. See [LICENSE](../LICENSE).
