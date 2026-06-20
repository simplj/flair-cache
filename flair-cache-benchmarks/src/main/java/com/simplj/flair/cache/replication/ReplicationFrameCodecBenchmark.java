package com.simplj.flair.cache.replication;

import com.simplj.flair.cache.hlc.HLCTimestamp;
import com.simplj.flair.cache.store.CacheEntry;
import com.simplj.flair.cache.transport.RawFrame;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Benchmarks {@link FrameEncoder} and {@link FrameDecoder} for replication wire frames.
 *
 * <p>This is distinct from {@link com.simplj.flair.cache.benchmarks.FrameCodecBenchmark}
 * (transport layer): that benchmark measures the 8-byte header + raw payload framing via
 * {@link RawFrame#encodeTo}/{@link com.simplj.flair.cache.transport.FrameAssembler#feed}.
 * This benchmark measures the replication-level field encoding — HLC(16B) + block
 * name + key + value — which happens inside {@link ReplicationFanout} before handing
 * the resulting {@link RawFrame} to the transport layer.</p>
 *
 * <p>This class lives in {@code com.simplj.flair.cache.replication} to access the
 * package-private {@link FrameEncoder} and {@link FrameDecoder}.</p>
 *
 * <p>Frame types benchmarked:</p>
 * <ul>
 *   <li>PUT (1KB value): the most common replication frame</li>
 *   <li>DELETE: lightweight — no value, only key + HLC + nodeId</li>
 *   <li>ACK: smallest frame — just the 8-byte frameId</li>
 * </ul>
 */
@State(Scope.Thread)
@BenchmarkMode({Mode.SampleTime, Mode.Throughput})
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 2, timeUnit = TimeUnit.SECONDS)
@Fork(1)
public class ReplicationFrameCodecBenchmark {

    private static final long FRAME_ID   = 42L;
    private static final int  VALUE_SIZE = 1024; // 1KB value

    // Encode inputs
    private ReplicationEvent.PutEvent    putEvent;
    private ReplicationEvent.DeleteEvent deleteEvent;

    // Pre-encoded payloads for decode benchmarks
    private byte[] putPayload;
    private byte[] deletePayload;

    @Setup(Level.Trial)
    public void setup() {
        byte[]       key    = "benchmark-key-001".getBytes(StandardCharsets.UTF_8);
        byte[]       block  = "bench-block".getBytes(StandardCharsets.UTF_8);
        byte[]       val    = new byte[VALUE_SIZE];
        HLCTimestamp hlc    = new HLCTimestamp(System.currentTimeMillis(), 0L);
        UUID         nodeId = UUID.randomUUID();

        CacheEntry entry = new CacheEntry(val, hlc, 0L, 0L, 0L, nodeId);

        putEvent = ReplicationEvent.put(
                new String(block, StandardCharsets.UTF_8),
                key, entry, ConsistencyMode.EVENTUAL);

        deleteEvent = ReplicationEvent.delete(
                new String(block, StandardCharsets.UTF_8),
                key, hlc, nodeId, ConsistencyMode.EVENTUAL);

        // Pre-encode for decode benchmarks
        putPayload    = FrameEncoder.encodePut(FRAME_ID, false, putEvent).payload();
        deletePayload = FrameEncoder.encodeDelete(FRAME_ID, false, deleteEvent).payload();
    }

    // ── PUT ───────────────────────────────────────────────────────────────────────

    /**
     * Encodes a PUT replication frame (1KB value).
     * Payload layout: frameId(8) + needsAck(1) + hlc(16) + blockName + key + value(1024) + expiry(8) + nodeId(16).
     * Allocates one {@code byte[]} for the payload on each call.
     */
    @Benchmark
    public RawFrame encodePut() {
        return FrameEncoder.encodePut(FRAME_ID, false, putEvent);
    }

    /**
     * Decodes a pre-encoded PUT payload back to a {@link FrameDecoder.DecodedPut} record.
     * Measures: ByteBuffer wrap (zero-copy) + HLC decode + block/key/value extraction.
     */
    @Benchmark
    public FrameDecoder.DecodedPut decodePut(Blackhole bh) {
        FrameDecoder.DecodedPut result = FrameDecoder.decodePut(putPayload);
        bh.consume(result);
        return result;
    }

    // ── DELETE ────────────────────────────────────────────────────────────────────

    /**
     * Encodes a DELETE replication frame (no value — smaller and faster than PUT).
     * Payload: frameId(8) + needsAck(1) + hlc(16) + blockName + key + nodeId(16).
     */
    @Benchmark
    public RawFrame encodeDelete() {
        return FrameEncoder.encodeDelete(FRAME_ID, false, deleteEvent);
    }

    /** Decodes a pre-encoded DELETE payload. */
    @Benchmark
    public FrameDecoder.DecodedDelete decodeDelete(Blackhole bh) {
        FrameDecoder.DecodedDelete result = FrameDecoder.decodeDelete(deletePayload);
        bh.consume(result);
        return result;
    }

    // ── ACK ───────────────────────────────────────────────────────────────────────

    /**
     * Encodes an ACK frame (8-byte frameId only).
     * The simplest replication frame — one ByteBuffer allocation + one {@code putLong()}.
     */
    @Benchmark
    public RawFrame encodeAck() {
        return FrameEncoder.encodeAck(FRAME_ID);
    }
}
