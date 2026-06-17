package com.simplj.flair.cache.bootstrap;

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

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Benchmarks the bootstrap wire-protocol codec: {@link ChunkEncoder} and {@link ChunkDecoder}.
 *
 * <p>Bootstrap sync is the most data-intensive operation in a FlairCache cluster: on node
 * join the donor streams the entire store as {@code SYNC_CHUNK} frames. The codec path
 * is traversed once per chunk on both the donor (encode) and joiner (decode) sides.
 * Chunk size and entry count directly drive CPU and allocation cost.</p>
 *
 * <p>This class lives in {@code com.simplj.flair.cache.bootstrap} to access the
 * package-private {@link ChunkEncoder} and {@link ChunkDecoder}.</p>
 *
 * <p>Benchmarks:</p>
 * <ul>
 *   <li>Encode a chunk of 100 entries (realistic single-chunk payload at 64KB budget)</li>
 *   <li>Decode a pre-encoded 100-entry chunk</li>
 *   <li>Partition 1k entries into 64KB chunks (donor pre-processing step)</li>
 *   <li>Partition 10k entries into 64KB chunks (large-store join scenario)</li>
 * </ul>
 *
 * <p>Entry data: 64-byte values, 8-byte keys, {@code "bench-block"} block name.
 * Each encoded entry is ~121 bytes (2+11 blockName + 2+8 key + 16 hlc + 8 expiry + 16 nodeId + 4+64 value).</p>
 */
@State(Scope.Thread)
@BenchmarkMode({Mode.SampleTime, Mode.Throughput})
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 2, timeUnit = TimeUnit.SECONDS)
@Fork(1)
public class BootstrapChunkCodecBenchmark {

    private static final int CHUNK_ENTRIES = 100;       // entries per encode/decode benchmark
    private static final int PARTITION_1K  = 1_000;
    private static final int PARTITION_10K = 10_000;
    private static final int CHUNK_BYTES   = 64 * 1024; // 64KB — default chunk size
    private static final int VALUE_SIZE    = 64;

    private List<ChunkEncoder.SnapshotEntry> entries100;
    private List<ChunkEncoder.SnapshotEntry> entries1k;
    private List<ChunkEncoder.SnapshotEntry> entries10k;

    private ByteBuffer encodeBuf;         // reused across encode invocations
    private byte[]     preEncodedPayload; // pre-encoded 100-entry chunk for decode benchmarks

    @Setup(Level.Trial)
    public void setup() {
        entries100  = buildEntries(CHUNK_ENTRIES);
        entries1k   = buildEntries(PARTITION_1K);
        entries10k  = buildEntries(PARTITION_10K);

        // Pre-allocate the encode buffer to the largest possible chunk
        List<List<ChunkEncoder.SnapshotEntry>> chunks100 =
                ChunkEncoder.partition(entries100, CHUNK_BYTES);
        int bufCap = chunks100.isEmpty()
                ? CHUNK_BYTES
                : ChunkEncoder.maxChunkBufferSize(chunks100, CHUNK_BYTES);
        encodeBuf = ByteBuffer.allocate(bufCap);

        // Pre-encode a 100-entry chunk for the decode benchmark
        preEncodedPayload = ChunkEncoder.encodeChunk(0, 1, entries100, encodeBuf).payload();
    }

    /**
     * Encodes 100 entries into one {@code SYNC_CHUNK} frame.
     * The reusable {@link ByteBuffer} is cleared by {@link ChunkEncoder#encodeChunk} on entry.
     * Measures: buffer writes + payload byte-array allocation + {@link RawFrame} construction.
     */
    @Benchmark
    public RawFrame encodeChunk100() {
        return ChunkEncoder.encodeChunk(0, 1, entries100, encodeBuf);
    }

    /**
     * Decodes a pre-encoded 100-entry {@code SYNC_CHUNK} payload.
     * Measures: header parse + 100 × (block/key/value extraction + {@link CacheEntry} construction).
     */
    @Benchmark
    public ChunkDecoder.DecodedChunk decodeChunk100(Blackhole bh) {
        ChunkDecoder.DecodedChunk result = ChunkDecoder.decodeChunk(preEncodedPayload);
        bh.consume(result);
        return result;
    }

    /**
     * Partitions 1k entries into 64KB chunks.
     * Donor calls this before streaming begins to determine chunk boundaries.
     * Measures: linear scan + encoded-size arithmetic × 1k.
     */
    @Benchmark
    public List<List<ChunkEncoder.SnapshotEntry>> partition1k() {
        return ChunkEncoder.partition(entries1k, CHUNK_BYTES);
    }

    /**
     * Partitions 10k entries into 64KB chunks.
     * Typical for a medium-sized store sync. Measures the same path as {@link #partition1k()}
     * at 10× the entry count.
     */
    @Benchmark
    public List<List<ChunkEncoder.SnapshotEntry>> partition10k() {
        return ChunkEncoder.partition(entries10k, CHUNK_BYTES);
    }

    // ── helpers ───────────────────────────────────────────────────────────────────

    private static List<ChunkEncoder.SnapshotEntry> buildEntries(int count) {
        List<ChunkEncoder.SnapshotEntry> list = new ArrayList<>(count);
        byte[] val    = new byte[VALUE_SIZE];
        UUID   nodeId = UUID.randomUUID();
        for (int i = 0; i < count; i++) {
            byte[]       key   = ("k" + i).getBytes(StandardCharsets.UTF_8);
            HLCTimestamp hlc   = new HLCTimestamp(1_000L + i, 0L);
            CacheEntry   entry = new CacheEntry(val, hlc, 0L, 0L, 0L, nodeId);
            list.add(new ChunkEncoder.SnapshotEntry("bench-block", key, entry));
        }
        return list;
    }
}
