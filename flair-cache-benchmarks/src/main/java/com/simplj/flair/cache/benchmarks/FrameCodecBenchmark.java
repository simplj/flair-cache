package com.simplj.flair.cache.benchmarks;

import com.simplj.flair.cache.transport.FrameAssembler;
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
import java.util.concurrent.TimeUnit;

/**
 * Benchmarks wire-protocol frame encoding and decoding for a 1KB payload.
 *
 * <p>Encoding: {@link RawFrame#encodeTo(ByteBuffer)} writes the 8-byte header
 * ({@code MAGIC(2) + VER(1) + TYPE(1) + LEN(4)}) followed by the 1KB payload into a
 * pre-allocated direct {@link ByteBuffer}. The buffer is cleared (O(1), no zero-fill)
 * before each invocation.</p>
 *
 * <p>Decoding: {@link FrameAssembler#feed(ByteBuffer, java.util.function.Consumer)} parses
 * the header and dispatches the payload to a consumer. The source {@link ByteBuffer} is
 * rewound before each invocation; the assembler is reused since it returns to AWAIT_HEADER
 * state after each complete frame.</p>
 *
 * <p>Benchmark target (from CLAUDE.md): frame encode/decode (1KB) &lt; 1µs p99.</p>
 */
@State(Scope.Thread)
@BenchmarkMode({Mode.SampleTime, Mode.Throughput})
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 2, timeUnit = TimeUnit.SECONDS)
@Fork(1)
public class FrameCodecBenchmark {

    private static final int  PAYLOAD_BYTES = 1024;
    private static final byte TYPE_PUT      = 0x01;

    // Encode state
    private RawFrame   frame;
    private ByteBuffer encodeBuf;

    // Decode state — encoded once; rewound before each invocation
    private ByteBuffer  encodedSrc;
    private FrameAssembler assembler;

    // Consumed to prevent dead-code elimination of the decoded frame
    private RawFrame lastDecoded;

    @Setup(Level.Trial)
    public void setup() {
        byte[] payload = new byte[PAYLOAD_BYTES];
        for (int i = 0; i < PAYLOAD_BYTES; i++) {
            payload[i] = (byte) (i & 0xFF);
        }
        frame = new RawFrame(TYPE_PUT, payload);

        // Pre-allocate a direct ByteBuffer for encoding — cleared, not re-allocated, per invocation
        encodeBuf = ByteBuffer.allocateDirect(frame.encodedLength());

        // Pre-encode into a heap ByteBuffer for the decode benchmark
        ByteBuffer tmp = ByteBuffer.allocate(frame.encodedLength());
        frame.encodeTo(tmp);
        tmp.flip();
        encodedSrc = tmp;

        assembler = new FrameAssembler();
    }

    /**
     * Encodes a 1KB-payload {@link RawFrame} (header + payload) into a pre-allocated
     * direct {@link ByteBuffer}. The buffer is cleared before each invocation.
     * Target: &lt; 1µs p99.
     */
    @Benchmark
    public ByteBuffer encode() {
        encodeBuf.clear();
        frame.encodeTo(encodeBuf);
        return encodeBuf;
    }

    /**
     * Decodes a pre-encoded 1KB frame via {@link FrameAssembler}.
     * The source buffer is rewound before each invocation; the assembler resets to
     * AWAIT_HEADER state after each complete frame, so it can be safely reused.
     * Target: &lt; 1µs p99.
     */
    @Benchmark
    public RawFrame decode(Blackhole bh) {
        encodedSrc.rewind();
        assembler.feed(encodedSrc, f -> lastDecoded = f);
        bh.consume(lastDecoded);
        return lastDecoded;
    }
}
