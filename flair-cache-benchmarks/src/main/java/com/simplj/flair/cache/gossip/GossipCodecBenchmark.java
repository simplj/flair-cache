package com.simplj.flair.cache.gossip;

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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Benchmarks gossip UDP message encode/decode via {@link GossipProtocol}.
 *
 * <p>This class is intentionally placed in {@code com.simplj.flair.cache.gossip}
 * to access the package-private {@link GossipProtocol} and {@link GossipMessage} types.
 * At runtime, all classes from all modules are loaded by the same classloader in the fat
 * benchmarks JAR, so package-private access resolves correctly.</p>
 *
 * <p>The measured message is a realistic PING with 3 piggybacked {@link NodeInfo} deltas —
 * a common gossip tick payload for a small cluster. The encoded form stays well under the
 * 1400-byte MTU limit ({@link GossipProtocol#MAX_PACKET}).</p>
 */
@State(Scope.Thread)
@BenchmarkMode({Mode.SampleTime, Mode.Throughput})
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 2, timeUnit = TimeUnit.SECONDS)
@Fork(1)
public class GossipCodecBenchmark {

    private GossipMessage msg;
    private byte[]        encoded;

    @Setup(Level.Trial)
    public void setup() throws UnknownHostException {
        UUID senderId = UUID.randomUUID();
        InetAddress addr = InetAddress.getByAddress(new byte[]{127, 0, 0, 1});

        List<NodeInfo> piggybacked = List.of(
                new NodeInfo(UUID.randomUUID(), addr, 18001, NodeStatus.ALIVE,     1L, 0L),
                new NodeInfo(UUID.randomUUID(), addr, 18002, NodeStatus.ALIVE,     1L, 0L),
                new NodeInfo(UUID.randomUUID(), addr, 18003, NodeStatus.SUSPECTED, 1L, 0L)
        );

        msg     = GossipMessage.ping(senderId, 42L, piggybacked);
        encoded = GossipProtocol.encode(msg);
    }

    /**
     * Encodes a PING message with 3 piggybacked node deltas.
     * Measures: UUID serialization × 4 + NodeInfo serialization × 3 + ByteBuffer allocation.
     */
    @Benchmark
    public byte[] encode() {
        return GossipProtocol.encode(msg);
    }

    /**
     * Decodes a pre-encoded PING message back to a {@link GossipMessage}.
     * Measures: header parse + UUID deserialization × 4 + InetAddress reconstruction × 3.
     */
    @Benchmark
    public GossipMessage decode(Blackhole bh) throws GossipProtocolException {
        GossipMessage result = GossipProtocol.decode(encoded, encoded.length);
        bh.consume(result);
        return result;
    }
}
