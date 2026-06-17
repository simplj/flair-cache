package com.simplj.flair.cache.benchmarks;

import com.simplj.flair.cache.serial.Codec;
import com.simplj.flair.cache.serial.Codecs;
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

import java.time.LocalDateTime;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Benchmarks {@link Codecs} encode/decode for the types registered in {@link Codecs#standard()}.
 *
 * <p>Two code paths are measured:</p>
 * <ol>
 *   <li><b>Registry path</b>: {@code codecs.encode(Class, T)} → registry lookup + ByteBuffer
 *       allocation + codec serialize + {@code array()} return (no extra copy).</li>
 *   <li><b>Reflective codec</b>: {@link Codecs#reflect(Class)} applied to a simple record
 *       ({@link Order}), measuring reflection-based field access vs hand-rolled codecs.</li>
 * </ol>
 *
 * <p>Types covered: {@code String} (short 8B / long 256B), {@code UUID} (16B fixed),
 * {@code Long} (8B fixed), {@code LocalDateTime} (12B fixed),
 * {@code int[100]} (4+400B), and {@link Order} (via reflect codec).</p>
 */
@State(Scope.Thread)
@BenchmarkMode({Mode.SampleTime, Mode.Throughput})
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 2, timeUnit = TimeUnit.SECONDS)
@Fork(1)
public class SerialBenchmark {

    /** Simple record used to benchmark the reflective codec path. */
    public record Order(String orderId, String customerId, String status) {}

    private Codecs codecs;

    // Encode inputs
    private String        shortStr;
    private String        longStr;
    private UUID          uuid;
    private Long          longVal;
    private LocalDateTime dateTime;
    private int[]         intArray;
    private Order         order;

    // Pre-encoded bytes for decode benchmarks (avoid re-encoding on every decode call)
    private byte[] encodedShortStr;
    private byte[] encodedLongStr;
    private byte[] encodedUuid;
    private byte[] encodedLong;
    private byte[] encodedDateTime;
    private byte[] encodedIntArray;
    private byte[] encodedOrder;

    @Setup(Level.Trial)
    public void setup() {
        codecs = Codecs.standard();

        // Register the reflect codec once — it self-caches in the registry
        codecs.reflect(Order.class);

        shortStr  = "hello123";               // 8 chars → 2+8 = 10 bytes on wire
        longStr   = "X".repeat(256);          // 256 chars → 2+256 = 258 bytes
        uuid      = UUID.randomUUID();        // 16 bytes fixed
        longVal   = 9_876_543_210L;           // 8 bytes fixed
        dateTime  = LocalDateTime.of(2024, 6, 17, 12, 0, 0); // 12 bytes
        intArray  = new int[100];
        for (int i = 0; i < 100; i++) intArray[i] = i;
        order = new Order("ord-0001", "cust-007", "SHIPPED");

        encodedShortStr  = codecs.encode(String.class, shortStr);
        encodedLongStr   = codecs.encode(String.class, longStr);
        encodedUuid      = codecs.encode(UUID.class, uuid);
        encodedLong      = codecs.encode(Long.class, longVal);
        encodedDateTime  = codecs.encode(LocalDateTime.class, dateTime);
        encodedIntArray  = codecs.encode(int[].class, intArray);
        encodedOrder     = codecs.encode(Order.class, order);
    }

    // ── String ───────────────────────────────────────────────────────────────────

    /** Encode a short (8-char) String via the registry path. */
    @Benchmark
    public byte[] encodeStringShort() {
        return codecs.encode(String.class, shortStr);
    }

    /** Encode a long (256-char) String via the registry path. */
    @Benchmark
    public byte[] encodeStringLong() {
        return codecs.encode(String.class, longStr);
    }

    /** Decode a short String from pre-encoded bytes. */
    @Benchmark
    public String decodeString() {
        return codecs.decode(String.class, encodedShortStr);
    }

    // ── UUID ─────────────────────────────────────────────────────────────────────

    /** Encode a UUID (16 bytes, fixed size — no length prefix). */
    @Benchmark
    public byte[] encodeUuid() {
        return codecs.encode(UUID.class, uuid);
    }

    /** Decode a UUID from pre-encoded bytes. */
    @Benchmark
    public UUID decodeUuid() {
        return codecs.decode(UUID.class, encodedUuid);
    }

    // ── Long ─────────────────────────────────────────────────────────────────────

    /** Encode a Long (8 bytes, fixed size). */
    @Benchmark
    public byte[] encodeLong() {
        return codecs.encode(Long.class, longVal);
    }

    /** Decode a Long from pre-encoded bytes. */
    @Benchmark
    public Long decodeLong() {
        return codecs.decode(Long.class, encodedLong);
    }

    // ── LocalDateTime ─────────────────────────────────────────────────────────────

    /** Encode a LocalDateTime (12 bytes: epochDay(4) + nanoOfDay(8)). */
    @Benchmark
    public byte[] encodeLocalDateTime() {
        return codecs.encode(LocalDateTime.class, dateTime);
    }

    /** Decode a LocalDateTime from pre-encoded bytes. */
    @Benchmark
    public LocalDateTime decodeLocalDateTime() {
        return codecs.decode(LocalDateTime.class, encodedDateTime);
    }

    // ── int[] ────────────────────────────────────────────────────────────────────

    /** Encode a 100-element int[] (4+400=404 bytes). */
    @Benchmark
    public byte[] encodeIntArray() {
        return codecs.encode(int[].class, intArray);
    }

    /** Decode a 100-element int[] from pre-encoded bytes. */
    @Benchmark
    public int[] decodeIntArray() {
        return codecs.decode(int[].class, encodedIntArray);
    }

    // ── Reflective codec ─────────────────────────────────────────────────────────

    /**
     * Encode a record via the reflective codec.
     * Measures: registry lookup + reflection-based field read × 3 + String codec × 3.
     * Compare against {@link #encodeStringShort()} to quantify reflection overhead.
     */
    @Benchmark
    public byte[] encodeReflectRecord() {
        return codecs.encode(Order.class, order);
    }

    /** Decode a record via the reflective codec. */
    @Benchmark
    public Order decodeReflectRecord() {
        return codecs.decode(Order.class, encodedOrder);
    }
}
