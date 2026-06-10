package com.simplj.flair.cache.serial;

import com.simplj.flair.cache.serial.codecs.*;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class CodecRoundTripTest {

    // ── helpers ────────────────────────────────────────────────────────────────

    private static <T> T roundTrip(Codec<T> codec, T value) {
        int size = codec.sizeOf(value);
        ByteBuffer buf = ByteBuffer.allocate(size);
        codec.serialize(value, buf);
        buf.flip();
        return codec.deserialize(buf);
    }

    private static <T> void assertRoundTrip(Codec<T> codec, T value) {
        T decoded = roundTrip(codec, value);
        assertEquals(value, decoded);
    }

    private static <T> void assertExactBufferConsumption(Codec<T> codec, T value) {
        int size = codec.sizeOf(value);
        ByteBuffer buf = ByteBuffer.allocate(size);
        codec.serialize(value, buf);
        assertEquals(size, buf.position(), "serialize wrote a different number of bytes than sizeOf declared");
        buf.flip();
        codec.deserialize(buf);
        assertFalse(buf.hasRemaining(), "deserialize left unconsumed bytes");
    }

    // ── primitives ─────────────────────────────────────────────────────────────

    @Test
    void byte_roundTrip() {
        assertRoundTrip(PrimitiveCodecs.BYTE, (byte) 42);
        assertRoundTrip(PrimitiveCodecs.BYTE, Byte.MIN_VALUE);
        assertRoundTrip(PrimitiveCodecs.BYTE, Byte.MAX_VALUE);
        assertRoundTrip(PrimitiveCodecs.BYTE, (byte) 0);
    }

    @Test
    void short_roundTrip() {
        assertRoundTrip(PrimitiveCodecs.SHORT, (short) 1000);
        assertRoundTrip(PrimitiveCodecs.SHORT, Short.MIN_VALUE);
        assertRoundTrip(PrimitiveCodecs.SHORT, Short.MAX_VALUE);
    }

    @Test
    void int_roundTrip() {
        assertRoundTrip(PrimitiveCodecs.INT, 0);
        assertRoundTrip(PrimitiveCodecs.INT, Integer.MIN_VALUE);
        assertRoundTrip(PrimitiveCodecs.INT, Integer.MAX_VALUE);
        assertRoundTrip(PrimitiveCodecs.INT, -1);
    }

    @Test
    void long_roundTrip() {
        assertRoundTrip(PrimitiveCodecs.LONG, 0L);
        assertRoundTrip(PrimitiveCodecs.LONG, Long.MIN_VALUE);
        assertRoundTrip(PrimitiveCodecs.LONG, Long.MAX_VALUE);
        assertRoundTrip(PrimitiveCodecs.LONG, -1L);
    }

    @Test
    void float_roundTrip() {
        assertRoundTrip(PrimitiveCodecs.FLOAT, 3.14f);
        assertRoundTrip(PrimitiveCodecs.FLOAT, Float.MIN_VALUE);
        assertRoundTrip(PrimitiveCodecs.FLOAT, Float.MAX_VALUE);
        assertRoundTrip(PrimitiveCodecs.FLOAT, Float.NaN);
        assertRoundTrip(PrimitiveCodecs.FLOAT, Float.NEGATIVE_INFINITY);
    }

    @Test
    void double_roundTrip() {
        assertRoundTrip(PrimitiveCodecs.DOUBLE, 3.14159265358979);
        assertRoundTrip(PrimitiveCodecs.DOUBLE, Double.MIN_VALUE);
        assertRoundTrip(PrimitiveCodecs.DOUBLE, Double.MAX_VALUE);
        assertRoundTrip(PrimitiveCodecs.DOUBLE, Double.NaN);
    }

    @Test
    void boolean_roundTrip() {
        assertRoundTrip(PrimitiveCodecs.BOOLEAN, true);
        assertRoundTrip(PrimitiveCodecs.BOOLEAN, false);
    }

    // ── String ─────────────────────────────────────────────────────────────────

    @Test
    void string_ascii_roundTrip() {
        assertRoundTrip(StringCodec.INSTANCE, "hello world");
        assertRoundTrip(StringCodec.INSTANCE, "FlairCache");
    }

    @Test
    void string_empty_roundTrip() {
        assertEquals("", roundTrip(StringCodec.INSTANCE, ""));
        assertEquals("", roundTrip(StringCodec.INSTANCE, null));
    }

    @Test
    void string_multibyte_roundTrip() {
        assertRoundTrip(StringCodec.INSTANCE, "日本語テスト");
        assertRoundTrip(StringCodec.INSTANCE, "café résumé naïve");
        assertRoundTrip(StringCodec.INSTANCE, "😀"); // emoji (surrogate pair)
    }

    @Test
    void string_maxLength_roundTrip() {
        // ASCII: 1 byte per char — exactly at the uint16 ceiling
        String atLimit = "A".repeat(StringCodec.MAX_UTF8_BYTES);
        assertRoundTrip(StringCodec.INSTANCE, atLimit);
        assertExactBufferConsumption(StringCodec.INSTANCE, atLimit);
    }

    @Test
    void string_overLimit_sizeOf_throws() {
        String overLimit = "A".repeat(StringCodec.MAX_UTF8_BYTES + 1);
        assertThrows(IllegalArgumentException.class, () -> StringCodec.INSTANCE.sizeOf(overLimit));
    }

    @Test
    void string_overLimit_serialize_throws() {
        // Safety-net path: serialize called directly without sizeOf, buffer is big enough to hold the content.
        // 2-byte prefix + 65536 content bytes = 65538 bytes needed.
        String overLimit = "A".repeat(StringCodec.MAX_UTF8_BYTES + 1);
        ByteBuffer buf = ByteBuffer.allocate(StringCodec.MAX_UTF8_BYTES + 3);
        assertThrows(IllegalArgumentException.class,
                () -> StringCodec.INSTANCE.serialize(overLimit, buf));
    }

    @Test
    void string_nullAndEmpty_identicalWireBytes() {
        // Both must produce exactly 2 bytes: 0x00 0x00
        ByteBuffer bufNull = ByteBuffer.allocate(2);
        ByteBuffer bufEmpty = ByteBuffer.allocate(2);
        StringCodec.INSTANCE.serialize(null, bufNull);
        StringCodec.INSTANCE.serialize("", bufEmpty);
        assertArrayEquals(bufNull.array(), bufEmpty.array());
    }

    @Test
    void string_exactBufferConsumption() {
        assertExactBufferConsumption(StringCodec.INSTANCE, "hello");
        assertExactBufferConsumption(StringCodec.INSTANCE, "");
        assertExactBufferConsumption(StringCodec.INSTANCE, "日本語");
    }

    // ── byte[] ─────────────────────────────────────────────────────────────────

    @Test
    void byteArray_roundTrip() {
        assertArrayEquals(new byte[]{1, 2, 3}, roundTrip(ByteArrayCodec.INSTANCE, new byte[]{1, 2, 3}));
        assertArrayEquals(new byte[0], roundTrip(ByteArrayCodec.INSTANCE, new byte[0]));
        assertArrayEquals(new byte[0], roundTrip(ByteArrayCodec.INSTANCE, null));
    }

    @Test
    void byteArray_largePayload() {
        byte[] large = new byte[64 * 1024];
        new Random(42).nextBytes(large);
        assertArrayEquals(large, roundTrip(ByteArrayCodec.INSTANCE, large));
    }

    @Test
    void byteArray_exactBufferConsumption() {
        assertExactBufferConsumption(ByteArrayCodec.INSTANCE, new byte[]{10, 20, 30});
        assertExactBufferConsumption(ByteArrayCodec.INSTANCE, new byte[0]);
    }

    // ── UUID ───────────────────────────────────────────────────────────────────

    @Test
    void uuid_roundTrip() {
        UUID id = UUID.randomUUID();
        assertRoundTrip(UuidCodec.INSTANCE, id);
        assertRoundTrip(UuidCodec.INSTANCE, new UUID(0L, 0L));
        assertRoundTrip(UuidCodec.INSTANCE, new UUID(Long.MIN_VALUE, Long.MAX_VALUE));
    }

    @Test
    void uuid_exactBufferConsumption() {
        assertExactBufferConsumption(UuidCodec.INSTANCE, UUID.randomUUID());
    }

    // ── Instant ────────────────────────────────────────────────────────────────

    @Test
    void instant_roundTrip() {
        assertRoundTrip(InstantCodec.INSTANCE, Instant.now());
        assertRoundTrip(InstantCodec.INSTANCE, Instant.EPOCH);
        assertRoundTrip(InstantCodec.INSTANCE, Instant.ofEpochSecond(Long.MAX_VALUE / 1000, 999_999_999));
        assertRoundTrip(InstantCodec.INSTANCE, Instant.ofEpochSecond(0, 0));
    }

    @Test
    void instant_exactBufferConsumption() {
        assertExactBufferConsumption(InstantCodec.INSTANCE, Instant.now());
    }

    // ── List ───────────────────────────────────────────────────────────────────

    @Test
    void list_roundTrip() {
        Codec<List<String>> codec = new ListCodec<>(StringCodec.INSTANCE);
        assertRoundTrip(codec, List.of("a", "b", "c"));
        assertRoundTrip(codec, List.of());
        assertRoundTrip(codec, List.of("single"));
    }

    @Test
    void list_null_decodesAsEmpty() {
        Codec<List<String>> codec = new ListCodec<>(StringCodec.INSTANCE);
        assertEquals(List.of(), roundTrip(codec, null));
    }

    @Test
    void list_exactBufferConsumption() {
        Codec<List<Integer>> codec = new ListCodec<>(PrimitiveCodecs.INT);
        assertExactBufferConsumption(codec, List.of(1, 2, 3));
        assertExactBufferConsumption(codec, List.of());
    }

    // ── Map ────────────────────────────────────────────────────────────────────

    @Test
    void map_roundTrip() {
        Codec<Map<String, Integer>> codec = new MapCodec<>(StringCodec.INSTANCE, PrimitiveCodecs.INT);
        Map<String, Integer> original = new LinkedHashMap<>();
        original.put("one", 1);
        original.put("two", 2);
        original.put("three", 3);
        assertEquals(original, roundTrip(codec, original));
    }

    @Test
    void map_empty_roundTrip() {
        Codec<Map<String, Long>> codec = new MapCodec<>(StringCodec.INSTANCE, PrimitiveCodecs.LONG);
        assertEquals(Map.of(), roundTrip(codec, Map.of()));
        assertEquals(Map.of(), roundTrip(codec, null));
    }

    @Test
    void map_exactBufferConsumption() {
        Codec<Map<String, Integer>> codec = new MapCodec<>(StringCodec.INSTANCE, PrimitiveCodecs.INT);
        Map<String, Integer> m = Map.of("k", 99);
        assertExactBufferConsumption(codec, m);
    }

    // ── Optional ───────────────────────────────────────────────────────────────

    @Test
    void optional_present_roundTrip() {
        Codec<Optional<String>> codec = new OptionalCodec<>(StringCodec.INSTANCE);
        assertEquals(Optional.of("hello"), roundTrip(codec, Optional.of("hello")));
    }

    @Test
    void optional_empty_roundTrip() {
        Codec<Optional<String>> codec = new OptionalCodec<>(StringCodec.INSTANCE);
        assertEquals(Optional.empty(), roundTrip(codec, Optional.empty()));
        assertEquals(Optional.empty(), roundTrip(codec, null));
    }

    @Test
    void optional_exactBufferConsumption() {
        Codec<Optional<Integer>> codec = new OptionalCodec<>(PrimitiveCodecs.INT);
        assertExactBufferConsumption(codec, Optional.of(42));
        assertExactBufferConsumption(codec, Optional.empty());
    }

    // ── Enum ───────────────────────────────────────────────────────────────────

    enum Colour { RED, GREEN, BLUE }

    @Test
    void enum_roundTrip() {
        Codec<Colour> codec = new EnumCodec<>(Colour.class);
        assertRoundTrip(codec, Colour.RED);
        assertRoundTrip(codec, Colour.GREEN);
        assertRoundTrip(codec, Colour.BLUE);
    }

    @Test
    void enum_exactBufferConsumption() {
        Codec<Colour> codec = new EnumCodec<>(Colour.class);
        assertExactBufferConsumption(codec, Colour.GREEN);
    }

    @Test
    void enum_constructorRejectsMoreThan256Constants() {
        // Enums with > 256 constants cannot be encoded in 1 byte without silent ordinal wrap.
        assertThrows(IllegalArgumentException.class, () -> new EnumCodec<>(TooLargeEnum.class));
    }

    // ── Map — nested value type ────────────────────────────────────────────────

    @Test
    void map_nestedListValue_roundTrip() {
        Codec<Map<String, List<Integer>>> codec =
                new MapCodec<>(StringCodec.INSTANCE, new ListCodec<>(PrimitiveCodecs.INT));
        Map<String, List<Integer>> original = new LinkedHashMap<>();
        original.put("primes", List.of(2, 3, 5, 7));
        original.put("empty", List.of());
        original.put("single", List.of(42));
        assertEquals(original, roundTrip(codec, original));
        assertExactBufferConsumption(codec, original);
    }

    // ── CodecRegistry ──────────────────────────────────────────────────────────

    @Test
    void registry_registerAndLookup() {
        CodecRegistry registry = new CodecRegistry();
        registry.register(String.class, StringCodec.INSTANCE);
        registry.register(Integer.class, PrimitiveCodecs.INT);

        assertSame(StringCodec.INSTANCE, registry.lookup(String.class));
        assertSame(PrimitiveCodecs.INT, registry.lookup(Integer.class));
        assertNull(registry.lookup(Long.class));
    }

    @Test
    void registry_register_overwritesPreviousCodec() {
        CodecRegistry registry = new CodecRegistry();
        Codec<Integer> custom = new Codec<Integer>() {
            @Override public void serialize(Integer v, ByteBuffer buf) { buf.putInt(v); }
            @Override public int sizeOf(Integer v) { return 4; }
            @Override public Integer deserialize(ByteBuffer buf) { return buf.getInt(); }
        };
        registry.register(Integer.class, PrimitiveCodecs.INT);
        registry.register(Integer.class, custom);
        assertSame(custom, registry.lookup(Integer.class));
    }

    // ── FrameBuffer ────────────────────────────────────────────────────────────

    @Test
    void frameBuffer_writeThenToBytes() {
        FrameBuffer fb = FrameBuffer.allocateDirect(64);
        fb.buffer().putInt(0xCAFEBABE);
        fb.buffer().putLong(Long.MAX_VALUE);
        byte[] bytes = fb.toBytes();
        assertEquals(12, bytes.length);

        // position unchanged after toBytes
        assertEquals(12, fb.buffer().position());
    }

    @Test
    void frameBuffer_resetClearsPosition() {
        FrameBuffer fb = FrameBuffer.allocateDirect(64);
        fb.buffer().putInt(1);
        fb.reset();
        assertEquals(0, fb.buffer().position());
    }

    @Test
    void frameBuffer_reuseAfterReset() {
        FrameBuffer fb = FrameBuffer.allocateDirect(64);

        PrimitiveCodecs.INT.serialize(42, fb.buffer());
        byte[] first = fb.toBytes();

        fb.reset();
        PrimitiveCodecs.INT.serialize(99, fb.buffer());
        byte[] second = fb.toBytes();

        ByteBuffer b1 = ByteBuffer.wrap(first);
        ByteBuffer b2 = ByteBuffer.wrap(second);
        assertEquals(42, b1.getInt());
        assertEquals(99, b2.getInt());
    }

    // ── Big-endian byte order ──────────────────────────────────────────────────

    @Test
    void int_isBigEndian() {
        ByteBuffer buf = ByteBuffer.allocate(4);
        PrimitiveCodecs.INT.serialize(0x01020304, buf);
        byte[] raw = buf.array();
        assertEquals(0x01, raw[0] & 0xFF);
        assertEquals(0x02, raw[1] & 0xFF);
        assertEquals(0x03, raw[2] & 0xFF);
        assertEquals(0x04, raw[3] & 0xFF);
    }

    @Test
    void long_isBigEndian() {
        ByteBuffer buf = ByteBuffer.allocate(8);
        PrimitiveCodecs.LONG.serialize(0x0102030405060708L, buf);
        byte[] raw = buf.array();
        for (int i = 0; i < 8; i++) {
            assertEquals(i + 1, raw[i] & 0xFF, "byte " + i);
        }
    }

    // ── char ──────────────────────────────────────────────────────────────────

    @Test
    void char_roundTrip() {
        assertRoundTrip(PrimitiveCodecs.CHAR, 'A');
        assertRoundTrip(PrimitiveCodecs.CHAR, ' ');
        assertRoundTrip(PrimitiveCodecs.CHAR, '￿');
        assertRoundTrip(PrimitiveCodecs.CHAR, '中'); // CJK character
    }

    @Test
    void char_exactBufferConsumption() {
        assertExactBufferConsumption(PrimitiveCodecs.CHAR, 'Z');
    }

    @Test
    void char_sizeIsTwo() {
        assertEquals(2, PrimitiveCodecs.CHAR.sizeOf('x'));
    }

    // ── BigInteger ────────────────────────────────────────────────────────────

    @Test
    void bigInteger_roundTrip() {
        assertRoundTrip(BigIntegerCodec.INSTANCE, BigInteger.ZERO);
        assertRoundTrip(BigIntegerCodec.INSTANCE, BigInteger.ONE);
        assertRoundTrip(BigIntegerCodec.INSTANCE, BigInteger.valueOf(-1));
        assertRoundTrip(BigIntegerCodec.INSTANCE, new BigInteger("123456789012345678901234567890"));
        assertRoundTrip(BigIntegerCodec.INSTANCE, new BigInteger("-987654321098765432109876543210"));
    }

    @Test
    void bigInteger_nullSerializesAsZero() {
        assertEquals(BigInteger.ZERO, roundTrip(BigIntegerCodec.INSTANCE, null));
    }

    @Test
    void bigInteger_exactBufferConsumption() {
        assertExactBufferConsumption(BigIntegerCodec.INSTANCE, new BigInteger("99999999999999999999"));
        assertExactBufferConsumption(BigIntegerCodec.INSTANCE, BigInteger.ZERO);
    }

    // ── BigDecimal ────────────────────────────────────────────────────────────

    @Test
    void bigDecimal_roundTrip() {
        assertRoundTrip(BigDecimalCodec.INSTANCE, BigDecimal.ZERO);
        assertRoundTrip(BigDecimalCodec.INSTANCE, BigDecimal.ONE);
        assertRoundTrip(BigDecimalCodec.INSTANCE, new BigDecimal("123456789.99"));
        assertRoundTrip(BigDecimalCodec.INSTANCE, new BigDecimal("-0.0000001"));
        assertRoundTrip(BigDecimalCodec.INSTANCE, new BigDecimal("1E+100"));
    }

    @Test
    void bigDecimal_nullSerializesAsZero() {
        assertEquals(BigDecimal.ZERO, roundTrip(BigDecimalCodec.INSTANCE, null));
    }

    @Test
    void bigDecimal_exactBufferConsumption() {
        assertExactBufferConsumption(BigDecimalCodec.INSTANCE, new BigDecimal("3.14159265358979323846"));
    }

    // ── Set ───────────────────────────────────────────────────────────────────

    @Test
    void set_roundTrip() {
        SetCodec<String> codec = new SetCodec<>(StringCodec.INSTANCE);
        Set<String> s = new LinkedHashSet<>(Arrays.asList("alpha", "beta", "gamma"));
        assertEquals(s, roundTrip(codec, s));
    }

    @Test
    void set_empty() {
        SetCodec<Integer> codec = new SetCodec<>(PrimitiveCodecs.INT);
        assertEquals(new LinkedHashSet<>(), roundTrip(codec, new LinkedHashSet<>()));
    }

    @Test
    void set_null() {
        SetCodec<String> codec = new SetCodec<>(StringCodec.INSTANCE);
        assertEquals(new LinkedHashSet<>(), roundTrip(codec, null));
    }

    @Test
    void set_exactBufferConsumption() {
        SetCodec<Integer> codec = new SetCodec<>(PrimitiveCodecs.INT);
        assertExactBufferConsumption(codec, new LinkedHashSet<>(Arrays.asList(1, 2, 3)));
    }

    // ── LocalDate ─────────────────────────────────────────────────────────────

    @Test
    void localDate_roundTrip() {
        assertRoundTrip(LocalDateCodec.INSTANCE, LocalDate.of(2024, 6, 15));
        assertRoundTrip(LocalDateCodec.INSTANCE, LocalDate.MIN);
        assertRoundTrip(LocalDateCodec.INSTANCE, LocalDate.MAX);
        assertRoundTrip(LocalDateCodec.INSTANCE, LocalDate.of(1970, 1, 1));
    }

    @Test
    void localDate_exactBufferConsumption() {
        assertExactBufferConsumption(LocalDateCodec.INSTANCE, LocalDate.of(2024, 12, 31));
    }

    @Test
    void localDate_sizeIsSix() {
        assertEquals(6, LocalDateCodec.INSTANCE.sizeOf(LocalDate.now()));
    }

    // ── LocalTime ─────────────────────────────────────────────────────────────

    @Test
    void localTime_roundTrip() {
        assertRoundTrip(LocalTimeCodec.INSTANCE, LocalTime.MIDNIGHT);
        assertRoundTrip(LocalTimeCodec.INSTANCE, LocalTime.NOON);
        assertRoundTrip(LocalTimeCodec.INSTANCE, LocalTime.of(23, 59, 59, 999_999_999));
        assertRoundTrip(LocalTimeCodec.INSTANCE, LocalTime.of(12, 34, 56, 789_000_000));
    }

    @Test
    void localTime_exactBufferConsumption() {
        assertExactBufferConsumption(LocalTimeCodec.INSTANCE, LocalTime.of(10, 30));
    }

    @Test
    void localTime_sizeIsEight() {
        assertEquals(8, LocalTimeCodec.INSTANCE.sizeOf(LocalTime.now()));
    }

    // ── LocalDateTime ─────────────────────────────────────────────────────────

    @Test
    void localDateTime_roundTrip() {
        assertRoundTrip(LocalDateTimeCodec.INSTANCE, LocalDateTime.of(2024, 6, 15, 12, 0, 0, 0));
        assertRoundTrip(LocalDateTimeCodec.INSTANCE, LocalDateTime.MIN);
        assertRoundTrip(LocalDateTimeCodec.INSTANCE, LocalDateTime.MAX);
        assertRoundTrip(LocalDateTimeCodec.INSTANCE, LocalDateTime.of(1970, 1, 1, 0, 0, 0, 123_456_789));
    }

    @Test
    void localDateTime_exactBufferConsumption() {
        assertExactBufferConsumption(LocalDateTimeCodec.INSTANCE, LocalDateTime.now());
    }

    @Test
    void localDateTime_sizeIsFourteen() {
        assertEquals(14, LocalDateTimeCodec.INSTANCE.sizeOf(LocalDateTime.now()));
    }

    // ── Duration ──────────────────────────────────────────────────────────────

    @Test
    void duration_roundTrip() {
        assertRoundTrip(DurationCodec.INSTANCE, Duration.ZERO);
        assertRoundTrip(DurationCodec.INSTANCE, Duration.ofDays(1));
        assertRoundTrip(DurationCodec.INSTANCE, Duration.ofSeconds(-3600, 999_999_999));
        assertRoundTrip(DurationCodec.INSTANCE, Duration.ofNanos(Long.MAX_VALUE));
    }

    @Test
    void duration_exactBufferConsumption() {
        assertExactBufferConsumption(DurationCodec.INSTANCE, Duration.ofHours(5));
    }

    @Test
    void duration_sizeIsTwelve() {
        assertEquals(12, DurationCodec.INSTANCE.sizeOf(Duration.ZERO));
    }

    // ── OffsetDateTime ────────────────────────────────────────────────────────

    @Test
    void offsetDateTime_roundTrip() {
        assertRoundTrip(OffsetDateTimeCodec.INSTANCE,
                OffsetDateTime.of(2024, 6, 15, 12, 0, 0, 0, ZoneOffset.UTC));
        assertRoundTrip(OffsetDateTimeCodec.INSTANCE,
                OffsetDateTime.of(2024, 1, 1, 0, 0, 0, 0, ZoneOffset.ofHoursMinutes(5, 30)));
        assertRoundTrip(OffsetDateTimeCodec.INSTANCE,
                OffsetDateTime.of(2024, 7, 4, 20, 0, 0, 123_000_000, ZoneOffset.ofHours(-8)));
    }

    @Test
    void offsetDateTime_exactBufferConsumption() {
        assertExactBufferConsumption(OffsetDateTimeCodec.INSTANCE,
                OffsetDateTime.now(ZoneOffset.UTC));
    }

    @Test
    void offsetDateTime_sizeIsSixteen() {
        assertEquals(16, OffsetDateTimeCodec.INSTANCE.sizeOf(OffsetDateTime.now(ZoneOffset.UTC)));
    }

    // ── ZonedDateTime ─────────────────────────────────────────────────────────

    @Test
    void zonedDateTime_roundTrip() {
        ZoneId ny = ZoneId.of("America/New_York");
        ZoneId ist = ZoneId.of("Asia/Kolkata");
        assertRoundTrip(ZonedDateTimeCodec.INSTANCE,
                ZonedDateTime.of(2024, 6, 15, 12, 0, 0, 0, ny));
        assertRoundTrip(ZonedDateTimeCodec.INSTANCE,
                ZonedDateTime.of(2024, 1, 1, 8, 30, 0, 0, ist));
        // Offset-based zone
        assertRoundTrip(ZonedDateTimeCodec.INSTANCE,
                ZonedDateTime.of(2024, 3, 10, 0, 0, 0, 0, ZoneOffset.ofHours(3)));
    }

    @Test
    void zonedDateTime_exactBufferConsumption() {
        assertExactBufferConsumption(ZonedDateTimeCodec.INSTANCE,
                ZonedDateTime.now(ZoneId.of("Europe/London")));
    }

    // ── Period ────────────────────────────────────────────────────────────────

    @Test
    void period_roundTrip() {
        assertRoundTrip(PeriodCodec.INSTANCE, Period.ZERO);
        assertRoundTrip(PeriodCodec.INSTANCE, Period.of(1, 2, 3));
        assertRoundTrip(PeriodCodec.INSTANCE, Period.of(-5, 0, 15));
        assertRoundTrip(PeriodCodec.INSTANCE, Period.ofYears(Integer.MAX_VALUE));
    }

    @Test
    void period_exactBufferConsumption() {
        assertExactBufferConsumption(PeriodCodec.INSTANCE, Period.of(2, 6, 10));
    }

    @Test
    void period_sizeIsTwelve() {
        assertEquals(12, PeriodCodec.INSTANCE.sizeOf(Period.ZERO));
    }

    // ── Primitive arrays ──────────────────────────────────────────────────────

    @Test
    void intArray_roundTrip() {
        assertArrayEquals(new int[]{1, -1, Integer.MIN_VALUE, Integer.MAX_VALUE},
                roundTrip(PrimitiveArrayCodecs.INT, new int[]{1, -1, Integer.MIN_VALUE, Integer.MAX_VALUE}));
        assertArrayEquals(new int[0], roundTrip(PrimitiveArrayCodecs.INT, new int[0]));
        assertArrayEquals(new int[0], roundTrip(PrimitiveArrayCodecs.INT, null));
    }

    @Test
    void intArray_exactBufferConsumption() {
        assertExactBufferConsumption(PrimitiveArrayCodecs.INT, new int[]{10, 20, 30});
    }

    @Test
    void longArray_roundTrip() {
        assertArrayEquals(new long[]{0L, Long.MIN_VALUE, Long.MAX_VALUE, -1L},
                roundTrip(PrimitiveArrayCodecs.LONG, new long[]{0L, Long.MIN_VALUE, Long.MAX_VALUE, -1L}));
        assertArrayEquals(new long[0], roundTrip(PrimitiveArrayCodecs.LONG, null));
    }

    @Test
    void longArray_exactBufferConsumption() {
        assertExactBufferConsumption(PrimitiveArrayCodecs.LONG, new long[]{1L, 2L});
    }

    @Test
    void doubleArray_roundTrip() {
        double[] arr = {3.14, Double.NaN, Double.NEGATIVE_INFINITY, -0.0};
        assertArrayEquals(arr, roundTrip(PrimitiveArrayCodecs.DOUBLE, arr));
        assertArrayEquals(new double[0], roundTrip(PrimitiveArrayCodecs.DOUBLE, null));
    }

    @Test
    void doubleArray_exactBufferConsumption() {
        assertExactBufferConsumption(PrimitiveArrayCodecs.DOUBLE, new double[]{1.1, 2.2, 3.3});
    }

    @Test
    void floatArray_roundTrip() {
        float[] arr = {1.5f, Float.MIN_VALUE, Float.MAX_VALUE};
        assertArrayEquals(arr, roundTrip(PrimitiveArrayCodecs.FLOAT, arr));
        assertArrayEquals(new float[0], roundTrip(PrimitiveArrayCodecs.FLOAT, null));
    }

    @Test
    void floatArray_exactBufferConsumption() {
        assertExactBufferConsumption(PrimitiveArrayCodecs.FLOAT, new float[]{1.0f, 2.0f});
    }

    @Test
    void shortArray_roundTrip() {
        short[] arr = {Short.MIN_VALUE, 0, Short.MAX_VALUE};
        assertArrayEquals(arr, roundTrip(PrimitiveArrayCodecs.SHORT, arr));
        assertArrayEquals(new short[0], roundTrip(PrimitiveArrayCodecs.SHORT, null));
    }

    @Test
    void shortArray_exactBufferConsumption() {
        assertExactBufferConsumption(PrimitiveArrayCodecs.SHORT, new short[]{10, 20});
    }

    @Test
    void charArray_roundTrip() {
        char[] arr = {'H', 'e', 'l', 'l', 'o', '中', ' '};
        assertArrayEquals(arr, roundTrip(PrimitiveArrayCodecs.CHAR, arr));
        assertArrayEquals(new char[0], roundTrip(PrimitiveArrayCodecs.CHAR, null));
    }

    @Test
    void charArray_exactBufferConsumption() {
        assertExactBufferConsumption(PrimitiveArrayCodecs.CHAR, new char[]{'a', 'b', 'c'});
    }

    @Test
    void booleanArray_roundTrip() {
        boolean[] arr = {true, false, true, true, false};
        assertArrayEquals(arr, roundTrip(PrimitiveArrayCodecs.BOOLEAN, arr));
        assertArrayEquals(new boolean[0], roundTrip(PrimitiveArrayCodecs.BOOLEAN, null));
    }

    @Test
    void booleanArray_exactBufferConsumption() {
        assertExactBufferConsumption(PrimitiveArrayCodecs.BOOLEAN, new boolean[]{true, false});
    }
}
