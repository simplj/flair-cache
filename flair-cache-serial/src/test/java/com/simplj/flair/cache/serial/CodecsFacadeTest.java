package com.simplj.flair.cache.serial;

import com.simplj.flair.cache.serial.codecs.*;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class CodecsFacadeTest {

    // ── Test model types ───────────────────────────────────────────────────────

    record Point(int x, int y) {}

    record Labeled(String label, double value) {}

    enum Status { ACTIVE, INACTIVE }

    record RichRecord(String id, List<String> tags, Optional<String> note, Status status) {}

    static class MutableBean {
        String name;
        int count;
        MutableBean() {}

        @Override public boolean equals(Object o) {
            if (!(o instanceof MutableBean)) return false;
            MutableBean b = (MutableBean) o;
            return count == b.count && Objects.equals(name, b.name);
        }
        @Override public int hashCode() { return Objects.hash(name, count); }
    }

    record Address(String city, String street) {}
    record Person(String name, Address address, int age) {}

    static class HasFinalField {
        final String name = "immutable";
        HasFinalField() {}
    }

    static class NoDefaultCtor {
        String name;
        NoDefaultCtor(String name) { this.name = name; }
    }

    // ── Helpers ────────────────────────────────────────────────────────────────

    /** Asserts encode → decode produces an equal value. */
    private static <T> void assertEncodeRoundTrip(Codecs codecs, Class<T> type, T value) {
        assertEquals(value, codecs.decode(type, codecs.encode(type, value)));
    }

    /**
     * Asserts serialize → deserialize produces an equal value AND that sizeOf matches
     * bytes written and all bytes are consumed.
     */
    private static <T> void assertSerializeRoundTrip(Codecs codecs, Class<T> type, T value) {
        int size = codecs.sizeOf(type, value);
        ByteBuffer buf = ByteBuffer.allocate(size);
        codecs.serialize(type, value, buf);
        assertEquals(size, buf.position(), "sizeOf must equal bytes written");
        buf.flip();
        assertEquals(value, codecs.deserialize(type, buf));
        assertFalse(buf.hasRemaining(), "deserialize must consume exactly sizeOf bytes");
    }

    // ── Tier 1: Primitives and scalars ────────────────────────────────────────

    @Test
    void primitives_serializeDeserialize_allTypes() {
        Codecs c = Codecs.standard();
        ByteBuffer buf = ByteBuffer.allocate(66); // 1+2+4+8+4+8+1+2 = 30 + slack

        c.serializeByte((byte) 127, buf);
        c.serializeShort((short) 1000, buf);
        c.serializeInt(-1, buf);
        c.serializeLong(Long.MAX_VALUE, buf);
        c.serializeFloat(1.5f, buf);
        c.serializeDouble(Math.PI, buf);
        c.serializeBoolean(true, buf);
        c.serializeChar('Z', buf);
        buf.flip();

        assertEquals((byte) 127,     c.deserializeByte(buf));
        assertEquals((short) 1000,   c.deserializeShort(buf));
        assertEquals(-1,             c.deserializeInt(buf));
        assertEquals(Long.MAX_VALUE, c.deserializeLong(buf));
        assertEquals(1.5f,           c.deserializeFloat(buf));
        assertEquals(Math.PI,        c.deserializeDouble(buf));
        assertTrue(c.deserializeBoolean(buf));
        assertEquals('Z',            c.deserializeChar(buf));
        assertFalse(buf.hasRemaining());
    }

    @Test
    void string_serializeDeserialize_variousCases() {
        Codecs c = Codecs.standard();
        for (String s : new String[]{ "hello", "", "日本語", "café", "😀" }) {
            int size = c.sizeOfString(s);
            ByteBuffer buf = ByteBuffer.allocate(size);
            c.serializeString(s, buf);
            assertEquals(size, buf.position(), "sizeOfString mismatch for: " + s);
            buf.flip();
            assertEquals(s, c.deserializeString(buf));
        }
    }

    @Test
    void string_sizeOf_knownValues() {
        Codecs c = Codecs.standard();
        assertEquals(2, c.sizeOfString(""));      // 2-byte prefix + 0
        assertEquals(7, c.sizeOfString("hello")); // 2 + 5 ASCII bytes
        // "café": c(1)+a(1)+f(1)+é(2) = 5 UTF-8 bytes
        assertEquals(7, c.sizeOfString("café"));
    }

    @Test
    void bytes_serializeDeserialize_roundTrip() {
        Codecs c = Codecs.standard();
        byte[] data = {10, 20, 30};
        int size = c.sizeOfBytes(data);
        ByteBuffer buf = ByteBuffer.allocate(size);
        c.serializeBytes(data, buf);
        assertEquals(size, buf.position(), "sizeOfBytes mismatch");
        buf.flip();
        assertArrayEquals(data, c.deserializeBytes(buf));
    }

    @Test
    void bytes_sizeOf_knownValues() {
        Codecs c = Codecs.standard();
        assertEquals(4,     c.sizeOfBytes(new byte[0]));        // 4-byte length prefix + 0
        assertEquals(4 + 3, c.sizeOfBytes(new byte[]{1, 2, 3}));
    }

    @Test
    void uuid_serializeDeserialize_roundTrip() {
        Codecs c = Codecs.standard();
        UUID id = UUID.randomUUID();
        ByteBuffer buf = ByteBuffer.allocate(16);
        c.serializeUuid(id, buf);
        assertEquals(16, buf.position());
        buf.flip();
        assertEquals(id, c.deserializeUuid(buf));
    }

    @Test
    void instant_serializeDeserialize_roundTrip() {
        Codecs c = Codecs.standard();
        Instant ts = Instant.now();
        ByteBuffer buf = ByteBuffer.allocate(12);
        c.serializeInstant(ts, buf);
        assertEquals(12, buf.position());
        buf.flip();
        assertEquals(ts, c.deserializeInstant(buf));
    }

    @Test
    void mixed_frame_roundTrip() {
        // Simulates building a small wire frame with mixed field types.
        Codecs c = Codecs.standard();
        String key = "session-1";
        int count = 42;
        UUID id = UUID.fromString("00000000-0000-0000-0000-000000000001");

        ByteBuffer buf = ByteBuffer.allocate(c.sizeOfString(key) + 4 + 16);
        c.serializeString(key, buf);
        c.serializeInt(count, buf);
        c.serializeUuid(id, buf);
        buf.flip();

        assertEquals(key,   c.deserializeString(buf));
        assertEquals(count, c.deserializeInt(buf));
        assertEquals(id,    c.deserializeUuid(buf));
        assertFalse(buf.hasRemaining());
    }

    // ── Tier 2: Registered types — ByteBuffer ─────────────────────────────────

    @Test
    void serialize_deserialize_builtInString() {
        assertSerializeRoundTrip(Codecs.standard(), String.class, "FlairCache");
        assertSerializeRoundTrip(Codecs.standard(), String.class, "");
    }

    @Test
    void serialize_deserialize_builtInInteger() {
        assertSerializeRoundTrip(Codecs.standard(), Integer.class, Integer.MIN_VALUE);
        assertSerializeRoundTrip(Codecs.standard(), Integer.class, 0);
    }

    @Test
    void serialize_deserialize_builtInUUID() {
        assertSerializeRoundTrip(Codecs.standard(), UUID.class, UUID.randomUUID());
    }

    @Test
    void sizeOf_knownValues_builtInTypes() {
        Codecs c = Codecs.standard();
        assertEquals(1,  c.sizeOf(Byte.class,      (byte) 0));
        assertEquals(2,  c.sizeOf(Short.class,     (short) 0));
        assertEquals(4,  c.sizeOf(Integer.class,   0));
        assertEquals(8,  c.sizeOf(Long.class,      0L));
        assertEquals(4,  c.sizeOf(Float.class,     0f));
        assertEquals(8,  c.sizeOf(Double.class,    0.0));
        assertEquals(1,  c.sizeOf(Boolean.class,   true));
        assertEquals(2,  c.sizeOf(Character.class, 'x'));
        assertEquals(16, c.sizeOf(UUID.class,      UUID.randomUUID()));
        assertEquals(12, c.sizeOf(Instant.class,   Instant.now()));
        assertEquals(7,  c.sizeOf(String.class,    "hello"));  // 2 + 5
        assertEquals(6,  c.sizeOf(LocalDate.class,      LocalDate.of(2024, 1, 1)));
        assertEquals(8,  c.sizeOf(LocalTime.class,      LocalTime.NOON));
        assertEquals(14, c.sizeOf(LocalDateTime.class,  LocalDateTime.now()));
        assertEquals(12, c.sizeOf(Duration.class,       Duration.ZERO));
        assertEquals(16, c.sizeOf(OffsetDateTime.class, OffsetDateTime.now(ZoneOffset.UTC)));
        assertEquals(12, c.sizeOf(Period.class,         Period.ZERO));
    }

    @Test
    void serialize_deserialize_customRegisteredCodec() {
        Codecs c = Codecs.standard();
        Codec<Point> codec = new Codec<Point>() {
            @Override public void serialize(Point p, ByteBuffer buf) { buf.putInt(p.x()); buf.putInt(p.y()); }
            @Override public int sizeOf(Point p) { return 8; }
            @Override public Point deserialize(ByteBuffer buf) { return new Point(buf.getInt(), buf.getInt()); }
        };
        c.register(Point.class, codec);
        assertSerializeRoundTrip(c, Point.class, new Point(3, 7));
        assertSerializeRoundTrip(c, Point.class, new Point(-1, 0));
    }

    // ── Tier 3: Registered types — byte[] ─────────────────────────────────────

    @Test
    void encode_decode_builtInTypes() {
        Codecs c = Codecs.standard();
        assertEncodeRoundTrip(c, String.class,    "hello");
        assertEncodeRoundTrip(c, Integer.class,   42);
        assertEncodeRoundTrip(c, Long.class,      Long.MAX_VALUE);
        assertEncodeRoundTrip(c, Double.class,    Math.PI);
        assertEncodeRoundTrip(c, Boolean.class,   true);
        assertEncodeRoundTrip(c, Character.class, '中');
        assertEncodeRoundTrip(c, UUID.class,      UUID.randomUUID());
        assertEncodeRoundTrip(c, Instant.class,   Instant.now());
        assertEncodeRoundTrip(c, BigInteger.class, new BigInteger("999999999999999999999"));
        assertEncodeRoundTrip(c, BigDecimal.class, new BigDecimal("3.14159265358979323846"));
        assertEncodeRoundTrip(c, LocalDate.class,      LocalDate.of(2024, 6, 15));
        assertEncodeRoundTrip(c, LocalTime.class,      LocalTime.of(12, 30, 45, 123_000_000));
        assertEncodeRoundTrip(c, LocalDateTime.class,  LocalDateTime.of(2024, 6, 15, 12, 0));
        assertEncodeRoundTrip(c, Duration.class,       Duration.ofHours(3));
        assertEncodeRoundTrip(c, OffsetDateTime.class, OffsetDateTime.of(2024, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC));
        assertEncodeRoundTrip(c, ZonedDateTime.class,  ZonedDateTime.of(2024, 6, 1, 9, 0, 0, 0, ZoneId.of("America/New_York")));
        assertEncodeRoundTrip(c, Period.class,         Period.of(1, 6, 15));
    }

    @Test
    void encode_decode_byteArray() {
        Codecs c = Codecs.standard();
        byte[] data = {1, 2, 3, 4};
        assertArrayEquals(data, c.decode(byte[].class, c.encode(byte[].class, data)));
    }

    @Test
    void encode_decode_customRegisteredCodec() {
        Codecs c = Codecs.standard();
        Codec<Point> codec = new Codec<Point>() {
            @Override public void serialize(Point p, ByteBuffer buf) { buf.putInt(p.x()); buf.putInt(p.y()); }
            @Override public int sizeOf(Point p) { return 8; }
            @Override public Point deserialize(ByteBuffer buf) { return new Point(buf.getInt(), buf.getInt()); }
        };
        c.register(Point.class, codec);
        assertEncodeRoundTrip(c, Point.class, new Point(5, -5));
    }

    // ── lookup ────────────────────────────────────────────────────────────────

    @Test
    void lookup_returnsRegisteredCodec() {
        Codecs c = Codecs.standard();
        assertSame(PrimitiveCodecs.INT,    c.lookup(Integer.class));
        assertSame(StringCodec.INSTANCE,   c.lookup(String.class));
        assertSame(ByteArrayCodec.INSTANCE, c.lookup(byte[].class));
    }

    @Test
    void lookup_unregisteredType_throws() {
        assertThrows(IllegalArgumentException.class,
                () -> Codecs.standard().lookup(Point.class));
    }

    // ── reflect() — records ───────────────────────────────────────────────────

    @Test
    void reflect_record_encode_decode() {
        Codecs c = Codecs.standard();
        c.reflect(Point.class);
        assertEncodeRoundTrip(c, Point.class, new Point(10, -20));
        assertEncodeRoundTrip(c, Point.class, new Point(0, 0));
    }

    @Test
    void reflect_record_serialize_deserialize() {
        Codecs c = Codecs.standard();
        c.reflect(Labeled.class);
        assertSerializeRoundTrip(c, Labeled.class, new Labeled("pi", Math.PI));
    }

    @Test
    void reflect_record_withGenericsAndEnum() {
        Codecs c = Codecs.standard();
        c.reflect(RichRecord.class);
        assertEncodeRoundTrip(c, RichRecord.class,
                new RichRecord("id-1", List.of("a", "b"), Optional.of("note"), Status.ACTIVE));
        assertEncodeRoundTrip(c, RichRecord.class,
                new RichRecord("id-2", List.of(), Optional.empty(), Status.INACTIVE));
    }

    // ── reflect() — mutable POJO ──────────────────────────────────────────────

    @Test
    void reflect_pojo_encode_decode() {
        Codecs c = Codecs.standard();
        c.reflect(MutableBean.class);
        MutableBean b = new MutableBean();
        b.name = "FlairCache";
        b.count = 99;
        assertEncodeRoundTrip(c, MutableBean.class, b);
    }

    @Test
    void reflect_pojo_serialize_deserialize() {
        Codecs c = Codecs.standard();
        c.reflect(MutableBean.class);
        MutableBean b = new MutableBean();
        b.name = "test";
        b.count = 0;
        assertSerializeRoundTrip(c, MutableBean.class, b);
    }

    // ── reflect() — nested types ──────────────────────────────────────────────

    @Test
    void reflect_nestedRecord_encode_decode() {
        Codecs c = Codecs.standard();
        c.reflect(Person.class);
        assertEncodeRoundTrip(c, Person.class,
                new Person("Alice", new Address("London", "Baker St"), 30));
    }

    @Test
    void reflect_nestedRecord_serialize_deserialize() {
        Codecs c = Codecs.standard();
        c.reflect(Person.class);
        assertSerializeRoundTrip(c, Person.class,
                new Person("Bob", new Address("Berlin", "Unter den Linden"), 25));
    }

    @Test
    void reflect_nestedType_autoRegistered() {
        Codecs c = Codecs.standard();
        c.reflect(Person.class);
        // Reflecting Person must also auto-register Address as a side effect.
        Codec<Address> addrCodec = c.lookup(Address.class);
        assertNotNull(addrCodec);
        // Second call returns the cached instance — no new codec created.
        assertSame(addrCodec, c.reflect(Address.class));
    }

    @Test
    void reflect_sizeOf_matchesBytesWritten() {
        Codecs c = Codecs.standard();
        c.reflect(Point.class);
        Point p = new Point(5, -3);
        int declared = c.sizeOf(Point.class, p);
        ByteBuffer buf = ByteBuffer.allocate(declared);
        c.serialize(Point.class, p, buf);
        assertEquals(declared, buf.position());
    }

    // ── reflect() — error cases ───────────────────────────────────────────────

    @Test
    void reflect_finalField_throws() {
        assertThrows(IllegalArgumentException.class,
                () -> Codecs.standard().reflect(HasFinalField.class));
    }

    @Test
    void reflect_noDefaultConstructor_throws() {
        assertThrows(IllegalArgumentException.class,
                () -> Codecs.standard().reflect(NoDefaultCtor.class));
    }

    // ── reflect() — Collection/Iterable field gives a clear error ─────────────

    record HasCollectionField(Collection<String> items) {}
    record HasIterableField(Iterable<String> items) {}
    record HasAtomicField(AtomicInteger counter) {}
    record HasIntArrayField(int[] values, String name) {}

    @Test
    void reflect_collectionField_throwsWithGuidance() {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> Codecs.standard().reflect(HasCollectionField.class));
        assertTrue(ex.getMessage().contains("Collection"), "error must name the type");
        assertTrue(ex.getMessage().contains("List") || ex.getMessage().contains("Set"),
                "error must suggest List<T> or Set<T>");
    }

    @Test
    void reflect_iterableField_throwsWithGuidance() {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> Codecs.standard().reflect(HasIterableField.class));
        assertTrue(ex.getMessage().contains("Iterable"), "error must name the type");
    }

    @Test
    void reflect_atomicField_throwsWithGuidance() {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> Codecs.standard().reflect(HasAtomicField.class));
        assertTrue(ex.getMessage().contains("AtomicInteger"), "error must name the atomic type");
    }

    @Test
    void reflect_failedConstruction_doesNotLeaveStaleCodecInRegistry() {
        // First call: throws because Collection<T> field is unsupported
        Codecs c = Codecs.standard();
        assertThrows(IllegalArgumentException.class, () -> c.reflect(HasCollectionField.class));
        // Second call: must throw the same clear error, NOT return a broken null-fields codec
        assertThrows(IllegalArgumentException.class, () -> c.reflect(HasCollectionField.class));
    }

    record HasWildcardField(List<? extends Number> numbers) {}

    @Test
    void reflect_wildcardField_throwsWithGuidance() {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> Codecs.standard().reflect(HasWildcardField.class));
        assertTrue(ex.getMessage().contains("wildcard"), "error must name wildcard");
    }

    @Test
    void reflect_primitiveArrayField_roundTrip() {
        Codecs c = Codecs.standard();
        c.reflect(HasIntArrayField.class);
        HasIntArrayField orig = new HasIntArrayField(new int[]{1, 2, 3}, "test");
        HasIntArrayField decoded = c.decode(HasIntArrayField.class, c.encode(HasIntArrayField.class, orig));
        assertArrayEquals(orig.values(), decoded.values());
        assertEquals(orig.name(), decoded.name());
    }
}
