package com.simplj.flair.cache.serial;

import com.simplj.flair.cache.serial.codecs.*;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.*;
import java.util.UUID;

/**
 * Single entry-point for all serialization operations.
 *
 * <p>Three usage tiers — pick the one that matches your situation:
 * <ol>
 *   <li><b>Primitives and scalars</b> — named methods (e.g. {@link #serializeInt},
 *       {@link #deserializeString}) that write directly into a caller-managed
 *       {@link ByteBuffer}. No registry lookup, no boxing. For frame construction.</li>
 *   <li><b>Any registered type — ByteBuffer</b> — {@link #serialize(Class, Object, ByteBuffer)},
 *       {@link #deserialize(Class, ByteBuffer)}, and {@link #sizeOf(Class, Object)} for
 *       composing mixed-type frames without allocation.</li>
 *   <li><b>Any registered type — byte[] / ByteBuffer</b> — {@link #encode(Class, Object)},
 *       {@link #encodeToBuffer(Class, Object)}, and {@link #decode(Class, byte[])} for
 *       standalone payloads and testing.</li>
 * </ol>
 *
 * <p>Obtain an instance via {@link #standard()}, which pre-registers codecs for all built-in
 * types. Register custom codecs via {@link #register(Class, Codec)}, or use
 * {@link #reflect(Class)} for a prototype-quality auto-generated codec (see that method
 * for caveats).
 *
 * <pre>{@code
 * Codecs codecs = Codecs.standard();
 * codecs.register(Product.class, productCodec);
 *
 * byte[] bytes = codecs.encode(Product.class, product);
 * Product p    = codecs.decode(Product.class, bytes);
 * }</pre>
 */
public final class Codecs {

    private final CodecRegistry registry;

    private Codecs(CodecRegistry registry) {
        this.registry = registry;
    }

    /**
     * Returns a new {@code Codecs} instance with codecs pre-registered for all built-in types:
     * boxed primitives ({@code Byte} through {@code Character}), {@code String}, {@code byte[]},
     * {@code UUID}, {@code Instant}, {@code BigInteger}, {@code BigDecimal}, all
     * {@code java.time.*} types, and all Java primitive array types.
     */
    public static Codecs standard() {
        CodecRegistry r = new CodecRegistry();
        // Boxed primitives
        r.register(Byte.class,      PrimitiveCodecs.BYTE);
        r.register(Short.class,     PrimitiveCodecs.SHORT);
        r.register(Integer.class,   PrimitiveCodecs.INT);
        r.register(Long.class,      PrimitiveCodecs.LONG);
        r.register(Float.class,     PrimitiveCodecs.FLOAT);
        r.register(Double.class,    PrimitiveCodecs.DOUBLE);
        r.register(Boolean.class,   PrimitiveCodecs.BOOLEAN);
        r.register(Character.class, PrimitiveCodecs.CHAR);
        // Common value types
        r.register(String.class,    StringCodec.INSTANCE);
        r.register(byte[].class,    ByteArrayCodec.INSTANCE);
        r.register(UUID.class,      UuidCodec.INSTANCE);
        r.register(Instant.class,   InstantCodec.INSTANCE);
        r.register(BigInteger.class, BigIntegerCodec.INSTANCE);
        r.register(BigDecimal.class, BigDecimalCodec.INSTANCE);
        // java.time types
        r.register(LocalDate.class,      LocalDateCodec.INSTANCE);
        r.register(LocalTime.class,      LocalTimeCodec.INSTANCE);
        r.register(LocalDateTime.class,  LocalDateTimeCodec.INSTANCE);
        r.register(Duration.class,       DurationCodec.INSTANCE);
        r.register(OffsetDateTime.class, OffsetDateTimeCodec.INSTANCE);
        r.register(ZonedDateTime.class,  ZonedDateTimeCodec.INSTANCE);
        r.register(Period.class,         PeriodCodec.INSTANCE);
        // Primitive arrays
        r.register(int[].class,     PrimitiveArrayCodecs.INT);
        r.register(long[].class,    PrimitiveArrayCodecs.LONG);
        r.register(double[].class,  PrimitiveArrayCodecs.DOUBLE);
        r.register(float[].class,   PrimitiveArrayCodecs.FLOAT);
        r.register(short[].class,   PrimitiveArrayCodecs.SHORT);
        r.register(char[].class,    PrimitiveArrayCodecs.CHAR);
        r.register(boolean[].class, PrimitiveArrayCodecs.BOOLEAN);
        return new Codecs(r);
    }

    // ── Registration ─────────────────────────────────────────────────────────

    /** Registers (or replaces) the codec for {@code type}. */
    public <T> void register(Class<T> type, Codec<T> codec) {
        registry.register(type, codec);
    }

    /**
     * Returns the codec registered for {@code type}.
     * Throws {@link IllegalArgumentException} if none is registered — use
     * {@link #register} or {@link #reflect} to add one first.
     */
    public <T> Codec<T> lookup(Class<T> type) {
        Codec<T> c = registry.lookup(type);
        if (c == null) {
            throw new IllegalArgumentException(
                    "No codec registered for " + type.getName() + ". "
                    + "Use register() for a hand-crafted codec, or reflect() for a prototype auto-codec.");
        }
        return c;
    }

    /**
     * Creates, caches, and returns a reflection-generated codec for {@code type}.
     *
     * <p><strong>Intended for prototyping / lazy-developer use only.</strong>
     * Production code should use {@link CompositeCodec} explicitly. Caveats:
     *
     * <ul>
     *   <li><b>Wire format stability:</b> for regular classes, fields are encoded in
     *       <em>alphabetical order within each level of the class hierarchy</em> (parent
     *       fields first). For records, component declaration order is used. Any rename,
     *       addition, or removal of a field <em>silently changes the binary layout</em> and
     *       will corrupt existing encoded data.</li>
     *   <li><b>Construction requirements:</b> regular classes must have a reachable no-arg
     *       constructor and non-final, non-static, non-transient fields. Records are fully
     *       supported via their canonical constructor and do not need a no-arg constructor.</li>
     *   <li><b>Module encapsulation:</b> may throw {@code InaccessibleObjectException}
     *       under strong module encapsulation without {@code --add-opens}.</li>
     *   <li><b>Unsupported field types:</b> object arrays (e.g. {@code String[]},
     *       {@code MyPojo[]}) cause an {@link IllegalArgumentException}; use
     *       {@code List<T>} instead. Primitive arrays ({@code int[]}, {@code long[]}, etc.)
     *       and {@code byte[]} are fully supported. Unbound type variables and unknown
     *       parameterized types also throw at codec-construction time.</li>
     *   <li><b>Thread safety:</b> call {@code reflect()} at startup before concurrent
     *       access begins — do not call it from multiple threads simultaneously for the
     *       same type for the first time.</li>
     * </ul>
     */
    public <T> Codec<T> reflect(Class<T> type) {
        Codec<T> existing = registry.lookup(type);
        if (existing != null) return existing;
        // ReflectiveCodec registers itself in the registry during construction so that
        // circular field references find it rather than recursing infinitely.
        return new ReflectiveCodec<>(type, registry);
    }

    // ── Any registered type — byte[] ─────────────────────────────────────────

    /**
     * Encodes {@code obj} to a freshly allocated byte array using the codec registered
     * for {@code type}.
     *
     * <p>Note: {@link ByteBuffer#allocate} backs the buffer with a single {@code byte[]}
     * and {@link ByteBuffer#array()} returns that same backing array — no copy is made.
     */
    public <T> byte[] encode(Class<T> type, T obj) {
        return encodeToBuffer(type, obj).array();
    }

    /**
     * Encodes {@code obj} to a freshly allocated, ready-to-read {@link ByteBuffer}
     * (position=0, limit=encoded length) using the codec registered for {@code type}.
     *
     * <p>Use this when the caller needs a {@code ByteBuffer} directly — e.g. to feed
     * into another buffer operation — without an intermediate {@code byte[]} copy.
     */
    public <T> ByteBuffer encodeToBuffer(Class<T> type, T obj) {
        Codec<T> c = lookup(type);
        ByteBuffer buf = ByteBuffer.allocate(c.sizeOf(obj));
        c.serialize(obj, buf);
        buf.flip();
        return buf;
    }

    /** Decodes a value of {@code type} from {@code bytes}. */
    public <T> T decode(Class<T> type, byte[] bytes) {
        return lookup(type).deserialize(ByteBuffer.wrap(bytes));
    }

    // ── Any registered type — ByteBuffer ─────────────────────────────────────

    /**
     * Serializes {@code obj} into {@code buf} using the codec registered for {@code type}.
     * The buffer must have at least {@code sizeOf(type, obj)} bytes remaining.
     */
    public <T> void serialize(Class<T> type, T obj, ByteBuffer buf) {
        lookup(type).serialize(obj, buf);
    }

    /** Deserializes a value of {@code type} from {@code buf}. */
    public <T> T deserialize(Class<T> type, ByteBuffer buf) {
        return lookup(type).deserialize(buf);
    }

    /**
     * Returns the number of bytes that {@link #serialize(Class, Object, ByteBuffer)} will
     * write for {@code obj}. Use this to pre-size a shared buffer before writing.
     */
    public <T> int sizeOf(Class<T> type, T obj) {
        return lookup(type).sizeOf(obj);
    }

    // ── Primitives and scalars — ByteBuffer only ─────────────────────────────
    // These bypass the registry: no lookup, no boxing.
    // Fixed wire sizes (bytes): byte=1  short=2  int=4  long=8  float=4
    //                           double=8  boolean=1  char=2  UUID=16  Instant=12
    // Variable: String (2+utf8Len)  byte[] (4+length)

    public void  serializeByte(byte v, ByteBuffer buf)    { buf.put(v); }
    public byte  deserializeByte(ByteBuffer buf)          { return buf.get(); }

    public void  serializeShort(short v, ByteBuffer buf)  { buf.putShort(v); }
    public short deserializeShort(ByteBuffer buf)         { return buf.getShort(); }

    public void  serializeInt(int v, ByteBuffer buf)      { buf.putInt(v); }
    public int   deserializeInt(ByteBuffer buf)           { return buf.getInt(); }

    public void  serializeLong(long v, ByteBuffer buf)    { buf.putLong(v); }
    public long  deserializeLong(ByteBuffer buf)          { return buf.getLong(); }

    public void  serializeFloat(float v, ByteBuffer buf)  { buf.putFloat(v); }
    public float deserializeFloat(ByteBuffer buf)         { return buf.getFloat(); }

    public void   serializeDouble(double v, ByteBuffer buf) { buf.putDouble(v); }
    public double deserializeDouble(ByteBuffer buf)         { return buf.getDouble(); }

    public void    serializeBoolean(boolean v, ByteBuffer buf) { buf.put(v ? (byte) 1 : (byte) 0); }
    public boolean deserializeBoolean(ByteBuffer buf)          { return buf.get() != 0; }

    public void serializeChar(char v, ByteBuffer buf) { buf.putChar(v); }
    public char deserializeChar(ByteBuffer buf)       { return buf.getChar(); }

    public void   serializeString(String s, ByteBuffer buf) { StringCodec.INSTANCE.serialize(s, buf); }
    public int    sizeOfString(String s)                    { return StringCodec.INSTANCE.sizeOf(s); }
    public String deserializeString(ByteBuffer buf)         { return StringCodec.INSTANCE.deserialize(buf); }

    public void   serializeBytes(byte[] v, ByteBuffer buf)  { ByteArrayCodec.INSTANCE.serialize(v, buf); }
    public int    sizeOfBytes(byte[] v)                     { return ByteArrayCodec.INSTANCE.sizeOf(v); }
    public byte[] deserializeBytes(ByteBuffer buf)          { return ByteArrayCodec.INSTANCE.deserialize(buf); }

    public void serializeUuid(UUID v, ByteBuffer buf)          { UuidCodec.INSTANCE.serialize(v, buf); }
    public UUID deserializeUuid(ByteBuffer buf)                { return UuidCodec.INSTANCE.deserialize(buf); }

    public void    serializeInstant(Instant v, ByteBuffer buf) { InstantCodec.INSTANCE.serialize(v, buf); }
    public Instant deserializeInstant(ByteBuffer buf)          { return InstantCodec.INSTANCE.deserialize(buf); }
}
