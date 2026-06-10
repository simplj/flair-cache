package com.simplj.flair.cache.serial.codecs;

import com.simplj.flair.cache.serial.Codec;

import java.nio.ByteBuffer;

/**
 * Codecs for all Java primitive array types. Wire format for each:
 * count(4) + elements (each element at its native width).
 *
 * <p>{@code null} arrays serialize as count=0 and deserialize as a zero-length array.
 *
 * <table>
 *   <tr><th>Field</th><th>Element bytes</th><th>Total for n elements</th></tr>
 *   <tr><td>{@link #INT}</td><td>4</td><td>4 + n×4</td></tr>
 *   <tr><td>{@link #LONG}</td><td>8</td><td>4 + n×8</td></tr>
 *   <tr><td>{@link #DOUBLE}</td><td>8</td><td>4 + n×8</td></tr>
 *   <tr><td>{@link #FLOAT}</td><td>4</td><td>4 + n×4</td></tr>
 *   <tr><td>{@link #SHORT}</td><td>2</td><td>4 + n×2</td></tr>
 *   <tr><td>{@link #CHAR}</td><td>2</td><td>4 + n×2</td></tr>
 *   <tr><td>{@link #BOOLEAN}</td><td>1</td><td>4 + n×1</td></tr>
 * </table>
 *
 * <p>For {@code byte[]}, use {@link ByteArrayCodec} — it uses a uint32 length prefix and
 * is already registered in {@code Codecs.standard()}.
 */
public final class PrimitiveArrayCodecs {

    private PrimitiveArrayCodecs() {}

    public static final Codec<int[]> INT = new Codec<>() {
        @Override public void serialize(int[] arr, ByteBuffer buf) {
            if (arr == null) { buf.putInt(0); return; }
            buf.putInt(arr.length);
            for (int v : arr) buf.putInt(v);
        }
        @Override public int sizeOf(int[] arr) { return 4 + (arr == null ? 0 : arr.length * 4); }
        @Override public int[] deserialize(ByteBuffer buf) {
            int[] arr = new int[buf.getInt()];
            for (int i = 0; i < arr.length; i++) arr[i] = buf.getInt();
            return arr;
        }
    };

    public static final Codec<long[]> LONG = new Codec<>() {
        @Override public void serialize(long[] arr, ByteBuffer buf) {
            if (arr == null) { buf.putInt(0); return; }
            buf.putInt(arr.length);
            for (long v : arr) buf.putLong(v);
        }
        @Override public int sizeOf(long[] arr) { return 4 + (arr == null ? 0 : arr.length * 8); }
        @Override public long[] deserialize(ByteBuffer buf) {
            long[] arr = new long[buf.getInt()];
            for (int i = 0; i < arr.length; i++) arr[i] = buf.getLong();
            return arr;
        }
    };

    public static final Codec<double[]> DOUBLE = new Codec<>() {
        @Override public void serialize(double[] arr, ByteBuffer buf) {
            if (arr == null) { buf.putInt(0); return; }
            buf.putInt(arr.length);
            for (double v : arr) buf.putDouble(v);
        }
        @Override public int sizeOf(double[] arr) { return 4 + (arr == null ? 0 : arr.length * 8); }
        @Override public double[] deserialize(ByteBuffer buf) {
            double[] arr = new double[buf.getInt()];
            for (int i = 0; i < arr.length; i++) arr[i] = buf.getDouble();
            return arr;
        }
    };

    public static final Codec<float[]> FLOAT = new Codec<>() {
        @Override public void serialize(float[] arr, ByteBuffer buf) {
            if (arr == null) { buf.putInt(0); return; }
            buf.putInt(arr.length);
            for (float v : arr) buf.putFloat(v);
        }
        @Override public int sizeOf(float[] arr) { return 4 + (arr == null ? 0 : arr.length * 4); }
        @Override public float[] deserialize(ByteBuffer buf) {
            float[] arr = new float[buf.getInt()];
            for (int i = 0; i < arr.length; i++) arr[i] = buf.getFloat();
            return arr;
        }
    };

    public static final Codec<short[]> SHORT = new Codec<>() {
        @Override public void serialize(short[] arr, ByteBuffer buf) {
            if (arr == null) { buf.putInt(0); return; }
            buf.putInt(arr.length);
            for (short v : arr) buf.putShort(v);
        }
        @Override public int sizeOf(short[] arr) { return 4 + (arr == null ? 0 : arr.length * 2); }
        @Override public short[] deserialize(ByteBuffer buf) {
            short[] arr = new short[buf.getInt()];
            for (int i = 0; i < arr.length; i++) arr[i] = buf.getShort();
            return arr;
        }
    };

    public static final Codec<char[]> CHAR = new Codec<>() {
        @Override public void serialize(char[] arr, ByteBuffer buf) {
            if (arr == null) { buf.putInt(0); return; }
            buf.putInt(arr.length);
            for (char v : arr) buf.putChar(v);
        }
        @Override public int sizeOf(char[] arr) { return 4 + (arr == null ? 0 : arr.length * 2); }
        @Override public char[] deserialize(ByteBuffer buf) {
            char[] arr = new char[buf.getInt()];
            for (int i = 0; i < arr.length; i++) arr[i] = buf.getChar();
            return arr;
        }
    };

    public static final Codec<boolean[]> BOOLEAN = new Codec<>() {
        @Override public void serialize(boolean[] arr, ByteBuffer buf) {
            if (arr == null) { buf.putInt(0); return; }
            buf.putInt(arr.length);
            for (boolean v : arr) buf.put(v ? (byte) 1 : (byte) 0);
        }
        @Override public int sizeOf(boolean[] arr) { return 4 + (arr == null ? 0 : arr.length); }
        @Override public boolean[] deserialize(ByteBuffer buf) {
            boolean[] arr = new boolean[buf.getInt()];
            for (int i = 0; i < arr.length; i++) arr[i] = buf.get() != 0;
            return arr;
        }
    };
}
