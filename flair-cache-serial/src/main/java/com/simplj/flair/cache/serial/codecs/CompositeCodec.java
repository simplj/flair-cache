package com.simplj.flair.cache.serial.codecs;

import com.simplj.flair.cache.serial.Codec;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Codec for user-defined POJOs. Fields are encoded in the order they are declared —
 * field order is part of the binary contract and must not change once deployed.
 *
 * <pre>{@code
 * Codec<Product> productCodec = CompositeCodec.of(Product.class)
 *     .field(StringCodec.INSTANCE,      Product::getId,    Product.Builder::id)
 *     .field(StringCodec.INSTANCE,      Product::getName,  Product.Builder::name)
 *     .field(PrimitiveCodecs.DOUBLE,    Product::getPrice, Product.Builder::price)
 *     .field(PrimitiveCodecs.INT,       Product::getStock, Product.Builder::stock)
 *     .build(Product.Builder::new, Product.Builder::build);
 * }</pre>
 */
public final class CompositeCodec<T, B> implements Codec<T> {

    @SuppressWarnings("rawtypes")
    private final List<FieldDef> fields;
    private final Supplier<B> builderFactory;
    private final Function<B, T> buildFn;

    @SuppressWarnings("rawtypes")
    private CompositeCodec(List<FieldDef> fields, Supplier<B> builderFactory, Function<B, T> buildFn) {
        this.fields = List.copyOf(fields);
        this.builderFactory = builderFactory;
        this.buildFn = buildFn;
    }

    public static <T> Accumulator<T> of(Class<T> ignored) {
        return new Accumulator<>();
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void serialize(T obj, ByteBuffer buf) {
        for (FieldDef field : fields) {
            field.serialize(obj, buf);
        }
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public int sizeOf(T obj) {
        int size = 0;
        for (FieldDef field : fields) {
            size += field.sizeOf(obj);
        }
        return size;
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public T deserialize(ByteBuffer buf) {
        B builder = builderFactory.get();
        for (FieldDef field : fields) {
            builder = (B) field.deserialize(buf, builder);
        }
        return buildFn.apply(builder);
    }

    private static final class FieldDef<T, B, F> {

        private final Codec<F> codec;
        private final Function<T, F> getter;
        private final BiFunction<B, F, B> setter;

        FieldDef(Codec<F> codec, Function<T, F> getter, BiFunction<B, F, B> setter) {
            this.codec = codec;
            this.getter = getter;
            this.setter = setter;
        }

        void serialize(T obj, ByteBuffer buf) {
            codec.serialize(getter.apply(obj), buf);
        }

        int sizeOf(T obj) {
            return codec.sizeOf(getter.apply(obj));
        }

        B deserialize(ByteBuffer buf, B builder) {
            return setter.apply(builder, codec.deserialize(buf));
        }
    }

    public static final class Accumulator<T> {

        @SuppressWarnings("rawtypes")
        private final List<FieldDef> fields = new ArrayList<>();

        private Accumulator() {}

        @SuppressWarnings({"unchecked", "rawtypes"})
        public <B, F> Accumulator<T> field(
                Codec<F> codec,
                Function<T, F> getter,
                BiFunction<B, F, B> setter) {
            fields.add(new FieldDef<>(codec, getter, setter));
            return this;
        }

        @SuppressWarnings({"unchecked", "rawtypes"})
        public <B> CompositeCodec<T, B> build(Supplier<B> builderFactory, Function<B, T> buildFn) {
            return new CompositeCodec<>(fields, builderFactory, buildFn);
        }
    }
}
