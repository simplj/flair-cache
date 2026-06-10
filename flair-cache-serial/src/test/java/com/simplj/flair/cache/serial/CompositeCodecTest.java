package com.simplj.flair.cache.serial;

import com.simplj.flair.cache.serial.codecs.*;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class CompositeCodecTest {

    // ── Sample POJO with a fluent builder ─────────────────────────────────────

    static final class Product {
        private final String id;
        private final String name;
        private final double price;
        private final int stock;
        private final Optional<String> description;
        private final List<String> tags;

        private Product(Builder b) {
            this.id = b.id;
            this.name = b.name;
            this.price = b.price;
            this.stock = b.stock;
            this.description = b.description;
            this.tags = b.tags;
        }

        String getId()                    { return id; }
        String getName()                  { return name; }
        double getPrice()                 { return price; }
        int getStock()                    { return stock; }
        Optional<String> getDescription() { return description; }
        List<String> getTags()            { return tags; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Product)) return false;
            Product p = (Product) o;
            return Double.compare(p.price, price) == 0
                    && stock == p.stock
                    && Objects.equals(id, p.id)
                    && Objects.equals(name, p.name)
                    && Objects.equals(description, p.description)
                    && Objects.equals(tags, p.tags);
        }

        @Override public int hashCode() {
            return Objects.hash(id, name, price, stock, description, tags);
        }

        static Builder builder() { return new Builder(); }

        static final class Builder {
            private String id = "";
            private String name = "";
            private double price;
            private int stock;
            private Optional<String> description = Optional.empty();
            private List<String> tags = List.of();

            Builder id(String v)                     { this.id = v; return this; }
            Builder name(String v)                   { this.name = v; return this; }
            Builder price(double v)                  { this.price = v; return this; }
            Builder stock(int v)                     { this.stock = v; return this; }
            Builder description(Optional<String> v)  { this.description = v; return this; }
            Builder tags(List<String> v)             { this.tags = v; return this; }
            Product build()                          { return new Product(this); }
        }
    }

    // ── Codec definition ──────────────────────────────────────────────────────

    private static final Codec<Product> PRODUCT_CODEC = CompositeCodec.of(Product.class)
            .field(StringCodec.INSTANCE,
                    Product::getId,
                    Product.Builder::id)
            .field(StringCodec.INSTANCE,
                    Product::getName,
                    Product.Builder::name)
            .field(PrimitiveCodecs.DOUBLE,
                    Product::getPrice,
                    Product.Builder::price)
            .field(PrimitiveCodecs.INT,
                    Product::getStock,
                    Product.Builder::stock)
            .field(new OptionalCodec<>(StringCodec.INSTANCE),
                    Product::getDescription,
                    Product.Builder::description)
            .field(new ListCodec<>(StringCodec.INSTANCE),
                    Product::getTags,
                    Product.Builder::tags)
            .build(Product::builder, Product.Builder::build);

    // ── helpers ────────────────────────────────────────────────────────────────

    private static Product roundTrip(Product p) {
        int size = PRODUCT_CODEC.sizeOf(p);
        ByteBuffer buf = ByteBuffer.allocate(size);
        PRODUCT_CODEC.serialize(p, buf);
        buf.flip();
        return PRODUCT_CODEC.deserialize(buf);
    }

    // ── tests ─────────────────────────────────────────────────────────────────

    @Test
    void fullProduct_roundTrip() {
        Product original = Product.builder()
                .id("prod-001")
                .name("Widget")
                .price(9.99)
                .stock(100)
                .description(Optional.of("A fine widget"))
                .tags(List.of("sale", "featured"))
                .build();

        assertEquals(original, roundTrip(original));
    }

    @Test
    void product_withEmptyOptionalAndTags() {
        Product original = Product.builder()
                .id("prod-002")
                .name("Gadget")
                .price(0.0)
                .stock(0)
                .description(Optional.empty())
                .tags(List.of())
                .build();

        assertEquals(original, roundTrip(original));
    }

    @Test
    void product_extremeValues() {
        Product original = Product.builder()
                .id("X".repeat(100))
                .name("")
                .price(Double.MAX_VALUE)
                .stock(Integer.MIN_VALUE)
                .description(Optional.of(""))
                .tags(List.of("a", "b", "c", "d", "e"))
                .build();

        assertEquals(original, roundTrip(original));
    }

    @Test
    void sizeOf_matchesActualBytesWritten() {
        Product p = Product.builder()
                .id("abc")
                .name("xyz")
                .price(1.5)
                .stock(10)
                .description(Optional.of("desc"))
                .tags(List.of("t1", "t2"))
                .build();

        int declared = PRODUCT_CODEC.sizeOf(p);
        ByteBuffer buf = ByteBuffer.allocate(declared);
        PRODUCT_CODEC.serialize(p, buf);
        assertEquals(declared, buf.position(), "sizeOf must match bytes written");
    }

    @Test
    void deserialize_consumesExactBytes() {
        Product p = Product.builder()
                .id("id")
                .name("name")
                .price(3.14)
                .stock(7)
                .description(Optional.empty())
                .tags(List.of("tag"))
                .build();

        int size = PRODUCT_CODEC.sizeOf(p);
        ByteBuffer buf = ByteBuffer.allocate(size);
        PRODUCT_CODEC.serialize(p, buf);
        buf.flip();
        PRODUCT_CODEC.deserialize(buf);
        assertFalse(buf.hasRemaining(), "deserialize left unconsumed bytes");
    }
}
