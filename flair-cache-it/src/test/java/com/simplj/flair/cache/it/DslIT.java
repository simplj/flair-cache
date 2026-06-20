package com.simplj.flair.cache.it;

import com.simplj.flair.cache.FlairCache;
import com.simplj.flair.cache.dsl.Aggregators;
import com.simplj.flair.cache.dsl.Order;
import com.simplj.flair.cache.dsl.QueryEngine;
import com.simplj.flair.cache.replication.ConsistencyMode;
import com.simplj.flair.cache.store.CacheBlock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.simplj.flair.cache.it.ITSupport.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Scenarios 7, 8, 9 — DSL: filter, join, and aggregate queries.
 *
 * <p>All tests run on a single node — DSL queries are local in-memory operations and
 * do not require network replication.</p>
 *
 * <p>Data model: values are stored as CSV strings {@code "field1,field2,..."}
 * so the DSL decoder can parse them without a custom codec. Example for products:
 * {@code "productId,name,category,price"} — e.g. {@code "p1,Laptop,Electronics,999.99"}.</p>
 */
@Timeout(value = 30, unit = TimeUnit.SECONDS)
class DslIT {

    private FlairCache cache;

    @AfterEach
    void tearDown() {
        if (cache != null) {
            cache.shutdown();
            cache = null;
        }
    }

    // ── Scenario 7: Filter ────────────────────────────────────────────────────

    @Test
    void filter_whereAndOrOrderByLimitFetch_returnsCorrectResults() throws IOException {
        cache = singleNode();

        CacheBlock<String, String> block = cache.<String, String>registerBlock("words")
                .keyCodec(STRING_CODEC)
                .valueCodec(STRING_CODEC)
                .build();

        String[] words = {"apple", "avocado", "banana", "blueberry", "cherry", "apricot",
                          "date", "elderberry", "fig", "grape"};
        for (String w : words) {
            block.put(w, w);
        }

        // where: starts with "a" OR starts with "b"
        // orderBy: natural order ASC
        // limit: 4
        @SuppressWarnings("unchecked")
        List<String> results = (List<String>) cache.query()
                .from("words", String.class, v -> (String) v)
                .where(s -> s.startsWith("a"))
                .or(s -> s.startsWith("b"))
                .orderBy(s -> s, Order.ASC)
                .limit(4)
                .fetch();

        assertEquals(4, results.size(), "limit(4) must return exactly 4 results");
        assertEquals("apple",     results.get(0));
        assertEquals("apricot",   results.get(1));
        assertEquals("avocado",   results.get(2));
        assertEquals("banana",    results.get(3));
        // "blueberry" is the 5th match and must be cut off by limit(4)
        assertFalse(results.contains("blueberry"), "blueberry must be cut off by limit");
    }

    @Test
    void filter_andChain_narrowsResultSet() throws IOException {
        cache = singleNode();

        CacheBlock<String, String> block = cache.<String, String>registerBlock("andtest")
                .keyCodec(STRING_CODEC)
                .valueCodec(STRING_CODEC)
                .build();

        block.put("ant",  "ant");
        block.put("arm",  "arm");
        block.put("arc",  "arc");
        block.put("art",  "art");
        block.put("bear", "bear");

        // where: starts with "a" AND contains "r"
        @SuppressWarnings("unchecked")
        List<String> results = (List<String>) cache.query()
                .from("andtest", String.class, v -> (String) v)
                .where(s -> s.startsWith("a"))
                .and(s -> s.contains("r"))
                .fetch();

        assertEquals(3, results.size(), "arm, arc, art must match");
        assertTrue(results.containsAll(List.of("arm", "arc", "art")));
        assertFalse(results.contains("ant"),  "ant does not contain 'r'");
        assertFalse(results.contains("bear"), "bear does not start with 'a'");
    }

    // ── Scenario 8: Join ──────────────────────────────────────────────────────

    @Test
    void join_matchedPairsReturned_unmatchedDropped() throws IOException {
        cache = singleNode();

        // Left block: "productId,name"
        CacheBlock<String, String> products = cache.<String, String>registerBlock("products")
                .keyCodec(STRING_CODEC)
                .valueCodec(STRING_CODEC)
                .build();
        products.put("p1", "p1,Laptop");
        products.put("p2", "p2,Phone");
        products.put("p3", "p3,Tablet");   // no matching review

        // Right block: "productId,score"
        CacheBlock<String, String> reviews = cache.<String, String>registerBlock("reviews")
                .keyCodec(STRING_CODEC)
                .valueCodec(STRING_CODEC)
                .build();
        reviews.put("p1", "p1,4.5");
        reviews.put("p2", "p2,3.8");
        // p3 has no review — unmatched on the right side

        // Hash join on the productId field (index 0 in the CSV)
        @SuppressWarnings("unchecked")
        List<String> joined = (List<String>) cache.query()
                .from("products", String.class, v -> (String) v)
                .join("reviews", String.class, v -> (String) v)
                .on(l -> l.split(",")[0], r -> r.split(",")[0])   // join on productId
                .select((l, r) -> l.split(",")[1] + "=" + r.split(",")[1]) // "name=score"
                .fetch();

        assertEquals(2, joined.size(), "only p1 and p2 have matching reviews");
        assertTrue(joined.contains("Laptop=4.5"), "p1 join result must be present");
        assertTrue(joined.contains("Phone=3.8"),  "p2 join result must be present");
        // p3 has no right side — must be absent (inner join semantics)
        assertFalse(joined.stream().anyMatch(s -> s.startsWith("Tablet")),
                "Tablet (p3) must be absent — no matching review");
    }

    @Test
    void join_nestedLoop_arbitraryPredicateWorks() throws IOException {
        cache = singleNode();

        CacheBlock<String, Integer> left = cache.<String, Integer>registerBlock("nl-left")
                .keyCodec(STRING_CODEC)
                .valueCodec(INT_CODEC)
                .build();
        left.put("a", 10);
        left.put("b", 20);
        left.put("c", 30);

        CacheBlock<String, Integer> right = cache.<String, Integer>registerBlock("nl-right")
                .keyCodec(STRING_CODEC)
                .valueCodec(INT_CODEC)
                .build();
        right.put("x", 15);
        right.put("y", 25);

        // Nested-loop join: match pairs where left + right > 35
        // Pairs: (10,15)=25, (10,25)=35, (20,15)=35, (20,25)=45✓, (30,15)=45✓, (30,25)=55✓
        @SuppressWarnings("unchecked")
        List<Integer> sums = (List<Integer>) cache.query()
                .from("nl-left", Integer.class, v -> (Integer) v)
                .join("nl-right", Integer.class, v -> (Integer) v)
                .on((l, r) -> l + r > 35)
                .select(Integer::sum)
                .fetch();

        assertEquals(3, sums.size(), "exactly 3 pairs sum to > 35");
        assertTrue(sums.contains(45), "20+25=45 must be in results");
        assertTrue(sums.contains(45), "30+15=45 must be in results");
        assertTrue(sums.contains(55), "30+25=55 must be in results");
    }

    // ── Scenario 9: Aggregate ─────────────────────────────────────────────────

    @Test
    void aggregate_groupByCount_correctCountsPerGroup() throws IOException {
        cache = singleNode();

        // Value format: "category,price"
        CacheBlock<String, String> block = cache.<String, String>registerBlock("agg-count")
                .keyCodec(STRING_CODEC)
                .valueCodec(STRING_CODEC)
                .build();

        block.put("p1", "Electronics,999");
        block.put("p2", "Electronics,499");
        block.put("p3", "Clothing,49");
        block.put("p4", "Clothing,89");
        block.put("p5", "Clothing,29");
        block.put("p6", "Books,15");

        Map<String, Long> counts = cache.query()
                .from("agg-count", String.class, v -> (String) v)
                .groupBy(s -> s.split(",")[0])
                .aggregate(Aggregators.count());

        assertEquals(3, counts.size(), "must have 3 distinct categories");
        assertEquals(2L, counts.get("Electronics"), "Electronics count");
        assertEquals(3L, counts.get("Clothing"),    "Clothing count");
        assertEquals(1L, counts.get("Books"),        "Books count");
    }

    @Test
    void aggregate_groupBySum_correctSumsPerGroup() throws IOException {
        cache = singleNode();

        CacheBlock<String, String> block = cache.<String, String>registerBlock("agg-sum")
                .keyCodec(STRING_CODEC)
                .valueCodec(STRING_CODEC)
                .build();

        block.put("p1", "Electronics,100.0");
        block.put("p2", "Electronics,200.0");
        block.put("p3", "Clothing,50.0");
        block.put("p4", "Clothing,75.0");

        Map<String, Double> sums = cache.query()
                .from("agg-sum", String.class, v -> (String) v)
                .groupBy(s -> s.split(",")[0])
                .aggregate(Aggregators.sum(s -> Double.parseDouble(s.split(",")[1])));

        assertEquals(300.0, sums.get("Electronics"), 0.001, "Electronics sum");
        assertEquals(125.0, sums.get("Clothing"),    0.001, "Clothing sum");
    }

    @Test
    void aggregate_groupByAvg_correctAveragesPerGroup() throws IOException {
        cache = singleNode();

        CacheBlock<String, String> block = cache.<String, String>registerBlock("agg-avg")
                .keyCodec(STRING_CODEC)
                .valueCodec(STRING_CODEC)
                .build();

        block.put("p1", "A,10.0");
        block.put("p2", "A,20.0");
        block.put("p3", "A,30.0");
        block.put("p4", "B,100.0");
        block.put("p5", "B,200.0");

        Map<String, Double> avgs = cache.query()
                .from("agg-avg", String.class, v -> (String) v)
                .groupBy(s -> s.split(",")[0])
                .aggregate(Aggregators.avg(s -> Double.parseDouble(s.split(",")[1])));

        assertEquals(20.0,  avgs.get("A"), 0.001, "average of 10,20,30 must be 20");
        assertEquals(150.0, avgs.get("B"), 0.001, "average of 100,200 must be 150");
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static FlairCache singleNode() throws IOException {
        return FlairCache.builder()
                .bindAddress("127.0.0.1")
                .bindPort(freePort())
                .consistency(ConsistencyMode.EVENTUAL)
                .build()
                .start();
    }
}
