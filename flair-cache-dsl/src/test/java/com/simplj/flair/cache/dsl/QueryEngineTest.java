package com.simplj.flair.cache.dsl;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class QueryEngineTest {

    // ── test model ────────────────────────────────────────────────────────────

    private record Product(String id, String category, double price, int stock) {}
    private record Purchase(String id, String customerId, double total, String status) {}
    private record Customer(String id, String name) {}
    private record OrderSummary(String orderId, String customerName) {}

    // ── fixtures ──────────────────────────────────────────────────────────────

    private Map<String, Product>  products;
    private Map<String, Purchase> orders;
    private Map<String, Customer> customers;

    private QueryEngine engine;
    private DataRegistry registry;

    @BeforeEach
    void setUp() {
        products = new LinkedHashMap<>();
        products.put("p1", new Product("p1", "electronics", 800.0, 5));
        products.put("p2", new Product("p2", "electronics", 1200.0, 0));
        products.put("p3", new Product("p3", "books",        25.0,  10));
        products.put("p4", new Product("p4", "books",        35.0,  8));
        products.put("p5", new Product("p5", "clothing",    200.0, 3));

        customers = new LinkedHashMap<>();
        customers.put("c1", new Customer("c1", "Alice"));
        customers.put("c2", new Customer("c2", "Bob"));

        orders = new LinkedHashMap<>();
        orders.put("o1", new Purchase("o1", "c1", 600.0, "SHIPPED"));
        orders.put("o2", new Purchase("o2", "c2", 300.0, "PENDING"));
        orders.put("o3", new Purchase("o3", "c1", 900.0, "DELIVERED"));

        registry = new DataRegistry()
                .register("products",  products)
                .register("orders",    orders)
                .register("customers", customers);

        engine = QueryEngine.over(registry);
    }

    // ── where ─────────────────────────────────────────────────────────────────

    @Test
    void whereSinglePredicate() {
        List<Product> result = engine
                .from("products", Product.class, Decoder.identity())
                .where(p -> p.category().equals("books"))
                .fetch();

        assertEquals(2, result.size());
        assertTrue(result.stream().allMatch(p -> p.category().equals("books")));
    }

    @Test
    void whereAndPredicate() {
        List<Product> result = engine
                .from("products", Product.class, Decoder.identity())
                .where(p -> p.category().equals("electronics"))
                .and(p -> p.price() < 1000)
                .fetch();

        assertEquals(1, result.size());
        assertEquals("p1", result.get(0).id());
    }

    @Test
    void whereOrPredicate() {
        List<Product> result = engine
                .from("products", Product.class, Decoder.identity())
                .where(p -> p.category().equals("clothing"))
                .or(p -> p.price() < 30)
                .fetch();

        assertEquals(2, result.size());
        assertTrue(result.stream().anyMatch(p -> p.id().equals("p3")));
        assertTrue(result.stream().anyMatch(p -> p.id().equals("p5")));
    }

    @Test
    void whereNoMatchReturnsEmpty() {
        List<Product> result = engine
                .from("products", Product.class, Decoder.identity())
                .where(p -> p.price() > 99_999)
                .fetch();

        assertTrue(result.isEmpty());
    }

    // ── orderBy ───────────────────────────────────────────────────────────────

    @Test
    void orderByAsc() {
        List<Product> result = engine
                .from("products", Product.class, Decoder.identity())
                .orderBy(Product::price, Order.ASC)
                .fetch();

        assertEquals(5, result.size());
        for (int i = 0; i < result.size() - 1; i++) {
            assertTrue(result.get(i).price() <= result.get(i + 1).price());
        }
    }

    @Test
    void orderByDesc() {
        List<Product> result = engine
                .from("products", Product.class, Decoder.identity())
                .orderBy(Product::price, Order.DESC)
                .fetch();

        assertEquals(5, result.size());
        for (int i = 0; i < result.size() - 1; i++) {
            assertTrue(result.get(i).price() >= result.get(i + 1).price());
        }
    }

    // ── limit / offset ────────────────────────────────────────────────────────

    @Test
    void limitReturnsAtMostNEntries() {
        List<Product> result = engine
                .from("products", Product.class, Decoder.identity())
                .limit(2)
                .fetch();

        assertEquals(2, result.size());
    }

    @Test
    void offsetSkipsFirstNEntries() {
        List<Product> all    = engine.from("products", Product.class, Decoder.identity()).fetch();
        List<Product> paged  = engine
                .from("products", Product.class, Decoder.identity())
                .orderBy(Product::id, Order.ASC)
                .offset(2)
                .fetch();

        assertEquals(3, paged.size());
        assertEquals(all.stream().map(Product::id).sorted().skip(2).findFirst().orElse(null),
                     paged.get(0).id());
    }

    @Test
    void limitAndOffsetPagination() {
        List<Product> sorted = engine
                .from("products", Product.class, Decoder.identity())
                .orderBy(Product::price, Order.ASC)
                .fetch();

        List<Product> page2 = engine
                .from("products", Product.class, Decoder.identity())
                .orderBy(Product::price, Order.ASC)
                .offset(2)
                .limit(2)
                .fetch();

        assertEquals(2, page2.size());
        assertEquals(sorted.get(2).id(), page2.get(0).id());
        assertEquals(sorted.get(3).id(), page2.get(1).id());
    }

    // ── count / countMatching / findFirst ─────────────────────────────────────

    @Test
    void countWithoutPredicateIsOFast() {
        long count = engine.from("products", Product.class, Decoder.identity()).count();
        assertEquals(5L, count);
    }

    @Test
    void countWithPredicate() {
        long count = engine
                .from("products", Product.class, Decoder.identity())
                .where(p -> p.stock() == 0)
                .count();
        assertEquals(1L, count);
    }

    @Test
    void countMatchingIgnoresPagination() {
        // 4 products match price < 1000 (p1, p3, p4, p5)
        // limit(1) must constrain count() but not countMatching()
        SingleBlockQuery<Product> q = engine
                .from("products", Product.class, Decoder.identity())
                .where(p -> p.price() < 1000)
                .limit(1);

        assertEquals(4L, q.countMatching());
        assertEquals(1L, q.count());
    }

    @Test
    void countMatchingWithNoPredicateReturnsFullSize() {
        long total = engine
                .from("products", Product.class, Decoder.identity())
                .offset(2)
                .limit(1)
                .countMatching();

        // countMatching ignores offset/limit — returns full dataset size
        assertEquals(5L, total);
    }

    @Test
    void findFirstReturnsValue() {
        Optional<Product> first = engine
                .from("products", Product.class, Decoder.identity())
                .orderBy(Product::price, Order.ASC)
                .findFirst();

        assertTrue(first.isPresent());
        assertEquals(25.0, first.get().price());
    }

    @Test
    void findFirstOnEmptyResultReturnsEmpty() {
        Optional<Product> first = engine
                .from("products", Product.class, Decoder.identity())
                .where(p -> p.price() > 1_000_000)
                .findFirst();

        assertFalse(first.isPresent());
    }

    // ── summarize ─────────────────────────────────────────────────────────────

    @Test
    void summarizeCorrectStats() {
        SummaryStatistics stats = engine
                .from("products", Product.class, Decoder.identity())
                .where(p -> p.category().equals("electronics"))
                .summarize(Product::price);

        assertEquals(2L, stats.count());
        assertEquals(800.0 + 1200.0, stats.sum(), 0.001);
        assertEquals(800.0, stats.min(), 0.001);
        assertEquals(1200.0, stats.max(), 0.001);
        assertEquals(1000.0, stats.avg(), 0.001);
    }

    @Test
    void summarizeEmptyReturnsEmpty() {
        SummaryStatistics stats = engine
                .from("products", Product.class, Decoder.identity())
                .where(p -> false)
                .summarize(Product::price);

        assertSame(SummaryStatistics.EMPTY, stats);
    }

    // ── join ──────────────────────────────────────────────────────────────────

    @Test
    void joinHashJoinCorrectPairs() {
        List<OrderSummary> joined = engine
                .from("orders", Purchase.class, Decoder.identity())
                .<Customer>join("customers", Customer.class, Decoder.identity())
                .on(Purchase::customerId, Customer::id)
                .select((o, c) -> new OrderSummary(o.id(), c.name()))
                .fetch();

        assertEquals(3, joined.size());
        assertTrue(joined.stream().anyMatch(s -> s.orderId().equals("o1") && s.customerName().equals("Alice")));
        assertTrue(joined.stream().anyMatch(s -> s.orderId().equals("o3") && s.customerName().equals("Alice")));
        assertTrue(joined.stream().anyMatch(s -> s.orderId().equals("o2") && s.customerName().equals("Bob")));
    }

    @Test
    void joinNestedLoopCorrectPairs() {
        List<OrderSummary> joined = engine
                .from("orders", Purchase.class, Decoder.identity())
                .<Customer>join("customers", Customer.class, Decoder.identity())
                .on((o, c) -> o.customerId().equals(c.id()))
                .select((o, c) -> new OrderSummary(o.id(), c.name()))
                .fetch();

        assertEquals(3, joined.size());
    }

    @Test
    void joinWithWhereFiltersLeft() {
        List<OrderSummary> joined = engine
                .from("orders", Purchase.class, Decoder.identity())
                .<Customer>join("customers", Customer.class, Decoder.identity())
                .on(Purchase::customerId, Customer::id)
                .where(o -> o.total() > 500)
                .select((o, c) -> new OrderSummary(o.id(), c.name()))
                .fetch();

        assertEquals(2, joined.size());
        assertTrue(joined.stream().allMatch(s -> s.customerName().equals("Alice")));
    }

    @Test
    void joinWithNoMatchesReturnsEmpty() {
        orders.put("o99", new Purchase("o99", "cx_unknown", 1.0, "NEW"));

        List<OrderSummary> joined = engine
                .from("orders", Purchase.class, Decoder.identity())
                .<Customer>join("customers", Customer.class, Decoder.identity())
                .on(Purchase::customerId, Customer::id)
                .where(o -> o.customerId().equals("cx_unknown"))
                .select((o, c) -> new OrderSummary(o.id(), c.name()))
                .fetch();

        assertTrue(joined.isEmpty());
    }

    @Test
    void joinRequiresOnBeforeFetch() {
        // select() without on() must throw at fetch() time
        assertThrows(IllegalStateException.class, () ->
                engine.from("orders", Purchase.class, Decoder.identity())
                      .<Customer>join("customers", Customer.class, Decoder.identity())
                      .select((o, c) -> new OrderSummary(o.id(), c.name()))
                      .fetch());
    }

    @Test
    void hashJoinNullKeyDoesNotMatch() {
        // order with null customerId must not match any customer (SQL NULL semantics)
        Map<String, Purchase> ordersWithNull = new LinkedHashMap<>(orders);
        ordersWithNull.put("o_null", new Purchase("o_null", null, 50.0, "NEW"));

        DataRegistry reg = new DataRegistry()
                .register("ordersWithNull", ordersWithNull)
                .register("customersB",     customers);

        List<OrderSummary> result = QueryEngine.over(reg)
                .from("ordersWithNull", Purchase.class, Decoder.identity())
                .<Customer>join("customersB", Customer.class, Decoder.identity())
                .on(Purchase::customerId, Customer::id)
                .select((o, c) -> new OrderSummary(o.id(), c.name()))
                .fetch();

        assertTrue(result.stream().noneMatch(s -> s.orderId().equals("o_null")));
        assertEquals(3, result.size()); // o1, o2, o3 still match correctly
    }

    @Test
    void onStrategyReplacementClearsOtherStrategy() {
        // switching from BiPredicate to key-extractor must not leave joinCondition as extra filter
        List<OrderSummary> joined = engine
                .from("orders", Purchase.class, Decoder.identity())
                .<Customer>join("customers", Customer.class, Decoder.identity())
                .on((o, c) -> false)                            // nested-loop: matches nothing
                .on(Purchase::customerId, Customer::id)         // replace: hash-join takes over
                .select((o, c) -> new OrderSummary(o.id(), c.name()))
                .fetch();

        // if joinCondition were still active, result would be empty; correct result is 3
        assertEquals(3, joined.size());
    }

    // ── groupBy / aggregate ───────────────────────────────────────────────────

    @Test
    void groupByCountAllGroupsPresent() {
        Map<String, Long> countByCategory = engine
                .from("products", Product.class, Decoder.identity())
                .groupBy(Product::category)
                .aggregate(Aggregators.count());

        assertEquals(3, countByCategory.size());
        assertEquals(2L, countByCategory.get("electronics"));
        assertEquals(2L, countByCategory.get("books"));
        assertEquals(1L, countByCategory.get("clothing"));
    }

    @Test
    void groupBySumCorrect() {
        Map<String, Double> sumByCategory = engine
                .from("products", Product.class, Decoder.identity())
                .groupBy(Product::category)
                .aggregate(Aggregators.sum(Product::price));

        assertEquals(800.0 + 1200.0, sumByCategory.get("electronics"), 0.001);
        assertEquals(25.0 + 35.0,    sumByCategory.get("books"),       0.001);
        assertEquals(200.0,          sumByCategory.get("clothing"),    0.001);
    }

    @Test
    void groupByAvgCorrect() {
        Map<String, Double> avg = engine
                .from("products", Product.class, Decoder.identity())
                .groupBy(Product::category)
                .aggregate(Aggregators.avg(Product::price));

        assertEquals(1000.0, avg.get("electronics"), 0.001);
        assertEquals(30.0,   avg.get("books"),       0.001);
    }

    @Test
    void groupByWithWhereFiltersBeforeGrouping() {
        Map<String, Long> count = engine
                .from("products", Product.class, Decoder.identity())
                .where(p -> p.stock() > 0)
                .groupBy(Product::category)
                .aggregate(Aggregators.count());

        // p2 (electronics, stock=0) is excluded
        assertEquals(1L, count.get("electronics"));
        assertEquals(2L, count.get("books"));
    }

    // ── parallel ──────────────────────────────────────────────────────────────

    @Test
    void parallelProducesSameResultAsSequential() {
        List<Product> seq = engine
                .from("products", Product.class, Decoder.identity())
                .where(p -> p.price() < 1000)
                .orderBy(Product::id, Order.ASC)
                .fetch();

        List<Product> par = engine
                .from("products", Product.class, Decoder.identity())
                .parallel()
                .where(p -> p.price() < 1000)
                .orderBy(Product::id, Order.ASC)
                .fetch();

        assertEquals(seq, par);
    }

    @Test
    void parallelCountMatchesSequentialCount() {
        long seqCount = engine
                .from("products", Product.class, Decoder.identity())
                .where(p -> p.category().equals("books"))
                .count();

        long parCount = engine
                .from("products", Product.class, Decoder.identity())
                .parallel()
                .where(p -> p.category().equals("books"))
                .count();

        assertEquals(seqCount, parCount);
    }

    // ── empty source ──────────────────────────────────────────────────────────

    @Test
    void emptySourceFetchReturnsEmpty() {
        DataRegistry empty = new DataRegistry().register("empty", new HashMap<>());
        List<Product> result = QueryEngine.over(empty)
                .from("empty", Product.class, Decoder.identity())
                .where(p -> true)
                .fetch();

        assertTrue(result.isEmpty());
    }

    @Test
    void emptySourceCountReturnsZero() {
        DataRegistry empty = new DataRegistry().register("empty", new HashMap<>());
        long count = QueryEngine.over(empty)
                .from("empty", Product.class, Decoder.identity())
                .count();

        assertEquals(0L, count);
    }

    @Test
    void emptySourceFindFirstReturnsEmpty() {
        DataRegistry empty = new DataRegistry().register("empty", new HashMap<>());
        Optional<Product> first = QueryEngine.over(empty)
                .from("empty", Product.class, Decoder.identity())
                .findFirst();

        assertFalse(first.isPresent());
    }

    // ── single-map engine mode ─────────────────────────────────────────────────

    @Test
    void singleMapEngineWorksWithAnyFromName() {
        QueryEngine singleEngine = QueryEngine.over(products);
        List<Product> result = singleEngine
                .from("any_label", Product.class, Decoder.identity())
                .where(p -> p.category().equals("books"))
                .fetch();

        assertEquals(2, result.size());
    }

    @Test
    void singleMapEngineJoinThrowsUnsupported() {
        QueryEngine singleEngine = QueryEngine.over(products);
        SingleBlockQuery<Product> q = singleEngine
                .from("x", Product.class, Decoder.identity());

        assertThrows(UnsupportedOperationException.class,
                () -> q.join("other", Customer.class, Decoder.identity()));
    }

    // ── fetchResult metadata ──────────────────────────────────────────────────

    @Test
    void fetchResultContainsCorrectItems() {
        QueryResult<Product> result = engine
                .from("products", Product.class, Decoder.identity())
                .where(p -> p.category().equals("electronics"))
                .fetchResult();

        assertEquals(2, result.size());
        assertTrue(result.executionNanos() >= 0);
    }

    @Test
    void fetchResultSizeMatchesFetch() {
        List<Product> direct = engine
                .from("products", Product.class, Decoder.identity())
                .where(p -> p.price() < 500)
                .limit(2)
                .fetch();

        QueryResult<Product> result = engine
                .from("products", Product.class, Decoder.identity())
                .where(p -> p.price() < 500)
                .limit(2)
                .fetchResult();

        assertEquals(direct.size(), result.size());
    }

    // ── DataRegistry: duplicate registration and replacement ─────────────────

    @Test
    void dataRegistryDuplicateRegistrationThrows() {
        DataRegistry reg = new DataRegistry().register("x", products);
        assertThrows(IllegalStateException.class, () -> reg.register("x", products));
    }

    @Test
    void dataRegistryDuplicateCollectionRegistrationThrows() {
        List<Product> list = new ArrayList<>(products.values());
        DataRegistry reg = new DataRegistry().register("items", list);
        assertThrows(IllegalStateException.class, () -> reg.register("items", list));
    }

    @Test
    void dataRegistryRegisterOrReplaceUpdates() {
        Map<String, Product> replacement = new LinkedHashMap<>();
        replacement.put("pX", new Product("pX", "other", 50.0, 1));

        DataRegistry reg = new DataRegistry()
                .register("items", products)
                .registerOrReplace("items", replacement);

        long count = QueryEngine.over(reg)
                .from("items", Product.class, Decoder.identity())
                .count();

        assertEquals(1L, count);
    }

    // ── Decoder.typed ─────────────────────────────────────────────────────────

    @Test
    void decoderTypedAcceptsCorrectType() {
        List<Product> result = engine
                .from("products", Product.class, Decoder.typed(Product.class))
                .where(p -> p.category().equals("books"))
                .fetch();

        assertEquals(2, result.size());
    }

    @Test
    void decoderTypedRejectsWrongType() {
        Map<String, Object> mixed = new LinkedHashMap<>();
        mixed.put("k1", new Product("p1", "cat", 10.0, 1));
        mixed.put("k2", "not-a-product");

        DataRegistry reg = new DataRegistry().register("mixed", mixed);

        assertThrows(ClassCastException.class, () ->
                QueryEngine.over(reg)
                           .from("mixed", Product.class, Decoder.typed(Product.class))
                           .fetch());
    }

    // ── immutable builder isolation ───────────────────────────────────────────

    @Test
    void immutableBuilderDoesNotAffectIndependentChains() {
        // Base query: products under 1000 → 4 entries (p1, p3, p4, p5)
        SingleBlockQuery<Product> base = engine
                .from("products", Product.class, Decoder.identity())
                .where(p -> p.price() < 1000);

        // Derive two independent queries — neither should affect the other or the base
        List<Product> electronics = base.where(p -> p.category().equals("electronics")).fetch();
        List<Product> books       = base.where(p -> p.category().equals("books")).fetch();

        assertEquals(4L, base.count());
        assertEquals(1,  electronics.size()); // only p1 (electronics, 800)
        assertEquals(2,  books.size());       // p3 (25) and p4 (35)
    }

    @Test
    void immutableBuilderGroupByNotAffectedByLaterChaining() {
        // groupBy() captures a snapshot; subsequent changes on the base must not bleed in
        SingleBlockQuery<Product> base = engine
                .from("products", Product.class, Decoder.identity())
                .where(p -> p.stock() > 0);

        GroupByQuery<Product, String> gbq = base.groupBy(Product::category);

        // Further where() on base does NOT return this; gbq is unaffected
        List<Product> furtherFiltered = base.where(p -> p.price() > 100).fetch();

        Map<String, Long> grouped = gbq.aggregate(Aggregators.count());

        // gbq should still reflect only stock>0, not the extra price>100 filter
        assertEquals(1L, grouped.get("electronics")); // p2 excluded (stock=0), p1 included
        assertEquals(2L, grouped.get("books"));       // p3 and p4 both have stock > 0
        assertEquals(2, furtherFiltered.size());      // p1(800,stock>0) + p5(200,stock>0)
    }

    // ── regression: review fixes ──────────────────────────────────────────────

    @Test
    void joinRespectsWherePredicate() {
        List<OrderSummary> joined = engine
                .from("orders", Purchase.class, Decoder.identity())
                .where(o -> o.total() > 500)   // only o1 (600) and o3 (900)
                .<Customer>join("customers", Customer.class, Decoder.identity())
                .on(Purchase::customerId, Customer::id)
                .select((o, c) -> new OrderSummary(o.id(), c.name()))
                .fetch();

        assertEquals(2, joined.size());
        assertTrue(joined.stream().allMatch(s -> s.customerName().equals("Alice")));
    }

    @Test
    void findFirstRespectsLimit() {
        Optional<Product> result = engine
                .from("products", Product.class, Decoder.identity())
                .limit(0)
                .findFirst();

        assertFalse(result.isPresent());
    }

    @Test
    void findFirstRespectsLimitWindow() {
        Optional<Product> result = engine
                .from("products", Product.class, Decoder.identity())
                .orderBy(Product::price, Order.ASC)
                .limit(2)
                .offset(1)
                .findFirst();

        // ordered: p3(25), p4(35), p5(200), p1(800), p2(1200)
        // skip 1 → p4; limit 2 → p4, p5; findFirst → p4
        assertTrue(result.isPresent());
        assertEquals("p4", result.get().id());
    }

    @Test
    void summarizeRespectsLimit() {
        SummaryStatistics stats = engine
                .from("products", Product.class, Decoder.identity())
                .orderBy(Product::price, Order.ASC)
                .limit(1)
                .summarize(Product::price);

        assertEquals(1L, stats.count());
        assertEquals(25.0, stats.min(), 0.001);
        assertEquals(25.0, stats.max(), 0.001);
    }

    @Test
    void summarizeRespectsOffset() {
        SummaryStatistics stats = engine
                .from("products", Product.class, Decoder.identity())
                .orderBy(Product::price, Order.ASC)
                .offset(4)
                .summarize(Product::price);

        assertEquals(1L, stats.count());
        assertEquals(1200.0, stats.avg(), 0.001);
    }

    @Test
    void countShortCircuitIgnoresOrdering() {
        long count = engine
                .from("products", Product.class, Decoder.identity())
                .orderBy(Product::price, Order.ASC)
                .count();

        assertEquals(5L, count);
    }

    @Test
    void countWithLimitReturnsConstrainedSize() {
        long count = engine
                .from("products", Product.class, Decoder.identity())
                .limit(2)
                .count();

        assertEquals(2L, count);
    }

    @Test
    void parallelGroupByMatchesSequential() {
        Map<String, Long> seq = engine
                .from("products", Product.class, Decoder.identity())
                .groupBy(Product::category)
                .aggregate(Aggregators.count());

        Map<String, Long> par = engine
                .from("products", Product.class, Decoder.identity())
                .parallel()
                .groupBy(Product::category)
                .aggregate(Aggregators.count());

        assertEquals(seq, par);
    }

    @Test
    void parallelWithOffsetAndLimitMatchesSequential() {
        List<Product> seq = engine
                .from("products", Product.class, Decoder.identity())
                .where(p -> p.price() < 1000)
                .orderBy(Product::price, Order.ASC)
                .offset(1)
                .limit(2)
                .fetch();

        List<Product> par = engine
                .from("products", Product.class, Decoder.identity())
                .parallel()
                .where(p -> p.price() < 1000)
                .orderBy(Product::price, Order.ASC)
                .offset(1)
                .limit(2)
                .fetch();

        assertEquals(seq, par);
        assertEquals(2, par.size());
    }

    // ── findAny ───────────────────────────────────────────────────────────────

    @Test
    void findAnySequentialMatchesFindFirst() {
        Optional<Product> first = engine
                .from("products", Product.class, Decoder.identity())
                .orderBy(Product::price, Order.ASC)
                .findFirst();

        Optional<Product> any = engine
                .from("products", Product.class, Decoder.identity())
                .where(p -> p.category().equals("books"))
                .findAny();

        assertTrue(any.isPresent());
        assertTrue(any.get().category().equals("books"));
        assertTrue(first.isPresent());
    }

    @Test
    void findAnyParallelReturnsAMatchingEntry() {
        Optional<Product> result = engine
                .from("products", Product.class, Decoder.identity())
                .parallel()
                .where(p -> p.category().equals("electronics"))
                .findAny();

        assertTrue(result.isPresent());
        assertEquals("electronics", result.get().category());
    }

    @Test
    void findAnyOnEmptyReturnsEmpty() {
        Optional<Product> result = engine
                .from("products", Product.class, Decoder.identity())
                .parallel()
                .where(p -> p.price() > 999_999)
                .findAny();

        assertFalse(result.isPresent());
    }

    // ── parallel summarize / countMatching ────────────────────────────────────

    @Test
    void parallelSummarizeMatchesSequential() {
        SummaryStatistics seq = engine
                .from("products", Product.class, Decoder.identity())
                .where(p -> p.stock() > 0)
                .summarize(Product::price);

        SummaryStatistics par = engine
                .from("products", Product.class, Decoder.identity())
                .parallel()
                .where(p -> p.stock() > 0)
                .summarize(Product::price);

        assertEquals(seq.count(), par.count());
        assertEquals(seq.sum(),   par.sum(),   0.001);
        assertEquals(seq.min(),   par.min(),   0.001);
        assertEquals(seq.max(),   par.max(),   0.001);
    }

    @Test
    void parallelCountMatchingMatchesSequential() {
        long seq = engine
                .from("products", Product.class, Decoder.identity())
                .where(p -> p.price() < 1000)
                .limit(1)
                .countMatching();

        long par = engine
                .from("products", Product.class, Decoder.identity())
                .parallel()
                .where(p -> p.price() < 1000)
                .limit(1)
                .countMatching();

        assertEquals(seq, par); // both ignore limit — should be 4
        assertEquals(4L, par);
    }

    // ── offset / decoder edge cases ───────────────────────────────────────────

    @Test
    void offsetExceedingDatasetSizeReturnsEmpty() {
        List<Product> result = engine
                .from("products", Product.class, Decoder.identity())
                .offset(100)
                .fetch();

        assertTrue(result.isEmpty());
    }

    @Test
    void decoderTypedNullValueThrowsCCE() {
        List<Object> withNull = new ArrayList<>();
        withNull.add(null);

        DataRegistry reg = new DataRegistry().register("nulls", withNull);

        ClassCastException ex = assertThrows(ClassCastException.class, () ->
                QueryEngine.over(reg)
                           .from("nulls", Product.class, Decoder.typed(Product.class))
                           .fetch());

        assertTrue(ex.getMessage().contains("null"));
    }

    @Test
    void dataRegistryRegisterOrReplaceFreshKeySucceeds() {
        DataRegistry reg = new DataRegistry();
        reg.registerOrReplace("fresh", products);

        assertTrue(reg.contains("fresh"));

        long count = QueryEngine.over(reg)
                .from("fresh", Product.class, Decoder.identity())
                .count();

        assertEquals(5L, count);
    }

    // ── argument validation ───────────────────────────────────────────────────

    @Test
    void limitNegativeThrows() {
        assertThrows(IllegalArgumentException.class, () ->
                engine.from("products", Product.class, Decoder.identity()).limit(-1));
    }

    @Test
    void offsetNegativeThrows() {
        assertThrows(IllegalArgumentException.class, () ->
                engine.from("products", Product.class, Decoder.identity()).offset(-1));
    }

    @Test
    void limitZeroFetchReturnsEmpty() {
        List<Product> result = engine
                .from("products", Product.class, Decoder.identity())
                .limit(0)
                .fetch();
        assertTrue(result.isEmpty());
    }

    @Test
    void limitZeroCountReturnsZero() {
        long count = engine
                .from("products", Product.class, Decoder.identity())
                .limit(0)
                .count();
        assertEquals(0L, count);
    }

    // ── Aggregators: min / max / minBy / maxBy ────────────────────────────────

    @Test
    void groupByMinDoubleCorrect() {
        Map<String, Double> min = engine
                .from("products", Product.class, Decoder.identity())
                .groupBy(Product::category)
                .aggregate(Aggregators.min(Product::price));

        assertEquals(800.0, min.get("electronics"), 0.001);
        assertEquals(25.0,  min.get("books"),       0.001);
        assertEquals(200.0, min.get("clothing"),    0.001);
    }

    @Test
    void groupByMaxDoubleCorrect() {
        Map<String, Double> max = engine
                .from("products", Product.class, Decoder.identity())
                .groupBy(Product::category)
                .aggregate(Aggregators.max(Product::price));

        assertEquals(1200.0, max.get("electronics"), 0.001);
        assertEquals(35.0,   max.get("books"),       0.001);
        assertEquals(200.0,  max.get("clothing"),    0.001);
    }

    @Test
    void groupByMinByCorrect() {
        Map<String, String> minId = engine
                .from("products", Product.class, Decoder.identity())
                .groupBy(Product::category)
                .aggregate(Aggregators.minBy(Product::id));

        assertEquals("p1", minId.get("electronics"));
        assertEquals("p3", minId.get("books"));
        assertEquals("p5", minId.get("clothing"));
    }

    @Test
    void groupByMaxByCorrect() {
        Map<String, String> maxId = engine
                .from("products", Product.class, Decoder.identity())
                .groupBy(Product::category)
                .aggregate(Aggregators.maxBy(Product::id));

        assertEquals("p2", maxId.get("electronics"));
        assertEquals("p4", maxId.get("books"));
        assertEquals("p5", maxId.get("clothing"));
    }

    @Test
    void aggregatorsAvgOnSingleElementGroup() {
        Map<String, Double> avg = engine
                .from("products", Product.class, Decoder.identity())
                .where(p -> p.category().equals("clothing"))
                .groupBy(Product::category)
                .aggregate(Aggregators.avg(Product::price));

        assertEquals(200.0, avg.get("clothing"), 0.001);
    }

    // ── QueryPredicate.not ────────────────────────────────────────────────────

    @Test
    void queryPredicateNotInvertsCondition() {
        QueryPredicate<Product> isBooks  = QueryPredicate.of(p -> p.category().equals("books"));
        QueryPredicate<Product> notBooks = isBooks.not();

        assertFalse(notBooks.test(new Product("x", "books",       25.0,  5)));
        assertTrue( notBooks.test(new Product("y", "electronics", 800.0, 2)));
    }

    @Test
    void queryPredicateNotComposesWithAnd() {
        QueryPredicate<Product> notExpensive = QueryPredicate.of((Product p) -> p.price() > 500).not();
        QueryPredicate<Product> inStock      = QueryPredicate.of(p -> p.stock() > 0);
        QueryPredicate<Product> cheapInStock = notExpensive.and(inStock);

        // cheap and in stock → true
        assertTrue( cheapInStock.test(new Product("p3", "books",       25.0, 10)));
        // expensive → false (not() fails)
        assertFalse(cheapInStock.test(new Product("p1", "electronics", 800.0,  5)));
        // cheap but out of stock → false (and() fails)
        assertFalse(cheapInStock.test(new Product("px", "books",       25.0,   0)));
    }

    // ── DataRegistry: unregistered name / collection sources ─────────────────

    @Test
    void fromUnregisteredNameThrows() {
        assertThrows(IllegalArgumentException.class, () ->
                engine.from("nonexistent", Product.class, Decoder.identity()));
    }

    @Test
    void queryFromCollectionSource() {
        List<Product> list = new ArrayList<>(products.values());
        DataRegistry reg = new DataRegistry().register("items", list);

        List<Product> result = QueryEngine.over(reg)
                .from("items", Product.class, Decoder.identity())
                .where(p -> p.category().equals("books"))
                .fetch();

        assertEquals(2, result.size());
        assertTrue(result.stream().allMatch(p -> p.category().equals("books")));
    }

    @Test
    void dataRegistryRegisterOrReplaceCollectionSucceeds() {
        List<Product> original    = new ArrayList<>(products.values());
        List<Product> replacement = Collections.singletonList(
                new Product("pX", "other", 50.0, 1));

        DataRegistry reg = new DataRegistry()
                .register("items", original)
                .registerOrReplace("items", replacement);

        long count = QueryEngine.over(reg)
                .from("items", Product.class, Decoder.identity())
                .count();

        assertEquals(1L, count);
    }

    // ── join: empty sides / nested-loop with where ────────────────────────────

    @Test
    void hashJoinWithEmptyLeftReturnsEmpty() {
        DataRegistry reg = new DataRegistry()
                .register("empty",      Collections.emptyMap())
                .register("customersC", customers);

        List<OrderSummary> result = QueryEngine.over(reg)
                .from("empty", Purchase.class, Decoder.identity())
                .<Customer>join("customersC", Customer.class, Decoder.identity())
                .on(Purchase::customerId, Customer::id)
                .select((o, c) -> new OrderSummary(o.id(), c.name()))
                .fetch();

        assertTrue(result.isEmpty());
    }

    @Test
    void hashJoinWithEmptyRightReturnsEmpty() {
        DataRegistry reg = new DataRegistry()
                .register("ordersC", orders)
                .register("empty",   Collections.emptyMap());

        List<OrderSummary> result = QueryEngine.over(reg)
                .from("ordersC", Purchase.class, Decoder.identity())
                .<Customer>join("empty", Customer.class, Decoder.identity())
                .on(Purchase::customerId, Customer::id)
                .select((o, c) -> new OrderSummary(o.id(), c.name()))
                .fetch();

        assertTrue(result.isEmpty());
    }

    @Test
    void nestedLoopJoinWithWhereFilter() {
        List<OrderSummary> result = engine
                .from("orders", Purchase.class, Decoder.identity())
                .<Customer>join("customers", Customer.class, Decoder.identity())
                .on((o, c) -> o.customerId().equals(c.id()))
                .where(o -> o.total() > 500)
                .select((o, c) -> new OrderSummary(o.id(), c.name()))
                .fetch();

        // o1 (600, Alice) and o3 (900, Alice) pass; o2 (300, Bob) does not
        assertEquals(2, result.size());
        assertTrue(result.stream().allMatch(s -> s.customerName().equals("Alice")));
    }

    @Test
    void joinMultipleWhereComposesWithAnd() {
        List<OrderSummary> result = engine
                .from("orders", Purchase.class, Decoder.identity())
                .<Customer>join("customers", Customer.class, Decoder.identity())
                .on(Purchase::customerId, Customer::id)
                .where(o -> o.total() > 200)
                .where(o -> o.status().equals("SHIPPED"))
                .select((o, c) -> new OrderSummary(o.id(), c.name()))
                .fetch();

        // o1: total=600 > 200, SHIPPED → matches; o3: DELIVERED → excluded; o2: PENDING → excluded
        assertEquals(1, result.size());
        assertEquals("o1", result.get(0).orderId());
    }

    // ── hash join: duplicate right-side keys ─────────────────────────────────

    @Test
    void hashJoinDuplicateRightKeysProducesOneRowPerMatch() {
        // Right side: two discount codes both applicable to "books"
        Map<String, String> discountMap = new LinkedHashMap<>();
        discountMap.put("d1", "books");
        discountMap.put("d2", "books");

        DataRegistry reg = new DataRegistry()
                .register("products",  products)
                .register("discounts", discountMap);

        List<String> result = QueryEngine.over(reg)
                .from("products", Product.class, Decoder.identity())
                .where(p -> p.category().equals("books"))
                .<String>join("discounts", String.class, Decoder.identity())
                .on(Product::category, s -> s)
                .select((p, d) -> p.id() + "+" + d)
                .fetch();

        // 2 book products × 2 discount entries = 4 output rows
        assertEquals(4, result.size());
        assertTrue(result.stream().allMatch(r -> r.startsWith("p3+") || r.startsWith("p4+")));
    }

    @Test
    void hashJoinSingleLeftMultipleRightMatchesAllRight() {
        // One left entry matching two right entries — verifies multi-value index path
        Map<String, String> tags = new LinkedHashMap<>();
        tags.put("t1", "electronics");
        tags.put("t2", "electronics");
        tags.put("t3", "books");       // not electronics — should not match

        DataRegistry reg = new DataRegistry()
                .register("products", products)
                .register("tags",     tags);

        List<String> result = QueryEngine.over(reg)
                .from("products", Product.class, Decoder.identity())
                .where(p -> p.id().equals("p1"))           // single electronics product
                .<String>join("tags", String.class, Decoder.identity())
                .on(Product::category, s -> s)
                .select((p, t) -> p.id() + ":" + t)
                .fetch();

        // p1 matches both t1 and t2 (both "electronics") but not t3
        assertEquals(2, result.size());
        assertTrue(result.stream().allMatch(r -> r.equals("p1:electronics")));
    }

    // ── null parameter guards ─────────────────────────────────────────────────

    @Test
    void whereNullPredicateThrows() {
        assertThrows(IllegalArgumentException.class, () ->
                engine.from("products", Product.class, Decoder.identity()).where(null));
    }

    @Test
    void andNullPredicateThrows() {
        assertThrows(IllegalArgumentException.class, () ->
                engine.from("products", Product.class, Decoder.identity())
                      .where(p -> true)
                      .and(null));
    }

    @Test
    void orNullPredicateThrows() {
        assertThrows(IllegalArgumentException.class, () ->
                engine.from("products", Product.class, Decoder.identity()).or(null));
    }

    @Test
    void joinOnNullBiPredicateThrows() {
        assertThrows(IllegalArgumentException.class, () ->
                engine.from("orders", Purchase.class, Decoder.identity())
                      .<Customer>join("customers", Customer.class, Decoder.identity())
                      .on((java.util.function.BiPredicate<Purchase, Customer>) null));
    }

    @Test
    void joinOnNullLeftKeyThrows() {
        assertThrows(IllegalArgumentException.class, () ->
                engine.from("orders", Purchase.class, Decoder.identity())
                      .<Customer>join("customers", Customer.class, Decoder.identity())
                      .on(null, Customer::id));
    }

    @Test
    void joinOnNullRightKeyThrows() {
        assertThrows(IllegalArgumentException.class, () ->
                engine.from("orders", Purchase.class, Decoder.identity())
                      .<Customer>join("customers", Customer.class, Decoder.identity())
                      .on(Purchase::customerId, null));
    }

    @Test
    void fromNullDecoderThrows() {
        assertThrows(IllegalArgumentException.class, () ->
                engine.from("products", Product.class, null));
    }

    @Test
    void dataRegistryRegisterNullNameThrows() {
        assertThrows(IllegalArgumentException.class, () ->
                new DataRegistry().register(null, products));
    }

    @Test
    void dataRegistryRegisterNullDataThrows() {
        assertThrows(IllegalArgumentException.class, () ->
                new DataRegistry().register("x", (Map<?, ?>) null));
    }

    @Test
    void dataRegistryRegisterNullCollectionThrows() {
        assertThrows(IllegalArgumentException.class, () ->
                new DataRegistry().register("x", (java.util.List<?>) null));
    }

    @Test
    void dataRegistryRegisterOrReplaceNullNameThrows() {
        assertThrows(IllegalArgumentException.class, () ->
                new DataRegistry().registerOrReplace(null, products));
    }

    // ── findAny: ignores pagination ───────────────────────────────────────────

    @Test
    void findAnyIgnoresPaginationLimit() {
        // findFirst() with limit(0) returns empty; findAny() must still return a match
        Optional<Product> result = engine
                .from("products", Product.class, Decoder.identity())
                .where(p -> p.category().equals("books"))
                .limit(0)
                .findAny();

        assertTrue(result.isPresent());
        assertEquals("books", result.get().category());
    }

    // ── live-view semantics ───────────────────────────────────────────────────

    @Test
    void liveViewReflectsMutationsToOriginalMap() {
        Map<String, Product> live = new LinkedHashMap<>(products);
        DataRegistry reg = new DataRegistry().register("live", live);
        QueryEngine liveEngine = QueryEngine.over(reg);

        assertEquals(5L, liveEngine.from("live", Product.class, Decoder.identity()).count());

        live.put("p99", new Product("p99", "toys", 15.0, 20));

        assertEquals(6L, liveEngine.from("live", Product.class, Decoder.identity()).count());
    }

    // ── SummaryStatistics.EMPTY sentinel values ───────────────────────────────

    @Test
    void summaryStatisticsEmptyHasZeroCountAndNaN() {
        SummaryStatistics empty = SummaryStatistics.EMPTY;

        assertEquals(0L, empty.count());
        assertEquals(0.0, empty.sum(), 0.0);
        assertTrue(Double.isNaN(empty.min()));
        assertTrue(Double.isNaN(empty.max()));
        assertTrue(Double.isNaN(empty.avg()));
    }

    // ── orderBy: multiple calls (last wins) ───────────────────────────────────

    @Test
    void multipleOrderByLastWins() {
        List<Product> result = engine
                .from("products", Product.class, Decoder.identity())
                .orderBy(Product::price, Order.DESC)    // overridden by next call
                .orderBy(Product::id, Order.ASC)
                .fetch();

        // Final sort is by id ASC: p1, p2, p3, p4, p5
        assertEquals("p1", result.get(0).id());
        assertEquals("p2", result.get(1).id());
        assertEquals("p5", result.get(4).id());
    }

    // ── or() without prior where ──────────────────────────────────────────────

    @Test
    void orWithoutPriorWhereActsLikeWhere() {
        // When no predicate exists, or() creates a fresh predicate rather than OR-with-nothing
        List<Product> result = engine
                .from("products", Product.class, Decoder.identity())
                .or(p -> p.category().equals("books"))
                .fetch();

        assertEquals(2, result.size());
        assertTrue(result.stream().allMatch(p -> p.category().equals("books")));
    }

    // ── orderBy NullOrdering ──────────────────────────────────────────────────

    @Test
    void orderByDefaultNullOrdering_nullsFirst_onAsc() {
        Map<String, Product> withNull = new LinkedHashMap<>(products);
        withNull.put("p0", new Product("p0", null, 50.0, 5));

        DataRegistry reg = new DataRegistry().register("items", withNull);
        List<Product> result = QueryEngine.over(reg)
                .from("items", Product.class, Decoder.identity())
                .orderBy(Product::category, Order.ASC)
                .fetch();

        assertNull(result.get(0).category());  // null is minimum → first in ASC
    }

    @Test
    void orderByDefaultNullOrdering_nullsLast_onDesc() {
        Map<String, Product> withNull = new LinkedHashMap<>(products);
        withNull.put("p0", new Product("p0", null, 50.0, 5));

        DataRegistry reg = new DataRegistry().register("items", withNull);
        List<Product> result = QueryEngine.over(reg)
                .from("items", Product.class, Decoder.identity())
                .orderBy(Product::category, Order.DESC)
                .fetch();

        assertNull(result.get(result.size() - 1).category());  // null is minimum → last in DESC
    }

    @Test
    void orderByExplicitNullsFirst() {
        Map<String, Product> withNull = new LinkedHashMap<>(products);
        withNull.put("p0", new Product("p0", null, 50.0, 5));

        DataRegistry reg = new DataRegistry().register("items", withNull);
        List<Product> result = QueryEngine.over(reg)
                .from("items", Product.class, Decoder.identity())
                .orderBy(Product::category, Order.ASC, NullOrdering.NULLS_FIRST)
                .fetch();

        assertNull(result.get(0).category());
    }

    @Test
    void orderByExplicitNullsLast() {
        Map<String, Product> withNull = new LinkedHashMap<>(products);
        withNull.put("p0", new Product("p0", null, 50.0, 5));

        DataRegistry reg = new DataRegistry().register("items", withNull);
        List<Product> result = QueryEngine.over(reg)
                .from("items", Product.class, Decoder.identity())
                .orderBy(Product::category, Order.ASC, NullOrdering.NULLS_LAST)
                .fetch();

        assertNull(result.get(result.size() - 1).category());
    }

    @Test
    void orderByNullsLastDesc_nullsAppearFirst() {
        // NULLS_LAST + DESC → reversed(nullsLast(naturalOrder())) → nulls before any real value
        Map<String, Product> withNull = new LinkedHashMap<>(products);
        withNull.put("p0", new Product("p0", null, 50.0, 5));

        DataRegistry reg = new DataRegistry().register("items", withNull);
        List<Product> result = QueryEngine.over(reg)
                .from("items", Product.class, Decoder.identity())
                .orderBy(Product::category, Order.DESC, NullOrdering.NULLS_LAST)
                .fetch();

        assertNull(result.get(0).category());  // NULLS_LAST + DESC reversal → null first
    }

    // ── JoinQuery immutability ────────────────────────────────────────────────

    @Test
    void joinQueryWhereDoesNotMutateBase() {
        JoinQuery<Purchase, Customer> base = engine
                .from("orders", Purchase.class, Decoder.identity())
                .<Customer>join("customers", Customer.class, Decoder.identity())
                .on(Purchase::customerId, Customer::id);

        List<OrderSummary> filtered = base
                .where(o -> o.total() > 500)
                .select((o, c) -> new OrderSummary(o.id(), c.name()))
                .fetch();

        // base is unaffected — must still return all 3 matches
        List<OrderSummary> all = base
                .select((o, c) -> new OrderSummary(o.id(), c.name()))
                .fetch();

        assertEquals(2, filtered.size());  // o1(600) and o3(900) only
        assertEquals(3, all.size());       // unaffected by the filtered chain
    }

    @Test
    void joinQueryOnDoesNotMutateBase() {
        // Start from a base with no strategy set
        JoinQuery<Purchase, Customer> base = engine
                .from("orders", Purchase.class, Decoder.identity())
                .<Customer>join("customers", Customer.class, Decoder.identity());

        // Apply two independent strategies from the same base
        List<OrderSummary> hashResult = base
                .on(Purchase::customerId, Customer::id)
                .select((o, c) -> new OrderSummary(o.id(), c.name()))
                .fetch();

        List<OrderSummary> loopResult = base
                .on((o, c) -> o.customerId().equals(c.id()))
                .select((o, c) -> new OrderSummary(o.id(), c.name()))
                .fetch();

        assertEquals(3, hashResult.size());
        assertEquals(3, loopResult.size());

        // base itself has no strategy — fetch() must still throw
        assertThrows(IllegalStateException.class, () ->
                base.select((o, c) -> new OrderSummary(o.id(), c.name())).fetch());
    }

    @Test
    void joinQueryOnReplacementLeavesOriginalIntact() {
        // Calling on() twice from the same base must not affect the first chain
        JoinQuery<Purchase, Customer> withHash = engine
                .from("orders", Purchase.class, Decoder.identity())
                .<Customer>join("customers", Customer.class, Decoder.identity())
                .on(Purchase::customerId, Customer::id);

        // Derive a nested-loop version from the hash-join instance
        JoinQuery<Purchase, Customer> withLoop = withHash
                .on((o, c) -> o.customerId().equals(c.id()));

        List<OrderSummary> fromHash = withHash
                .select((o, c) -> new OrderSummary(o.id(), c.name()))
                .fetch();

        List<OrderSummary> fromLoop = withLoop
                .select((o, c) -> new OrderSummary(o.id(), c.name()))
                .fetch();

        assertEquals(3, fromHash.size());
        assertEquals(3, fromLoop.size());
    }

    @Test
    void joinQueryWhereNullThrows() {
        assertThrows(IllegalArgumentException.class, () ->
                engine.from("orders", Purchase.class, Decoder.identity())
                      .<Customer>join("customers", Customer.class, Decoder.identity())
                      .where(null));
    }

    // ── null parameter guards (additional) ───────────────────────────────────

    @Test
    void dataRegistryRegisterOrReplaceNullDataThrows() {
        assertThrows(IllegalArgumentException.class, () ->
                new DataRegistry().registerOrReplace("x", (Map<?, ?>) null));
    }

    @Test
    void dataRegistryRegisterOrReplaceNullCollectionThrows() {
        assertThrows(IllegalArgumentException.class, () ->
                new DataRegistry().registerOrReplace("x", (java.util.List<?>) null));
    }

    // ── custom aggregator ─────────────────────────────────────────────────────

    @Test
    void customAggregatorConcatenatesIds() {
        Map<String, String> idList = engine
                .from("products", Product.class, Decoder.identity())
                .groupBy(Product::category)
                .aggregate(stream -> stream.map(Product::id)
                        .sorted()
                        .collect(java.util.stream.Collectors.joining(",")));

        assertEquals("p1,p2", idList.get("electronics"));
        assertEquals("p3,p4", idList.get("books"));
        assertEquals("p5",    idList.get("clothing"));
    }

    // ── custom decoder ────────────────────────────────────────────────────────

    @Test
    void customDecoderTransformsValues() {
        Map<String, Integer> prices = new LinkedHashMap<>();
        prices.put("p1", 800);
        prices.put("p3", 25);

        DataRegistry reg = new DataRegistry().register("prices", prices);

        Decoder<String> labelDecoder = raw -> "price:" + raw;

        List<String> result = QueryEngine.over(reg)
                .from("prices", String.class, labelDecoder)
                .where(s -> s.equals("price:800"))
                .fetch();

        assertEquals(1, result.size());
        assertEquals("price:800", result.get(0));
    }

    // ── QueryResult.isEmpty ───────────────────────────────────────────────────

    @Test
    void fetchResultIsEmptyOnZeroMatches() {
        QueryResult<Product> result = engine
                .from("products", Product.class, Decoder.identity())
                .where(p -> false)
                .fetchResult();

        assertTrue(result.isEmpty());
        assertEquals(0, result.size());
        assertTrue(result.executionNanos() >= 0);
    }

    @Test
    void fetchResultIsNotEmptyOnMatches() {
        QueryResult<Product> result = engine
                .from("products", Product.class, Decoder.identity())
                .where(p -> p.category().equals("books"))
                .fetchResult();

        assertFalse(result.isEmpty());
        assertEquals(2, result.size());
    }
}
