package com.simplj.flair.cache.bootstrap;

import com.simplj.flair.cache.hlc.HLCTimestamp;
import com.simplj.flair.cache.hlc.HybridLogicalClock;
import com.simplj.flair.cache.replication.ConsistencyMode;
import com.simplj.flair.cache.replication.LWWResolver;
import com.simplj.flair.cache.replication.ReplicationEvent;
import com.simplj.flair.cache.serial.codecs.ByteArrayCodec;
import com.simplj.flair.cache.serial.codecs.StringCodec;
import com.simplj.flair.cache.store.CacheBlock;
import com.simplj.flair.cache.store.CacheEntry;
import com.simplj.flair.cache.transport.TcpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@Timeout(15)
class BootstrapSyncTest {

    private TcpServer donorServer;
    private CacheBlock<String, byte[]> donorBlock;
    private CacheBlock<String, byte[]> joinerBlock;

    @AfterEach
    void tearDown() {
        if (donorServer  != null) { try { donorServer.shutdown(); }  catch (Exception ignored) {} }
        if (donorBlock   != null) donorBlock.close();
        if (joinerBlock  != null) joinerBlock.close();
    }

    // ── helpers ──────────────────────────────────────────────────────────────

    private static CacheBlock<String, byte[]> newBlock(String name) {
        return CacheBlock.<String, byte[]>builder()
                .name(name)
                .keyCodec(StringCodec.INSTANCE)
                .valueCodec(ByteArrayCodec.INSTANCE)
                .build();
    }

    private static byte[] val(String s) { return s.getBytes(StandardCharsets.UTF_8); }

    /** Returns the StringCodec-encoded key bytes (length-prefixed UTF-8) matching CacheBlock's serializeKey. */
    private static byte[] encodeKey(String key) {
        int size = StringCodec.INSTANCE.sizeOf(key);
        ByteBuffer buf = ByteBuffer.allocate(size);
        StringCodec.INSTANCE.serialize(key, buf);
        buf.flip();
        byte[] bytes = new byte[buf.remaining()];
        buf.get(bytes);
        return bytes;
    }

    /** Returns the ByteArrayCodec-encoded value bytes (4-byte length prefix + raw) matching CacheBlock's serializeValue. */
    private static byte[] encodeVal(byte[] raw) {
        int size = ByteArrayCodec.INSTANCE.sizeOf(raw);
        ByteBuffer buf = ByteBuffer.allocate(size);
        ByteArrayCodec.INSTANCE.serialize(raw, buf);
        buf.flip();
        byte[] bytes = new byte[buf.remaining()];
        buf.get(bytes);
        return bytes;
    }

    /** Returns a port that is not currently listening — used only for unreachable-donor tests. */
    private static int unusedPort() {
        try (ServerSocket s = new ServerSocket(0)) { return s.getLocalPort(); }
        catch (IOException e) { throw new RuntimeException(e); }
    }

    /** Starts a donor TcpServer bound to an OS-assigned port (avoids TOCTOU races). */
    private TcpServer startDonorServer(Map<String, CacheBlock<?, ?>> blocks,
                                       int chunkSize) throws IOException {
        BootstrapServer bootstrapServer = new BootstrapServer(blocks, chunkSize);
        TcpServer srv = TcpServer.builder()
                .port(0)   // OS assigns a free port atomically
                .handler(bootstrapServer)
                .build();
        srv.start();
        return srv;
    }

    // ── tests ─────────────────────────────────────────────────────────────────

    @Test
    void freshNodeReceivesAllEntries() throws Exception {
        donorBlock  = newBlock("data");
        joinerBlock = newBlock("data");

        donorBlock.put("k1", val("v1"));
        donorBlock.put("k2", val("v2"));
        donorBlock.put("k3", val("v3"));

        donorServer = startDonorServer(Map.of("data", donorBlock), 65536);

        SyncResult result = BootstrapSync.builder()
                .blocks(Map.of("data", joinerBlock))
                .donorAddress("127.0.0.1", donorServer.localPort())
                .syncTimeoutMs(10_000)
                .build()
                .syncFromPeer();

        assertArrayEquals(val("v1"), joinerBlock.get("k1"));
        assertArrayEquals(val("v2"), joinerBlock.get("k2"));
        assertArrayEquals(val("v3"), joinerBlock.get("k3"));
        assertEquals(3, result.totalEntries());
        assertTrue(result.chunksReceived() >= 1);
        assertTrue(result.durationMs() >= 0);
    }

    @Test
    void emptyDonorProducesEmptyJoiner() throws Exception {
        donorBlock  = newBlock("data");
        joinerBlock = newBlock("data");

        donorServer = startDonorServer(Map.of("data", donorBlock), 65536);

        SyncResult result = BootstrapSync.builder()
                .blocks(Map.of("data", joinerBlock))
                .donorAddress("127.0.0.1", donorServer.localPort())
                .syncTimeoutMs(10_000)
                .build()
                .syncFromPeer();

        assertEquals(0, result.totalEntries());
        assertEquals(0, result.chunksReceived());
        assertNull(joinerBlock.get("k1"));
    }

    @Test
    void multipleChunksReassembleCorrectly() throws Exception {
        donorBlock  = newBlock("data");
        joinerBlock = newBlock("data");

        // Tiny chunk size forces multiple chunks
        for (int i = 0; i < 50; i++) {
            donorBlock.put("key-" + i, val("value-" + i));
        }

        // 64 bytes per chunk forces many chunks (each entry is ~20+ bytes)
        donorServer = startDonorServer(Map.of("data", donorBlock), 64);

        SyncResult result = BootstrapSync.builder()
                .blocks(Map.of("data", joinerBlock))
                .donorAddress("127.0.0.1", donorServer.localPort())
                .syncTimeoutMs(10_000)
                .build()
                .syncFromPeer();

        assertEquals(50, result.totalEntries());
        assertTrue(result.chunksReceived() > 1, "Expected multiple chunks");
        for (int i = 0; i < 50; i++) {
            assertArrayEquals(val("value-" + i), joinerBlock.get("key-" + i),
                    "Missing key-" + i);
        }
    }

    @Test
    void lwwResolutionKeepsNewerEntryOnJoiner() throws Exception {
        donorBlock  = newBlock("data");
        joinerBlock = newBlock("data");

        HybridLogicalClock hlc = new HybridLogicalClock();

        // Joiner already has a newer version (higher HLC) of key "k1"
        HLCTimestamp older = hlc.now();
        HLCTimestamp newer = hlc.now();
        // Must use codec-serialized key bytes to match CacheBlock.get("k1") lookup
        byte[] keyBytes = encodeKey("k1");

        // Values must be codec-encoded so CacheBlock.get() can deserialize them correctly.
        donorBlock.putRaw(keyBytes, new CacheEntry(encodeVal(val("donor-v1")), older, 0L, 0L, 0L, UUID.randomUUID()));
        joinerBlock.putRaw(keyBytes, new CacheEntry(encodeVal(val("joiner-v2")), newer, 0L, 0L, 0L, UUID.randomUUID()));

        donorServer = startDonorServer(Map.of("data", donorBlock), 65536);

        BootstrapSync.builder()
                .blocks(Map.of("data", joinerBlock))
                .donorAddress("127.0.0.1", donorServer.localPort())
                .syncTimeoutMs(10_000)
                .build()
                .syncFromPeer();

        // Joiner's newer version must survive
        assertArrayEquals(val("joiner-v2"), joinerBlock.get("k1"),
                "LWW must keep the newer entry on the joiner");
    }

    @Test
    void lwwResolutionAppliesDonorEntryWhenItIsNewer() throws Exception {
        donorBlock  = newBlock("data");
        joinerBlock = newBlock("data");

        HybridLogicalClock hlc = new HybridLogicalClock();
        HLCTimestamp older = hlc.now();
        HLCTimestamp newer = hlc.now();
        byte[] keyBytes = encodeKey("k1");

        joinerBlock.putRaw(keyBytes, new CacheEntry(encodeVal(val("joiner-old")), older, 0L, 0L, 0L, UUID.randomUUID()));
        donorBlock.putRaw(keyBytes,  new CacheEntry(encodeVal(val("donor-new")),  newer, 0L, 0L, 0L, UUID.randomUUID()));

        donorServer = startDonorServer(Map.of("data", donorBlock), 65536);

        BootstrapSync.builder()
                .blocks(Map.of("data", joinerBlock))
                .donorAddress("127.0.0.1", donorServer.localPort())
                .syncTimeoutMs(10_000)
                .build()
                .syncFromPeer();

        assertArrayEquals(val("donor-new"), joinerBlock.get("k1"),
                "Donor's newer entry must win");
    }

    @Test
    void replicationBufferDrainAppliesBufferedEventsAfterSync() throws Exception {
        donorBlock  = newBlock("data");
        joinerBlock = newBlock("data");

        donorBlock.put("k1", val("from-donor"));

        donorServer = startDonorServer(Map.of("data", donorBlock), 65536);

        ReplicationBuffer buffer = new ReplicationBuffer();
        buffer.startBuffering();

        // Simulate a replication event arriving during sync (newer than donor's snapshot).
        // Key must be codec-encoded to match what CacheBlock.get() looks up.
        HLCTimestamp futureTs = new HLCTimestamp(Long.MAX_VALUE - 1, 0L);
        byte[] keyBytes = encodeKey("k1");
        // Value must be codec-encoded so CacheBlock.get() can deserialize it correctly.
        CacheEntry newerEntry = new CacheEntry(encodeVal(val("from-replication")), futureTs, 0L, 0L, 0L, UUID.randomUUID());
        buffer.offer(ReplicationEvent.put("data", keyBytes, newerEntry, ConsistencyMode.EVENTUAL));

        SyncResult result = BootstrapSync.builder()
                .blocks(Map.of("data", joinerBlock))
                .donorAddress("127.0.0.1", donorServer.localPort())
                .syncTimeoutMs(10_000)
                .replicationBuffer(buffer)
                .build()
                .syncFromPeer();

        // After drain, the buffered (newer) event must win over donor snapshot
        assertArrayEquals(val("from-replication"), joinerBlock.get("k1"),
                "Buffered replication event with higher HLC must win after drain");
        assertEquals(1, result.totalEntries());
    }

    @Test
    void syncTimeoutThrowsSyncTimeoutException() {
        joinerBlock = newBlock("data");
        // Port with nothing listening
        assertThrows(SyncTimeoutException.class, () ->
                BootstrapSync.builder()
                        .blocks(Map.of("data", joinerBlock))
                        .donorAddress("127.0.0.1", unusedPort())
                        .syncTimeoutMs(500)
                        .build()
                        .syncFromPeer());
    }

    @Test
    void hlcTieBreakDonorWinsWithHigherUuid() throws Exception {
        donorBlock  = newBlock("data");
        joinerBlock = newBlock("data");

        HLCTimestamp tieTs = new HLCTimestamp(1_000_000L, 0L);
        UUID loUUID = new UUID(0L, 1L);
        UUID hiUUID = new UUID(0L, 2L);
        assertTrue(hiUUID.compareTo(loUUID) > 0, "test precondition: hiUUID must sort higher");

        byte[] keyBytes = encodeKey("k1");
        donorBlock.putRaw(keyBytes,  new CacheEntry(encodeVal(val("donor")),  tieTs, 0L, 0L, 0L, hiUUID));
        joinerBlock.putRaw(keyBytes, new CacheEntry(encodeVal(val("joiner")), tieTs, 0L, 0L, 0L, loUUID));

        donorServer = startDonorServer(Map.of("data", donorBlock), 65536);

        BootstrapSync.builder()
                .blocks(Map.of("data", joinerBlock))
                .donorAddress("127.0.0.1", donorServer.localPort())
                .syncTimeoutMs(10_000)
                .build()
                .syncFromPeer();

        assertArrayEquals(val("donor"), joinerBlock.get("k1"),
                "Donor with higher originNodeId must win the HLC tie");
    }

    @Test
    void hlcTieBreakJoinerSurvivesWithHigherUuid() throws Exception {
        donorBlock  = newBlock("data");
        joinerBlock = newBlock("data");

        HLCTimestamp tieTs = new HLCTimestamp(1_000_000L, 0L);
        UUID loUUID = new UUID(0L, 1L);
        UUID hiUUID = new UUID(0L, 2L);

        byte[] keyBytes = encodeKey("k1");
        donorBlock.putRaw(keyBytes,  new CacheEntry(encodeVal(val("donor")),  tieTs, 0L, 0L, 0L, loUUID));
        joinerBlock.putRaw(keyBytes, new CacheEntry(encodeVal(val("joiner")), tieTs, 0L, 0L, 0L, hiUUID));

        donorServer = startDonorServer(Map.of("data", donorBlock), 65536);

        BootstrapSync.builder()
                .blocks(Map.of("data", joinerBlock))
                .donorAddress("127.0.0.1", donorServer.localPort())
                .syncTimeoutMs(10_000)
                .build()
                .syncFromPeer();

        assertArrayEquals(val("joiner"), joinerBlock.get("k1"),
                "Joiner with higher originNodeId must survive when HLC timestamps tie");
    }

    @Test
    void expiredEntriesOnDonorAreNotTransferred() throws Exception {
        donorBlock  = newBlock("data");
        joinerBlock = newBlock("data");

        HybridLogicalClock hlc = new HybridLogicalClock();
        byte[] liveKey    = encodeKey("live");
        byte[] expiredKey = encodeKey("expired");

        donorBlock.putRaw(liveKey,
                new CacheEntry(encodeVal(val("live-value")), hlc.now(), 0L, 0L, 0L, UUID.randomUUID()));
        donorBlock.putRaw(expiredKey,
                new CacheEntry(encodeVal(val("expired-value")), hlc.now(),
                        System.currentTimeMillis() - 1_000, 0L, 0L, UUID.randomUUID()));

        donorServer = startDonorServer(Map.of("data", donorBlock), 65536);

        SyncResult result = BootstrapSync.builder()
                .blocks(Map.of("data", joinerBlock))
                .donorAddress("127.0.0.1", donorServer.localPort())
                .syncTimeoutMs(10_000)
                .build()
                .syncFromPeer();

        assertArrayEquals(val("live-value"), joinerBlock.get("live"),
                "Live entry must be transferred");
        assertNull(joinerBlock.get("expired"),
                "Expired entry must not be transferred");
        assertEquals(1, result.totalEntries(), "Only non-expired entry must be counted");
    }

    @Test
    void ttlIsPreservedOnJoiner() throws Exception {
        donorBlock  = newBlock("data");
        joinerBlock = newBlock("data");

        HybridLogicalClock hlc = new HybridLogicalClock();
        byte[] keyBytes = encodeKey("k1");
        long expiryEpochMs = System.currentTimeMillis() + 60_000;

        donorBlock.putRaw(keyBytes,
                new CacheEntry(encodeVal(val("v1")), hlc.now(), expiryEpochMs, 0L, 0L, UUID.randomUUID()));

        donorServer = startDonorServer(Map.of("data", donorBlock), 65536);

        BootstrapSync.builder()
                .blocks(Map.of("data", joinerBlock))
                .donorAddress("127.0.0.1", donorServer.localPort())
                .syncTimeoutMs(10_000)
                .build()
                .syncFromPeer();

        CacheEntry transferred = joinerBlock.getRaw(keyBytes);
        assertNotNull(transferred, "Entry must be transferred");
        assertEquals(expiryEpochMs, transferred.expiryEpochMs(),
                "Expiry epoch must be preserved exactly on the joiner");
    }

    @Test
    void unknownBlockOnDonorIsSkipped() throws Exception {
        CacheBlock<String, byte[]> donorKnown  = newBlock("known");
        CacheBlock<String, byte[]> donorExtra  = newBlock("extra");
        CacheBlock<String, byte[]> joinerKnown = newBlock("known");
        try {
            donorKnown.put("k1", val("v1"));
            donorExtra.put("e1", val("extra-v1"));

            donorServer = startDonorServer(
                    Map.of("known", donorKnown, "extra", donorExtra), 65536);

            SyncResult result = BootstrapSync.builder()
                    .blocks(Map.of("known", joinerKnown))
                    .donorAddress("127.0.0.1", donorServer.localPort())
                    .syncTimeoutMs(10_000)
                    .build()
                    .syncFromPeer();

            assertArrayEquals(val("v1"), joinerKnown.get("k1"),
                    "Entries for known block must be transferred");
            assertEquals(2, result.totalEntries(),
                    "SYNC_DONE totalEntries reflects donor total, not joiner subset");
        } finally {
            donorKnown.close();
            donorExtra.close();
            joinerKnown.close();
        }
    }

    @Test
    void bufferDrainSnapshotWinsOverOlderBufferedEvent() throws Exception {
        donorBlock  = newBlock("data");
        joinerBlock = newBlock("data");

        HybridLogicalClock hlc = new HybridLogicalClock();
        HLCTimestamp older = hlc.now();
        HLCTimestamp newer = hlc.now();

        byte[] keyBytes = encodeKey("k1");
        donorBlock.putRaw(keyBytes,
                new CacheEntry(encodeVal(val("donor-new")), newer, 0L, 0L, 0L, UUID.randomUUID()));

        donorServer = startDonorServer(Map.of("data", donorBlock), 65536);

        ReplicationBuffer buffer = new ReplicationBuffer();
        buffer.startBuffering();

        CacheEntry olderEntry = new CacheEntry(
                encodeVal(val("replication-old")), older, 0L, 0L, 0L, UUID.randomUUID());
        buffer.offer(ReplicationEvent.put("data", keyBytes, olderEntry, ConsistencyMode.EVENTUAL));

        BootstrapSync.builder()
                .blocks(Map.of("data", joinerBlock))
                .donorAddress("127.0.0.1", donorServer.localPort())
                .syncTimeoutMs(10_000)
                .replicationBuffer(buffer)
                .build()
                .syncFromPeer();

        assertArrayEquals(val("donor-new"), joinerBlock.get("k1"),
                "Snapshot with newer HLC must win over older buffered replication event");
    }

    @Test
    void stopBufferingClearsBufferAndIgnoresSubsequentOffers() {
        CacheBlock<String, byte[]> block = newBlock("data");
        try {
            ReplicationBuffer buffer = new ReplicationBuffer();
            buffer.startBuffering();

            byte[] keyBytes = encodeKey("k1");
            buffer.offer(ReplicationEvent.put("data", keyBytes,
                    new CacheEntry(encodeVal(val("v1")), new HLCTimestamp(1L, 0L), 0L, 0L, 0L, UUID.randomUUID()),
                    ConsistencyMode.EVENTUAL));

            buffer.stopBuffering();

            // Offer after stop must be ignored
            buffer.offer(ReplicationEvent.put("data", keyBytes,
                    new CacheEntry(encodeVal(val("v2")), new HLCTimestamp(2L, 0L), 0L, 0L, 0L, UUID.randomUUID()),
                    ConsistencyMode.EVENTUAL));

            buffer.drainAndApply(Map.of("data", block), LWWResolver.INSTANCE);

            assertNull(block.get("k1"),
                    "No events must be applied: stopBuffering clears all captured events");
        } finally {
            block.close();
        }
    }

    @Test
    void syncDoneNeverArrivesThrowsTimeout() throws Exception {
        joinerBlock = newBlock("data");
        // Server that accepts the connection but never sends any frames
        donorServer = TcpServer.builder()
                .port(0)   // OS assigns a free port atomically
                .handler((conn, frame) -> { /* swallow — no response */ })
                .build();
        donorServer.start();

        assertThrows(SyncTimeoutException.class, () ->
                BootstrapSync.builder()
                        .blocks(Map.of("data", joinerBlock))
                        .donorAddress("127.0.0.1", donorServer.localPort())
                        .syncTimeoutMs(500)
                        .build()
                        .syncFromPeer());
    }

    @Test
    void multipleBlocksSyncedTogether() throws Exception {
        CacheBlock<String, byte[]> donorUsers   = newBlock("users");
        CacheBlock<String, byte[]> donorOrders  = newBlock("orders");
        CacheBlock<String, byte[]> joinerUsers  = newBlock("users");
        CacheBlock<String, byte[]> joinerOrders = newBlock("orders");

        donorUsers.put("u1", val("alice"));
        donorUsers.put("u2", val("bob"));
        donorOrders.put("o1", val("order-1"));

        donorServer = startDonorServer(
                Map.of("users", donorUsers, "orders", donorOrders), 65536);

        try {
            SyncResult result = BootstrapSync.builder()
                    .blocks(Map.of("users", joinerUsers, "orders", joinerOrders))
                    .donorAddress("127.0.0.1", donorServer.localPort())
                    .syncTimeoutMs(10_000)
                    .build()
                    .syncFromPeer();

            assertArrayEquals(val("alice"),   joinerUsers.get("u1"));
            assertArrayEquals(val("bob"),     joinerUsers.get("u2"));
            assertArrayEquals(val("order-1"), joinerOrders.get("o1"));
            assertEquals(3, result.totalEntries());
        } finally {
            donorUsers.close();
            donorOrders.close();
            joinerUsers.close();
            joinerOrders.close();
        }
    }
}
