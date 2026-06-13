package com.simplj.flair.cache.replication;

import com.simplj.flair.cache.hlc.HLCTimestamp;
import com.simplj.flair.cache.store.CacheEntry;
import com.simplj.flair.cache.transport.RawFrame;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class FrameCodecTest {

    @Test
    void putRoundTrip() {
        UUID nodeId = UUID.randomUUID();
        HLCTimestamp hlc = new HLCTimestamp(1_000_000L, 3L);
        byte[] value = "hello".getBytes();
        CacheEntry entry = new CacheEntry(value, hlc, 9_999_999L, 0L, 0L, nodeId);

        byte[] key = "mykey".getBytes();
        ReplicationEvent.PutEvent event = new ReplicationEvent.PutEvent("block1", key, entry, ConsistencyMode.EVENTUAL);

        RawFrame frame = FrameEncoder.encodePut(42L, false, event);
        assertEquals(FrameEncoder.TYPE_PUT, frame.type());

        FrameDecoder.DecodedPut decoded = FrameDecoder.decodePut(frame.payload());
        assertNotNull(decoded);
        assertEquals(42L, decoded.frameId());
        assertFalse(decoded.needsAck());
        assertEquals("block1", decoded.blockName());
        assertArrayEquals(key, decoded.key());
        assertEquals(hlc, decoded.entry().hlc());
        assertArrayEquals(value, decoded.entry().value());
        assertEquals(9_999_999L, decoded.entry().expiryEpochMs());
        assertEquals(nodeId, decoded.entry().originNodeId());
    }

    @Test
    void putWithNeedsAck() {
        UUID nodeId = UUID.randomUUID();
        HLCTimestamp hlc = new HLCTimestamp(2_000_000L, 0L);
        CacheEntry entry = new CacheEntry(new byte[]{1, 2, 3}, hlc, 0L, 0L, 0L, nodeId);
        ReplicationEvent.PutEvent event = new ReplicationEvent.PutEvent("testBlock", new byte[]{9}, entry, ConsistencyMode.QUORUM);

        RawFrame frame = FrameEncoder.encodePut(100L, true, event);
        FrameDecoder.DecodedPut decoded = FrameDecoder.decodePut(frame.payload());

        assertNotNull(decoded);
        assertTrue(decoded.needsAck());
        assertEquals(100L, decoded.frameId());
    }

    @Test
    void deleteRoundTrip() {
        UUID nodeId = UUID.randomUUID();
        HLCTimestamp hlc = new HLCTimestamp(5_000L, 1L);
        byte[] key = "deleteKey".getBytes();
        ReplicationEvent.DeleteEvent event = new ReplicationEvent.DeleteEvent(
                "block2", key, hlc, nodeId, ConsistencyMode.STRONG);

        RawFrame frame = FrameEncoder.encodeDelete(77L, true, event);
        assertEquals(FrameEncoder.TYPE_DELETE, frame.type());

        FrameDecoder.DecodedDelete decoded = FrameDecoder.decodeDelete(frame.payload());
        assertNotNull(decoded);
        assertEquals(77L, decoded.frameId());
        assertTrue(decoded.needsAck());
        assertEquals("block2", decoded.blockName());
        assertArrayEquals(key, decoded.key());
        assertEquals(hlc, decoded.hlc());
        assertEquals(nodeId, decoded.originNodeId());
    }

    @Test
    void deleteNoAck() {
        HLCTimestamp hlc = new HLCTimestamp(1L, 0L);
        ReplicationEvent.DeleteEvent event = new ReplicationEvent.DeleteEvent(
                "b", new byte[]{1}, hlc, UUID.randomUUID(), ConsistencyMode.EVENTUAL);

        RawFrame frame = FrameEncoder.encodeDelete(0L, false, event);
        FrameDecoder.DecodedDelete decoded = FrameDecoder.decodeDelete(frame.payload());

        assertNotNull(decoded);
        assertFalse(decoded.needsAck());
        assertEquals(0L, decoded.frameId());
    }

    @Test
    void ackRoundTrip() {
        RawFrame frame = FrameEncoder.encodeAck(12345L);
        assertEquals(FrameEncoder.TYPE_ACK, frame.type());

        long frameId = FrameDecoder.decodeAck(frame.payload());
        assertEquals(12345L, frameId);
    }

    @Test
    void decodePutTooShortReturnsNull() {
        assertNull(FrameDecoder.decodePut(new byte[]{1, 2, 3}));
    }

    @Test
    void decodeDeleteTooShortReturnsNull() {
        assertNull(FrameDecoder.decodeDelete(new byte[]{1, 2}));
    }

    @Test
    void decodeAckTooShortReturnsNegative() {
        assertEquals(-1L, FrameDecoder.decodeAck(new byte[]{1, 2, 3}));
    }

    @Test
    void putEmptyValue() {
        UUID nodeId = UUID.randomUUID();
        HLCTimestamp hlc = new HLCTimestamp(1L, 0L);
        CacheEntry entry = new CacheEntry(new byte[0], hlc, 0L, 0L, 0L, nodeId);
        ReplicationEvent.PutEvent event = new ReplicationEvent.PutEvent("b", new byte[]{1}, entry, ConsistencyMode.EVENTUAL);

        FrameDecoder.DecodedPut decoded = FrameDecoder.decodePut(
                FrameEncoder.encodePut(1L, false, event).payload());

        assertNotNull(decoded);
        assertArrayEquals(new byte[0], decoded.entry().value());
    }
}
