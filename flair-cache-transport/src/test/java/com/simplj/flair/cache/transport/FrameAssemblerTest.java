package com.simplj.flair.cache.transport;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class FrameAssemblerTest {

    private static final byte TYPE = 0x01;

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static ByteBuffer encode(RawFrame frame) {
        ByteBuffer buf = ByteBuffer.allocate(frame.encodedLength());
        frame.encodeTo(buf);
        buf.flip();
        return buf;
    }

    private static List<RawFrame> collect(ByteBuffer src) {
        FrameAssembler asm = new FrameAssembler();
        List<RawFrame> frames = new ArrayList<>();
        asm.feed(src, frames::add);
        return frames;
    }

    // ── Single frame — whole buffer ───────────────────────────────────────────

    @Test
    void singleFrame_fullBuffer_decoded() {
        byte[] payload = "hello".getBytes();
        RawFrame original = new RawFrame(TYPE, payload);
        List<RawFrame> frames = collect(encode(original));

        assertEquals(1, frames.size());
        assertEquals(TYPE, frames.get(0).type());
        assertArrayEquals(payload, frames.get(0).payload());
    }

    // ── Zero-length payload ───────────────────────────────────────────────────

    @Test
    void zeroPayload_decoded() {
        RawFrame original = new RawFrame(TYPE, new byte[0]);
        List<RawFrame> frames = collect(encode(original));

        assertEquals(1, frames.size());
        assertArrayEquals(new byte[0], frames.get(0).payload());
    }

    // ── Multiple frames in one buffer ─────────────────────────────────────────

    @Test
    void multipleFrames_singleBuffer_allDecoded() {
        RawFrame f1 = new RawFrame((byte) 0x01, new byte[]{1, 2, 3});
        RawFrame f2 = new RawFrame((byte) 0x02, new byte[]{4, 5});
        RawFrame f3 = new RawFrame((byte) 0x03, new byte[]{6});

        ByteBuffer buf = ByteBuffer.allocate(f1.encodedLength() + f2.encodedLength() + f3.encodedLength());
        f1.encodeTo(buf);
        f2.encodeTo(buf);
        f3.encodeTo(buf);
        buf.flip();

        List<RawFrame> frames = collect(buf);
        assertEquals(3, frames.size());
        assertArrayEquals(new byte[]{1, 2, 3}, frames.get(0).payload());
        assertArrayEquals(new byte[]{4, 5}, frames.get(1).payload());
        assertArrayEquals(new byte[]{6}, frames.get(2).payload());
    }

    // ── Partial read — byte at a time ─────────────────────────────────────────

    @Test
    void partialRead_byteAtATime_decoded() {
        byte[] payload = "partial-read-test".getBytes();
        RawFrame original = new RawFrame(TYPE, payload);
        ByteBuffer full = encode(original);

        FrameAssembler asm = new FrameAssembler();
        List<RawFrame> frames = new ArrayList<>();

        while (full.hasRemaining()) {
            ByteBuffer single = ByteBuffer.allocate(1);
            single.put(full.get());
            single.flip();
            asm.feed(single, frames::add);
        }

        assertEquals(1, frames.size());
        assertArrayEquals(payload, frames.get(0).payload());
    }

    // ── Partial read — two chunks split mid-header ────────────────────────────

    @Test
    void partialRead_splitMidHeader_decoded() {
        byte[] payload = new byte[100];
        Arrays.fill(payload, (byte) 0x7F);
        RawFrame original = new RawFrame(TYPE, payload);
        ByteBuffer full = encode(original);

        int splitAt = 3; // inside the 8-byte header
        byte[] first = new byte[splitAt];
        byte[] second = new byte[full.remaining() - splitAt];
        full.get(first);
        full.get(second);

        FrameAssembler asm = new FrameAssembler();
        List<RawFrame> frames = new ArrayList<>();

        asm.feed(ByteBuffer.wrap(first), frames::add);
        assertEquals(0, frames.size(), "No frame yet after partial header");

        asm.feed(ByteBuffer.wrap(second), frames::add);
        assertEquals(1, frames.size());
        assertArrayEquals(payload, frames.get(0).payload());
    }

    // ── Partial read — split mid-payload ─────────────────────────────────────

    @Test
    void partialRead_splitMidPayload_decoded() {
        byte[] payload = new byte[1000];
        Arrays.fill(payload, (byte) 0x42);
        RawFrame original = new RawFrame(TYPE, payload);
        ByteBuffer full = encode(original);

        int splitAt = RawFrame.HEADER_BYTES + 400; // 400 bytes into the payload
        byte[] first = new byte[splitAt];
        byte[] second = new byte[full.remaining() - splitAt];
        full.get(first);
        full.get(second);

        FrameAssembler asm = new FrameAssembler();
        List<RawFrame> frames = new ArrayList<>();

        asm.feed(ByteBuffer.wrap(first), frames::add);
        assertEquals(0, frames.size());

        asm.feed(ByteBuffer.wrap(second), frames::add);
        assertEquals(1, frames.size());
        assertArrayEquals(payload, frames.get(0).payload());
    }

    // ── Consecutive partial reads — multiple frames ────────────────────────────

    @Test
    void partialRead_multipleFrames_interleaved_allDecoded() {
        RawFrame f1 = new RawFrame((byte) 0x01, "first".getBytes());
        RawFrame f2 = new RawFrame((byte) 0x02, "second".getBytes());

        ByteBuffer full = ByteBuffer.allocate(f1.encodedLength() + f2.encodedLength());
        f1.encodeTo(full);
        f2.encodeTo(full);
        full.flip();

        FrameAssembler asm = new FrameAssembler();
        List<RawFrame> frames = new ArrayList<>();

        // Feed one byte at a time
        while (full.hasRemaining()) {
            ByteBuffer one = ByteBuffer.allocate(1);
            one.put(full.get());
            one.flip();
            asm.feed(one, frames::add);
        }

        assertEquals(2, frames.size());
    }

    // ── Invalid magic bytes — frame discarded ─────────────────────────────────

    @Test
    void invalidMagic_frameDiscarded() {
        ByteBuffer bad = ByteBuffer.allocate(RawFrame.HEADER_BYTES);
        bad.put((byte) 0xDE).put((byte) 0xAD) // wrong magic
           .put(RawFrame.VERSION).put(TYPE)
           .putInt(4);
        bad.flip();

        FrameAssembler asm = new FrameAssembler();
        List<RawFrame> frames = new ArrayList<>();
        asm.feed(bad, frames::add);

        assertEquals(0, frames.size(), "Bad-magic frame must be discarded");
    }

    // ── Invalid version — frame discarded ────────────────────────────────────

    @Test
    void invalidVersion_frameDiscarded() {
        ByteBuffer bad = ByteBuffer.allocate(RawFrame.HEADER_BYTES);
        bad.put(RawFrame.MAGIC_0).put(RawFrame.MAGIC_1)
           .put((byte) 0xFF)  // bad version
           .put(TYPE)
           .putInt(4);
        bad.flip();

        FrameAssembler asm = new FrameAssembler();
        List<RawFrame> frames = new ArrayList<>();
        asm.feed(bad, frames::add);

        assertEquals(0, frames.size(), "Bad-version frame must be discarded");
    }

    // ── Payload too large — frame discarded ───────────────────────────────────

    @Test
    void payloadTooLarge_frameDiscarded() {
        int smallMax = 100;
        FrameAssembler asm = new FrameAssembler(smallMax);
        List<RawFrame> frames = new ArrayList<>();

        ByteBuffer bad = ByteBuffer.allocate(RawFrame.HEADER_BYTES);
        bad.put(RawFrame.MAGIC_0).put(RawFrame.MAGIC_1)
           .put(RawFrame.VERSION).put(TYPE)
           .putInt(smallMax + 1);
        bad.flip();

        asm.feed(bad, frames::add);
        assertEquals(0, frames.size(), "Oversized payload must be discarded");
    }

    // ── Constructor rejects out-of-range maxPayload ───────────────────────────

    @Test
    void constructor_negativeMax_throws() {
        assertThrows(IllegalArgumentException.class, () -> new FrameAssembler(-1));
    }

    @Test
    void constructor_beyondAbsoluteMax_throws() {
        assertThrows(IllegalArgumentException.class,
                () -> new FrameAssembler(RawFrame.ABSOLUTE_MAX_PAYLOAD + 1));
    }

    // ── Large payload (1 MB) round-trips correctly ────────────────────────────

    @Test
    void largePayload_1MB_roundTrip() {
        byte[] payload = new byte[1 << 20]; // 1 MB
        Arrays.fill(payload, (byte) 0x55);
        RawFrame original = new RawFrame(TYPE, payload);
        List<RawFrame> frames = collect(encode(original));

        assertEquals(1, frames.size());
        assertArrayEquals(payload, frames.get(0).payload());
    }

    // ── Assembler state resets correctly after each frame ─────────────────────

    @Test
    void stateReset_afterEachFrame_nextFrameDecoded() {
        RawFrame f1 = new RawFrame((byte) 0x01, new byte[]{10});
        RawFrame f2 = new RawFrame((byte) 0x02, new byte[]{20, 30});

        FrameAssembler asm = new FrameAssembler();
        List<RawFrame> frames = new ArrayList<>();

        asm.feed(encode(f1), frames::add);
        asm.feed(encode(f2), frames::add);

        assertEquals(2, frames.size());
        assertArrayEquals(new byte[]{10}, frames.get(0).payload());
        assertArrayEquals(new byte[]{20, 30}, frames.get(1).payload());
    }

    // ── Consumer exception — state machine resets, next frame delivered ───────

    @Test
    void consumerException_assemblerResetsState_nextFrameDelivered() {
        RawFrame f1 = new RawFrame((byte) 0x01, "first".getBytes());
        RawFrame f2 = new RawFrame((byte) 0x02, "second".getBytes());

        ByteBuffer both = ByteBuffer.allocate(f1.encodedLength() + f2.encodedLength());
        f1.encodeTo(both);
        f2.encodeTo(both);
        both.flip();

        FrameAssembler asm = new FrameAssembler();
        List<RawFrame> delivered = new ArrayList<>();
        int[] callCount = {0};

        asm.feed(both, frame -> {
            callCount[0]++;
            if (callCount[0] == 1) {
                throw new RuntimeException("Simulated consumer crash on first frame");
            }
            delivered.add(frame);
        });

        // The assembler must recover and deliver the second frame
        assertEquals(1, delivered.size(), "Second frame must be delivered after first consumer threw");
        assertArrayEquals("second".getBytes(), delivered.get(0).payload());
    }

    // ── Consumer exception on zero-length payload — recovers cleanly ──────────

    @Test
    void consumerException_zeroPayload_recovers() {
        RawFrame f1 = new RawFrame((byte) 0x01, new byte[0]);
        RawFrame f2 = new RawFrame((byte) 0x02, new byte[]{42});

        ByteBuffer both = ByteBuffer.allocate(f1.encodedLength() + f2.encodedLength());
        f1.encodeTo(both);
        f2.encodeTo(both);
        both.flip();

        FrameAssembler asm = new FrameAssembler();
        List<RawFrame> delivered = new ArrayList<>();
        int[] callCount = {0};

        asm.feed(both, frame -> {
            callCount[0]++;
            if (callCount[0] == 1) {
                throw new RuntimeException("crash on zero-payload frame");
            }
            delivered.add(frame);
        });

        assertEquals(1, delivered.size());
        assertArrayEquals(new byte[]{42}, delivered.get(0).payload());
    }
}
