package com.kafkalite.network;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Deque;

/**
 * Accumulates bytes for one connection across multiple OP_READ events and
 * yields complete [length-prefixed] frames as they become available. This
 * solves the classic NIO gotcha: TCP gives you a byte stream, not message
 * boundaries — a single read() is never guaranteed to contain a whole frame.
 */
public class FrameAccumulator {
    private final Deque<Byte> buffer = new ArrayDeque<>();

    public void feed(ByteBuffer chunk) {
        while (chunk.hasRemaining()) buffer.addLast(chunk.get());
    }

    /** Returns the next complete frame body (request type + payload), or null if not enough data yet. */
    public ByteBuffer nextFrame() {
        if (buffer.size() < 4) return null;
        Byte[] lenBytes = buffer.stream().limit(4).toArray(Byte[]::new);
        int frameLen = ((lenBytes[0] & 0xFF) << 24) | ((lenBytes[1] & 0xFF) << 16)
                     | ((lenBytes[2] & 0xFF) << 8) | (lenBytes[3] & 0xFF);
        if (buffer.size() < 4 + frameLen) return null;

        for (int i = 0; i < 4; i++) buffer.removeFirst(); // drop length prefix
        ByteBuffer frame = ByteBuffer.allocate(frameLen);
        for (int i = 0; i < frameLen; i++) frame.put(buffer.removeFirst());
        frame.flip();
        return frame;
    }
}
