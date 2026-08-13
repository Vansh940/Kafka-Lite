package com.kafkalite.network.protocol;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/** Shared helpers for the wire format. Frame = [4-byte length][2-byte request type][body]. */
public class ProtocolCodec {

    public static ByteBuffer frame(short requestType, ByteBuffer body) {
        ByteBuffer framed = ByteBuffer.allocate(4 + 2 + body.remaining());
        framed.putInt(2 + body.remaining());
        framed.putShort(requestType);
        framed.put(body);
        framed.flip();
        return framed;
    }

    public static void writeString(ByteBuffer buf, String s) {
        byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
        buf.putInt(bytes.length);
        buf.put(bytes);
    }

    public static String readString(ByteBuffer buf) {
        int len = buf.getInt();
        byte[] bytes = new byte[len];
        buf.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    public static void writeBytes(ByteBuffer buf, byte[] data) {
        if (data == null) { buf.putInt(-1); return; }
        buf.putInt(data.length);
        buf.put(data);
    }

    public static byte[] readBytes(ByteBuffer buf) {
        int len = buf.getInt();
        if (len < 0) return null;
        byte[] data = new byte[len];
        buf.get(data);
        return data;
    }
}
