package com.kafkalite.core.model;

import java.nio.ByteBuffer;
import java.util.zip.CRC32;

/** One message. On-disk layout: [offset(8)][length(4)][crc32(4)][keyLen(4)][key][valueLen(4)][value] */
public class Record {
    public final long offset;
    public final byte[] key;   // nullable
    public final byte[] value;
    public final long timestamp;

    public Record(long offset, byte[] key, byte[] value, long timestamp) {
        this.offset = offset;
        this.key = key;
        this.value = value;
        this.timestamp = timestamp;
    }

    public byte[] serialize() {
        int keyLen = key == null ? -1 : key.length;
        int valueLen = value == null ? -1 : value.length;
        int payloadSize = 8 + 4 + (keyLen > 0 ? keyLen : 0) + 4 + (valueLen > 0 ? valueLen : 0);

        ByteBuffer payload = ByteBuffer.allocate(payloadSize);
        payload.putLong(timestamp);
        payload.putInt(keyLen);
        if (keyLen > 0) payload.put(key);
        payload.putInt(valueLen);
        if (valueLen > 0) payload.put(value);
        byte[] payloadBytes = payload.array();

        CRC32 crc = new CRC32();
        crc.update(payloadBytes);

        ByteBuffer full = ByteBuffer.allocate(8 + 4 + 4 + payloadBytes.length);
        full.putLong(offset);
        full.putInt(payloadBytes.length);
        full.putInt((int) crc.getValue());
        full.put(payloadBytes);
        return full.array();
    }

    /** Reads exactly one record starting at buf's current position. */
    public static Record deserialize(ByteBuffer buf) {
        long offset = buf.getLong();
        int length = buf.getInt();
        int expectedCrc = buf.getInt();

        byte[] payloadBytes = new byte[length];
        buf.get(payloadBytes);

        CRC32 crc = new CRC32();
        crc.update(payloadBytes);
        if ((int) crc.getValue() != expectedCrc) {
            throw new IllegalStateException("CRC mismatch at offset " + offset + " — corrupted segment");
        }

        ByteBuffer payload = ByteBuffer.wrap(payloadBytes);
        long timestamp = payload.getLong();
        int keyLen = payload.getInt();
        byte[] key = null;
        if (keyLen > 0) { key = new byte[keyLen]; payload.get(key); }
        int valueLen = payload.getInt();
        byte[] value = null;
        if (valueLen > 0) { value = new byte[valueLen]; payload.get(value); }

        return new Record(offset, key, value, timestamp);
    }

    public int sizeOnDisk() { return serialize().length; }
}
