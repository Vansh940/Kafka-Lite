package com.kafkalite.network.protocol;

import java.nio.ByteBuffer;

public class ProduceRequest {
    public final String topic;
    public final int partition;
    public final byte[] key;
    public final byte[] value;
    public final short acks; // 0, 1, or -1 (all ISR)

    public ProduceRequest(String topic, int partition, byte[] key, byte[] value, short acks) {
        this.topic = topic; this.partition = partition; this.key = key; this.value = value; this.acks = acks;
    }

    public ByteBuffer encodeBody() {
        int valueLen = value == null ? 0 : value.length;
        ByteBuffer buf = ByteBuffer.allocate(topic.length() + 32 + valueLen + (key == null ? 0 : key.length));
        ProtocolCodec.writeString(buf, topic);
        buf.putInt(partition);
        ProtocolCodec.writeBytes(buf, key);
        ProtocolCodec.writeBytes(buf, value);
        buf.putShort(acks);
        buf.flip();
        return buf;
    }

    public static ProduceRequest decode(ByteBuffer buf) {
        String topic = ProtocolCodec.readString(buf);
        int partition = buf.getInt();
        byte[] key = ProtocolCodec.readBytes(buf);
        byte[] value = ProtocolCodec.readBytes(buf);
        short acks = buf.getShort();
        return new ProduceRequest(topic, partition, key, value, acks);
    }
}
