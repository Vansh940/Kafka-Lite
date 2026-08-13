package com.kafkalite.network.protocol;

import java.nio.ByteBuffer;

public class FetchRequest {
    public final String topic;
    public final int partition;
    public final long fetchOffset;
    public final int maxRecords;

    public FetchRequest(String topic, int partition, long fetchOffset, int maxRecords) {
        this.topic = topic; this.partition = partition; this.fetchOffset = fetchOffset; this.maxRecords = maxRecords;
    }

    public ByteBuffer encodeBody() {
        ByteBuffer buf = ByteBuffer.allocate(topic.length() + 32);
        ProtocolCodec.writeString(buf, topic);
        buf.putInt(partition);
        buf.putLong(fetchOffset);
        buf.putInt(maxRecords);
        buf.flip();
        return buf;
    }

    public static FetchRequest decode(ByteBuffer buf) {
        String topic = ProtocolCodec.readString(buf);
        int partition = buf.getInt();
        long offset = buf.getLong();
        int max = buf.getInt();
        return new FetchRequest(topic, partition, offset, max);
    }
}
