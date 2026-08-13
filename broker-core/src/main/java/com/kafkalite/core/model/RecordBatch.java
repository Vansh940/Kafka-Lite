package com.kafkalite.core.model;

import java.util.List;

/** A batch of records a producer sends in one PRODUCE request (same topic-partition). */
public class RecordBatch {
    public final String topic;
    public final int partition;
    public final List<byte[]> keys;
    public final List<byte[]> values;

    public RecordBatch(String topic, int partition, List<byte[]> keys, List<byte[]> values) {
        if (keys.size() != values.size()) throw new IllegalArgumentException("keys/values size mismatch");
        this.topic = topic;
        this.partition = partition;
        this.keys = keys;
        this.values = values;
    }

    public int size() { return values.size(); }
}
