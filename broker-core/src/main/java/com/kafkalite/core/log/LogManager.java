package com.kafkalite.core.log;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/** Owns every partition's PartitionLog on this broker — both leader and follower replicas. */
public class LogManager {
    public record TopicPartitionKey(String topic, int partition) {
        public TopicPartitionKey {
            Objects.requireNonNull(topic);
        }
    }

    private final Path logDir;
    private final ConcurrentMap<TopicPartitionKey, PartitionLog> logs = new ConcurrentHashMap<>();

    public LogManager(Path logDir) {
        this.logDir = logDir;
    }

    public PartitionLog getOrCreateLog(String topic, int partition) throws IOException {
        TopicPartitionKey key = new TopicPartitionKey(topic, partition);
        PartitionLog existing = logs.get(key);
        if (existing != null) return existing;
        synchronized (this) {
            existing = logs.get(key);
            if (existing != null) return existing;
            PartitionLog newLog = new PartitionLog(logDir.resolve(topic + "-" + partition));
            logs.put(key, newLog);
            return newLog;
        }
    }

    public PartitionLog getLog(String topic, int partition) {
        return logs.get(new TopicPartitionKey(topic, partition));
    }
}
