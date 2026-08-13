package com.kafkalite.replication;

import com.kafkalite.core.log.LogManager;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

/** Tracks in-sync replicas (ISR) per partition on the broker that leads it. */
public class ReplicaManager {
    private static final long MAX_LAG_RECORDS = 100; // fall out of ISR beyond this lag

    private final LogManager logManager;
    private final ConcurrentHashMap<String, Set<Integer>> isrByPartitionKey = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, ConcurrentHashMap<Integer, Long>> replicaOffsets = new ConcurrentHashMap<>();

    public ReplicaManager(LogManager logManager) {
        this.logManager = logManager;
    }

    private String key(String topic, int partition) { return topic + "-" + partition; }

    /** Called when a follower's fetch request reports the offset it has caught up to. */
    public void recordFollowerFetch(String topic, int partition, int followerBrokerId, long fetchedOffset) throws java.io.IOException {
        String k = key(topic, partition);
        long leo = logManager.getOrCreateLog(topic, partition).logEndOffset();
        replicaOffsets.computeIfAbsent(k, x -> new ConcurrentHashMap<>()).put(followerBrokerId, fetchedOffset);

        Set<Integer> isr = isrByPartitionKey.computeIfAbsent(k, x -> new CopyOnWriteArraySet<>());
        if (leo - fetchedOffset <= MAX_LAG_RECORDS) isr.add(followerBrokerId);
        else isr.remove(followerBrokerId);

        long newHw = isr.stream()
                .mapToLong(id -> replicaOffsets.get(k).getOrDefault(id, 0L))
                .min().orElse(leo);
        logManager.getOrCreateLog(topic, partition).advanceHighWatermark(newHw);
    }

    public Set<Integer> isr(String topic, int partition) {
        return isrByPartitionKey.getOrDefault(key(topic, partition), Set.of());
    }
}
