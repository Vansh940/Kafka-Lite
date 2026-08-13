package com.kafkalite.client;

import java.util.Arrays;

public class Partitioner {
    /** Hash-based: same key always lands on the same partition, preserving per-key ordering. */
    public static int partitionFor(byte[] key, int numPartitions) {
        if (key == null) return (int) (System.nanoTime() % numPartitions); // round-robin-ish fallback
        int hash = Arrays.hashCode(key);
        return Math.floorMod(hash, numPartitions);
    }
}
